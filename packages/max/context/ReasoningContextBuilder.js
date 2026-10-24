'use strict';

const { NODE_TYPES, EDGE_TYPES } = require('../../knowledge');
const {
  deepFreeze,
  sortById,
  evidenceRef,
} = require('../reasoning/ReasoningTypes');

/**
 * Builds an immutable ReasoningContext from the Knowledge Query Engine only.
 * Strategies never query the graph and never mutate this object.
 */
class ReasoningContextBuilder {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   *   KnowledgeService exposing query API (findCompanies, findPeople, …). No repository access.
   */
  constructor(deps) {
    if (!deps || !deps.knowledge) {
      throw new Error('ReasoningContextBuilder requires knowledge (KnowledgeService)');
    }
    assertQuerySurface(deps.knowledge);
    this._knowledge = deps.knowledge;
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {string} [input.asOf] - ISO timestamp for deterministic recency (defaults to now)
   * @returns {Promise<import('../reasoning/ReasoningTypes').ReasoningContext>}
   */
  async build(input) {
    const tenantId = requireTenant(input && input.tenantId);
    const companyId = requireId(input && input.companyId, 'companyId');
    const asOf =
      input && input.asOf
        ? String(input.asOf)
        : new Date().toISOString();
    const k = this._knowledge;
    const queryCount = { n: 0 };
    const track = async (fn) => {
      queryCount.n += 1;
      return fn();
    };

    if (typeof k.clearQueryMetricsHistory === 'function') {
      // optional — KnowledgeService may not expose clear; ignore
    }

    const companies = await track(() =>
      k.findCompanies({ tenantId, limit: 1000 })
    );
    const subject = companies.find((c) => c.id === companyId) || null;
    if (!subject || subject.type !== NODE_TYPES.COMPANY) {
      throw new Error(`Company not found via findCompanies: ${companyId}`);
    }

    const people = sortById(
      await track(() =>
        k.findPeople({ tenantId, companyId: subject.id, limit: 200 })
      )
    );

    // Also collect people linked via HAS_CONTACT / WORKS_FOR neighbors if findPeople miss
    const neighborPack = await track(() =>
      k.neighbors({
        tenantId,
        nodeId: subject.id,
        edgeTypes: [EDGE_TYPES.HAS_CONTACT, EDGE_TYPES.WORKS_FOR],
        direction: 'both',
      })
    );
    const peopleById = new Map(people.map((p) => [p.id, p]));
    for (const n of neighborPack) {
      if (n.node && n.node.type === NODE_TYPES.PERSON && !peopleById.has(n.node.id)) {
        peopleById.set(n.node.id, n.node);
      }
    }
    const allPeople = sortById([...peopleById.values()]);

    const personIds = allPeople.map((p) => p.id);
    const interactionsById = new Map();
    for (const personId of personIds) {
      const rows = await track(() =>
        k.findInteractions({
          tenantId,
          relatedNodeId: personId,
          limit: 200,
        })
      );
      for (const row of rows) interactionsById.set(row.id, row);
    }
    // Company-linked interactions
    const companyInteractions = await track(() =>
      k.findInteractions({
        tenantId,
        relatedNodeId: subject.id,
        limit: 200,
      })
    );
    for (const row of companyInteractions) interactionsById.set(row.id, row);
    const interactions = sortById([...interactionsById.values()]);

    const claims = sortById(
      await track(() =>
        k.findClaims({ tenantId, subjectId: subject.id, limit: 200 })
      )
    );

    const evidenceById = new Map();
    const aboutEvidence = await track(() =>
      k.findEvidence({ tenantId, aboutNodeId: subject.id, limit: 200 })
    );
    for (const e of aboutEvidence) evidenceById.set(e.id, e);

    // Evidence supporting claims (via explain chain)
    for (const claim of claims) {
      const explanation = await track(() =>
        k.explain({ tenantId, nodeId: claim.id })
      );
      if (explanation && Array.isArray(explanation.evidence)) {
        for (const e of explanation.evidence) {
          if (e && e.id) evidenceById.set(e.id, e);
        }
      }
    }
    const evidence = sortById([...evidenceById.values()]);

    const timeline = await track(() =>
      k.timeline({ tenantId, nodeId: subject.id, limit: 200 })
    );

    const related = await track(() =>
      k.related({
        tenantId,
        nodeId: subject.id,
        depth: 2,
        edgeTypes: [
          EDGE_TYPES.KNOWS,
          EDGE_TYPES.WORKS_FOR,
          EDGE_TYPES.HAS_CONTACT,
          EDGE_TYPES.USES,
        ],
      })
    );
    const relatedCompanies = sortById(
      (related || [])
        .map((r) => r.node)
        .filter((n) => n && n.type === NODE_TYPES.COMPANY && n.id !== subject.id)
    );

    const neighborEdges = sortById(
      (neighborPack || []).map((n) => n.edge).filter(Boolean),
      (a, b) => String(a.type || '').localeCompare(String(b.type || ''))
    );

    // Additional relationship edges among people (KNOWS)
    for (const person of allPeople) {
      const knows = await track(() =>
        k.neighbors({
          tenantId,
          nodeId: person.id,
          edgeTypes: [EDGE_TYPES.KNOWS],
          direction: 'both',
        })
      );
      for (const n of knows) {
        if (n.edge) neighborEdges.push(n.edge);
      }
    }
    const uniqueEdges = sortById(
      dedupeById(neighborEdges),
      (a, b) => String(a.type || '').localeCompare(String(b.type || ''))
    );

    const lastMetrics = typeof k.getLastQueryMetrics === 'function' ? k.getLastQueryMetrics() : null;
    const history =
      typeof k.getQueryMetricsHistory === 'function' ? k.getQueryMetricsHistory() : [];
    const nodesTraversed = history.reduce(
      (sum, m) => sum + (m.nodesVisited || 0) + (m.edgesTraversed || 0),
      0
    );

    const metrics = {
      graphQueries: queryCount.n,
      nodesTraversed,
      repositoryType:
        (lastMetrics && lastMetrics.repositoryType) ||
        (history[0] && history[0].repositoryType) ||
        'unknown',
      peopleCount: allPeople.length,
      interactionCount: interactions.length,
      claimCount: claims.length,
      evidenceCount: evidence.length,
      relatedCompanyCount: relatedCompanies.length,
      timelineLength: Array.isArray(timeline) ? timeline.length : 0,
    };

    const context = {
      tenantId,
      company: subject,
      people: allPeople,
      interactions,
      claims,
      evidence,
      timeline: Array.isArray(timeline) ? timeline : [],
      relatedCompanies,
      metrics,
      neighborEdges: uniqueEdges,
      builtAt: asOf,
      repositoryType: metrics.repositoryType,
      _signalIndex: buildSignalIndex({
        company: subject,
        people: allPeople,
        interactions,
        claims,
        evidence,
        neighborEdges: uniqueEdges,
      }),
    };

    return deepFreeze(context);
  }
}

/**
 * Precompute lowercase text bags for strategies (deterministic, read-only).
 * @param {object} pack
 */
function buildSignalIndex(pack) {
  const claimTexts = (pack.claims || []).map((c) => ({
    id: c.id,
    text: `${c.statement || ''} ${JSON.stringify(c.metadata || {})}`.toLowerCase(),
    confidence: c.confidence == null ? null : Number(c.confidence),
    status: c.status || null,
  }));
  const evidenceTexts = (pack.evidence || []).map((e) => ({
    id: e.id,
    text: `${e.summary || ''} ${e.sourceType || ''} ${JSON.stringify(e.metadata || {})}`.toLowerCase(),
    confidence: e.confidence == null ? null : Number(e.confidence),
    sourceId: e.sourceId || null,
    sourceType: e.sourceType || null,
  }));
  const interactionTexts = (pack.interactions || []).map((i) => ({
    id: i.id,
    actionType: String(i.actionType || '').toLowerCase(),
    channel: String(i.channel || '').toLowerCase(),
    text: `${i.summary || ''} ${JSON.stringify(i.metadata || {})}`.toLowerCase(),
    occurredAt: i.occurredAt || i.createdAt || null,
  }));
  const companyMeta = {
    ...(pack.company && pack.company.metadata && typeof pack.company.metadata === 'object'
      ? pack.company.metadata
      : {}),
  };
  const people = (pack.people || []).map((p) => ({
    id: p.id,
    name: p.name || null,
    email: p.email || null,
    title: p.title || null,
    titleLower: String(p.title || '').toLowerCase(),
    metadata: p.metadata || {},
  }));
  const edgeTypes = (pack.neighborEdges || []).map((e) => ({
    id: e.id,
    type: e.type,
    fromId: e.fromId,
    toId: e.toId,
  }));

  return Object.freeze({
    claimTexts: Object.freeze(claimTexts),
    evidenceTexts: Object.freeze(evidenceTexts),
    interactionTexts: Object.freeze(interactionTexts),
    companyMeta: Object.freeze(companyMeta),
    people: Object.freeze(people),
    edgeTypes: Object.freeze(edgeTypes),
  });
}

/**
 * @param {object} knowledge
 */
function assertQuerySurface(knowledge) {
  const required = [
    'findCompanies',
    'findPeople',
    'findInteractions',
    'findClaims',
    'findEvidence',
    'neighbors',
    'timeline',
    'related',
    'explain',
  ];
  for (const method of required) {
    if (typeof knowledge[method] !== 'function') {
      throw new Error(`ReasoningContextBuilder knowledge missing ${method}()`);
    }
  }
}

function requireTenant(tenantId) {
  if (tenantId == null || tenantId === '') {
    throw new Error('tenantId is required');
  }
  return String(tenantId);
}

function requireId(id, label) {
  if (id == null || id === '') {
    throw new Error(`${label} is required`);
  }
  return String(id);
}

/**
 * @template {{ id?: string }} T
 * @param {T[]} rows
 */
function dedupeById(rows) {
  const map = new Map();
  for (const row of rows || []) {
    if (row && row.id != null) map.set(String(row.id), row);
  }
  return [...map.values()];
}

/**
 * Match keywords against precomputed signal index; returns EvidenceRefs from graph only.
 * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
 * @param {string[]} keywords
 * @param {'supporting'|'contradicting'} polarity
 * @returns {{ refs: import('../reasoning/ReasoningTypes').EvidenceRef[], claimIds: string[] }}
 */
function matchSignals(context, keywords, polarity = 'supporting') {
  const index = context._signalIndex;
  const refs = [];
  const claimIds = [];
  const seen = new Set();
  const needles = (keywords || []).map((k) => String(k).toLowerCase()).filter(Boolean);

  const push = (ref) => {
    const key = `${ref.kind}:${ref.id}:${ref.summary}`;
    if (seen.has(key)) return;
    seen.add(key);
    refs.push(ref);
  };

  for (const c of index.claimTexts) {
    if (c.status && c.status !== 'active') continue;
    if (needles.some((n) => c.text.includes(n))) {
      claimIds.push(c.id);
      push(
        evidenceRef({
          id: c.id,
          kind: 'claim',
          summary: `Claim matched (${polarity}): ${c.id}`,
          confidence: c.confidence,
        })
      );
    }
  }
  for (const e of index.evidenceTexts) {
    if (needles.some((n) => e.text.includes(n))) {
      push(
        evidenceRef({
          id: e.id,
          kind: 'evidence',
          summary: e.text.slice(0, 160) || `Evidence ${e.id}`,
          sourceId: e.sourceId,
          sourceType: e.sourceType,
          confidence: e.confidence,
        })
      );
    }
  }
  for (const i of index.interactionTexts) {
    const hay = `${i.actionType} ${i.channel} ${i.text}`;
    if (needles.some((n) => hay.includes(n))) {
      push(
        evidenceRef({
          id: i.id,
          kind: 'interaction',
          summary: `Interaction ${i.actionType || 'unknown'}: ${i.id}`,
          confidence: null,
        })
      );
    }
  }

  // Metadata flags on company (only if present — never invent)
  for (const needle of needles) {
    for (const [key, value] of Object.entries(index.companyMeta)) {
      const hay = `${key}:${JSON.stringify(value)}`.toLowerCase();
      if (hay.includes(needle)) {
        push(
          evidenceRef({
            id: `meta:${context.company.id}:${key}`,
            kind: 'company',
            summary: `Company metadata.${key}=${JSON.stringify(value)}`,
            confidence:
              typeof index.companyMeta.confidence === 'number'
                ? index.companyMeta.confidence
                : null,
          })
        );
      }
    }
  }

  refs.sort((a, b) => String(a.id).localeCompare(String(b.id)));
  claimIds.sort();
  return { refs, claimIds: [...new Set(claimIds)] };
}

/**
 * Days since ISO timestamp (null if unparseable).
 * @param {string|null|undefined} iso
 * @param {string} [asOf]
 */
function daysSince(iso, asOf) {
  if (!iso) return null;
  const t = Date.parse(iso);
  const now = asOf ? Date.parse(asOf) : Date.now();
  if (!Number.isFinite(t) || !Number.isFinite(now)) return null;
  return (now - t) / (1000 * 60 * 60 * 24);
}

module.exports = {
  ReasoningContextBuilder,
  matchSignals,
  daysSince,
  buildSignalIndex,
};
