'use strict';

const { deepFreeze } = require('../reasoning/ReasoningTypes');
const { ACTION_TYPES } = require('../commandDeck/CommandDeckTypes');
const {
  NAV_TYPES,
  parseRecommendationId,
  buildNavRef,
} = require('./IntelligenceTypes');
const { RelatedIntelligenceBuilder } = require('./RelatedIntelligence');

/**
 * Recommendation Detail Composer — assemble explainability for one recommendation.
 * May not score, rank, or invent. Prefer memory snapshot over re-evaluate.
 */
class RecommendationDetailComposer {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../memory/MemoryEngine').MemoryEngine} deps.memory
   * @param {import('../policy/engine/PolicyEngine').PolicyEngine} deps.policy
   * @param {RelatedIntelligenceBuilder} [deps.related]
   */
  constructor(deps) {
    if (!deps || !deps.knowledge) {
      throw new Error('RecommendationDetailComposer requires knowledge');
    }
    if (!deps.memory) {
      throw new Error('RecommendationDetailComposer requires memory');
    }
    if (!deps.policy) {
      throw new Error('RecommendationDetailComposer requires policy');
    }
    this._knowledge = deps.knowledge;
    this._memory = deps.memory;
    this._policy = deps.policy;
    this._related =
      deps.related ||
      new RelatedIntelligenceBuilder({
        knowledge: deps.knowledge,
        memory: deps.memory,
      });
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.recommendationId
   * @param {string} [input.asOf]
   * @param {string} [input.operator]
   * @returns {Promise<object>} RecommendationDetailModel
   */
  async compose(input) {
    if (!input || !input.tenantId) {
      throw new Error('composeRecommendation requires tenantId');
    }
    if (!input.recommendationId) {
      throw new Error('composeRecommendation requires recommendationId');
    }

    const started = process.hrtime.bigint();
    const tenantId = String(input.tenantId);
    const recommendationId = String(input.recommendationId);
    const asOf = input.asOf || new Date().toISOString();
    const parsed = parseRecommendationId(recommendationId);

    if (!parsed || parsed.tenantId !== tenantId) {
      return emptyRecommendationModel({
        tenantId,
        recommendationId,
        asOf,
        reason: 'recommendation_not_found',
        buildTimeMs: elapsedMs(started),
      });
    }

    const companyId = parsed.companyId;
    let company = null;
    try {
      company = await this._knowledge.findNode(tenantId, companyId);
    } catch (_err) {
      company = null;
    }
    if (!company) {
      try {
        const companies = await this._knowledge.findCompanies({
          tenantId,
          limit: 500,
        });
        company = companies.find((c) => c.id === companyId) || null;
      } catch (_err) {
        company = null;
      }
    }

    let snapshots = [];
    try {
      snapshots = await this._memory.repository.listByCompany(
        tenantId,
        companyId
      );
    } catch (_err) {
      snapshots = [];
    }
    const latest = snapshots.length ? snapshots[snapshots.length - 1] : null;
    const recommendation =
      (latest && latest.recommendation) ||
      null;

    if (!recommendation) {
      return emptyRecommendationModel({
        tenantId,
        recommendationId,
        companyId,
        companyName: company && company.name,
        asOf,
        reason: 'recommendation_unavailable',
        buildTimeMs: elapsedMs(started),
      });
    }

    let policy = null;
    try {
      policy = await this._policy.evaluate({
        tenantId,
        recommendation,
        context: {
          asOf,
          operator: input.operator || null,
          companyId,
        },
      });
    } catch (_err) {
      policy = null;
    }

    let timeline = [];
    try {
      timeline = await this._knowledge.timeline({
        tenantId,
        nodeId: companyId,
      });
    } catch (_err) {
      timeline = [];
    }

    let history = [];
    try {
      history = await this._memory.history(tenantId, companyId);
    } catch (_err) {
      history = [];
    }

    let trend = null;
    try {
      const evolution = await this._memory.evolve(tenantId, companyId);
      trend = evolution && evolution.trend ? evolution.trend : null;
    } catch (_err) {
      trend = null;
    }

    const supportingSignals = mapSignals(recommendation.supportingSignals);
    const contradictingSignals = mapSignals(recommendation.opposingSignals);
    const evidenceSummary = supportingSignals.slice(0, 12).map((s) => ({
      ...s,
      depth: 'summary',
    }));

    const related = await this._related.forRecommendation({
      tenantId,
      companyId,
      recommendation,
      changes: extractChanges(history),
    });

    const companyName =
      (recommendation.subject && recommendation.subject.name) ||
      (company && company.name) ||
      companyId;

    const model = {
      kind: 'recommendation_detail',
      recommendationId,
      companyId,
      companyName,
      opportunity: {
        type: recommendation.type || null,
        priority: recommendation.priority || null,
        score:
          recommendation.score != null ? Number(recommendation.score) : null,
        recommendedAction: recommendation.recommendedAction || null,
        summary:
          (recommendation.reasoningSummary &&
            (recommendation.reasoningSummary.whyThis || [])
              .slice(0, 3)
              .join(' · ')) ||
          recommendation.recommendedAction ||
          'Recommendation',
      },
      confidence:
        recommendation.confidence != null
          ? Number(recommendation.confidence)
          : null,
      trend,
      supportingSignals,
      contradictingSignals,
      evidence: evidenceSummary,
      history: summarizeHistory(history),
      timeline: (timeline || []).slice(0, 24).map(mapTimelineEvent),
      policy: policy
        ? {
            outcome: policy.outcome,
            auditId: (policy.audit && policy.audit.id) || null,
            reasons: policy.reasons || policy.explanations || [],
            audit: policy.audit || null,
          }
        : null,
      reasoning: recommendation.reasoningSummary || null,
      related,
      actions: [
        {
          id: 'ask_max',
          type: ACTION_TYPES.ASK_MAX,
          label: 'Ask Max about this recommendation',
          payload: {
            recommendationId,
            companyId,
            page: 'recommendation',
            context: 'recommendation_detail',
          },
        },
        {
          id: 'open_company',
          type: ACTION_TYPES.OPEN_COMPANY,
          label: 'Open company intelligence',
          payload: { companyId, recommendationId },
        },
        {
          id: 'back_deck',
          type: 'return_deck',
          label: 'Back to Command Deck',
          payload: {},
        },
      ],
      empty: false,
      emptyReason: null,
      meta: {
        tenantId,
        asOf,
        generatedAt: asOf,
        buildTimeMs: elapsedMs(started),
        snapshotId: latest ? latest.id : null,
      },
    };

    return deepFreeze(model);
  }
}

function mapSignals(signals) {
  return (signals || [])
    .filter((s) => s && s.id != null)
    .map((s) => {
      const ref = buildNavRef({
        type:
          s.kind === 'claim'
            ? NAV_TYPES.CLAIM
            : s.kind === 'interaction'
              ? NAV_TYPES.INTERACTION
              : NAV_TYPES.EVIDENCE,
        id: s.id,
        label: s.summary || s.id,
      });
      return {
        id: String(s.id),
        kind: s.kind || 'evidence',
        summary: s.summary || String(s.id),
        confidence: s.confidence == null ? null : Number(s.confidence),
        sourceId: s.sourceId || null,
        sourceType: s.sourceType || null,
        nav: ref,
      };
    });
}

function mapTimelineEvent(ev) {
  return {
    id: ev.id || ev.nodeId || null,
    type: ev.type || ev.nodeType || null,
    summary: ev.summary || ev.label || ev.id || '',
    at: ev.at || ev.timestamp || ev.occurredAt || null,
    nav: buildNavRef({
      type: mapNodeTypeToNav(ev.type || ev.nodeType),
      id: ev.id || ev.nodeId,
      label: ev.summary || ev.label || ev.id,
    }),
  };
}

function mapNodeTypeToNav(type) {
  const t = String(type || '').toLowerCase();
  if (t.includes('company')) return NAV_TYPES.COMPANY;
  if (t.includes('interaction')) return NAV_TYPES.INTERACTION;
  if (t.includes('claim')) return NAV_TYPES.CLAIM;
  return NAV_TYPES.EVIDENCE;
}

function extractChanges(history) {
  if (!history) return [];
  if (Array.isArray(history.events)) return history.events;
  if (Array.isArray(history.timeline)) return history.timeline;
  if (Array.isArray(history)) return history;
  return [];
}

function summarizeHistory(history) {
  const events = extractChanges(history);
  return events.slice(0, 20).map((e) => ({
    type: e.type || e.changeType || null,
    summary: e.summary || e.field || String(e.type || 'event'),
    at: e.at || e.timestamp || null,
  }));
}

function emptyRecommendationModel(input) {
  return deepFreeze({
    kind: 'recommendation_detail',
    recommendationId: input.recommendationId,
    companyId: input.companyId || null,
    companyName: input.companyName || null,
    opportunity: null,
    confidence: null,
    trend: null,
    supportingSignals: [],
    contradictingSignals: [],
    evidence: [],
    history: [],
    timeline: [],
    policy: null,
    reasoning: null,
    related: {
      similarCompanies: [],
      sharedSignals: [],
      competingOpportunities: [],
      recentChanges: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      alternativeRecommendations: [],
      otherRecommendations: [],
      sourceInteractions: [],
    },
    actions: [
      {
        id: 'back_deck',
        type: 'return_deck',
        label: 'Back to Command Deck',
        payload: {},
      },
      {
        id: 'ask_max',
        type: ACTION_TYPES.ASK_MAX,
        label: 'Ask Max',
        payload: {
          recommendationId: input.recommendationId,
          page: 'recommendation',
        },
      },
    ],
    empty: true,
    emptyReason: input.reason || 'unavailable',
    meta: {
      tenantId: input.tenantId,
      asOf: input.asOf,
      generatedAt: input.asOf,
      buildTimeMs: input.buildTimeMs || 0,
      snapshotId: null,
    },
  });
}

function elapsedMs(started) {
  const ns = process.hrtime.bigint() - started;
  return Number(ns) / 1e6;
}

module.exports = {
  RecommendationDetailComposer,
  createRecommendationDetailComposer(deps) {
    return new RecommendationDetailComposer(deps);
  },
};
