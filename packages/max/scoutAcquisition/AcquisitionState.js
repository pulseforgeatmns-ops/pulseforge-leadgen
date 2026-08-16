'use strict';

/**
 * SPEC-100 — Acquisition / AO intelligence state.
 * Written only by Max after evaluation. Scout cannot mutate this store.
 */

const { asText, clone, nowIso, isPlainObject } = require('./Types');

function createMemoryAcquisitionState(snapshot = null) {
  /** @type {Map<string, object>} */
  const rows = new Map();
  if (snapshot && Array.isArray(snapshot.rows)) {
    for (const row of snapshot.rows) {
      rows.set(String(row.tenantId), clone(row));
    }
  }

  return {
    kind: 'memory',
    async get(tenantId) {
      const row = rows.get(String(tenantId));
      return row ? clone(row) : null;
    },
    async put(row) {
      const tenantId = asText(row && row.tenantId);
      if (!tenantId) throw new Error('acquisition state requires tenantId');
      const next = {
        tenantId,
        summary: asText(row.summary) || 'Pipeline steady',
        opportunityCount: Number(row.opportunityCount || 0),
        timelyCount: Number(row.timelyCount || 0),
        segmentHighlights: Array.isArray(row.segmentHighlights)
          ? row.segmentHighlights.slice()
          : [],
        unknowns: Array.isArray(row.unknowns) ? row.unknowns.slice() : [],
        acceptedClaims: Array.isArray(row.acceptedClaims) ? clone(row.acceptedClaims) : [],
        rejectedClaims: Array.isArray(row.rejectedClaims) ? clone(row.rejectedClaims) : [],
        unresolvedClaims: Array.isArray(row.unresolvedClaims)
          ? clone(row.unresolvedClaims)
          : [],
        materiality: asText(row.materiality) || 'immaterial',
        priorityImpact: row.priorityImpact || null,
        evaluationId: asText(row.evaluationId),
        resultId: asText(row.resultId),
        delegationId: asText(row.delegationId),
        opportunities: Array.isArray(row.opportunities) ? clone(row.opportunities) : [],
        updatedAt: row.updatedAt || nowIso(),
      };
      rows.set(tenantId, next);
      return clone(next);
    },
    serialize() {
      return { rows: [...rows.values()].map(clone) };
    },
    clear() {
      rows.clear();
    },
  };
}

function buildAcquisitionSummary(input = {}) {
  const timely = Number(input.timelyCount || 0);
  const total = Number(input.opportunityCount || 0);
  const highlights = Array.isArray(input.segmentHighlights)
    ? input.segmentHighlights.filter(Boolean)
    : [];
  if (!total) return 'Pipeline steady';
  const lines = [];
  if (timely) {
    lines.push(
      `${timely} timely opportunit${timely === 1 ? 'y' : 'ies'} identified`
    );
  } else {
    lines.push(`${total} matching compan${total === 1 ? 'y' : 'ies'} on file`);
  }
  if (highlights.length) {
    lines.push(`${highlights[0]} signals strengthened`);
  }
  return lines.join('. ');
}

function deriveStateFromEvaluation(input = {}) {
  const evaluation = input.evaluation || {};
  const result = input.result || {};
  const opportunities = Array.isArray(input.opportunities)
    ? input.opportunities
    : ((result.payload && result.payload.opportunities) ||
        (result.artifactRefs || []).filter((a) => a && a.kind === 'acquisition_opportunity'));
  const timely = opportunities.filter((o) => Number(o.timing || 0) >= 0.6);
  const segments = new Set();
  for (const opp of opportunities) {
    for (const signal of opp.signals || []) {
      if (signal.type === 'portfolio_growth' || signal.type === 'expansion') {
        segments.add('Property-management');
      }
    }
  }
  const materiality =
    evaluation.materiality ||
    (evaluation.materialChange ? 'material' : 'immaterial');
  const summary = buildAcquisitionSummary({
    opportunityCount: opportunities.length,
    timelyCount: timely.length,
    segmentHighlights: [...segments],
  });

  return {
    tenantId: evaluation.tenantId || result.tenantId,
    summary,
    opportunityCount: opportunities.length,
    timelyCount: timely.length,
    segmentHighlights: [...segments],
    unknowns: result.uncertainties || [],
    acceptedClaims: evaluation.acceptedClaims || [],
    rejectedClaims: evaluation.rejectedClaims || [],
    unresolvedClaims: evaluation.unresolvedClaims || [],
    materiality,
    priorityImpact:
      materiality === 'material' ? evaluation.suggestedPriorityChange || null : null,
    evaluationId: evaluation.id,
    resultId: evaluation.resultId || result.id,
    delegationId: evaluation.delegationId || result.delegationId,
    opportunities,
    updatedAt: nowIso(),
  };
}

function createPostgresAcquisitionState(pool) {
  const db = pool || require('../../../db');
  let ensured = false;
  async function ensureTable() {
    if (ensured) return;
    await db.query(`
      CREATE TABLE IF NOT EXISTS acquisition_intelligence_state (
        tenant_id TEXT PRIMARY KEY,
        summary TEXT NOT NULL DEFAULT 'Pipeline steady',
        opportunity_count INTEGER NOT NULL DEFAULT 0,
        timely_count INTEGER NOT NULL DEFAULT 0,
        segment_highlights JSONB NOT NULL DEFAULT '[]'::jsonb,
        unknowns JSONB NOT NULL DEFAULT '[]'::jsonb,
        accepted_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
        rejected_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
        unresolved_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
        materiality TEXT NOT NULL DEFAULT 'immaterial',
        priority_impact JSONB,
        evaluation_id TEXT,
        result_id TEXT,
        delegation_id TEXT,
        opportunities JSONB NOT NULL DEFAULT '[]'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    ensured = true;
  }

  function mapRow(row) {
    if (!row) return null;
    return {
      tenantId: String(row.tenant_id),
      summary: row.summary,
      opportunityCount: Number(row.opportunity_count || 0),
      timelyCount: Number(row.timely_count || 0),
      segmentHighlights: row.segment_highlights || [],
      unknowns: row.unknowns || [],
      acceptedClaims: row.accepted_claims || [],
      rejectedClaims: row.rejected_claims || [],
      unresolvedClaims: row.unresolved_claims || [],
      materiality: row.materiality,
      priorityImpact: row.priority_impact || null,
      evaluationId: row.evaluation_id,
      resultId: row.result_id,
      delegationId: row.delegation_id,
      opportunities: row.opportunities || [],
      updatedAt:
        row.updated_at instanceof Date
          ? row.updated_at.toISOString()
          : String(row.updated_at),
    };
  }

  return {
    kind: 'postgres',
    async get(tenantId) {
      await ensureTable();
      const result = await db.query(
        `SELECT * FROM acquisition_intelligence_state WHERE tenant_id = $1`,
        [String(tenantId)]
      );
      return mapRow(result.rows[0] || null);
    },
    async put(row) {
      await ensureTable();
      const tenantId = String(row.tenantId);
      const result = await db.query(
        `INSERT INTO acquisition_intelligence_state (
          tenant_id, summary, opportunity_count, timely_count, segment_highlights,
          unknowns, accepted_claims, rejected_claims, unresolved_claims,
          materiality, priority_impact, evaluation_id, result_id, delegation_id,
          opportunities, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,
          $10,$11::jsonb,$12,$13,$14,$15::jsonb,NOW()
        )
        ON CONFLICT (tenant_id) DO UPDATE SET
          summary = EXCLUDED.summary,
          opportunity_count = EXCLUDED.opportunity_count,
          timely_count = EXCLUDED.timely_count,
          segment_highlights = EXCLUDED.segment_highlights,
          unknowns = EXCLUDED.unknowns,
          accepted_claims = EXCLUDED.accepted_claims,
          rejected_claims = EXCLUDED.rejected_claims,
          unresolved_claims = EXCLUDED.unresolved_claims,
          materiality = EXCLUDED.materiality,
          priority_impact = EXCLUDED.priority_impact,
          evaluation_id = EXCLUDED.evaluation_id,
          result_id = EXCLUDED.result_id,
          delegation_id = EXCLUDED.delegation_id,
          opportunities = EXCLUDED.opportunities,
          updated_at = NOW()
        RETURNING *`,
        [
          tenantId,
          row.summary || 'Pipeline steady',
          Number(row.opportunityCount || 0),
          Number(row.timelyCount || 0),
          JSON.stringify(row.segmentHighlights || []),
          JSON.stringify(row.unknowns || []),
          JSON.stringify(row.acceptedClaims || []),
          JSON.stringify(row.rejectedClaims || []),
          JSON.stringify(row.unresolvedClaims || []),
          row.materiality || 'immaterial',
          JSON.stringify(row.priorityImpact || null),
          row.evaluationId || null,
          row.resultId || null,
          row.delegationId || null,
          JSON.stringify(row.opportunities || []),
        ]
      );
      return mapRow(result.rows[0]);
    },
  };
}

function toCommandDeckSignal(state) {
  if (!state || !isPlainObject(state)) return null;
  return {
    summary: state.summary,
    opportunityCount: state.opportunityCount,
    timelyCount: state.timelyCount,
    segmentHighlights: state.segmentHighlights || [],
    aoIntelligence: true,
    priorityImpact: state.priorityImpact || null,
    evaluationId: state.evaluationId,
    resultId: state.resultId,
    delegationId: state.delegationId,
  };
}

module.exports = {
  createMemoryAcquisitionState,
  createPostgresAcquisitionState,
  buildAcquisitionSummary,
  deriveStateFromEvaluation,
  toCommandDeckSignal,
};
