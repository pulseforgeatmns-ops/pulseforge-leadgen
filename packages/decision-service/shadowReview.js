'use strict';

const { listShadowEvents, reviewOptions } = require('./ShadowEventRepository');
const { INSPECTION_THRESHOLD, classifyDecisionMismatch } = require('./mismatchClassifier');
const { likelyMissionInspection } = require('./shadowRoutingWarning');

function buildShadowReview(rows, options) {
  const scope = reviewOptions(options);
  const mismatches = rows.filter(row => row.comparison === 'mismatch');
  const errors = rows.filter(row => row.status === 'error' || row.errors?.length > 0);
  const likely = rows.filter(row => classifyDecisionMismatch(row) != null);
  const count = (items, key) => items.reduce((result, row) => {
    const label = row[key] ?? 'unknown';
    result[label] = (result[label] || 0) + 1;
    return result;
  }, Object.create(null));
  const comparable = rows.filter(row => ['match', 'mismatch'].includes(row.comparison)).length;
  return {
    spec: 'SPEC-JEV-002', mode: 'shadow_review',
    scope: { latest: scope.limit, tenant_id: scope.tenantId, filter: scope.filter,
      summary_basis: 'returned rows only' },
    thresholds: {
      confidence: 0.9,
      inspection_probability: INSPECTION_THRESHOLD,
    },
    summary: { total: rows.length, comparable, mismatches: mismatches.length,
      mismatch_rate: comparable ? mismatches.length / comparable : null,
      likely_mission_inspections: likely.length, errors: errors.length,
      by_status: count(rows, 'status'), mismatch_intents: count(mismatches, 'intent'),
      error_codes: count(errors.flatMap(row => row.errors || []), 'code') },
    evaluations: scope.filter === 'warnings' ? likely : rows,
    mismatches, likely_mission_inspections: likely, operator_warnings: likely, errors,
  };
}

async function queryShadowReview(db, options) {
  return buildShadowReview(await listShadowEvents(db, options), options);
}

module.exports = { likelyMissionInspection, buildShadowReview, queryShadowReview };
