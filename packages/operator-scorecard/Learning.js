'use strict';

/**
 * SPEC-116 — operator feedback becomes learning.
 * Learning adjusts future recommendations. It never rewrites history.
 */

const { asText, nowIso, newId, clone } = require('./types');

function recordLearning(store, input = {}) {
  const row = {
    id: input.id || newId('learn'),
    tenantId: asText(input.tenantId || input.tenant_id),
    clientId: input.clientId || input.client_id || null,
    scorecardId: input.scorecardId || null,
    metricKey: asText(input.metricKey || input.metric_key),
    metricName: asText(input.metricName || input.metric_name),
    action: asText(input.action),
    reason: asText(input.reason) || null,
    suppress: input.suppress === true,
    prioritize: input.prioritize === true,
    createdAt: nowIso(),
  };
  if (store && typeof store.addLearning === 'function') store.addLearning(row);
  return row;
}

function learningForTenant(store, tenantId) {
  if (!store || typeof store.listLearning !== 'function') return [];
  return store.listLearning(tenantId);
}

function applyReasonToLearning(row, reason) {
  const next = clone(row);
  next.reason = asText(reason) || next.reason;
  return next;
}

module.exports = {
  recordLearning,
  learningForTenant,
  applyReasonToLearning,
};
