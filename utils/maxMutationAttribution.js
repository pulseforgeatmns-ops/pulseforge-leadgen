'use strict';

function sameValue(left, right) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function diffOperationalSnapshots(before = {}, after = {}, context = {}) {
  const fields = new Set([...Object.keys(before || {}), ...Object.keys(after || {})]);
  return [...fields]
    .filter(field => !sameValue(before[field], after[field]))
    .map(field => ({
      entity: context.entity || 'prospect',
      entity_id: context.entityId || null,
      client_id: context.clientId ?? null,
      field,
      before: before[field] ?? null,
      after: after[field] ?? null,
      correlation_id: context.correlationId || null,
      transaction_owner: context.transactionOwner || null,
      max_decision_id: context.maxDecisionId || null,
      max_action_id: context.maxActionId || null,
    }));
}

function exactOriginalHandlerEvidence(mutation, evidence) {
  if (!evidence || evidence.handler_status !== 'success') return false;
  if (!mutation.correlation_id || evidence.correlation_id !== mutation.correlation_id) return false;
  if (evidence.entity && evidence.entity !== mutation.entity) return false;
  if (evidence.entity_id && String(evidence.entity_id) !== String(mutation.entity_id)) return false;
  if (evidence.client_id != null && Number(evidence.client_id) !== Number(mutation.client_id)) return false;
  if (!Array.isArray(evidence.allowed_fields) || !evidence.allowed_fields.includes(mutation.field)) return false;
  if (Object.hasOwn(evidence, 'expected_after') && !sameValue(evidence.expected_after, mutation.after)) return false;
  return true;
}

function classifyOperationalMutations(mutations = [], {
  maxDecisionIds = [],
  maxActionIds = [],
  maxCorrelationIds = [],
  originalHandlerEvidence = [],
} = {}) {
  const decisionIds = new Set(maxDecisionIds.map(String));
  const actionIds = new Set(maxActionIds.map(String));
  const correlationIds = new Set(maxCorrelationIds.map(String));
  const report = {
    global_operational_mutations: [],
    max_attributable_operational_mutations: [],
    expected_original_handler_mutations: [],
    unattributed_operational_mutations: [],
    stop_required: false,
  };
  for (const mutation of mutations) {
    report.global_operational_mutations.push(mutation);
    const maxAttributed = mutation.transaction_owner === 'max'
      || (mutation.max_decision_id && decisionIds.has(String(mutation.max_decision_id)))
      || (mutation.max_action_id && actionIds.has(String(mutation.max_action_id)))
      || (mutation.correlation_id && correlationIds.has(String(mutation.correlation_id)));
    if (maxAttributed) {
      report.max_attributable_operational_mutations.push(mutation);
      continue;
    }
    if (originalHandlerEvidence.some(evidence => exactOriginalHandlerEvidence(mutation, evidence))) {
      report.expected_original_handler_mutations.push(mutation);
      continue;
    }
    report.unattributed_operational_mutations.push(mutation);
  }
  report.stop_required = report.max_attributable_operational_mutations.length > 0
    || report.unattributed_operational_mutations.length > 0;
  report.counts = {
    global: report.global_operational_mutations.length,
    max_attributable: report.max_attributable_operational_mutations.length,
    expected_original_handler: report.expected_original_handler_mutations.length,
    unattributed: report.unattributed_operational_mutations.length,
  };
  return report;
}

module.exports = {
  classifyOperationalMutations,
  diffOperationalSnapshots,
  exactOriginalHandlerEvidence,
  sameValue,
};
