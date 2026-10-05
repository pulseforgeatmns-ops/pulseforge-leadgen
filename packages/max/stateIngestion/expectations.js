'use strict';

const { EXPECTATION_STATUS } = require('./types');

function expectationFromMutations(mutations = [], { clientId, prospectId, aoId, ingestionId, claimId, sourceEvidence = {} }) {
  const next = mutations.find(m => m.field_name === 'next_expected_event');
  const window = mutations.find(m => m.field_name === 'expected_window');
  if (!next) return null;
  return {
    client_id: clientId,
    prospect_id: prospectId,
    ao_id: aoId || null,
    expectation_type: next.intended_value?.kind || 'future_event',
    description: next.intended_value?.kind === 'inbound_call' ? 'Awaiting inbound call' : 'Awaiting expected event',
    expected_window: window?.intended_value || {},
    status: EXPECTATION_STATUS.WAITING,
    source_ingestion_id: ingestionId,
    source_claim_id: claimId || null,
    source_evidence: sourceEvidence,
  };
}

function markOverdueExpectations(expectations = [], now = new Date()) {
  const overdue = [];
  for (const exp of expectations) {
    if (!['OPEN', 'WAITING'].includes(exp.status)) continue;
    const end = exp.expected_window?.ends_at || exp.expected_window?.end;
    if (!end) continue;
    const endDate = new Date(end);
    if (!Number.isNaN(endDate.getTime()) && endDate < now) {
      overdue.push({ ...exp, status: EXPECTATION_STATUS.OVERDUE });
    }
  }
  return overdue;
}

function followUpPromptForExpectation(exp, aoName = 'AO') {
  if (exp.status !== EXPECTATION_STATUS.OVERDUE) return null;
  const account = exp.source_evidence?.account_name || 'this account';
  return `${aoName} — did you hear back from ${account}?`;
}

module.exports = {
  expectationFromMutations,
  markOverdueExpectations,
  followUpPromptForExpectation,
  EXPECTATION_STATUS,
};
