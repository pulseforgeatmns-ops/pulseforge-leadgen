'use strict';

const RESOLVING_KINDS = new Set(['inbound_call', 'meeting_scheduled', 'call_completed', 'reply_received']);

// AO CRM records outcomes, not generic "completed" events. Only explicit,
// supported positive outcomes are promoted. Notes, attempted calls, interest,
// gatekeepers and requests for help do not prove an expected conversation occurred.
function normalizeAoActivity(row) {
  let kind = null;
  const consistent = !row.metadata?.internal_outcome || row.metadata.internal_outcome === row.outcome;
  if (consistent && row.outcome === 'booked_walkthrough'
    && ['outcome_logged', 'walkthrough_booked'].includes(row.activity_type)) {
    kind = 'meeting_scheduled';
  } else if (consistent && row.outcome === 'spoke_decision_maker'
    && ['outcome_logged', 'call'].includes(row.activity_type)) {
    kind = 'call_completed';
  }
  return { id: row.id, tenant_id: row.tenant_id, prospect_id: row.prospect_id,
    ao_id: row.ao_id, kind, at: row.created_at, activity_type: row.activity_type,
    outcome: row.outcome, metadata: row.metadata, source: 'ao_prospect_activity' };
}

function resolvesExpectation(activity, expectation) {
  // The existing inbound-call rule permits a documented conversation or a booked
  // meeting to close that follow-up. An unrelated/future expectation is not covered.
  return expectation?.expectation_type === 'inbound_call' && RESOLVING_KINDS.has(activity.kind);
}

module.exports = { normalizeAoActivity, resolvesExpectation };
