'use strict';

/**
 * Explicit irreversible outbound provider boundary for governed dispatch.
 * Pre-provider validation failures must not be classified as provider uncertainty.
 */

const PRE_PROVIDER_FAILURE_CODES = new Set([
  'ak_observed_provenance_required',
  'ak_inferred_evidence_required',
  'ak_inferred_derivation_required',
  'governed_paige_lineage_missing',
  'governed_paige_lineage_revision_mismatch',
  'governed_paige_lineage_copy_mismatch',
  'governed_schedule_binding_required',
  'governed_schedule_not_sent',
  'governed_executor_required',
  'governed_item_not_authorized',
  'governed_outreach_asset_changed',
  'governed_schedule_payload_changed',
  'emmett_capacity_required',
  'emmett_capacity_exhausted',
  'emmett_envelope_missing',
  'emmett_envelope_expired',
  'emmett_outside_send_window',
  'emmett_governor_halted',
  'emmett_governor_pause',
  'emmett_governor_emergency',
  'dispatch_unavailable_now',
  'provider_payload_changed',
  'provider_call_budget_exceeded',
  'pre_provider_stop',
  'pre_provider_persist_failed',
  'scheduler_overlap',
  'schedule_not_pending',
  'outreach_asset_required',
  'outreach_asset_not_found',
  'outreach_asset_invalid',
  'missing_explicit_sender',
  'missing_recipient',
  'missing_api_key',
  'canonical_execution_did_not_dispatch',
  'item_not_pending',
  'copy_changed',
  'crm_binding_changed',
  'envelope_invalid',
  'kill_switch',
  'program_not_active',
  'suppressed_or_claimed',
  'budget_or_uncertain_block',
  'reply_poll_stale',
  'cross_path_spacing',
  'dnc',
  'outreach_review_required',
  'outreach_identity_required',
  'tenant_mailbox_not_ready',
  'governed_scheduler_required',
  'governed_scheduler_binding_changed',
  'mailboxError',
]);

const RETRYABLE_PRE_PROVIDER_SCHEDULE_REJECTIONS = new Set([
  'emmett_capacity_exhausted',
  'emmett_envelope_missing',
  'emmett_envelope_expired',
  'emmett_outside_send_window',
  'emmett_governor_halted',
  'emmett_governor_pause',
  'emmett_governor_emergency',
  'dispatch_unavailable_now',
  'outside_business_hours',
  'weekend',
  'spacing',
]);

function createProviderBoundaryTracker() {
  const state = { crossed: false };
  return {
    get crossed() { return state.crossed; },
    markCrossed() { state.crossed = true; },
  };
}

function attachProviderBoundaryCrossed(error, crossed = true) {
  if (!error || typeof error !== 'object') return error;
  error.providerBoundaryCrossed = crossed;
  return error;
}

function providerBoundaryWasCrossed(error = {}, tracker = null) {
  if (tracker?.crossed) return true;
  if (error.providerBoundaryCrossed === true) return true;
  return false;
}

function isPreProviderOutboundFailure(error = {}, tracker = null) {
  if (providerBoundaryWasCrossed(error, tracker)) return false;
  const code = String(error.code || error.message || '').trim();
  if (PRE_PROVIDER_FAILURE_CODES.has(code)) return true;
  if (/^brevo_http_4/.test(code)) return true;
  if (code === '23502') return !providerBoundaryWasCrossed(error, tracker);
  if (code === 'provider_rejected') return true;
  return false;
}

function markLeafProviderSend(sendFn, providerBoundary) {
  if (!providerBoundary || typeof providerBoundary.markCrossed !== 'function') return;
  if (sendFn?.isGovernedTenantMailboxTransport) return;
  providerBoundary.markCrossed();
}

function terminalPreProviderReason(error = {}) {
  return String(error.code || error.message || 'pre_provider_failed').slice(0, 200);
}

function isRetryablePreProviderScheduleRejection(reason) {
  return RETRYABLE_PRE_PROVIDER_SCHEDULE_REJECTIONS.has(String(reason || '').trim());
}

module.exports = {
  PRE_PROVIDER_FAILURE_CODES,
  RETRYABLE_PRE_PROVIDER_SCHEDULE_REJECTIONS,
  createProviderBoundaryTracker,
  attachProviderBoundaryCrossed,
  providerBoundaryWasCrossed,
  isPreProviderOutboundFailure,
  terminalPreProviderReason,
  isRetryablePreProviderScheduleRejection,
  markLeafProviderSend,
};
