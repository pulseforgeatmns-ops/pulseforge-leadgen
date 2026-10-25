'use strict';

// Observation only. Never serialize an Error, message, stack, SQL, input,
// identity, session or mission payload. Unknown codes/labels stay unknown.
const PHASES = new Set([
  'input', 'preflight_connect', 'preflight_begin', 'preflight_timeouts',
  'preflight_bindings', 'preflight_infrastructure', 'preflight_commit', 'preflight_release',
  'claim', 'preparation_connect', 'preparation_begin', 'preparation_timeouts',
  'postclaim_bindings', 'postclaim_infrastructure', 'existing_child_check',
  'canonical_connect', 'canonical_timeouts', 'canonical_hydration', 'child_creation',
  'specialist_setup', 'stage_bindings', 'stage_infrastructure', 'stage_intent', 'stage_execution',
  'final_bindings', 'final_snapshot', 'final_validation', 'ready_event',
  'canonical_release', 'preparation_commit', 'preparation_release',
]);
const INTENTS = new Set(['APPROVE_DISCOVERY', 'APPROVE_PRIORITIZATION',
  'DECIDE_ACQUISITION_APPROACH', 'GENERATE_VARIANTS', 'GENERATE_CAPACITY']);
const CODES = new Set([
  'MISSION_STATE_INCONSISTENT', 'MULTIPLE_ACQUISITION_RUNTIMES',
  'tme_planning', 'tme_precondition', 'tme_specialist', 'tme_validation',
  'tme_persistence', 'tme_transaction_overlap', 'tme_mission_changed',
  '23502', '23503', '23505', '23514', '40001', '40P01', '42P01', '42703', '55P03', '57014',
  'anchor_crm_preparation_connection_timeout', 'anchor_crm_preparation_input_invalid',
  'anchor_crm_preparation_action_authority_changed', 'anchor_crm_preparation_only_required',
  'anchor_crm_preparation_exact_batch_required', 'anchor_crm_preparation_source_changed',
  'anchor_crm_preparation_sender_changed', 'anchor_crm_preparation_inbox_unavailable',
  'anchor_crm_preparation_telemetry_stale', 'anchor_crm_preparation_capacity_unavailable',
  'anchor_crm_preparation_grant_changed', 'anchor_crm_preparation_already_exists',
  'anchor_crm_preparation_copy_blocked', 'anchor_crm_preparation_operator_judgment_required',
  'anchor_crm_preparation_not_review_ready', 'anchor_crm_preparation_output_identity_changed',
  'anchor_scoped_resume_operation_consumed',
]);
const CLASSES = new Set(['planning', 'precondition', 'specialist', 'validation', 'persistence', 'presentation']);
function safeCode(error) { const code = error?.code; return CODES.has(code) ? code : 'unknown'; }
function createPreparationTrace() {
  let phase = 'input', claimOutcome = 'not_attempted', operationId = null;
  let step = null, intent = null, emitted = false;
  return {
    enter(value) { phase = PHASES.has(value) ? value : 'unknown'; },
    operation(value) { operationId = typeof value === 'string' && /^[a-f0-9]{64}$/.test(value) ? value : null; },
    claimStarted() { claimOutcome = 'unknown'; },
    claimCommitted() { claimOutcome = 'committed'; },
    stage(index, value) {
      step = Number.isInteger(index) && index >= 0 && index < 6 ? index : null;
      intent = INTENTS.has(value) ? value : null;
    },
    failure(error) {
      if (emitted) return;
      emitted = true;
      // Sanitizing and the sink are both best-effort. An observation failure
      // cannot change the original rejection, consumption or public response.
      try {
        const causeCodes = [], seen = new Set([error]);
        let cause = error?.cause;
        for (let depth = 0; cause && depth < 3 && !seen.has(cause); depth++) {
          seen.add(cause);
          const code = safeCode(cause);
          if (code !== 'unknown' && !causeCodes.includes(code)) causeCodes.push(code);
          cause = cause.cause;
        }
        const tmeClass = error?.tmeClass;
        console.error('[anchor-crm-preparation]', JSON.stringify({
          event: 'anchor_crm_preparation_failed', version: 1, phase, claimOutcome,
          operationId, step, intent, errorCode: safeCode(error), causeCodes,
          executionErrorClass: CLASSES.has(tmeClass) ? tmeClass : null,
          retryAllowed: false,
        }));
      } catch (_) { /* Diagnostics never replace the original failure. */ }
    },
  };
}
module.exports = { createPreparationTrace };
