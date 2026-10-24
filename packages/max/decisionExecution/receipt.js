'use strict';

const { EXECUTION_STATUS, VERIFICATION_STATUS } = require('./types');

// Presentation must not promote a returned ID into evidence of successful work.
// Persisted decision status remains authoritative; a conflicting intent can only
// make the receipt more conservative, never turn pending/failed work into success.
function receiptStatus(decision, intent) {
  const status = decision.execution_status || EXECUTION_STATUS.PENDING;
  if (status !== EXECUTION_STATUS.EXECUTED) return status;
  if (decision.verification_status === VERIFICATION_STATUS.VERIFICATION_FAILED
    || intent?.verification_status === VERIFICATION_STATUS.VERIFICATION_FAILED) {
    return EXECUTION_STATUS.ACTION_VERIFICATION_FAILED;
  }
  if (intent && intent.execution_status !== EXECUTION_STATUS.EXECUTED) {
    return intent.execution_status || EXECUTION_STATUS.PENDING;
  }
  if (decision.verification_status !== VERIFICATION_STATUS.VERIFIED
    || (intent && intent.verification_status !== VERIFICATION_STATUS.VERIFIED)) {
    return EXECUTION_STATUS.PENDING;
  }
  return status;
}

function receiptVerification(decision, intent, status) {
  if ([EXECUTION_STATUS.ACTION_VERIFICATION_FAILED, EXECUTION_STATUS.EXECUTION_FAILURE].includes(status)
    || decision.verification_status === VERIFICATION_STATUS.VERIFICATION_FAILED
    || intent?.verification_status === VERIFICATION_STATUS.VERIFICATION_FAILED) {
    return VERIFICATION_STATUS.VERIFICATION_FAILED;
  }
  if (decision.verification_status !== VERIFICATION_STATUS.VERIFIED) {
    return decision.verification_status || VERIFICATION_STATUS.PENDING;
  }
  if (status !== EXECUTION_STATUS.EXECUTED) return VERIFICATION_STATUS.PENDING;
  if (intent && intent.verification_status !== VERIFICATION_STATUS.VERIFIED) {
    return intent.verification_status || VERIFICATION_STATUS.PENDING;
  }
  return VERIFICATION_STATUS.VERIFIED;
}

function buildDecisionReceipt({ decision, intent, snapshot }) {
  const account = snapshot?.account?.company_name || 'account';
  const owner = snapshot?.account?.owner_name || 'owner';
  const selected = decision.selected_action?.action_type || 'NO_ACTION';
  const status = receiptStatus(decision, intent);

  if (selected === 'NO_ACTION' || selected === 'ABSTAIN_INSUFFICIENT_EVIDENCE') {
    return {
      summary: decision.rationale,
      status,
      next_evaluation: decision.reevaluate_after || null,
    };
  }

  if (selected === 'ASK_AO_STATUS' || selected === 'CREATE_AO_TASK') {
    const waiting = status === EXECUTION_STATUS.EXECUTED && Boolean(intent?.output_payload?.task_id);
    return {
      summary: `${account}: ${decision.rationale}`,
      status: waiting ? 'Waiting on AO' : status,
      waiting_on: waiting ? owner : null,
      next_evaluation: waiting ? 'after AO response' : decision.reevaluate_after || null,
      prompt: waiting ? intent.output_payload.prompt || null : null,
    };
  }

  return {
    summary: decision.rationale,
    status,
    verification: receiptVerification(decision, intent, status),
    next_evaluation: decision.reevaluate_after || null,
  };
}

module.exports = {
  buildDecisionReceipt,
};
