'use strict';

function buildDecisionReceipt({ decision, intent, snapshot }) {
  const account = snapshot?.account?.company_name || 'account';
  const owner = snapshot?.account?.owner_name || 'owner';
  const selected = decision.selected_action?.action_type || 'NO_ACTION';

  if (selected === 'NO_ACTION' || selected === 'ABSTAIN_INSUFFICIENT_EVIDENCE') {
    return {
      summary: decision.rationale,
      status: decision.execution_status,
      next_evaluation: decision.reevaluate_after || null,
    };
  }

  if (selected === 'ASK_AO_STATUS' || selected === 'CREATE_AO_TASK') {
    return {
      summary: `${account}: ${decision.rationale}`,
      status: intent?.output_payload?.task_id ? 'Waiting on AO' : decision.execution_status,
      waiting_on: owner,
      next_evaluation: 'after AO response',
      prompt: intent?.output_payload?.prompt || null,
    };
  }

  return {
    summary: decision.rationale,
    status: decision.execution_status,
    verification: decision.verification_status,
    next_evaluation: decision.reevaluate_after || null,
  };
}

module.exports = {
  buildDecisionReceipt,
};
