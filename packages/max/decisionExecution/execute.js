'use strict';

const { ACTION_TYPE, EXECUTION_STATUS } = require('./types');
const { newIntentId } = require('./fingerprints');
const { followUpPromptForExpectation } = require('../stateIngestion/expectations');
const { EXPECTATION_STATUS } = require('../stateIngestion/types');

async function executeActionIntent({
  decisionStore,
  stateStore,
  decision,
  selected,
  snapshot,
}) {
  const actionType = selected?.action_type;
  const intent = {
    id: newIntentId(),
    decision_id: decision.id,
    client_id: decision.client_id,
    action_type: actionType,
    action_payload: {},
    authority_class: decision.authority_class,
    execution_status: EXECUTION_STATUS.EXECUTING,
    verification_status: null,
    idempotency_key: decision.idempotency_key,
    output_payload: {},
  };

  if (actionType === ACTION_TYPE.NO_ACTION || actionType === ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE) {
    intent.execution_status = EXECUTION_STATUS.NO_ACTION_RECORDED;
    intent.verification_status = 'NOT_APPLICABLE';
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { noop: true } };
  }

  if (actionType === ACTION_TYPE.ASK_AO_STATUS || actionType === ACTION_TYPE.CREATE_AO_TASK) {
    const exp = snapshot.expectation;
    const aoName = snapshot.account?.owner_name || 'AO';
    const prompt = exp
      ? followUpPromptForExpectation({ ...exp, status: 'OVERDUE' }, aoName)
      : `Please confirm status for ${snapshot.account?.company_name || 'account'}.`;
    const task = {
      id: newIntentId(),
      client_id: decision.client_id,
      decision_id: decision.id,
      prospect_id: snapshot.account?.id,
      owner_id: snapshot.account?.owner_id,
      owner_name: aoName,
      account_name: snapshot.account?.company_name,
      prompt,
      status: 'open',
      expectation_id: exp?.id || null,
      created_at: new Date().toISOString(),
    };
    intent.action_payload = { task };
    await decisionStore.createAoTask(task);
    intent.execution_status = EXECUTION_STATUS.EXECUTED;
    intent.output_payload = { task_id: task.id, prompt };
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { task } };
  }

  if (actionType === ACTION_TYPE.RESOLVE_EXPECTATION) {
    const exp = snapshot.expectation;
    if (exp && stateStore.updateExpectation) {
      await stateStore.updateExpectation(exp.id, { status: EXPECTATION_STATUS.RESOLVED });
    } else if (exp && stateStore.expectations) {
      const row = stateStore.expectations.find(e => e.id === exp.id);
      if (row) row.status = EXPECTATION_STATUS.RESOLVED;
    }
    intent.execution_status = EXECUTION_STATUS.EXECUTED;
    intent.output_payload = { expectation_id: exp?.id, status: 'RESOLVED' };
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { resolved: exp?.id } };
  }

  if (actionType === ACTION_TYPE.SUPERSEDE_PRIOR) {
    const superseded = await decisionStore.supersedeActiveDecisions({
      clientId: decision.client_id,
      subjectType: decision.subject_type,
      subjectId: decision.subject_id,
      supersededBy: decision.id,
    });
    intent.execution_status = EXECUTION_STATUS.EXECUTED;
    intent.output_payload = { superseded_count: superseded.length };
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { superseded } };
  }

  if (actionType === ACTION_TYPE.DELEGATE_AGENT) {
    const delegation = await decisionStore.createDelegation({
      client_id: decision.client_id,
      decision_id: decision.id,
      agent: selected.agent || 'scout',
      task: 'research',
      input_evidence: decision.supporting_evidence,
      expected_output: 'canonical_evidence',
      completion_criteria: 'verified_prospect_facts',
      status: 'requested',
    });
    intent.execution_status = EXECUTION_STATUS.EXECUTED;
    intent.output_payload = { delegation_id: delegation.id };
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { delegation } };
  }

  if (actionType === ACTION_TYPE.ESCALATE_OPERATOR) {
    const escalation = {
      id: newIntentId(),
      client_id: decision.client_id,
      decision_id: decision.id,
      kind: 'operator_escalation',
      status: 'pending_approval',
      summary: decision.rationale,
    };
    await decisionStore.createEscalation(escalation);
    intent.execution_status = EXECUTION_STATUS.EXECUTION_BLOCKED;
    intent.output_payload = { escalation_id: escalation.id };
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { escalation } };
  }

  if (actionType === ACTION_TYPE.RETURN_TO_COLD_OUTREACH) {
    intent.execution_status = EXECUTION_STATUS.EXECUTION_BLOCKED;
    intent.output_payload = { reason: 'external_side_effect_requires_policy' };
    await decisionStore.persistActionIntent(intent);
    return { intent, result: { blocked: true } };
  }

  intent.execution_status = EXECUTION_STATUS.EXECUTION_FAILURE;
  intent.error_code = 'UNSUPPORTED_ACTION';
  await decisionStore.persistActionIntent(intent);
  return { intent, result: { error: 'unsupported' } };
}

module.exports = {
  executeActionIntent,
};
