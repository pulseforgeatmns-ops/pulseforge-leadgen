'use strict';

const { ACTION_TYPE, VERIFICATION_STATUS, EXECUTION_STATUS } = require('./types');
const { expectationScope, matchesExpectation } = require('./expectationScope');

async function verifyActionIntent({ decisionStore, stateStore, intent, snapshot, options = {} }) {
  if (intent.execution_status === EXECUTION_STATUS.NO_ACTION_RECORDED) {
    return { ...intent, verification_status: VERIFICATION_STATUS.NOT_APPLICABLE };
  }
  if (intent.execution_status === EXECUTION_STATUS.EXECUTION_BLOCKED) {
    return { ...intent, verification_status: VERIFICATION_STATUS.NOT_APPLICABLE };
  }
  if (intent.execution_status !== EXECUTION_STATUS.EXECUTED) {
    return { ...intent, verification_status: VERIFICATION_STATUS.VERIFICATION_FAILED };
  }

  const actionType = intent.action_type;
  if (actionType === ACTION_TYPE.ASK_AO_STATUS || actionType === ACTION_TYPE.CREATE_AO_TASK) {
    const taskId = intent.output_payload?.task_id || intent.action_payload?.task?.id;
    const task = taskId ? await decisionStore.findAoTask(taskId) : null;
    if (!task) {
      intent.verification_status = VERIFICATION_STATUS.VERIFICATION_FAILED;
      intent.execution_status = EXECUTION_STATUS.ACTION_VERIFICATION_FAILED;
      return intent;
    }
    const ok = task.owner_id === snapshot.account?.owner_id
      && task.prospect_id === snapshot.account?.id
      && task.status === 'open';
    if (!ok || options.simulateVerificationFailure) {
      intent.verification_status = VERIFICATION_STATUS.VERIFICATION_FAILED;
      intent.execution_status = EXECUTION_STATUS.ACTION_VERIFICATION_FAILED;
      return intent;
    }
    intent.verification_status = VERIFICATION_STATUS.VERIFIED;
    return intent;
  }

  if (actionType === ACTION_TYPE.RESOLVE_EXPECTATION) {
    const expId = intent.output_payload?.expectation_id;
    const scope = expectationScope(intent.client_id, snapshot);
    let exp = null;
    try {
      if (scope && expId === snapshot.expectation.id && typeof stateStore?.readExpectation === 'function') {
        exp = await stateStore.readExpectation(expId, scope);
      }
    } catch {
      // A failed durable read is a failed verification, never evidence of success.
    }
    if (!matchesExpectation(exp, expId, scope) || exp.status !== 'RESOLVED'
      || !exp.resolved_at || options.simulateVerificationFailure) {
      intent.verification_status = VERIFICATION_STATUS.VERIFICATION_FAILED;
      intent.execution_status = EXECUTION_STATUS.ACTION_VERIFICATION_FAILED;
      intent.error_code = 'EXPECTATION_READBACK_FAILED';
      return intent;
    }
    intent.verification_status = VERIFICATION_STATUS.VERIFIED;
    return intent;
  }

  intent.verification_status = VERIFICATION_STATUS.VERIFIED;
  return intent;
}

module.exports = {
  verifyActionIntent,
};
