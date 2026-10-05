'use strict';

const { ACTION_TYPE, VERIFICATION_STATUS, EXECUTION_STATUS } = require('./types');

async function verifyActionIntent({ decisionStore, stateStore, intent, snapshot, options = {} }) {
  if (intent.execution_status === EXECUTION_STATUS.NO_ACTION_RECORDED) {
    return { ...intent, verification_status: VERIFICATION_STATUS.NOT_APPLICABLE };
  }
  if (intent.execution_status === EXECUTION_STATUS.EXECUTION_BLOCKED) {
    return { ...intent, verification_status: VERIFICATION_STATUS.NOT_APPLICABLE };
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
    let exp = null;
    if (expId && stateStore?.expectations) {
      exp = stateStore.expectations.find(e => e.id === expId);
    }
    if (exp && exp.status !== 'RESOLVED' && !options.skipExpectationRead) {
      intent.verification_status = VERIFICATION_STATUS.VERIFICATION_FAILED;
      intent.execution_status = EXECUTION_STATUS.ACTION_VERIFICATION_FAILED;
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
