'use strict';

const { AUTHORITY_CLASS, ACTION_TYPE, EXECUTION_STATUS } = require('./types');

const ACTION_AUTHORITY = Object.freeze({
  [ACTION_TYPE.NO_ACTION]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.ASK_AO_STATUS]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.CREATE_AO_TASK]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.DELEGATE_AGENT]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.SEEK_CLARIFICATION]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.EXECUTE_INTERNAL_UPDATE]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.RESOLVE_EXPECTATION]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.SUPERSEDE_PRIOR]: AUTHORITY_CLASS.A,
  [ACTION_TYPE.RETURN_TO_COLD_OUTREACH]: AUTHORITY_CLASS.B,
  [ACTION_TYPE.ESCALATE_OPERATOR]: AUTHORITY_CLASS.C,
});

function authorityForCandidate(candidate, snapshot) {
  if (candidate?.prohibited || candidate?.authority_class === AUTHORITY_CLASS.D) {
    return AUTHORITY_CLASS.D;
  }
  if (candidate?.action_type === ACTION_TYPE.RETURN_TO_COLD_OUTREACH) {
    const rel = snapshot?.relationship || {};
    if (rel.suppress_cold_outreach || rel.relationship_active) return AUTHORITY_CLASS.D;
    return AUTHORITY_CLASS.B;
  }
  return ACTION_AUTHORITY[candidate?.action_type] || AUTHORITY_CLASS.C;
}

function authorizeExecution({ selected, snapshot, policy = {} }) {
  const authority_class = authorityForCandidate(selected, snapshot);
  if (authority_class === AUTHORITY_CLASS.D) {
    return {
      authorized: false,
      authority_class,
      execution_status: EXECUTION_STATUS.EXECUTION_BLOCKED,
      reason: 'Policy prohibits this action regardless of model confidence',
    };
  }
  if (authority_class === AUTHORITY_CLASS.C) {
    return {
      authorized: false,
      authority_class,
      execution_status: EXECUTION_STATUS.EXECUTION_BLOCKED,
      reason: 'Approval required before execution',
      requires_approval: true,
    };
  }
  if (authority_class === AUTHORITY_CLASS.B) {
    const rel = snapshot?.relationship || {};
    if (selected?.action_type === ACTION_TYPE.RETURN_TO_COLD_OUTREACH) {
      if (rel.suppress_cold_outreach || rel.relationship_active || policy.blockColdOutreach) {
        return {
          authorized: false,
          authority_class: AUTHORITY_CLASS.D,
          execution_status: EXECUTION_STATUS.EXECUTION_BLOCKED,
          reason: 'Cold outreach policy conditions not satisfied',
        };
      }
    }
  }
  return {
    authorized: true,
    authority_class,
    execution_status: EXECUTION_STATUS.AUTHORIZED,
  };
}

module.exports = {
  ACTION_AUTHORITY,
  authorityForCandidate,
  authorizeExecution,
};
