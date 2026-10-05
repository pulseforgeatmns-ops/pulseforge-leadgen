'use strict';

const crypto = require('node:crypto');

function newAttentionId() {
  return `att_${crypto.randomBytes(12).toString('hex')}`;
}

function newSchedulerRunId() {
  return `att_run_${crypto.randomBytes(10).toString('hex')}`;
}

function attentionDedupFingerprint({ clientId, subjectType, subjectId, reasonKind, anchorId = null }) {
  const base = [clientId, subjectType, subjectId, reasonKind, anchorId || ''].join('|');
  return crypto.createHash('sha256').update(base).digest('hex').slice(0, 40);
}

function reasonKindFromDecision(decision) {
  const action = decision?.selected_action?.action_type || 'unknown';
  const subject = decision?.subject_id || 'unknown';
  if (action === 'NO_ACTION' && decision?.reevaluate_after) {
    return `waiting_window:${subject}`;
  }
  if (['ASK_AO_STATUS', 'CREATE_AO_TASK'].includes(action)) {
    return `ao_follow_up:${subject}`;
  }
  if (action === 'ESCALATE_OPERATOR') {
    return `operator_escalation:${subject}`;
  }
  return `decision:${decision?.id || subject}:${action}`;
}

module.exports = {
  newAttentionId,
  newSchedulerRunId,
  attentionDedupFingerprint,
  reasonKindFromDecision,
};
