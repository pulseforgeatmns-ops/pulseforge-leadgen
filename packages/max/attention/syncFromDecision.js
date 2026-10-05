'use strict';

const { ATTENTION_STATUS, REVIEW_TRIGGER } = require('./types');
const { audienceTierForDecision } = require('./budget');
const {
  attentionDedupFingerprint,
  reasonKindFromDecision,
  newAttentionId,
} = require('./fingerprints');
const { ACTION_TYPE, EXECUTION_STATUS } = require('../decisionExecution/types');

function buildReason(decision, snapshot) {
  const account = snapshot?.account?.name || snapshot?.account?.company_name || decision.subject_id;
  const action = decision?.selected_action?.action_type;
  if (action === ACTION_TYPE.NO_ACTION && decision.reevaluate_after) {
    return `Waiting on expected window for ${account}`;
  }
  if (['ASK_AO_STATUS', 'CREATE_AO_TASK'].includes(action)) {
    return `Waiting on ${decision.owner || 'AO'} for ${account}`;
  }
  if (action === ACTION_TYPE.ESCALATE_OPERATOR) {
    return `Operator decision required for ${account}`;
  }
  return decision.rationale || `Attention on ${account}`;
}

function attentionStatusFromDecision(decision, now = new Date()) {
  const action = decision?.selected_action?.action_type;
  if ([ACTION_TYPE.RESOLVE_EXPECTATION, ACTION_TYPE.SUPERSEDE_PRIOR].includes(action)
    && decision.execution_status === EXECUTION_STATUS.EXECUTED) {
    return ATTENTION_STATUS.RESOLVED;
  }
  if (decision.execution_status === EXECUTION_STATUS.SUPERSEDED) {
    return ATTENTION_STATUS.SUPERSEDED;
  }
  if (action === ACTION_TYPE.NO_ACTION && decision.reevaluate_after) {
    const due = new Date(decision.reevaluate_after);
    if (due <= now) return ATTENTION_STATUS.OVERDUE;
    return ATTENTION_STATUS.WAITING;
  }
  if (['ASK_AO_STATUS', 'CREATE_AO_TASK'].includes(action)) {
    return ATTENTION_STATUS.ACTIVE;
  }
  if (action === ACTION_TYPE.ESCALATE_OPERATOR) {
    return ATTENTION_STATUS.ACTIVE;
  }
  return ATTENTION_STATUS.ACTIVE;
}

function resolutionEvidenceFromDecision(decision) {
  if (!decision?.selected_action) return [];
  const action = decision.selected_action.action_type;
  if (![ACTION_TYPE.RESOLVE_EXPECTATION, ACTION_TYPE.SUPERSEDE_PRIOR].includes(action)) {
    return [];
  }
  return [
    {
      kind: 'decision',
      decision_id: decision.id,
      action_type: action,
      at: decision.resolved_at || decision.created_at,
    },
    ...(decision.supporting_evidence || []).slice(0, 5),
  ];
}

async function syncAttentionFromDecision({
  attentionStore,
  decision,
  snapshot = null,
  now = new Date(),
  parentAttentionId = null,
}) {
  if (!decision || !attentionStore) return null;

  const reasonKind = reasonKindFromDecision(decision);
  const fingerprint = attentionDedupFingerprint({
    clientId: decision.client_id,
    subjectType: decision.subject_type,
    subjectId: decision.subject_id,
    reasonKind,
  });

  const existing = await attentionStore.findByFingerprint(decision.client_id, fingerprint);
  const status = attentionStatusFromDecision(decision, now);
  const resolved = [ATTENTION_STATUS.RESOLVED, ATTENTION_STATUS.SUPERSEDED].includes(status);

  const mergedEvidence = [
    ...(existing?.supporting_evidence || []),
    ...(decision.supporting_evidence || []),
  ].slice(-20);

  const payload = {
    client_id: decision.client_id,
    subject_type: decision.subject_type,
    subject_id: decision.subject_id,
    reason: buildReason(decision, snapshot || decision.canonical_state_snapshot),
    source_decision_id: decision.id,
    supporting_evidence: mergedEvidence,
    status,
    priority: decision.priority || {},
    next_review_at: resolved ? null : (decision.reevaluate_after || null),
    review_trigger: decision.reevaluate_after ? REVIEW_TRIGGER.TIME : REVIEW_TRIGGER.STATE,
    owner: decision.owner || null,
    audience_tier: audienceTierForDecision(decision),
    parent_attention_id: parentAttentionId,
    dedup_fingerprint: fingerprint,
    resolution_evidence: resolved ? resolutionEvidenceFromDecision(decision) : [],
    last_evaluation_id: decision.id,
    last_reviewed_at: now.toISOString(),
    resolved_at: resolved ? now.toISOString() : null,
  };

  if (existing && !resolved) {
    return attentionStore.updateAttention(existing.id, {
      ...payload,
      id: existing.id,
    });
  }
  if (existing && resolved) {
    return attentionStore.resolveAttention(existing.id, {
      status,
      resolution_evidence: payload.resolution_evidence,
      resolved_at: payload.resolved_at,
      last_evaluation_id: decision.id,
    });
  }

  return attentionStore.createAttention({
    id: newAttentionId(),
    ...payload,
    created_at: now.toISOString(),
  });
}

module.exports = {
  syncAttentionFromDecision,
  buildReason,
  attentionStatusFromDecision,
};
