'use strict';

/**
 * MAX-P0-004 P6 — operator-facing status derived ONLY from the authoritative
 * durable task row (services/maxReliability/durableExecutionOwnership).
 *
 * Honesty rules (approved acceptance criterion 2):
 *   - only `completed` (verified with evidence) is reported as done;
 *   - requested/queued/claimed/running are never "done";
 *   - awaiting_verification is "finished by the specialist, not verified";
 *   - escalated and recovery_required always say what the operator must do;
 *   - an unknown state is reported as unknown, never as success.
 * Pure: no I/O, no clock, no mission or ledger reads.
 */

const REASON_TEXT = Object.freeze({
  external_action_unresolved: 'an external action may have happened and its outcome must be reconciled before anything else runs',
  deadline_exceeded: 'it passed its deadline before finishing',
  retry_budget_exhausted: 'it was interrupted more times than its retry budget allows',
  cancelled_after_dispatch: 'it was cancelled after an external action may already have been sent',
  failed_after_dispatch: 'it failed after an external action had already reached the provider; that effect is not reflected in the mission',
  lease_expired: 'its owner stopped renewing its lease (the process stopped or stalled)',
  dependency_drift: 'something it depends on changed since it was accepted',
  mission_has_active_task: 'another execution of this mission was already in progress',
  receipt_link_failed: 'its request receipt could not be linked',
});

function reasonText(code) {
  if (!code) return null;
  return REASON_TEXT[code] || String(code).replace(/_/g, ' ');
}

const STATE_VIEW = Object.freeze({
  requested: { label: 'Requested — not started', done: false, needsOperator: false },
  queued: { label: 'Queued — not started', done: false, needsOperator: false },
  claimed: { label: 'Starting', done: false, needsOperator: false },
  running: { label: 'In progress', done: false, needsOperator: false },
  awaiting_verification: { label: 'Finished by the specialist — not yet verified', done: false, needsOperator: false },
  completed: { label: 'Done — verified by evidence', done: true, needsOperator: false },
  failed: { label: 'Failed', done: false, needsOperator: false },
  cancelled: { label: 'Cancelled', done: false, needsOperator: false },
  recovery_required: { label: 'Interrupted — needs operator revalidation', done: false, needsOperator: true },
  blocked_dependency: { label: 'Blocked — a dependency changed', done: false, needsOperator: true },
  escalated: { label: 'Needs you', done: false, needsOperator: true },
});

/**
 * @param {object|null} task summary or row from the task store
 * @returns {object|null} { taskId, state, label, done, verified, needsOperator, reason, sentence }
 */
function presentDurableTaskStatus(task) {
  if (!task || typeof task !== 'object') return null;
  const state = String(task.state || '');
  const view = STATE_VIEW[state] || { label: 'Unknown state — not confirmed', done: false, needsOperator: true };
  const reasonCode = task.escalationReason || task.escalation_reason
    || task.recoveryReason || task.recovery_reason
    || task.cancellationReason || task.cancellation_reason || null;
  const reason = state === 'completed' ? null : reasonText(reasonCode);
  let sentence;
  if (state === 'completed') sentence = 'Durable task: done and verified by recorded evidence.';
  else if (state === 'escalated') sentence = `Durable task needs you: ${reason || 'it was escalated for operator review'}. It will not be retried automatically.`;
  else if (state === 'recovery_required') sentence = `Durable task interrupted: ${reason || 'its owner stopped'}. It will not be retried until an operator revalidates it.`;
  else if (state === 'awaiting_verification') sentence = 'Durable task: the specialist finished, but completion is not verified. Do not treat it as done.';
  else if (state === 'failed') sentence = `Durable task failed${reason ? `: ${reason}` : ''}. Nothing is in doubt.`;
  else if (state === 'cancelled') sentence = `Durable task cancelled${reason ? `: ${reason}` : ''}.`;
  else if (state === 'blocked_dependency') sentence = `Durable task blocked: ${reason || 'a dependency changed'}.`;
  else if (STATE_VIEW[state]) sentence = `Durable task: ${view.label.toLowerCase()}. Not done yet.`;
  else sentence = 'Durable task state is unknown; nothing is confirmed.';
  return {
    taskId: task.id || null,
    state: state || null,
    label: view.label,
    done: view.done,
    verified: state === 'completed',
    needsOperator: view.needsOperator,
    reason,
    sentence,
  };
}

/**
 * Apply task status to a Max response. Only adds information: metadata always,
 * and a visible notice whenever the task is not verifiably done, so a turn can
 * never imply completion the durable record does not support.
 */
function applyDurableTaskStatus(response, task) {
  const status = presentDurableTaskStatus(task);
  if (!response || !status) return response;
  const structured = response.structured || {};
  structured.metadata = { ...(structured.metadata || {}), durableTask: status };
  if (!status.done) {
    structured.answer = [structured.answer, status.sentence].filter(Boolean).join('\n\n');
    response.prose = [response.prose, status.sentence].filter(Boolean).join('\n\n');
  }
  response.structured = structured;
  return response;
}

module.exports = { presentDurableTaskStatus, applyDurableTaskStatus, REASON_TEXT };
