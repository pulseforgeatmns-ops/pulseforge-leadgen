'use strict';

const crypto = require('node:crypto');
const { DECISION_TRIGGER, SUBJECT_TYPE } = require('../decisionExecution/types');
const { evaluateOperationalDecision } = require('../decisionExecution/pipeline');
const { syncAttentionFromDecision } = require('./syncFromDecision');
const { newSchedulerRunId } = require('./fingerprints');
const { ATTENTION_STATUS, UNRESOLVED_STATUSES } = require('./types');
const { withClaimedStores } = require('./claimExecution');
const { claimLost } = require('./claimPolicy');
const { isManualReviewOnlyAttention } = require('./manualReviewOnly');

function buildTriggerForItem(item) {
  if (item.subject_type === SUBJECT_TYPE.EXPECTATION || item.subject_type === 'expectation') {
    const overdue = item.status === ATTENTION_STATUS.OVERDUE;
    return {
      type: overdue ? DECISION_TRIGGER.EXPECTATION_OVERDUE : DECISION_TRIGGER.SCHEDULED_REVIEW,
      payload: {
        expectation_id: item.subject_id,
        attention_id: item.id,
      },
    };
  }
  return {
    type: DECISION_TRIGGER.SCHEDULED_REVIEW,
    payload: {
      subject_type: item.subject_type,
      subject_id: item.subject_id,
      attention_id: item.id,
    },
  };
}

async function evaluateOwnedAttentionItem({
  item,
  claimToken,
  clientId,
  stateStore,
  decisionStore,
  attentionStore,
  now,
}) {
  // Re-read under the ownership lock. Ingestion may have resolved or rescheduled
  // this row after the initial batch claim, without changing its token.
  if (isManualReviewOnlyAttention(item) || !UNRESOLVED_STATUSES.includes(item.status) || !item.next_review_at
    || new Date(item.next_review_at) > now) {
    if (!await attentionStore.releaseClaim(item.id, { claimToken })) throw claimLost();
    return { skipped: true };
  }
  let expectation = null;
  let prospect = null;
  if (item.subject_type === SUBJECT_TYPE.EXPECTATION || item.subject_type === 'expectation') {
    expectation = (stateStore.expectations || []).find(e => e.id === item.subject_id)
      || (await stateStore.readExpectation?.(item.subject_id));
    if (expectation?.prospect_id) {
      prospect = await stateStore.readProspect(expectation.prospect_id);
    }
  } else if (item.subject_type === SUBJECT_TYPE.PROSPECT || item.subject_type === 'prospect') {
    prospect = await stateStore.readProspect(item.subject_id);
    expectation = (stateStore.expectations || []).find(e =>
      e.prospect_id === item.subject_id && ['OPEN', 'WAITING', 'OVERDUE'].includes(e.status)
    );
  }

  const trigger = buildTriggerForItem(item);
  const result = await evaluateOperationalDecision({
    clientId,
    trigger,
    stateStore,
    decisionStore,
    expectation,
    prospect,
    now,
  });

  // Unavailable canonical context is retryable. Do not consume the review or
  // erase its due time because uncertainty was truthfully recorded as abstention.
  if (result.snapshot?.context_status === 'unavailable'
    || result.snapshot?.activity_read_status === 'unavailable') {
    throw Object.assign(new Error('Attention canonical context is unavailable'), {
      code: 'ATTENTION_CONTEXT_UNAVAILABLE',
    });
  }

  // Old processes could have persisted a decision before creating its task. Never
  // turn that ambiguous replay into success or re-execute the consequential action.
  if (['PENDING', 'AUTHORIZED', 'EXECUTING', 'EXECUTION_FAILURE', 'ACTION_VERIFICATION_FAILED']
    .includes(result.decision?.execution_status)) {
    throw Object.assign(new Error('Attention decision has no completed, verified execution'), {
      code: 'ATTENTION_EXECUTION_INCOMPLETE',
    });
  }
  if (['ASK_AO_STATUS', 'CREATE_AO_TASK'].includes(result.decision?.selected_action?.action_type)
    && result.decision.execution_status !== 'SUPERSEDED') {
    const intent = result.intent || await decisionStore.findActionIntentByIdempotency?.(
      clientId, result.decision.idempotency_key);
    const taskId = intent?.output_payload?.task_id || intent?.action_payload?.task?.id;
    const task = taskId && await decisionStore.findAoTask(taskId);
    // Some duplicate paths omit snapshot. Always bind the task to the current
    // canonical subject loaded under this fence, never a historical receipt.
    const prospectId = prospect?.id || null;
    const ownerId = prospect?.assigned_ao_id || expectation?.ao_id || null;
    if (!intent || intent.client_id !== clientId || intent.decision_id !== result.decision.id
      || intent.action_type !== result.decision.selected_action.action_type
      || intent.execution_status !== 'EXECUTED' || intent.verification_status !== 'VERIFIED'
      || !task || task.client_id !== clientId || task.decision_id !== result.decision.id
      || task.status !== 'open' || task.prospect_id !== prospectId || task.owner_id !== ownerId) {
      throw Object.assign(new Error('Attention AO replay lacks a verified durable task'), {
        code: 'ATTENTION_EXECUTION_INCOMPLETE',
      });
    }
  }

  const synced = await syncAttentionFromDecision({
    attentionStore,
    decision: result.decision,
    snapshot: result.snapshot,
    now,
    parentAttentionId: item.parent_attention_id,
  });

  const resolved = synced && ['RESOLVED', 'SUPERSEDED'].includes(synced.status);

  const released = await attentionStore.releaseClaim(item.id, {
    claimToken,
    patch: {
      status: resolved
        ? synced.status
        : (result.decision?.reevaluate_after ? ATTENTION_STATUS.WAITING : synced?.status || item.status),
      next_review_at: resolved ? null : (result.decision?.reevaluate_after || null),
      last_reviewed_at: now.toISOString(),
      last_evaluation_id: result.decision?.id,
      resolved_at: resolved ? now.toISOString() : null,
      resolution_evidence: resolved ? synced.resolution_evidence : undefined,
    },
  });

  if (!released) throw claimLost();
  return result;
}

async function evaluateAttentionItem(options) {
  return withClaimedStores(options, scope => evaluateOwnedAttentionItem({ ...options, ...scope }));
}

async function runAttentionCycle({
  clientId,
  stateStore,
  decisionStore,
  attentionStore,
  now = new Date(),
  limit = 20,
  retryDelayMs = 15 * 60 * 1000,
}) {
  const runId = newSchedulerRunId();
  const startedAt = now.toISOString();
  const claimToken = crypto.randomBytes(16).toString('hex');
  const telemetry = {
    duplicate_evaluations: 0,
    wake_only: 0,
  };

  await attentionStore.recordSchedulerRun({
    id: runId,
    client_id: clientId,
    started_at: startedAt,
    status: 'running',
    items_claimed: 0,
    items_evaluated: 0,
    items_failed: 0,
    telemetry,
  });

  let claimed = [];
  let evaluated = 0;
  let failed = 0;

  try {
    claimed = await attentionStore.claimDueItems({
      clientId,
      now,
      limit,
      claimToken,
    });

    for (const item of claimed) {
      try {
        const result = await evaluateAttentionItem({
          item,
          claimToken,
          clientId,
          stateStore,
          decisionStore,
          attentionStore,
          now,
        });
        if (result.skipped) {
          telemetry.skipped = (telemetry.skipped || 0) + 1;
          continue;
        }
        if (result.duplicate) telemetry.duplicate_evaluations += 1;
        evaluated += 1;
      } catch (err) {
        failed += 1;
        const retryAt = new Date(now.getTime() + retryDelayMs).toISOString();
        telemetry[`error_${item.id}`] = err.message;
        try {
          // The evaluation transaction has rolled back before this retry release.
          // Expired/replaced ownership must never modify a newer worker's item.
          const released = await attentionStore.releaseClaim(item.id, {
            claimToken,
            patch: { next_review_at: retryAt, execution_failures_increment: true },
          });
          if (!released) telemetry[`claim_lost_${item.id}`] = true;
        } catch (releaseError) {
          telemetry[`release_error_${item.id}`] = releaseError.message;
        }
      }
    }

    const completedAt = new Date().toISOString();
    await attentionStore.finishSchedulerRun({
      id: runId,
      client_id: clientId,
      started_at: startedAt,
      completed_at: completedAt,
      status: failed ? 'failed' : 'completed',
      items_claimed: claimed.length,
      items_evaluated: evaluated,
      items_failed: failed,
      telemetry,
    });

    return {
      run_id: runId,
      claimed: claimed.length,
      evaluated,
      failed,
      telemetry,
    };
  } catch (err) {
    await attentionStore.finishSchedulerRun({
      id: runId,
      client_id: clientId,
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      status: 'failed',
      items_claimed: claimed.length,
      items_evaluated: evaluated,
      items_failed: failed + 1,
      error_message: err.message,
      telemetry,
    });
    throw err;
  }
}

module.exports = {
  runAttentionCycle,
  buildTriggerForItem,
  evaluateAttentionItem,
};
