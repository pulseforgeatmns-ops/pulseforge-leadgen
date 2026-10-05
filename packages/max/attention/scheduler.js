'use strict';

const crypto = require('node:crypto');
const { DECISION_TRIGGER, SUBJECT_TYPE } = require('../decisionExecution/types');
const { evaluateOperationalDecision } = require('../decisionExecution/pipeline');
const { syncAttentionFromDecision } = require('./syncFromDecision');
const { newSchedulerRunId } = require('./fingerprints');
const { ATTENTION_STATUS } = require('./types');

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

async function evaluateAttentionItem({
  item,
  claimToken,
  clientId,
  stateStore,
  decisionStore,
  attentionStore,
  now,
}) {
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

  const synced = await syncAttentionFromDecision({
    attentionStore,
    decision: result.decision,
    snapshot: result.snapshot,
    now,
    parentAttentionId: item.parent_attention_id,
  });

  const resolved = synced && ['RESOLVED', 'SUPERSEDED'].includes(synced.status);

  await attentionStore.releaseClaim(item.id, {
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

  return result;
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
        if (result.duplicate) telemetry.duplicate_evaluations += 1;
        evaluated += 1;
      } catch (err) {
        failed += 1;
        const retryAt = new Date(now.getTime() + retryDelayMs).toISOString();
        await attentionStore.releaseClaim(item.id, {
          claimToken,
          patch: {
            next_review_at: retryAt,
            execution_failures_increment: true,
          },
        });
        telemetry[`error_${item.id}`] = err.message;
      }
    }

    const completedAt = new Date().toISOString();
    await attentionStore.recordSchedulerRun({
      id: runId,
      client_id: clientId,
      started_at: startedAt,
      completed_at: completedAt,
      status: failed && !evaluated ? 'failed' : 'completed',
      items_claimed: claimed.length,
      items_evaluated: evaluated,
      items_failed: failed,
      telemetry,
    });

    await attentionStore.updateHeartbeat({
      last_successful_cycle_at: failed === claimed.length ? undefined : completedAt,
      last_run_id: runId,
      consecutive_failures: failed === claimed.length && claimed.length > 0
        ? ((await attentionStore.getHeartbeat())?.consecutive_failures || 0) + 1
        : 0,
    });

    return {
      run_id: runId,
      claimed: claimed.length,
      evaluated,
      failed,
      telemetry,
    };
  } catch (err) {
    await attentionStore.recordSchedulerRun({
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
    const hb = await attentionStore.getHeartbeat();
    await attentionStore.updateHeartbeat({
      consecutive_failures: (hb?.consecutive_failures || 0) + 1,
      last_run_id: runId,
    });
    throw err;
  }
}

module.exports = {
  runAttentionCycle,
  buildTriggerForItem,
  evaluateAttentionItem,
};
