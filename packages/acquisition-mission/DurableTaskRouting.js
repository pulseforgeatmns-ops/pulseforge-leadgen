'use strict';

/**
 * MAX-P0-004 P2 — durable task ownership for one Canonical Execution Request.
 *
 * Opt-in: active only when the router context carries `durableTasks` (a store
 * from services/maxReliability/durableExecutionOwnership). Production callers
 * pass it only with MAX_DURABLE_TASKS=true and the P0-004 schema installed
 * (resolveDurableTasks), so default behaviour is unchanged.
 *
 * Lifecycle, all inline (no worker, no background claimant):
 *   accept (durable before any specialist code; replay of the same logical key
 *   returns the recorded task and never dispatches again)
 *   → claimTask (token + fence epoch + DB-clock lease; one active task per mission)
 *   → start → dispatch inside runWithTaskFence (every leased mission commit and
 *     task-linked outbound marker re-checks the fence in PostgreSQL)
 *   → finish: verified execution evidence → completed; failed/blocked → failed;
 *     anything unverified → awaiting_verification (never reported as done).
 * A heartbeat renews the lease while the request runs; it is cleared when the
 * request ends. If the process dies, the lease expires and operator recovery
 * (scripts/max-reliability/recover.cjs) decides; nothing is replayed.
 */
const { randomUUID } = require('crypto');
const { requestFingerprint } = require('../../services/maxReliability/externalActionLedger');
const { runWithTaskFence } = require('../../services/maxReliability/taskExecutionContext');
const { EXECUTION_STATES } = require('./ExecutionEvidence');

const DEFAULT_LEASE_MS = 60000;
const DEFAULT_DEADLINE_MS = 20 * 60 * 1000;
const COMPLETED_EVIDENCE_STATES = new Set([
  EXECUTION_STATES.COMMITTED_INTERNAL,
  EXECUTION_STATES.COMPLETED_VERIFIED,
  EXECUTION_STATES.ALREADY_EXECUTED,
]);
const FAILED_EVIDENCE_STATES = new Set([EXECUTION_STATES.FAILED, EXECUTION_STATES.BLOCKED]);

function taskError(code, message, extra = {}) {
  return Object.assign(new Error(message), { code, spec: 'MAX-P0-004', ...extra });
}

/** Compact, non-secret view of a task for responses and receipts. */
function summarizeTask(task) {
  if (!task) return null;
  return {
    id: task.id,
    state: task.state,
    logicalKey: task.logicalKey || task.logical_key || null,
    fenceEpoch: task.fenceEpoch,
    missionId: task.missionId || null,
    specialist: task.specialist || null,
    recoveryReason: task.recoveryReason || null,
    escalationReason: task.escalationReason || null,
    cancellationReason: task.cancellationReason || null,
    deadlineAt: task.deadlineAt || null,
  };
}

/**
 * The production resolver: a durable task store only when explicitly enabled.
 * Never enabled implicitly; never enabled without a pool.
 */
function resolveDurableTasks(opts = {}, env = process.env) {
  if (opts.durableTasks) return opts.durableTasks;
  if (String(env.MAX_DURABLE_TASKS || '').toLowerCase() !== 'true') return null;
  if (!opts.pool) return null;
  const { createDurableExecutionOwnership } = require('../../services/maxReliability/durableExecutionOwnership');
  return createDurableExecutionOwnership(opts.pool);
}

function evidenceRefsFor(request, execution) {
  if (!execution) return [];
  const ref = {
    kind: 'execution_evidence',
    requestId: request.id,
    state: execution.state || null,
    reason: execution.reason || null,
    level: execution.level || null,
    transactionId: execution.transactionId
      || (execution.evidence && execution.evidence.transactionId) || null,
  };
  return [ref];
}

/**
 * Begin durable ownership of a request. Returns null when tasks are not
 * enabled for this call, `{ replay }` when the logical key was already
 * accepted and is no longer queued, or a run handle.
 */
async function beginDurableTask(request, context, mission, { specialist } = {}) {
  const store = context && context.durableTasks;
  if (!store) return null;
  const tenantId = String(context.tenantId);
  const leaseMs = Number(context.durableTaskLeaseMs) || DEFAULT_LEASE_MS;
  const deadlineMs = Number(context.durableTaskDeadlineMs) || DEFAULT_DEADLINE_MS;
  const logicalKey = context.durableTaskKey || `cer:${request.id}`;
  // A replay of an already accepted key never dispatches again, and a queued
  // one (accepted, never claimed) is resumed with its recorded deadline.
  const existing = await store.getByLogicalKey({ tenantId, logicalKey });
  if (existing && existing.state !== 'queued') return { replay: summarizeTask(existing) };
  const accepted = await store.accept({
    tenantId,
    logicalKey,
    specialist: specialist || 'operator',
    missionId: mission.id,
    objectiveId: mission.objectiveId || null,
    executionScope: { intent: request.intent, stage: request.stage || null, source: request.source || null },
    // A receipt-keyed request supplies its stable request fingerprint; an
    // unkeyed one is identified by its own CER id.
    immutableInput: context.durableTaskInput || {
      requestId: request.id,
      intent: request.intent,
      stage: request.stage || null,
      payloadFingerprint: requestFingerprint(request.payload || {}),
    },
    deadlineAt: existing ? existing.deadlineAt : new Date(Date.now() + deadlineMs).toISOString(),
    retryBudget: 0,
  }).catch((err) => {
    if (err && err.code === 'durable_task_key_reused') {
      throw taskError('durable_task_key_reused', 'This request key was already accepted for a different execution.', { statusCode: 422 });
    }
    throw err;
  });
  let task = accepted.task;
  if (accepted.replay && task.state !== 'queued') {
    return { replay: summarizeTask(task) };
  }
  // Link the request receipt (P4) before any specialist runs, so an
  // interrupted request always points at its task. A failed link stops here.
  if (typeof context.onDurableTaskAccepted === 'function') {
    try {
      await context.onDurableTaskAccepted(summarizeTask(task));
    } catch (err) {
      await store.cancelIfQueued({ tenantId, taskId: task.id, reason: 'receipt_link_failed' }).catch(() => {});
      throw err;
    }
  }

  const ownershipToken = `cer-owner:${randomUUID()}`;
  const claimed = await store.claimTask({ tenantId, taskId: task.id, ownerToken: ownershipToken, leaseMs });
  if (!claimed) {
    // Another task of this mission is active (or the deadline passed). Do not
    // leave this one queued for a later claimant: record why it never ran.
    // Atomic: cancels only while still queued, so a caller that lost a race
    // can never cancel the winner's claimed or running task.
    await store.cancelIfQueued({ tenantId, taskId: task.id, reason: 'mission_has_active_task' }).catch(() => {});
    throw taskError('durable_task_mission_busy', 'Another execution of this mission is in progress; nothing ran.', {
      statusCode: 409, missionId: mission.id, taskId: task.id,
    });
  }
  const owner = { tenantId, taskId: claimed.id, ownershipToken, fenceEpoch: claimed.fenceEpoch };
  task = await store.start(owner);

  let heartbeatLost = null;
  const timer = setInterval(() => {
    store.heartbeat({ ...owner, leaseMs }).catch((err) => { heartbeatLost = err; });
  }, Math.max(1000, Math.floor(leaseMs / 3)));
  if (typeof timer.unref === 'function') timer.unref();
  const stop = () => clearInterval(timer);

  async function current() {
    return summarizeTask(await store.get({ tenantId, taskId: owner.taskId }));
  }

  return {
    owner,
    get heartbeatLost() { return heartbeatLost; },
    run(fn) { return runWithTaskFence(owner, fn); },
    /** Record the outcome from execution evidence. Never throws: the stage outcome already happened. */
    async finish(execution) {
      stop();
      try {
        const state = execution && execution.state;
        const evidenceRefs = evidenceRefsFor(request, execution);
        if (FAILED_EVIDENCE_STATES.has(state)) {
          return summarizeTask(await store.fail({ ...owner, reason: `${state}:${(execution && execution.reason) || 'unknown'}`,
            resultRef: { executionState: state } }));
        }
        const reported = await store.reportCompletion({ ...owner, resultRef: { executionState: state || null }, evidenceRefs });
        if (reported.state === 'awaiting_verification' && COMPLETED_EVIDENCE_STATES.has(state)) {
          return summarizeTask(await store.verify({ tenantId, taskId: owner.taskId, evidenceRefs }));
        }
        return summarizeTask(reported);
      } catch (_) {
        // Ownership was lost (recovered, cancelled, past deadline): report the
        // durable state as it is now, which is what an operator must act on.
        return current().catch(() => null);
      }
    },
    /** The request threw before producing evidence. */
    async failed(err) {
      stop();
      try {
        return summarizeTask(await store.fail({ ...owner, reason: String((err && err.code) || 'execution_error').slice(0, 120) }));
      } catch (_) {
        return current().catch(() => null);
      }
    },
  };
}

/**
 * MAX-P0-004 criterion 1 — a stable run key for an operator script. Derived only
 * from the script and the logical inputs of the operation (mission, intent,
 * the prepared-artifact revision read before the call, prospect, limits), never
 * from a run id or clock: a retry of the same logical execution (e.g. after a
 * crash, which TME rolls back so the inputs are unchanged) yields the same key
 * and replays its task instead of running again.
 */
function operatorRunKey(script, parts = {}) {
  const name = String(script || '').trim();
  if (!/^[A-Za-z0-9_.-]{1,80}$/.test(name)) throw taskError('durable_task_invalid', 'operatorRunKey requires a script name.');
  return `script:${name}:${requestFingerprint(parts).slice(0, 40)}`;
}

/** Stable run-key parts for a mission operation: mission, intent, prepared revision. */
function missionOperationParts(engine, missionId, intent, extra = {}) {
  const { computePreparedArtifactRevision } = require('./index');
  const contributions = engine && engine.store && typeof engine.store.listContributions === 'function'
    ? engine.store.listContributions(missionId) : [];
  return { missionId: String(missionId), intent: String(intent), preparedArtifactRevision: computePreparedArtifactRevision(missionId, contributions) || null, ...extra };
}

/**
 * MAX-P0-004 criterion 1 — run one operator or system mutation that is not a
 * routed specialist request (an authorization amendment, outcome-learning
 * evaluation, governed envelope completion, an operator repair) as task-owned
 * work. Same lifecycle as the router: stable logical key (a retry of the same
 * logical operation replays, never re-runs), one active task per mission, the
 * fence held while fn runs, completion recorded only from fn's success.
 *
 * Without a store (durable tasks disabled) fn runs exactly as before.
 * @returns {Promise<{ result, durableTask, replay?: boolean }>}
 */
async function runTaskOwnedWork({ store, tenantId, missionId, logicalKey, specialist, immutableInput = {}, leaseMs, deadlineMs }, fn) {
  if (!store) return { result: await fn(), durableTask: null };
  // Already inside a task-owned execution of the same tenant: the work is part
  // of it (its writes are fenced by that owner). A second task for the same
  // mission could never be claimed alongside the first.
  const inherited = require('../../services/maxReliability/taskExecutionContext').currentTaskFence();
  if (inherited && inherited.tenantId === String(tenantId)) return { result: await fn(), durableTask: null, inherited: true };
  const tenant = String(tenantId);
  if (!logicalKey) throw taskError('durable_task_invalid', 'Task-owned work requires a stable logical key.');
  const existing = await store.getByLogicalKey({ tenantId: tenant, logicalKey });
  if (existing && existing.state !== 'queued') return { result: null, durableTask: summarizeTask(existing), replay: true };
  const accepted = await store.accept({
    tenantId: tenant, logicalKey, specialist: specialist || 'operator', missionId: missionId || null,
    immutableInput, retryBudget: 0,
    deadlineAt: existing ? existing.deadlineAt : new Date(Date.now() + (Number(deadlineMs) || DEFAULT_DEADLINE_MS)).toISOString(),
  });
  const ownershipToken = `work-owner:${randomUUID()}`;
  const lease = Number(leaseMs) || DEFAULT_LEASE_MS;
  const claimed = await store.claimTask({ tenantId: tenant, taskId: accepted.task.id, ownerToken: ownershipToken, leaseMs: lease });
  if (!claimed) {
    await store.cancelIfQueued({ tenantId: tenant, taskId: accepted.task.id, reason: 'mission_has_active_task' }).catch(() => {});
    throw taskError('durable_task_mission_busy', 'Another execution of this mission is in progress; nothing ran.', { statusCode: 409, missionId, taskId: accepted.task.id });
  }
  const owner = { tenantId: tenant, taskId: claimed.id, ownershipToken, fenceEpoch: claimed.fenceEpoch };
  await store.start(owner);
  const timer = setInterval(() => { store.heartbeat({ ...owner, leaseMs: lease }).catch(() => {}); }, Math.max(1000, Math.floor(lease / 3)));
  if (typeof timer.unref === 'function') timer.unref();
  let result;
  try {
    result = await runWithTaskFence(owner, fn);
  } catch (err) {
    clearInterval(timer);
    err.durableTask = await store.fail({ ...owner, reason: String((err && err.code) || 'work_error').slice(0, 120) })
      .then(summarizeTask, () => store.get({ tenantId: tenant, taskId: owner.taskId }).then(summarizeTask, () => null));
    throw err;
  }
  clearInterval(timer);
  const evidenceRefs = [{ kind: 'task_owned_work', logicalKey, state: EXECUTION_STATES.COMMITTED_INTERNAL }];
  try {
    const reported = await store.reportCompletion({ ...owner, resultRef: { executionState: EXECUTION_STATES.COMMITTED_INTERNAL }, evidenceRefs });
    const done = reported.state === 'awaiting_verification' ? await store.verify({ tenantId: tenant, taskId: owner.taskId, evidenceRefs }) : reported;
    return { result, durableTask: summarizeTask(done) };
  } catch (_) {
    return { result, durableTask: await store.get({ tenantId: tenant, taskId: owner.taskId }).then(summarizeTask, () => null) };
  }
}

/**
 * Read-only, tenant-scoped task summary for receipts and status views. Returns
 * null when durable tasks are not enabled or the schema is not installed.
 */
async function loadTaskSummary(pool, tenantId, taskId, env = process.env) {
  if (String(env.MAX_DURABLE_TASKS || '').toLowerCase() !== 'true' || !pool || !taskId) return null;
  const { createDurableExecutionOwnership } = require('../../services/maxReliability/durableExecutionOwnership');
  try {
    return summarizeTask(await createDurableExecutionOwnership(pool).get({ tenantId: String(tenantId), taskId: String(taskId) }));
  } catch (_) {
    return null;
  }
}

module.exports = {
  beginDurableTask,
  runTaskOwnedWork,
  operatorRunKey,
  missionOperationParts,
  loadTaskSummary,
  resolveDurableTasks,
  summarizeTask,
  DEFAULT_LEASE_MS,
  DEFAULT_DEADLINE_MS,
};
