'use strict';

/**
 * MAX-P0-004 P2/P3: the durable task fence of the execution currently in
 * progress.
 *
 * The Canonical Execution Router owns the task: it claims it, then runs the
 * specialist dispatch inside runWithTaskFence(). Every leased mission commit
 * (services/acquisitionMissionPersistence.persistStageCommit) and every
 * task-linked outbound claim (OutboundExecutionAdapter) reads the fence from
 * here instead of each handler threading it through. Outside a task-owned
 * execution there is no fence and behaviour is unchanged.
 *
 * The fence is a capability to *attempt* a write, never an authority: the
 * write is accepted only if PostgreSQL confirms, in the same transaction, that
 * the token and epoch are still current (assertTaskFenceOnClient).
 */
const { AsyncLocalStorage } = require('async_hooks');

const storage = new AsyncLocalStorage();

/** Fail-closed error for any write attempted under a stale or missing owner. */
function fencedError(message, extra = {}) {
  return Object.assign(new Error(message), { code: 'durable_task_fenced', spec: 'MAX-P0-004', ...extra });
}

function normalizeFence(fence) {
  if (!fence || typeof fence !== 'object') return null;
  const taskId = fence.taskId == null ? '' : String(fence.taskId);
  const tenantId = fence.tenantId == null ? '' : String(fence.tenantId);
  const ownershipToken = fence.ownershipToken == null ? '' : String(fence.ownershipToken);
  const fenceEpoch = Number(fence.fenceEpoch);
  if (!taskId || !tenantId || !ownershipToken || !Number.isSafeInteger(fenceEpoch) || fenceEpoch < 1) {
    throw fencedError('A task fence requires tenantId, taskId, ownershipToken and a positive fenceEpoch.');
  }
  return Object.freeze({ taskId, tenantId, ownershipToken, fenceEpoch });
}

/** Run fn with the given task fence as the current execution's fence. */
function runWithTaskFence(fence, fn) {
  return storage.run({ fence: normalizeFence(fence) }, fn);
}

/** The current task fence, or null outside a task-owned execution. */
function currentTaskFence() {
  const store = storage.getStore();
  return store && store.fence ? store.fence : null;
}

/**
 * Re-check the fence on a client inside an open transaction. The row is locked
 * FOR SHARE until that transaction ends, so a concurrent cancellation or
 * recovery (FOR UPDATE) either waits for this commit or makes it fail here.
 */
async function assertTaskFenceOnClient(client, fence) {
  if (!fence) return;
  if (!client || typeof client.query !== 'function') throw fencedError('A task-owned write requires a transaction client.');
  const r = await client.query(
    `SELECT id FROM max_durable_execution_tasks
      WHERE id = $1 AND tenant_id = $2 AND ownership_token = $3 AND fence_epoch = $4
        AND state IN ('claimed','running') AND lease_expires_at > clock_timestamp()
        AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
      FOR SHARE`,
    [fence.taskId, fence.tenantId, fence.ownershipToken, fence.fenceEpoch]
  );
  if (!r.rows[0]) {
    throw fencedError('Task ownership is stale, expired, cancelled or past its deadline; the write was refused.',
      { taskId: fence.taskId, fenceEpoch: fence.fenceEpoch });
  }
}

module.exports = { runWithTaskFence, currentTaskFence, assertTaskFenceOnClient, fencedError, normalizeFence };
