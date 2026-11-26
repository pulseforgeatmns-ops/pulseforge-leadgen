'use strict';

/**
 * MAX-P0-003 — the one sequence every irreversible external action follows:
 *
 *   claim ─┬─ replay   → original receipt; provider NOT invoked
 *          ├─ blocked  → duplicate / in progress / outcome unknown; provider NOT invoked
 *          └─ claimed  → beforeDispatch() (guards; refusal releases the claim)
 *                      → persistAttempt() (caller's own attempted record)
 *                      → markDispatchAttempted()   ← COMMITTED before the provider call
 *                                                    (task-fenced when taskFence is given, MAX-P0-004)
 *                      → dispatch()
 *                          throws  → outcome_unknown (rethrown; never retried)
 *                          returns → provider_acknowledged | failed_definitive | outcome_unknown
 */

const { CLAIM_OUTCOMES } = require('./externalActionLedger');

async function guardedDispatch({ ledger, claim: claimInput, beforeDispatch, persistAttempt, dispatch, taskFence }) {
  if (!ledger) throw Object.assign(new Error('An external-action ledger is required.'), { code: 'external_action_ledger_unavailable' });
  if (typeof dispatch !== 'function') throw Object.assign(new Error('dispatch is required.'), { code: 'external_action_invalid' });

  // MAX-P0-004: a task-linked action needs the task's ownership fence, and the
  // fence must name the same task; the marker then commits only while that
  // owner is current (externalActionLedger.markDispatchAttempted).
  if ((claimInput && claimInput.taskId) || taskFence) {
    if (!taskFence || !claimInput || String(taskFence.taskId) !== String(claimInput.taskId)) {
      throw Object.assign(new Error('A task-linked external action requires the matching task ownership fence.'), { code: 'durable_task_fenced' });
    }
  }

  const claimed = await ledger.claim(claimInput);
  if (claimed.outcome === CLAIM_OUTCOMES.REPLAY) return { outcome: 'replay', receipt: claimed.receipt };
  if (claimed.outcome !== CLAIM_OUTCOMES.CLAIMED) return { outcome: 'blocked', code: claimed.code, receipt: claimed.receipt };

  const handle = { tenantId: String(claimInput.tenantId), idempotencyKey: String(claimInput.idempotencyKey), ownerToken: claimed.ownerToken };
  const markerHandle = taskFence ? { ...handle, taskFence } : handle;
  try {
    if (typeof beforeDispatch === 'function') await beforeDispatch();
    if (typeof persistAttempt === 'function') await persistAttempt();
    await ledger.markDispatchAttempted(markerHandle);
  } catch (err) {
    // Nothing reached the provider: the claim may be taken again.
    await ledger.releaseUndispatched(handle, (err && err.code) || 'pre_dispatch_refused').catch(() => {});
    throw err;
  }

  let providerResult;
  try {
    providerResult = await dispatch();
  } catch (err) {
    await ledger.markOutcomeUnknown(handle, (err && err.code) || 'provider_exception').catch(() => {});
    throw err;
  }
  const receipt = await ledger.recordProviderResult(handle, providerResult);
  return { outcome: 'dispatched', providerResult, receipt, ledgerState: receipt.state };
}

module.exports = { guardedDispatch };
