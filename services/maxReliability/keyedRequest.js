'use strict';

/**
 * MAX-P0-003 — run one HTTP request under a durable idempotency receipt.
 *
 * Returns { status, body, headers } and never throws for receipt outcomes, so
 * routes stay thin and the behaviour is testable without Express.
 *
 *   no key             → run(), no receipt (unchanged behaviour)
 *   owner              → run(); success → complete (stored receipt, minus volatile
 *                        fields); error → fail (the failure is the receipt), except
 *                        pre-execution lock contention, whose receipt is released so
 *                        the client may retry with the same key
 *   replay             → the stored original response, `replayed: true`
 *   mismatch           → 422 idempotency_key_reused
 *   in_progress        → 409 request_in_progress
 *   interrupted        → 409 request_outcome_unknown (never re-run)
 *   schema missing     → 503 idempotency_unavailable (fail closed)
 */

const { createRequestReceipts, validIdempotencyKey } = require('./requestReceipts');
const { recordReliabilityAudit } = require('./reliabilityAudit');

/** Refused before any execution began: safe to release the receipt. */
const RELEASABLE_ERROR_CODES = new Set([
  'tme_transaction_overlap',
  'mission_lock_busy',
  'mission_lock_timeout',
]);

async function runKeyedRequest(input = {}) {
  const { pool, tenantId, scope, idempotencyKey, request, correlationId, run, errorStatus, volatileFields = [], receiptOf } = input;
  const headers = correlationId ? { 'X-Correlation-Id': correlationId } : {};
  const statusFor = (err) => (typeof errorStatus === 'function' ? errorStatus(err) : 500);
  const errorBody = (err) => ({ error: (err && err.code) || 'request_failed', message: String((err && err.message) || err), correlationId });

  if (idempotencyKey == null || idempotencyKey === '') {
    try {
      return { status: 200, body: await run(), headers };
    } catch (err) {
      return { status: statusFor(err), body: errorBody(err), headers, error: err };
    }
  }
  if (!validIdempotencyKey(idempotencyKey)) {
    return { status: 400, headers, body: { error: 'idempotency_key_invalid', message: 'Idempotency-Key must be 8-200 characters of [A-Za-z0-9._:-].', correlationId } };
  }

  const receipts = input.receipts || createRequestReceipts(pool);
  let handle;
  try {
    handle = await receipts.begin({ tenantId, scope, idempotencyKey, request, correlationId });
  } catch (err) {
    if (err && /^reliability_schema_/.test(String(err.code))) {
      return { status: 503, headers, body: { error: 'idempotency_unavailable', message: 'Idempotent execution is unavailable; nothing was executed.', correlationId } };
    }
    throw err;
  }

  // MAX-P0-004 P4 — a request linked to a durable task reports the task's
  // live state on every replay, conflict or interruption (never a re-run).
  const liveTask = async () => {
    if (!handle.receipt || !handle.receipt.taskId || typeof input.loadTask !== 'function') return undefined;
    try { return await input.loadTask(handle.receipt.taskId); } catch (_) { return undefined; }
  };
  if (handle.outcome === 'replay') {
    const task = await liveTask();
    return { status: handle.receipt.httpStatus || 200, headers,
      body: { ...(handle.receipt.response || {}), replayed: true, originalCorrelationId: handle.receipt.correlationId,
        ...(task !== undefined ? { durableTask: task } : {}) } };
  }
  if (handle.outcome === 'mismatch') {
    return { status: 422, headers, body: { error: 'idempotency_key_reused', message: 'This Idempotency-Key was already used for a different request.', correlationId } };
  }
  if (handle.outcome === 'interrupted') {
    const task = await liveTask();
    return { status: 409, headers, body: { error: 'request_outcome_unknown',
      message: 'An earlier request with this key was interrupted; its outcome must be reconciled from mission state. It will not be re-run.', correlationId,
      ...(task !== undefined ? { durableTask: task } : {}) } };
  }
  if (handle.outcome !== 'owner') {
    const task = await liveTask();
    return { status: 409, headers, body: { error: 'request_in_progress', message: 'A request with this Idempotency-Key is still running.', correlationId,
      ...(task !== undefined ? { durableTask: task } : {}) } };
  }

  // The receipt identity a task-owned execution derives its logical key from,
  // and the hook that links the receipt to the task before any specialist runs.
  let linkedTaskId = null;
  const receipt = {
    scope: handle.scope,
    idempotencyKey: handle.idempotencyKey,
    requestFingerprint: handle.requestFingerprint,
    linkTask: async (taskId) => { const view = await receipts.linkTask(handle, taskId); linkedTaskId = String(taskId); return view; },
  };

  let body;
  try {
    body = await run(receipt);
  } catch (err) {
    const status = statusFor(err);
    const failure = errorBody(err);
    const releasable = RELEASABLE_ERROR_CODES.has(err && err.code) || RELEASABLE_ERROR_CODES.has(err && err.cause && err.cause.code);
    // MAX-P0-004 — a receipt linked to a durable task is never released: the
    // task (and its logical key) is permanent, so the same key can only ever
    // replay it. Nothing ran; the client may retry with a NEW key.
    if (releasable && linkedTaskId) {
      await receipts.fail(handle, status, { error: failure.error, correlationId, retryableWithNewKey: true }, failure.error);
      return { status, body: { ...failure, retryable: false, retryableWithNewKey: true }, headers, error: err };
    }
    if (releasable) {
      await pool.query(
        `DELETE FROM max_request_receipts WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3
           AND owner_token = $4 AND state = 'in_progress'`,
        [handle.tenantId, handle.scope, handle.idempotencyKey, handle.ownerToken]
      );
      await recordReliabilityAudit(pool, { tenantId: handle.tenantId, correlationId, subjectKind: 'request_receipt',
        subjectKey: `${handle.scope}:${handle.idempotencyKey}`, event: 'request_released_not_executed',
        fromState: 'in_progress', toState: null, detail: { errorCode: failure.error } });
      return { status, body: { ...failure, retryable: true }, headers, error: err };
    }
    // Failure receipts are permanent too: keep the machine code only, never the
    // raw error text (it can carry data). The live response keeps the message.
    await receipts.fail(handle, status, { error: failure.error, correlationId }, failure.error);
    return { status, body: failure, headers, error: err };
  }
  // Receipts are permanent (they cannot be deleted), so store only what a
  // replay needs: an allowlisted summary when the caller provides receiptOf.
  const stored = typeof receiptOf === 'function' ? receiptOf(body) : { ...body };
  for (const field of volatileFields) delete stored[field];
  await receipts.complete(handle, 200, stored);
  return { status: 200, body, headers };
}

module.exports = { runKeyedRequest, RELEASABLE_ERROR_CODES };
