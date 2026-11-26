'use strict';

/**
 * MAX-P0-003 — durable request idempotency receipts.
 *
 * A client that sends an Idempotency-Key gets at most one execution of the
 * logical request per (tenant, scope, key):
 *
 *   begin()  → 'owner'     first caller; runs the request, then complete()/fail()
 *            → 'replay'    a completed or failed receipt: the ORIGINAL response
 *                          is returned and nothing runs again
 *            → 'in_progress'  another caller owns it right now
 *            → 'interrupted'  its owner died mid-request (recovery marked it);
 *                          the outcome must be established from mission state and
 *                          the external-action ledger — it is never re-run blindly
 *            → 'mismatch'  the key was already used for a different request
 *
 * Receipts do not expire. Expiry would let a replay re-execute an irreversible
 * action; a client that wants a new execution uses a new key.
 */

const os = require('os');
const crypto = require('crypto');
const { recordReliabilityAudit } = require('./reliabilityAudit');
const { assertReliabilitySchema } = require('./schema');
const { requestFingerprint } = require('./externalActionLedger');

const KEY_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{7,199}$/;

function receiptError(code, message, extra = {}) {
  return Object.assign(new Error(message), { code, spec: 'MAX-P0-003' }, extra);
}

function validIdempotencyKey(key) {
  return typeof key === 'string' && KEY_PATTERN.test(key);
}

function createRequestReceipts(pool, options = {}) {
  const now = typeof options.now === 'function' ? options.now : () => new Date();

  async function load(tenantId, scope, key) {
    return (await pool.query(
      'SELECT * FROM max_request_receipts WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3',
      [tenantId, scope, key]
    )).rows[0] || null;
  }

  async function audit(row, event, fromState, toState, detail) {
    await recordReliabilityAudit(pool, {
      tenantId: row.tenant_id, correlationId: row.correlation_id, subjectKind: 'request_receipt',
      subjectKey: `${row.scope}:${row.idempotency_key}`, event, fromState, toState,
      detail: { scope: row.scope, httpStatus: row.http_status, errorCode: row.error_code, ...(detail || {}) },
    });
  }

  function view(row) {
    return row && {
      state: row.state,
      httpStatus: row.http_status == null ? null : Number(row.http_status),
      response: row.response || null,
      errorCode: row.error_code || null,
      correlationId: row.correlation_id || null,
      createdAt: row.created_at ? new Date(row.created_at).toISOString() : null,
      completedAt: row.completed_at ? new Date(row.completed_at).toISOString() : null,
      // MAX-P0-004: the durable task this request created (column added by the
      // P0-004 migration; undefined on a P0-003-only database).
      ...(row.task_id ? { taskId: String(row.task_id) } : {}),
    };
  }

  async function begin(input = {}) {
    await assertReliabilitySchema(pool);
    const tenantId = String(input.tenantId == null ? '' : input.tenantId).trim();
    const scope = String(input.scope || '').trim();
    const key = input.idempotencyKey;
    if (!tenantId || !scope) throw receiptError('request_receipt_invalid', 'tenantId and scope are required.');
    if (!validIdempotencyKey(key)) throw receiptError('idempotency_key_invalid', 'Idempotency-Key must be 8-200 characters of [A-Za-z0-9._:-].');
    const fingerprint = requestFingerprint(input.request || {});
    const ownerToken = `${os.hostname().slice(0, 32)}:${process.pid}:${crypto.randomBytes(8).toString('hex')}`;
    const inserted = await pool.query(
      `INSERT INTO max_request_receipts
         (tenant_id, scope, idempotency_key, request_fingerprint, state, owner_token, correlation_id, created_at, updated_at)
       VALUES ($1,$2,$3,$4,'in_progress',$5,$6,$7,$7)
       ON CONFLICT (tenant_id, scope, idempotency_key) DO NOTHING
       RETURNING *`,
      [tenantId, scope, key, fingerprint, ownerToken, input.correlationId || null, now()]
    );
    if (inserted.rows[0]) {
      await audit(inserted.rows[0], 'request_started', null, 'in_progress');
      return { outcome: 'owner', ownerToken, tenantId, scope, idempotencyKey: key, requestFingerprint: fingerprint };
    }
    const existing = await load(tenantId, scope, key);
    if (!existing) return { outcome: 'in_progress' };
    if (existing.request_fingerprint !== fingerprint) {
      await audit(existing, 'request_rejected_fingerprint_mismatch', existing.state, existing.state);
      return { outcome: 'mismatch', receipt: view(existing) };
    }
    if (existing.state === 'completed' || existing.state === 'failed') return { outcome: 'replay', receipt: view(existing) };
    if (existing.state === 'interrupted') return { outcome: 'interrupted', receipt: view(existing) };
    return { outcome: 'in_progress', receipt: view(existing) };
  }

  async function finish(handle, state, httpStatus, response, errorCode) {
    const r = await pool.query(
      `UPDATE max_request_receipts
          SET state = $5, http_status = $6, response = $7, error_code = $8, completed_at = $9, updated_at = $9
        WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3 AND owner_token = $4 AND state = 'in_progress'
      RETURNING *`,
      [handle.tenantId, handle.scope, handle.idempotencyKey, handle.ownerToken, state,
        httpStatus, response == null ? null : response, errorCode || null, now()]
    );
    if (!r.rows[0]) {
      // Recovery already marked this receipt interrupted (owner considered dead).
      throw receiptError('request_receipt_state_conflict', 'Request receipt is no longer owned by this request.');
    }
    await audit(r.rows[0], `request_${state}`, 'in_progress', state);
    return view(r.rows[0]);
  }

  /**
   * MAX-P0-004 P4 — link the owning request's receipt to the durable task it
   * accepted, before any specialist runs. Only the owner of an in_progress
   * receipt may link it, and the link is immutable (migration trigger).
   */
  async function linkTask(handle, taskId) {
    const r = await pool.query(
      `UPDATE max_request_receipts SET task_id = $5, updated_at = $6
        WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3 AND owner_token = $4 AND state = 'in_progress'
          AND (task_id IS NULL OR task_id = $5)
      RETURNING *`,
      [handle.tenantId, handle.scope, handle.idempotencyKey, handle.ownerToken, taskId, now()]
    );
    if (!r.rows[0]) throw receiptError('request_receipt_state_conflict', 'Request receipt could not be linked to its task.');
    await audit(r.rows[0], 'request_task_linked', 'in_progress', 'in_progress', { taskId: String(taskId) });
    return view(r.rows[0]);
  }

  return {
    begin,
    linkTask,
    complete: (handle, httpStatus, response) => finish(handle, 'completed', httpStatus, response, null),
    fail: (handle, httpStatus, response, errorCode) => finish(handle, 'failed', httpStatus, response, errorCode),
    async get(tenantId, scope, key) {
      await assertReliabilitySchema(pool);
      return view(await load(String(tenantId), String(scope), key));
    },
  };
}

module.exports = { createRequestReceipts, validIdempotencyKey, KEY_PATTERN };
