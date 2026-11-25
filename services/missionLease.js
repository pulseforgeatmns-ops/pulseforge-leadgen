'use strict';

/**
 * MAX-P0-003 — mission lease (transaction-scoped mutual exclusion).
 *
 * Replaces the ADR-075 session-level advisory lock, which was taken with
 * `pool.query('SELECT pg_advisory_lock…')` and released with a second,
 * independent `pool.query('SELECT pg_advisory_unlock…')`. On a pg.Pool those
 * two statements can run on different physical connections: the unlock then
 * returns false (a server WARNING, not an error), the lock stays attached to
 * an idle pooled connection, every other process is locked out until that
 * connection closes, and any later request that happens to check out the same
 * connection re-acquires it re-entrantly and bypasses mutual exclusion.
 *
 * A lease instead owns ONE dedicated connection for its whole life:
 *
 *   connect → BEGIN → SET LOCAL lock_timeout → pg_advisory_xact_lock(key)
 *           → (tenant ownership check) → … work on lease.client …
 *           → COMMIT | ROLLBACK → release connection
 *
 * The lock is transaction-scoped, so it can only be released by the end of the
 * transaction on the connection that took it. A crashed process, a killed
 * backend or a dropped socket ends the transaction and therefore the lock;
 * nothing can leak onto a pooled connection.
 *
 * Lock identity is the canonical mission id alone (the primary key of
 * acquisition_missions). It deliberately does NOT include the tenant id or the
 * mission version: two writers holding different versions of the same mission,
 * or a caller passing a wrong tenant, must contend for the same lock. Tenant
 * isolation is enforced by refusing the lease when the durable mission belongs
 * to another tenant (the refusal does not reveal which tenant).
 */

const crypto = require('crypto');

const LEASE_ERRORS = Object.freeze({
  BUSY: 'mission_lock_busy',
  TIMEOUT: 'mission_lock_timeout',
  TENANT_MISMATCH: 'mission_tenant_mismatch',
  LOST: 'mission_lease_lost',
  UNSUPPORTED: 'mission_lock_unsupported',
  RELEASED: 'mission_lease_released',
});

const DEFAULT_LOCK_TIMEOUT_MS = 5000;
const MAX_LOCK_TIMEOUT_MS = 60000;
/**
 * A lease stays open while a specialist runs. A live-but-stuck holder keeps the
 * lock (it may still commit); the server ends the transaction — and the lock —
 * if the holder sits idle longer than this. A crashed holder loses its socket
 * and the lock immediately.
 */
const DEFAULT_MAX_IDLE_MS = 10 * 60 * 1000;
const MAX_MAX_IDLE_MS = 60 * 60 * 1000;

function leaseError(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  err.spec = 'MAX-P0-003';
  Object.assign(err, extra);
  return err;
}

function canonicalMissionId(missionId) {
  const id = String(missionId == null ? '' : missionId).trim();
  if (!id) throw leaseError('mission_id_required', 'missionId is required for a mission lease.');
  return id;
}

/** Text key hashed server-side with hashtextextended(); one 64-bit lock space. */
function missionLockText(missionId) {
  return `amo-mission:${canonicalMissionId(missionId)}`;
}

/**
 * For callers that already own ONE dedicated connection for their whole
 * critical section (e.g. Anchor recovery approval): a session-level lock on the
 * SAME key as the lease. Session and transaction advisory locks on one key
 * conflict, so these holders and lease holders exclude each other. Lock and
 * unlock MUST run on that same connection — never through a pg.Pool.
 */
const MISSION_SESSION_LOCK_SQL = 'SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked';
const MISSION_SESSION_UNLOCK_SQL = 'SELECT pg_advisory_unlock(hashtextextended($1, 0)) AS unlocked';

function newCorrelationId() {
  return `cor_${crypto.randomBytes(12).toString('hex')}`;
}

function bounded(ms, fallback, max) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}

/**
 * Acquire a mission lease.
 *
 * @param {import('pg').Pool} pool  must expose connect(); a bare query() pool
 *   cannot provide a dedicated connection and is refused (fail closed).
 * @param {object} opts
 * @param {string} opts.missionId
 * @param {string|number} [opts.tenantId]   when given, a durable mission owned by
 *   another tenant is refused before any work.
 * @param {'try'|'wait'} [opts.mode='try']  try = fail immediately when held;
 *   wait = wait up to lockTimeoutMs, then fail with mission_lock_timeout.
 * @param {number} [opts.lockTimeoutMs]
 * @param {number} [opts.maxIdleMs]
 * @param {string} [opts.correlationId]
 */
async function acquireMissionLease(pool, opts = {}) {
  const missionId = canonicalMissionId(opts.missionId);
  if (!pool || typeof pool.connect !== 'function') {
    throw leaseError(LEASE_ERRORS.UNSUPPORTED,
      'A mission lease requires a pool that can provide a dedicated connection.');
  }
  const mode = opts.mode === 'wait' ? 'wait' : 'try';
  const lockTimeoutMs = bounded(opts.lockTimeoutMs, DEFAULT_LOCK_TIMEOUT_MS, MAX_LOCK_TIMEOUT_MS);
  const maxIdleMs = bounded(opts.maxIdleMs, DEFAULT_MAX_IDLE_MS, MAX_MAX_IDLE_MS);
  const correlationId = opts.correlationId || newCorrelationId();
  const tenantId = opts.tenantId != null && opts.tenantId !== '' ? String(opts.tenantId) : null;

  const client = await pool.connect();
  let lost = null;
  const onError = (err) => { lost = err; };
  // Real pg clients are EventEmitters; minimal test doubles may not be.
  if (typeof client.on === 'function') client.on('error', onError);

  const lease = {
    missionId,
    tenantId,
    correlationId,
    client,
    state: 'open',
    acquiredAt: null,
    /** Set when the connection failed underneath the lease. */
    get lost() { return lost; },
  };

  async function end(sql, destroyError) {
    if (lease.state !== 'open') return;
    lease.state = sql === 'COMMIT' ? 'committing' : 'rolling_back';
    let failure = null;
    try {
      await client.query(sql);
    } catch (err) {
      failure = err;
    }
    lease.state = failure ? 'failed' : (sql === 'COMMIT' ? 'committed' : 'rolled_back');
    if (typeof client.removeListener === 'function') client.removeListener('error', onError);
    // A connection whose transaction state is unknown must never return to the pool.
    client.release(failure || lost || destroyError || undefined);
    if (failure) {
      // A killed backend reports its socket error before commit in some pg
      // versions. commit() then attempts the normal rollback, which necessarily
      // fails because that socket is no longer queryable. Preserve the terminal
      // fact for callers: the lease was lost, not that recovery SQL failed.
      const code = sql === 'COMMIT' || lost ? LEASE_ERRORS.LOST : 'mission_lease_rollback_failed';
      throw leaseError(code, `Mission lease ${sql} failed: ${failure.message}`,
        { cause: lost || failure, missionId, correlationId });
    }
  }

  lease.commit = () => {
    if (lease.state !== 'open') {
      return Promise.reject(leaseError(LEASE_ERRORS.RELEASED, 'Mission lease is no longer open.', { missionId }));
    }
    if (lost) {
      return end('ROLLBACK').then(() => {
        throw leaseError(LEASE_ERRORS.LOST, 'Mission lease connection failed before commit.', { cause: lost, missionId });
      });
    }
    return end('COMMIT');
  };
  lease.rollback = () => end('ROLLBACK').catch(() => {});
  lease.isOpen = () => lease.state === 'open' && !lost;

  try {
    await client.query('BEGIN');
    // SET LOCAL cannot take a bind parameter; both values are bounded integers.
    await client.query(`SET LOCAL lock_timeout = '${lockTimeoutMs}ms'`);
    await client.query(`SET LOCAL idle_in_transaction_session_timeout = '${maxIdleMs}ms'`);
    const keyText = missionLockText(missionId);
    if (mode === 'try') {
      const r = await client.query('SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS locked', [keyText]);
      if (!r || !r.rows || !r.rows[0] || typeof r.rows[0].locked !== 'boolean') {
        // PostgreSQL always answers with a boolean; anything else is not PostgreSQL.
        throw leaseError(LEASE_ERRORS.UNSUPPORTED, 'Advisory lock query returned no result.', { missionId });
      }
      if (r.rows[0].locked !== true) {
        throw leaseError(LEASE_ERRORS.BUSY, `Mission ${missionId} is locked by another transactional authority.`,
          { missionId, correlationId });
      }
    } else {
      try {
        await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [keyText]);
      } catch (err) {
        if (err && err.code === '55P03') {
          throw leaseError(LEASE_ERRORS.TIMEOUT,
            `Timed out after ${lockTimeoutMs}ms waiting for mission ${missionId}.`, { missionId, correlationId, cause: err });
        }
        throw err;
      }
    }
    if (tenantId != null) {
      const owner = await client.query('SELECT tenant_id FROM acquisition_missions WHERE id = $1', [missionId])
        .catch((err) => {
          // 42P01 — no mission table yet: no durable mission can belong to anyone.
          if (err && err.code === '42P01') return { rows: [] };
          throw err;
        });
      if (owner.rows.length && String(owner.rows[0].tenant_id) !== tenantId) {
        throw leaseError(LEASE_ERRORS.TENANT_MISMATCH, 'Mission does not belong to the active tenant.',
          { missionId, correlationId });
      }
    }
    lease.acquiredAt = new Date().toISOString();
    return lease;
  } catch (err) {
    await lease.rollback();
    throw err;
  }
}

/**
 * Run fn(lease) under a mission lease. The lease commits when fn resolves
 * (unless fn already committed or rolled back) and rolls back when it throws.
 */
async function withMissionLease(pool, opts, fn) {
  const lease = await acquireMissionLease(pool, opts);
  try {
    const result = await fn(lease);
    if (lease.state === 'open') await lease.commit();
    return result;
  } catch (err) {
    if (lease.state === 'open') await lease.rollback();
    throw err;
  }
}

module.exports = {
  LEASE_ERRORS,
  DEFAULT_LOCK_TIMEOUT_MS,
  DEFAULT_MAX_IDLE_MS,
  acquireMissionLease,
  withMissionLease,
  missionLockText,
  MISSION_SESSION_LOCK_SQL,
  MISSION_SESSION_UNLOCK_SQL,
  canonicalMissionId,
  newCorrelationId,
};
