'use strict';

/**
 * ADR-075 — Transactional Persistence Exclusivity.
 *
 * During an active TME transaction, exactly one component may mutate durable
 * mission state: persistStageCommit(). Legacy writers must not write.
 *
 * Cross-process: a transaction-scoped mission lease (MAX-P0-003,
 * services/missionLease.js) ensures only one runtime mutates a mission at a
 * time. The lock lives and dies with one transaction on one dedicated
 * connection, so it cannot leak onto a pooled connection.
 */

/**
 * Historical ADR-075 advisory namespace. MAX-P0-003 replaced the session-level
 * two-int lock with a transaction-scoped lease (services/missionLease.js);
 * the constant is kept for diagnostics and compatibility only.
 */
const MISSION_DURABLE_LOCK_NAMESPACE = 750825;

const {
  acquireMissionLease,
  LEASE_ERRORS,
} = require('../../services/missionLease');

/** @type {Set<string>} */
const activeTransactions = new Set();

/** @type {Map<string, { lease: object|null, transactionId: string|null }>} missionId -> held lease */
const heldLeases = new Map();

function transactionKey(missionId, transactionId) {
  return `${missionId || ''}:${transactionId || ''}`;
}

function globalLockKey(missionId) {
  return String(missionId || '').trim();
}

function beginTmeTransaction(missionId, transactionId) {
  activeTransactions.add(transactionKey(missionId, transactionId));
}

function endTmeTransaction(missionId, transactionId) {
  activeTransactions.delete(transactionKey(missionId, transactionId));
}

function isTmeTransactionActive(missionId = null) {
  if (missionId == null) return activeTransactions.size > 0;
  for (const key of activeTransactions) {
    if (key.startsWith(`${missionId}:`)) return true;
  }
  return false;
}

/**
 * Returns true when a legacy durable write was suppressed (ADR-075).
 * @param {string} [missionId]
 * @returns {boolean}
 */
function shouldSuppressLegacyDurableWrite(missionId = null) {
  return isTmeTransactionActive(missionId);
}

/** True while this process holds an OPEN lease for the mission. */
function isGlobalLockHeld(missionId) {
  const held = heldLeases.get(globalLockKey(missionId));
  return Boolean(held && held.lease && held.lease.isOpen());
}

/**
 * The open lease this process holds for a mission, or null. Writers that run
 * inside a TME stage commit on lease.client so the lock and the write share
 * one transaction.
 */
function getMissionLease(missionId) {
  const held = heldLeases.get(globalLockKey(missionId));
  return held && held.lease && held.lease.isOpen() ? held.lease : null;
}

function overlapError(missionId, cause) {
  const err = new Error(
    `Mission ${missionId} is locked by another transactional authority.`
  );
  err.code = 'tme_transaction_overlap';
  err.spec = 'ADR-075';
  if (cause) err.cause = cause;
  return err;
}

/**
 * Acquire the cross-process mission lease (ADR-075 / MAX-P0-003).
 *
 * Re-entrant only for the same transactionId within this process. A pool that
 * cannot provide a dedicated connection (bare query() test doubles) is skipped,
 * exactly as before; production pg.Pool always provides one.
 *
 * @param {string} missionId
 * @param {{ query: Function, connect?: Function }} pool
 * @param {{ tryOnly?: boolean, transactionId?: string, tenantId?: string,
 *           lockTimeoutMs?: number, correlationId?: string }} [opts]
 */
async function acquireMissionDurableLock(missionId, pool, opts = {}) {
  if (!missionId) return { acquired: false, skipped: true };
  const localKey = globalLockKey(missionId);
  const transactionId = opts.transactionId || null;

  const existing = heldLeases.get(localKey);
  if (existing) {
    if (existing.lease && !existing.lease.isOpen()) {
      heldLeases.delete(localKey);
    } else {
      if (transactionId && existing.transactionId === transactionId) {
        return { acquired: true, reentrant: true, lease: existing.lease };
      }
      throw overlapError(missionId);
    }
  }
  if (!pool || typeof pool.query !== 'function' || typeof pool.connect !== 'function') {
    return { acquired: false, skipped: true };
  }
  // Reserve the slot before awaiting so two same-process callers cannot both connect.
  const reservation = { lease: null, transactionId, pending: true };
  heldLeases.set(localKey, reservation);
  try {
    const lease = await acquireMissionLease(pool, {
      missionId: localKey,
      tenantId: opts.tenantId,
      mode: opts.tryOnly === false ? 'wait' : 'try',
      lockTimeoutMs: opts.lockTimeoutMs,
      correlationId: opts.correlationId,
    });
    reservation.lease = lease;
    reservation.pending = false;
    return { acquired: true, lease };
  } catch (err) {
    if (heldLeases.get(localKey) === reservation) heldLeases.delete(localKey);
    if (err && err.code === LEASE_ERRORS.BUSY) throw overlapError(missionId, err);
    // Non-PostgreSQL test doubles (no boolean lock result): same "skipped"
    // outcome the ADR-075 lock always returned for them. Real PostgreSQL never
    // takes this branch.
    if (err && err.code === LEASE_ERRORS.UNSUPPORTED) return { acquired: false, skipped: true };
    throw err;
  }
}

/**
 * Release the mission lease. A lease whose transaction is still open is rolled
 * back (its writes, if any, were never committed); a lease already committed by
 * its writer is simply forgotten. The lock is released by the transaction end
 * on the same connection that took it — never by a separate pooled statement.
 * @param {string} missionId
 */
async function releaseMissionDurableLock(missionId) {
  const localKey = globalLockKey(missionId);
  const held = heldLeases.get(localKey);
  if (!held) return;
  heldLeases.delete(localKey);
  if (held.lease && held.lease.state === 'open') {
    await held.lease.rollback();
  }
}

/** Test helper — reset in-process lock tracking (rolls back any open lease). */
function resetMissionDurableLocksForTests() {
  for (const held of heldLeases.values()) {
    if (held.lease && held.lease.state === 'open') held.lease.rollback();
  }
  heldLeases.clear();
  activeTransactions.clear();
}

module.exports = {
  MISSION_DURABLE_LOCK_NAMESPACE,
  beginTmeTransaction,
  endTmeTransaction,
  isTmeTransactionActive,
  shouldSuppressLegacyDurableWrite,
  isGlobalLockHeld,
  acquireMissionDurableLock,
  releaseMissionDurableLock,
  getMissionLease,
  resetMissionDurableLocksForTests,
};
