'use strict';

/**
 * ADR-075 — Transactional Persistence Exclusivity.
 *
 * During an active TME transaction, exactly one component may mutate durable
 * mission state: persistStageCommit(). Legacy writers must not write.
 *
 * Cross-process: PostgreSQL advisory locks ensure only one runtime mutates a
 * mission at a time (global transactional authority, not process-local).
 */

const crypto = require('crypto');

/** PostgreSQL advisory lock namespace for AMO mission durable writes (ADR-075). */
const MISSION_DURABLE_LOCK_NAMESPACE = 750825;

/** @type {Set<string>} */
const activeTransactions = new Set();

/** @type {Set<string>} missionIds held by this process via pg advisory lock */
const heldGlobalLocks = new Set();

/** @type {Map<string, string|null>} missionId -> owning transactionId */
const globalLockOwners = new Map();

function transactionKey(missionId, transactionId) {
  return `${missionId || ''}:${transactionId || ''}`;
}

function globalLockKey(missionId) {
  return String(missionId || '');
}

function missionLockKeys(missionId) {
  const hash = crypto.createHash('sha256').update(String(missionId)).digest();
  return {
    key1: MISSION_DURABLE_LOCK_NAMESPACE,
    key2: hash.readInt32BE(0),
  };
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

function isGlobalLockHeld(missionId) {
  return heldGlobalLocks.has(globalLockKey(missionId));
}

function overlapError(missionId) {
  const err = new Error(
    `Mission ${missionId} is locked by another transactional authority.`
  );
  err.code = 'tme_transaction_overlap';
  err.spec = 'ADR-075';
  return err;
}

/**
 * Acquire cross-process durable lock for a mission (ADR-075).
 * Reentrant within the same process. Skips when pool lacks advisory lock support.
 *
 * @param {string} missionId
 * @param {{ query: Function }} pool
 * @param {{ blocking?: boolean, tryOnly?: boolean }} [opts]
 */
async function acquireMissionDurableLock(missionId, pool, opts = {}) {
  if (!missionId) return { acquired: false, skipped: true };
  const localKey = globalLockKey(missionId);
  const transactionId = opts.transactionId || null;

  if (heldGlobalLocks.has(localKey)) {
    const existingOwner = globalLockOwners.get(localKey);
    if (transactionId && existingOwner === transactionId) {
      return { acquired: true, reentrant: true };
    }
    throw overlapError(missionId);
  }
  if (!pool || typeof pool.query !== 'function') {
    return { acquired: false, skipped: true };
  }

  const { key1, key2 } = missionLockKeys(missionId);
  const tryOnly = opts.tryOnly === true;
  const sql = tryOnly
    ? 'SELECT pg_try_advisory_lock($1, $2) AS locked'
    : 'SELECT pg_advisory_lock($1, $2) AS locked';

  try {
    const result = await pool.query(sql, [key1, key2]);
    const row = result.rows && result.rows[0];
    if (tryOnly) {
      if (!row || row.locked === undefined) {
        return { acquired: false, skipped: true };
      }
      if (row.locked !== true) {
        throw overlapError(missionId);
      }
    }
    heldGlobalLocks.add(localKey);
    globalLockOwners.set(localKey, transactionId);
    return { acquired: true };
  } catch (err) {
    if (err.code === 'tme_transaction_overlap') throw err;
    if (/Unhandled SQL|pg_advisory|pg_try_advisory/i.test(String(err.message))) {
      return { acquired: false, skipped: true };
    }
    throw err;
  }
}

/**
 * Release cross-process durable lock for a mission.
 * @param {string} missionId
 * @param {{ query: Function }} [pool]
 */
async function releaseMissionDurableLock(missionId, pool) {
  const localKey = globalLockKey(missionId);
  if (!heldGlobalLocks.has(localKey)) return;
  heldGlobalLocks.delete(localKey);
  globalLockOwners.delete(localKey);

  if (!pool || typeof pool.query !== 'function') return;
  const { key1, key2 } = missionLockKeys(missionId);
  try {
    await pool.query('SELECT pg_advisory_unlock($1, $2)', [key1, key2]);
  } catch (err) {
    if (!/Unhandled SQL|pg_advisory/i.test(String(err.message))) {
      console.error('[amo] advisory unlock failed:', err.message);
    }
  }
}

/** Test helper — reset in-process lock tracking. */
function resetMissionDurableLocksForTests() {
  heldGlobalLocks.clear();
  globalLockOwners.clear();
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
  resetMissionDurableLocksForTests,
};
