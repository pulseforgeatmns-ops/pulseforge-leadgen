'use strict';

/**
 * ADR-075 — Transactional Persistence Exclusivity.
 *
 * During an active TME transaction, exactly one component may mutate durable
 * mission state: persistStageCommit(). Legacy writers must not write.
 */

/** @type {Set<string>} */
const activeTransactions = new Set();

function transactionKey(missionId, transactionId) {
  return `${missionId || ''}:${transactionId || ''}`;
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

module.exports = {
  beginTmeTransaction,
  endTmeTransaction,
  isTmeTransactionActive,
  shouldSuppressLegacyDurableWrite,
};
