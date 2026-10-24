'use strict';

/**
 * SPEC-131 — execution audit independent of mission state.
 * Rollback records must not mutate the mission; they live here.
 */

const { clone, nowIso, newId } = require('./types');

const COMMIT_STATUS = Object.freeze({
  COMMITTED: 'committed',
  ROLLED_BACK: 'rolled_back',
});

/** @type {object[]} */
const _auditLog = [];

function recordExecutionAudit(entry = {}) {
  const row = {
    id: entry.id || newId('tme'),
    spec: 'SPEC-131',
    transactionId: entry.transactionId || newId('tme'),
    missionId: entry.missionId || null,
    tenantId: entry.tenantId != null ? String(entry.tenantId) : null,
    missionVersion: entry.missionVersion != null ? Number(entry.missionVersion) : 0,
    specialist: entry.specialist || null,
    stage: entry.stage || null,
    preconditions: entry.preconditions && typeof entry.preconditions === 'object'
      ? clone(entry.preconditions)
      : {},
    durationMs: Number.isFinite(Number(entry.durationMs)) ? Number(entry.durationMs) : 0,
    commitStatus: entry.commitStatus === COMMIT_STATUS.COMMITTED
      ? COMMIT_STATUS.COMMITTED
      : COMMIT_STATUS.ROLLED_BACK,
    rollbackReason: entry.rollbackReason || null,
    errorClass: entry.errorClass || null,
    exception: entry.exception || null,
    at: entry.at || nowIso(),
    payload: entry.payload && typeof entry.payload === 'object' ? clone(entry.payload) : {},
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test' && !process.env.NODE_TEST_CONTEXT) {
    console.info('[TME_EXECUTION_AUDIT]', JSON.stringify(row));
  }
  return clone(row);
}

function listExecutionAudit(filter = {}) {
  return _auditLog
    .filter((row) => {
      if (filter.missionId && row.missionId !== filter.missionId) return false;
      if (filter.transactionId && row.transactionId !== filter.transactionId) return false;
      if (filter.commitStatus && row.commitStatus !== filter.commitStatus) return false;
      return true;
    })
    .map(clone);
}

function clearExecutionAudit() {
  _auditLog.length = 0;
}

module.exports = {
  COMMIT_STATUS,
  recordExecutionAudit,
  listExecutionAudit,
  clearExecutionAudit,
};
