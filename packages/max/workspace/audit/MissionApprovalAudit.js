'use strict';

/**
 * SPEC-128 — Operator approval consumption audit instrumentation.
 * Tracks approval match, consumption, and stage execution lifecycle.
 */

/** @type {object[]} */
const _auditLog = [];

/**
 * @param {string} event
 * @param {object} payload
 * @returns {object}
 */
function logMissionApprovalEvent(event, payload = {}) {
  const row = {
    event,
    missionId: payload.missionId || null,
    tenantId: payload.tenantId || null,
    stage: payload.stage || null,
    action: payload.action || null,
    phase: payload.phase || null,
    timestamp: payload.timestamp || new Date().toISOString(),
    ...payload,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function logMissionApprovalReceived(payload = {}) {
  return logMissionApprovalEvent('MISSION_APPROVAL_RECEIVED', payload);
}

function logMissionApprovalMatched(payload = {}) {
  return logMissionApprovalEvent('MISSION_APPROVAL_MATCHED', payload);
}

function logMissionApprovalConsumed(payload = {}) {
  return logMissionApprovalEvent('MISSION_APPROVAL_CONSUMED', payload);
}

function logMissionStageExecutionStarted(payload = {}) {
  return logMissionApprovalEvent('MISSION_STAGE_EXECUTION_STARTED', payload);
}

function logMissionStageExecutionCompleted(payload = {}) {
  return logMissionApprovalEvent('MISSION_STAGE_EXECUTION_COMPLETED', payload);
}

function listMissionApprovalAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearMissionApprovalAuditLog() {
  _auditLog.length = 0;
}

function createMissionApprovalAudit() {
  const localLog = [];
  return {
    log: localLog,
    logApprovalReceived(payload) {
      const row = logMissionApprovalReceived(payload);
      localLog.push(row);
      return row;
    },
    logApprovalMatched(payload) {
      const row = logMissionApprovalMatched(payload);
      localLog.push(row);
      return row;
    },
    logApprovalConsumed(payload) {
      const row = logMissionApprovalConsumed(payload);
      localLog.push(row);
      return row;
    },
    logStageExecutionStarted(payload) {
      const row = logMissionStageExecutionStarted(payload);
      localLog.push(row);
      return row;
    },
    logStageExecutionCompleted(payload) {
      const row = logMissionStageExecutionCompleted(payload);
      localLog.push(row);
      return row;
    },
    list() {
      return localLog.map((row) => ({ ...row }));
    },
    clear() {
      localLog.length = 0;
    },
  };
}

module.exports = {
  logMissionApprovalEvent,
  logMissionApprovalReceived,
  logMissionApprovalMatched,
  logMissionApprovalConsumed,
  logMissionStageExecutionStarted,
  logMissionStageExecutionCompleted,
  listMissionApprovalAuditLog,
  clearMissionApprovalAuditLog,
  createMissionApprovalAudit,
};
