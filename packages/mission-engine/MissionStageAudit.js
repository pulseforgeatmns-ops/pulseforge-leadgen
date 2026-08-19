'use strict';

/**
 * AUDIT-003 — Mission stage execution instrumentation.
 * Every stage transition emits explicit audit events; no silent advisory fallback.
 */

/** @type {object[]} */
const _stageAuditLog = [];

/**
 * @param {string} event
 * @param {object} payload
 * @returns {object}
 */
function logMissionStageEvent(event, payload = {}) {
  const row = {
    event,
    missionId: payload.missionId || null,
    stage: payload.stage || payload.stageId || payload.stageName || null,
    executor: payload.executor || payload.executorId || null,
    timestamp: new Date().toISOString(),
    outcome: payload.outcome || null,
    ...payload,
  };
  _stageAuditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function logMissionStage(payload) {
  return logMissionStageEvent('MISSION_STAGE', payload);
}

function logMissionExecutorSelected(payload) {
  return logMissionStageEvent('MISSION_EXECUTOR_SELECTED', payload);
}

function logMissionExecutorInvoked(payload) {
  return logMissionStageEvent('MISSION_EXECUTOR_INVOKED', payload);
}

function logMissionExecutorResult(payload) {
  return logMissionStageEvent('MISSION_EXECUTOR_RESULT', payload);
}

function logMissionExecutorFallback(payload) {
  return logMissionStageEvent('MISSION_EXECUTOR_FALLBACK', {
    ...payload,
    selected: payload.selected || 'RecommendationEngine',
  });
}

function listMissionStageAuditLog() {
  return _stageAuditLog.map((row) => ({ ...row }));
}

function clearMissionStageAuditLog() {
  _stageAuditLog.length = 0;
}

module.exports = {
  logMissionStageEvent,
  logMissionStage,
  logMissionExecutorSelected,
  logMissionExecutorInvoked,
  logMissionExecutorResult,
  logMissionExecutorFallback,
  listMissionStageAuditLog,
  clearMissionStageAuditLog,
};
