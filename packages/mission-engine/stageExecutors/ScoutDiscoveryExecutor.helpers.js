'use strict';

/**
 * ScoutDiscoveryExecutor helpers — payload construction and legacy recordDiscoveryExecution.
 */

const ScoutDiscoveryAudit = require('../ScoutDiscoveryAudit');
const {
  buildDiscoveryExecutionReport,
  emitDiscoveryAuditEvents,
} = require('../discoveryExecutionReport');

/**
 * @param {object} mission
 * @param {string} [message]
 * @returns {object}
 */
function buildScoutDispatchPayload(mission, message) {
  const plan =
    (mission.plan && mission.plan.missionPlan) || mission.missionPlan || null;
  return {
    missionId: mission.id,
    objective:
      (plan && plan.objective) ||
      mission.objectiveText ||
      mission.title ||
      null,
    targetSegment:
      (plan && plan.subject) ||
      (mission.constraints && mission.constraints.vertical) ||
      null,
    geography:
      (mission.constraints && mission.constraints.locationHint) ||
      (plan && plan.geography) ||
      null,
    constraints: mission.constraints || {},
    approvalState: message && /\bapprov(ed|al)\b/i.test(message) ? 'approved' : 'pending',
    operatorMessage: message || null,
  };
}

/**
 * @param {object} mission
 * @param {object} scoutPayload
 * @returns {object}
 */
function recordDiscoveryExecution(mission, scoutPayload) {
  const report = buildDiscoveryExecutionReport(mission, scoutPayload);
  report.missionStatus = mission.status;
  emitDiscoveryAuditEvents(report, ScoutDiscoveryAudit);
  ScoutDiscoveryAudit.logMissionDiscoveryResponse({
    missionId: report.missionId,
    discoveryStrategy: report.discoveryStrategy,
    evidenceSources: report.evidenceSources,
    outcome: report.outcome,
    blockReason: report.blockReason,
    operatorResponseKind: 'mission_execution_outcome',
  });
  return report;
}

module.exports = {
  buildScoutDispatchPayload,
  recordDiscoveryExecution,
};
