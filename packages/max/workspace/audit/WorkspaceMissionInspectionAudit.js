'use strict';

/**
 * AUDIT-005 — Workspace Mission Inspection integration audit.
 * Emits ownership checkpoints for every WorkspaceEngine.ask() turn.
 */

const OWNER = 'WorkspaceEngine';

/** @type {object[]} */
const _auditLog = [];

function logWorkspaceMissionInspectionEvent(event, payload = {}) {
  const row = {
    event,
    missionId: payload.missionId != null ? payload.missionId : null,
    selectedPipeline: payload.selectedPipeline || payload.pipeline || null,
    timestamp: payload.timestamp || new Date().toISOString(),
    reason: payload.reason != null ? payload.reason : null,
    owner: payload.owner || OWNER,
    ...payload,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function logWorkspaceRequest(payload = {}) {
  return logWorkspaceMissionInspectionEvent('WORKSPACE_REQUEST', {
    conversation: payload.conversation || payload.sessionId || null,
    workspace: payload.workspace || 'max',
    operator: payload.operator || null,
    question: payload.question || null,
    ...payload,
  });
}

function logWorkspaceActiveMission(payload = {}) {
  return logWorkspaceMissionInspectionEvent('WORKSPACE_ACTIVE_MISSION', {
    missionFound: payload.missionFound === true,
    missionId: payload.missionId || null,
    stage: payload.stage || null,
    status: payload.status || null,
    ...payload,
  });
}

function logWorkspaceMissionInspection(payload = {}) {
  return logWorkspaceMissionInspectionEvent('WORKSPACE_MISSION_INSPECTION', {
    attempted: payload.attempted === true,
    ...payload,
  });
}

function logMissionInspectionResult(payload = {}) {
  return logWorkspaceMissionInspectionEvent('MISSION_INSPECTION_RESULT', {
    claimed: payload.claimed === true,
    property: payload.property || null,
    confidence: payload.confidence != null ? payload.confidence : null,
    ...payload,
  });
}

function logWorkspacePipeline(payload = {}) {
  return logWorkspaceMissionInspectionEvent('WORKSPACE_PIPELINE', {
    pipelineSelected: payload.pipelineSelected || payload.selectedPipeline || null,
    selectedPipeline: payload.selectedPipeline || payload.pipelineSelected || null,
    ...payload,
  });
}

function logMissionPropertyGuard(payload = {}) {
  return logWorkspaceMissionInspectionEvent('MISSION_PROPERTY_GUARD', {
    property: payload.property || null,
    retrievalBlocked: payload.retrievalBlocked === true,
    ...payload,
  });
}

function logWorkspaceResponse(payload = {}) {
  return logWorkspaceMissionInspectionEvent('WORKSPACE_RESPONSE', {
    complete: payload.complete !== false,
    ...payload,
  });
}

function buildOwnershipTrace(events = _auditLog) {
  const steps = [];
  for (const row of events) {
    switch (row.event) {
      case 'WORKSPACE_REQUEST':
        steps.push('Workspace Entry');
        break;
      case 'WORKSPACE_ACTIVE_MISSION':
        steps.push(row.missionFound ? 'Mission Found' : 'No Active Mission');
        break;
      case 'WORKSPACE_MISSION_INSPECTION':
        if (row.attempted) steps.push('Mission Inspection');
        break;
      case 'MISSION_INSPECTION_RESULT':
        if (row.claimed) steps.push('Property Claimed');
        break;
      case 'WORKSPACE_PIPELINE':
        if (row.selectedPipeline === 'MissionInspection') steps.push('Mission Response');
        else if (row.selectedPipeline === 'Retrieval') steps.push('Retrieval Selected');
        break;
      case 'WORKSPACE_RESPONSE':
        steps.push('Complete');
        break;
      default:
        break;
    }
  }
  return steps;
}

function listWorkspaceMissionInspectionAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearWorkspaceMissionInspectionAuditLog() {
  _auditLog.length = 0;
}

function createWorkspaceMissionInspectionAudit() {
  const localLog = [];
  return {
    log: localLog,
    logWorkspaceRequest(payload) {
      const row = logWorkspaceRequest(payload);
      localLog.push(row);
      return row;
    },
    logWorkspaceActiveMission(payload) {
      const row = logWorkspaceActiveMission(payload);
      localLog.push(row);
      return row;
    },
    logWorkspaceMissionInspection(payload) {
      const row = logWorkspaceMissionInspection(payload);
      localLog.push(row);
      return row;
    },
    logMissionInspectionResult(payload) {
      const row = logMissionInspectionResult(payload);
      localLog.push(row);
      return row;
    },
    logWorkspacePipeline(payload) {
      const row = logWorkspacePipeline(payload);
      localLog.push(row);
      return row;
    },
    logMissionPropertyGuard(payload) {
      const row = logMissionPropertyGuard(payload);
      localLog.push(row);
      return row;
    },
    logWorkspaceResponse(payload) {
      const row = logWorkspaceResponse(payload);
      localLog.push(row);
      return row;
    },
    buildOwnershipTrace() {
      return buildOwnershipTrace(localLog);
    },
  };
}

module.exports = {
  OWNER,
  logWorkspaceMissionInspectionEvent,
  logWorkspaceRequest,
  logWorkspaceActiveMission,
  logWorkspaceMissionInspection,
  logMissionInspectionResult,
  logWorkspacePipeline,
  logMissionPropertyGuard,
  logWorkspaceResponse,
  buildOwnershipTrace,
  listWorkspaceMissionInspectionAuditLog,
  clearWorkspaceMissionInspectionAuditLog,
  createWorkspaceMissionInspectionAudit,
};
