'use strict';

/**
 * AUDIT-007 — Operator Approval Routing.
 * Trace "approved" (and other operator decisions) through Workspace until
 * ownership leaves or stays on the Mission runtime.
 */

const { classifyMessage, MESSAGE_CLASS } = require('../../../mission-engine/classifyMessage');
const { isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { detectExecutionAction } = require('../AcquisitionMissionExecution');
const {
  resolveAcquisitionActiveMission,
} = require('../ActiveMissionGuard');
const {
  resolveAcquisitionEngine,
  resolveTenantId,
} = require('../WorkspaceMissionInspection');
const { hasPendingDiscoveryApproval } = require('../AmoOperatorApproval');

const PIPELINES = Object.freeze({
  ACQUISITION_MISSION: 'AcquisitionMission',
  MISSION_ENGINE: 'MissionEngine',
  RECOMMENDATION_ENGINE: 'RecommendationEngine',
  REASONING: 'Reasoning',
});

const BREAKPOINT_LEGACY_MISSION_FIRST =
  'WorkspaceEngine.ask:maybeHandleMissionFirstTurn return';

/** @type {object[]} */
const _auditLog = [];

function logOperatorApprovalRoutingEvent(event, payload = {}) {
  const row = {
    event,
    timestamp: payload.timestamp || new Date().toISOString(),
    ...payload,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function logWorkspaceEntry(payload = {}) {
  return logOperatorApprovalRoutingEvent('WORKSPACE_ENTRY', {
    question: payload.question || null,
    sessionId: payload.sessionId || null,
    ...payload,
  });
}

function logActiveMissionFound(payload = {}) {
  return logOperatorApprovalRoutingEvent('ACTIVE_MISSION_FOUND', {
    found: payload.found === true,
    source: payload.source || null,
    missionId: payload.missionId || null,
    runtime: payload.runtime || null,
    stage: payload.stage || null,
    ...payload,
  });
}

function logMissionPendingDecision(payload = {}) {
  return logOperatorApprovalRoutingEvent('MISSION_PENDING_DECISION', {
    found: payload.found === true,
    prompt: payload.prompt || null,
    stage: payload.stage || null,
    missionId: payload.missionId || null,
    ...payload,
  });
}

function logApprovalClassifier(payload = {}) {
  return logOperatorApprovalRoutingEvent('APPROVAL_CLASSIFIER', {
    utterance: payload.utterance || null,
    missionExecutionCommand: payload.missionExecutionCommand === true,
    executeStage: payload.executeStage === true,
    executeStageReason: payload.executeStageReason || null,
    amoAction: payload.amoAction || null,
    returnsTrue: payload.returnsTrue === true,
    ...payload,
  });
}

function logOwnerSelected(payload = {}) {
  return logOperatorApprovalRoutingEvent('OWNER_SELECTED', {
    owner: payload.owner || null,
    reason: payload.reason || null,
    source: payload.source || null,
    ...payload,
  });
}

function logPipelineSelected(payload = {}) {
  return logOperatorApprovalRoutingEvent('PIPELINE_SELECTED', {
    pipeline: payload.pipeline || null,
    claimedBy: payload.claimedBy || null,
    ...payload,
  });
}

function logMissionApprovalMatch(payload = {}) {
  return logOperatorApprovalRoutingEvent('MISSION_APPROVAL_MATCH', {
    matched: payload.matched === true,
    missionId: payload.missionId || null,
    action: payload.action || null,
    ...payload,
  });
}

function logMissionStageExecution(payload = {}) {
  return logOperatorApprovalRoutingEvent('MISSION_STAGE_EXECUTION', {
    started: payload.started === true,
    executor: payload.executor || null,
    missionId: payload.missionId || null,
    ...payload,
  });
}

function logFallbackReason(payload = {}) {
  return logOperatorApprovalRoutingEvent('FALLBACK_REASON', {
    reason: payload.reason || null,
    breakpoint: payload.breakpoint || null,
    claimedRuntime: payload.claimedRuntime || null,
    ...payload,
  });
}

/**
 * Emit WORKSPACE_ENTRY through APPROVAL_CLASSIFIER + OWNER_SELECTED for one ask.
 * @param {object} input
 * @returns {object} captured classifier snapshot
 */
function emitApprovalRoutingContext(input = {}) {
  const question = String(input.question || '').trim();
  const ownership = input.workspaceOwnership || {};
  const lock = ownership.missionLock || {};
  const session = input.session || null;

  logWorkspaceEntry({
    question,
    sessionId: session && session.id,
  });

  const runtime =
    lock.source === 'amo'
      ? 'AcquisitionMission'
      : lock.source === 'legacy'
        ? 'MissionEngine'
        : null;

  logActiveMissionFound({
    found: lock.active === true,
    source: lock.source || null,
    missionId: lock.missionId || null,
    runtime,
    stage: lock.mission && (lock.mission.stage || lock.mission.status) || null,
  });

  const amoMission = resolveAcquisitionActiveMission({
    question,
    session,
    context: input.context || (session && session.context),
    acquisitionMissionEngine: input.acquisitionMissionEngine,
    acquisitionMissionService: input.acquisitionMissionService,
  });
  const pending = amoMission && amoMission.pendingOperatorDecision;
  logMissionPendingDecision({
    found: Boolean(pending),
    prompt: pending ? pending.prompt : null,
    stage: pending ? pending.stage : null,
    missionId: amoMission ? amoMission.id : null,
  });

  let amoAction = null;
  let pendingDiscovery = false;
  const engine = resolveAcquisitionEngine({
    acquisitionMissionEngine: input.acquisitionMissionEngine,
    acquisitionMissionService: input.acquisitionMissionService,
    session,
    context: input.context || (session && session.context),
  });
  if (amoMission && engine && typeof engine.inspect === 'function') {
    const tenantId = resolveTenantId({
      session,
      context: input.context || (session && session.context),
    }) || amoMission.tenantId;
    const snapshot = engine.inspect(amoMission.id, { tenantId });
    pendingDiscovery = hasPendingDiscoveryApproval(snapshot);
    amoAction = detectExecutionAction(question, snapshot);
  }

  const classified = classifyMessage(question, lock.mission || amoMission || null);
  const missionExecutionCommand = isMissionExecutionCommand(question);
  const executeStage = classified.classification === MESSAGE_CLASS.EXECUTE_STAGE;

  logApprovalClassifier({
    utterance: question,
    missionExecutionCommand,
    executeStage,
    executeStageReason: classified.reason,
    amoAction,
    pendingDiscovery,
    returnsTrue: missionExecutionCommand === true,
  });

  logOwnerSelected({
    owner: ownership.owner || null,
    reason: ownership.reason || null,
    source: lock.source || null,
  });

  return {
    question,
    lock,
    amoMission,
    pending,
    amoAction,
    classified,
    missionExecutionCommand,
    executeStage,
    pendingDiscovery,
  };
}

function listOperatorApprovalRoutingAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearOperatorApprovalRoutingAuditLog() {
  _auditLog.length = 0;
}

function createOperatorApprovalRoutingAudit() {
  const localLog = [];
  const wrap = (fn) => (payload) => {
    const row = fn(payload);
    localLog.push(row);
    return row;
  };
  return {
    log: localLog,
    logWorkspaceEntry: wrap(logWorkspaceEntry),
    logActiveMissionFound: wrap(logActiveMissionFound),
    logMissionPendingDecision: wrap(logMissionPendingDecision),
    logApprovalClassifier: wrap(logApprovalClassifier),
    logOwnerSelected: wrap(logOwnerSelected),
    logPipelineSelected: wrap(logPipelineSelected),
    logMissionApprovalMatch: wrap(logMissionApprovalMatch),
    logMissionStageExecution: wrap(logMissionStageExecution),
    logFallbackReason: wrap(logFallbackReason),
    list() {
      return localLog.map((row) => ({ ...row }));
    },
    clear() {
      localLog.length = 0;
    },
  };
}

module.exports = {
  PIPELINES,
  BREAKPOINT_LEGACY_MISSION_FIRST,
  logOperatorApprovalRoutingEvent,
  logWorkspaceEntry,
  logActiveMissionFound,
  logMissionPendingDecision,
  logApprovalClassifier,
  logOwnerSelected,
  logPipelineSelected,
  logMissionApprovalMatch,
  logMissionStageExecution,
  logFallbackReason,
  emitApprovalRoutingContext,
  listOperatorApprovalRoutingAuditLog,
  clearOperatorApprovalRoutingAuditLog,
  createOperatorApprovalRoutingAudit,
};
