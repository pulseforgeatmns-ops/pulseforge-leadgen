'use strict';

/**
 * SPEC-129 — Mission runtime dispatch.
 * Owner selects the Mission domain. Mission type selects the runtime.
 * No runtime may preempt another because it was evaluated first.
 */

const { isActiveMissionStatus } = require('../../mission-engine/types');
const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');
const { resolveAcquisitionActiveMission, UNRESOLVED_BOUND_MISSION_REASON } = require('./ActiveMissionGuard');
const { isMissionPlanningTurn } = require('./MissionPlanningTurn');
const { sessionStateBlocksExecution } = require('./SessionState');
const askPathTrace = require('./audit/AskPathTrace');

const MISSION_RUNTIMES = Object.freeze({
  AMO: 'AMO',
  SPEC_022: 'SPEC-022',
});

const MISSION_TYPES = Object.freeze({
  ACQUISITION: 'acquisition',
  LEGACY: 'legacy',
});

/** @type {object[]} */
const _auditLog = [];

function logMissionRuntimeEvent(event, payload = {}) {
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

/**
 * @param {object} payload
 * @param {'AMO'|'SPEC-022'|null} payload.runtime
 * @param {string} [payload.reason]
 * @param {string} [payload.missionId]
 * @param {string} [payload.missionType]
 */
function normalizeRuntime(runtime) {
  if (runtime === MISSION_RUNTIMES.AMO) return MISSION_RUNTIMES.AMO;
  if (runtime === MISSION_RUNTIMES.SPEC_022) return MISSION_RUNTIMES.SPEC_022;
  return null;
}

function logMissionRuntimeSelected(payload = {}) {
  const runtime = normalizeRuntime(payload.runtime);
  return logMissionRuntimeEvent('MISSION_RUNTIME_SELECTED', {
    runtime,
    reason: payload.reason || null,
    missionId: payload.missionId || null,
    missionType: payload.missionType || null,
    question: payload.question || null,
  });
}

function listMissionRuntimeAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearMissionRuntimeAuditLog() {
  _auditLog.length = 0;
}

function isAmoMission(mission) {
  return Boolean(mission && mission.stage && mission.stage !== 'improve');
}

function hasPendingOperatorDecision(mission) {
  return Boolean(mission && mission.pendingOperatorDecision);
}

function sessionBoundToAmo(session, amoMission) {
  if (!session || !amoMission) return false;
  const ctx = (session.context && typeof session.context === 'object' && session.context) || {};
  const id = amoMission.id;
  return ctx.acquisitionMissionId === id || ctx.missionId === id;
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function resolveLegacyActiveMission(input = {}) {
  if (
    input.missionsEnabled === false ||
    !input.missionEngine ||
    !input.missionEngine.activeMissionResolver ||
    input.resolverEnabled === false ||
    !input.session ||
    !input.session.id
  ) {
    return null;
  }
  const legacy = await input.missionEngine.activeMissionResolver.resolveActiveMission(
    input.session.id
  );
  if (legacy && isActiveMissionStatus(legacy.status)) return legacy;
  return null;
}

/**
 * Determine which Mission runtime owns this turn.
 * Both implementations are resolved, then one is selected by type.
 *
 * @param {object} input
 * @returns {Promise<{ runtime: 'AMO'|'SPEC-022'|null, reason: string, missionType: string|null, mission: object|null, amoMission: object|null, legacyMission: object|null }>}
 */
async function resolveMissionRuntime(input = {}) {
  askPathTrace.traceEnter('resolveMissionRuntime');
  const question = String(input.question || '').trim();
  const operatorIntent = input.operatorIntent || null;
  const conversationContract =
    input.conversationContract ||
    (operatorIntent && operatorIntent.conversationContract) ||
    null;
  const sessionState =
    input.sessionState ||
    (operatorIntent && operatorIntent.sessionState) ||
    (input.session && input.session.sessionState) ||
    null;

  if (sessionState && sessionStateBlocksExecution(sessionState)) {
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'session_state_read_only');
    return {
      runtime: null,
      reason: 'session_state_read_only',
      missionType: null,
      mission: null,
      amoMission: null,
      legacyMission: null,
      readOnly: true,
      sessionState,
      sessionExecutionPolicy: sessionState.executionPolicy,
    };
  }

  if (conversationContract && conversationContract.executionAllowed === false) {
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'conversation_contract_read_only');
    return {
      runtime: null,
      reason: 'conversation_contract_read_only',
      missionType: null,
      mission: null,
      amoMission: null,
      legacyMission: null,
      readOnly: true,
      conversationContract,
    };
  }

  const amoResolution = await resolveAcquisitionActiveMission(input);
  const amoMission = amoResolution.mission;
  const unresolvedBoundMissionId = amoResolution.unresolvedBoundMissionId;
  const legacyMission = await resolveLegacyActiveMission(input);
  const amoActive = isAmoMission(amoMission);
  const legacyActive = Boolean(legacyMission);
  const executionCommand = operatorIntent
    ? operatorIntent.executionRequested || operatorIntent.planningRequested
    : isMissionExecutionCommand(question) || isMissionPlanningTurn(amoMission, question);

  if (unresolvedBoundMissionId) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.AMO, UNRESOLVED_BOUND_MISSION_REASON, {
      boundMissionId: unresolvedBoundMissionId,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', UNRESOLVED_BOUND_MISSION_REASON);
    return {
      runtime: MISSION_RUNTIMES.AMO,
      reason: UNRESOLVED_BOUND_MISSION_REASON,
      missionType: MISSION_TYPES.ACQUISITION,
      mission: null,
      amoMission: null,
      legacyMission,
      unresolvedBoundMission: true,
      boundMissionId: unresolvedBoundMissionId,
    };
  }

  if (
    amoActive &&
    hasPendingOperatorDecision(amoMission) &&
    (executionCommand ||
      (operatorIntent
        ? operatorIntent.planningRequested
        : isMissionPlanningTurn(amoMission, question)))
  ) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.AMO, 'amo_pending_approval', {
      missionId: amoMission && amoMission.id,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'amo_pending_approval');
    return {
      runtime: MISSION_RUNTIMES.AMO,
      reason: 'amo_pending_approval',
      missionType: MISSION_TYPES.ACQUISITION,
      mission: amoMission,
      amoMission,
      legacyMission,
    };
  }

  if (amoActive && !legacyActive) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.AMO, 'amo_mission', {
      missionId: amoMission && amoMission.id,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'amo_mission');
    return {
      runtime: MISSION_RUNTIMES.AMO,
      reason: 'amo_mission',
      missionType: MISSION_TYPES.ACQUISITION,
      mission: amoMission,
      amoMission,
      legacyMission,
    };
  }

  if (legacyActive && !amoActive) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.SPEC_022, 'legacy_mission', {
      missionId: legacyMission && legacyMission.id,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'legacy_mission');
    return {
      runtime: MISSION_RUNTIMES.SPEC_022,
      reason: 'legacy_mission',
      missionType: MISSION_TYPES.LEGACY,
      mission: legacyMission,
      amoMission,
      legacyMission,
    };
  }

  if (amoActive && sessionBoundToAmo(input.session, amoMission)) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.AMO, 'session_bound_amo', {
      missionId: amoMission && amoMission.id,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'session_bound_amo');
    return {
      runtime: MISSION_RUNTIMES.AMO,
      reason: 'session_bound_amo',
      missionType: MISSION_TYPES.ACQUISITION,
      mission: amoMission,
      amoMission,
      legacyMission,
    };
  }

  if (legacyActive) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.SPEC_022, 'legacy_mission', {
      missionId: legacyMission && legacyMission.id,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'legacy_mission');
    return {
      runtime: MISSION_RUNTIMES.SPEC_022,
      reason: 'legacy_mission',
      missionType: MISSION_TYPES.LEGACY,
      mission: legacyMission,
      amoMission,
      legacyMission,
    };
  }

  if (amoActive) {
    askPathTrace.traceRuntime(MISSION_RUNTIMES.AMO, 'amo_mission', {
      missionId: amoMission && amoMission.id,
    });
    askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'amo_mission');
    return {
      runtime: MISSION_RUNTIMES.AMO,
      reason: 'amo_mission',
      missionType: MISSION_TYPES.ACQUISITION,
      mission: amoMission,
      amoMission,
      legacyMission,
    };
  }

  askPathTrace.traceRuntime(null, 'no_active_mission');
  askPathTrace.traceEarlyReturn('resolveMissionRuntime', 'no_active_mission');
  return {
    runtime: null,
    reason: 'no_active_mission',
    missionType: null,
    mission: null,
    amoMission: null,
    legacyMission: null,
  };
}

module.exports = {
  MISSION_RUNTIMES,
  MISSION_TYPES,
  logMissionRuntimeSelected,
  listMissionRuntimeAuditLog,
  clearMissionRuntimeAuditLog,
  resolveLegacyActiveMission,
  resolveMissionRuntime,
  hasPendingOperatorDecision,
  sessionBoundToAmo,
  isAmoMission,
  normalizeRuntime,
};
