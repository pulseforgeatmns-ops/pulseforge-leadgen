'use strict';

/**
 * ADR-089 / SPEC-170 — Mission runtime ownership boundaries.
 *
 * A mission identifier is meaningful only inside the runtime that owns it.
 * Specialists execute within that authority; they do not establish their own.
 */

const { STAGE_ORDER, amoError } = require('./types');

const RUNTIME_OWNERS = Object.freeze({
  AMO: 'amo',
  MISSION_ENGINE: 'mission_engine',
});

const MISSION_RUNTIME_BOUNDARY_VIOLATION = 'MISSION_RUNTIME_BOUNDARY_VIOLATION';

function asText(value) {
  return value == null ? '' : String(value).trim();
}

/**
 * Whether a mission id was minted by the Acquisition Mission Runtime.
 * @param {string} missionId
 */
function isAmoMissionId(missionId) {
  const id = asText(missionId);
  return id.startsWith('mission_');
}

/**
 * Whether a mission record belongs to the Acquisition Mission Runtime (SPEC-118).
 * @param {object|null|undefined} mission
 */
function isAmoMissionRecord(mission) {
  if (!mission || typeof mission !== 'object') return false;
  if (mission.runtimeOwner === RUNTIME_OWNERS.AMO) return true;
  if (mission.structuredMissionApproved === true) return true;
  if (mission.structuredMission) return true;
  if (mission.missionPlanDraft) return true;
  if (mission.pendingOperatorDecision) return true;
  if (STAGE_ORDER.includes(asText(mission.stage).toLowerCase())) return true;
  if (isAmoMissionId(mission.id)) return true;
  return false;
}

/**
 * Resolve which runtime owns a mission record or id.
 * @param {object|string} missionOrId
 * @returns {'amo'|'mission_engine'|null}
 */
function resolveMissionRuntimeOwner(missionOrId) {
  if (missionOrId == null) return null;
  if (typeof missionOrId === 'string') {
    return isAmoMissionId(missionOrId) ? RUNTIME_OWNERS.AMO : RUNTIME_OWNERS.MISSION_ENGINE;
  }
  if (isAmoMissionRecord(missionOrId)) return RUNTIME_OWNERS.AMO;
  if (missionOrId.plan && Array.isArray(missionOrId.plan.steps)) {
    return RUNTIME_OWNERS.MISSION_ENGINE;
  }
  if (missionOrId.status && !STAGE_ORDER.includes(asText(missionOrId.stage).toLowerCase())) {
    return RUNTIME_OWNERS.MISSION_ENGINE;
  }
  return null;
}

/**
 * Whether Scout (or another specialist) may sync state into SPEC-022 Mission Engine.
 * @param {object} input
 * @param {object} [input.mission]
 * @param {object} [input.missionEngine]
 * @param {string} [input.amoMissionId]
 * @param {string} [input.runtimeOwner]
 */
function maySyncToMissionEngine(input = {}) {
  const { mission, missionEngine, amoMissionId, runtimeOwner } = input;
  if (!missionEngine) return false;
  if (runtimeOwner === RUNTIME_OWNERS.AMO) return false;
  if (amoMissionId) return false;
  if (isAmoMissionRecord(mission)) return false;
  return resolveMissionRuntimeOwner(mission) !== RUNTIME_OWNERS.AMO;
}

/**
 * Fail closed when a caller attempts cross-runtime mission resolution.
 * @param {object} input
 * @param {object} [input.mission]
 * @param {object} [input.missionEngine]
 * @param {string} [input.expectedOwner]
 * @param {string} [input.operation]
 */
function assertMissionRuntimeBoundary(input = {}) {
  const { mission, missionEngine, expectedOwner, operation } = input;
  const owner = resolveMissionRuntimeOwner(mission);
  const op = asText(operation) || 'mission execution';

  if (expectedOwner === RUNTIME_OWNERS.AMO && missionEngine && owner === RUNTIME_OWNERS.AMO) {
    throw runtimeBoundaryError(
      `Cannot ${op}: Acquisition Mission ${mission && mission.id} must not be resolved through Mission Engine.`,
      { missionId: mission && mission.id, owner, attemptedRuntime: RUNTIME_OWNERS.MISSION_ENGINE }
    );
  }

  if (expectedOwner === RUNTIME_OWNERS.MISSION_ENGINE && owner === RUNTIME_OWNERS.AMO) {
    throw runtimeBoundaryError(
      `Cannot ${op}: Acquisition Mission identifier ${mission && mission.id} is not a Mission Engine mission.`,
      { missionId: mission && mission.id, owner, attemptedRuntime: RUNTIME_OWNERS.AMO }
    );
  }

  return { owner, ok: true };
}

function runtimeBoundaryError(message, extras = {}) {
  const err = amoError(MISSION_RUNTIME_BOUNDARY_VIOLATION, message);
  Object.assign(err, extras);
  return err;
}

/**
 * Guard used by Scout.discover — returns sync policy for the invocation.
 * @param {object} input
 */
function resolveScoutDiscoveryRuntimePolicy(input = {}) {
  const { mission, missionEngine, opts = {} } = input;
  const amoMissionId = asText(opts.amoMissionId || opts.missionId);
  const runtimeOwner = asText(opts.runtimeOwner) || null;
  const amoOwned =
    runtimeOwner === RUNTIME_OWNERS.AMO ||
    Boolean(amoMissionId && isAmoMissionId(amoMissionId)) ||
    isAmoMissionRecord(mission);

  const syncToMissionEngine = maySyncToMissionEngine({
    mission,
    missionEngine,
    amoMissionId: amoOwned ? (amoMissionId || (mission && mission.id)) : null,
    runtimeOwner: amoOwned ? RUNTIME_OWNERS.AMO : runtimeOwner,
  });

  const attachViaLegacyFacade =
    opts.attachScoutDiscovery !== false &&
    !amoOwned &&
    Boolean(amoMissionId || (mission && mission.id));

  return {
    owner: amoOwned ? RUNTIME_OWNERS.AMO : RUNTIME_OWNERS.MISSION_ENGINE,
    amoOwned,
    syncToMissionEngine,
    attachViaLegacyFacade,
    amoMissionId: amoOwned ? (amoMissionId || (mission && mission.id) || null) : null,
  };
}

module.exports = {
  RUNTIME_OWNERS,
  MISSION_RUNTIME_BOUNDARY_VIOLATION,
  isAmoMissionId,
  isAmoMissionRecord,
  resolveMissionRuntimeOwner,
  maySyncToMissionEngine,
  assertMissionRuntimeBoundary,
  resolveScoutDiscoveryRuntimePolicy,
};
