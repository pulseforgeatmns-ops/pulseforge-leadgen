'use strict';

/**
 * Self-contained mission action builders.
 * Actions must carry everything needed to execute without response metadata.
 */

const { MISSION_RUNTIMES, normalizeRuntime } = require('./MissionRuntimeDispatch');

/**
 * Resolve canonical mission runtime for action routing.
 * @param {'AMO'|'SPEC-022'|string|null|undefined} runtime
 * @returns {'AMO'|'SPEC-022'}
 */
function resolveMissionActionRuntime(runtime) {
  return normalizeRuntime(runtime) || MISSION_RUNTIMES.SPEC_022;
}

/**
 * Build a self-contained open_mission action.
 * @param {object} input
 * @param {string} input.missionId
 * @param {'AMO'|'SPEC-022'|string|null|undefined} [input.runtime]
 * @param {string} [input.label]
 * @param {string} [input.id]
 */
function buildOpenMissionAction({
  missionId,
  runtime,
  label = 'Open Mission Workspace',
  id = 'open_mission',
} = {}) {
  if (!missionId) {
    throw new Error('missionId is required for open_mission action');
  }
  return {
    id,
    type: 'open_mission',
    label,
    payload: {
      missionId: String(missionId),
      runtime: resolveMissionActionRuntime(runtime),
    },
  };
}

module.exports = {
  MISSION_RUNTIMES,
  resolveMissionActionRuntime,
  buildOpenMissionAction,
};
