'use strict';

/**
 * AUDIT-003 — ScoutDiscoveryExecutor owns the Discovery stage.
 * SPEC-123 — Delegates to Scout.discover() — the canonical Discovery contract.
 * Operators never see implementation paths (prospect_discovery, acquisition intelligence).
 */

const { MISSION_STATUS } = require('../types');
const { Scout } = require('../../scout');
const { buildScoutDispatchPayload } = require('./ScoutDiscoveryExecutor.helpers');

const EXECUTOR_ID = 'ScoutDiscoveryExecutor';

/**
 * @param {object} input
 * @param {object} input.mission
 * @param {import('../MissionEngine').MissionEngine} input.missionEngine
 * @param {string} [input.operatorId]
 * @param {string} [input.message]
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function executeScoutDiscovery(input) {
  const { mission, missionEngine, operatorId, message, opts = {} } = input;
  if (!mission || !missionEngine) {
    throw new Error('ScoutDiscoveryExecutor requires mission and missionEngine');
  }

  const payload = buildScoutDispatchPayload(mission, message);

  const discoveryResult = await Scout.discover({
    mission,
    missionEngine,
    scoutPayload: payload,
    operatorId,
    message,
    opts,
  });

  const outcome =
    mission.status === MISSION_STATUS.PLANNING ||
    mission.status === MISSION_STATUS.REQUESTED
      ? 'executed'
      : discoveryResult.phases.some(
            (p) =>
              p.phase === 'external_discovery' &&
              p.result &&
              p.result.executed === true
          )
        ? 'discovery_re_executed'
        : 'executed';

  return {
    executorId: EXECUTOR_ID,
    success: true,
    outcome,
    mission: discoveryResult.mission,
    scoutPayload: payload,
    discoveryReport: discoveryResult.discoveryReport,
    discoveryResult,
    invocation: { attempted: true, skipped: false },
  };
}

module.exports = {
  EXECUTOR_ID,
  executeScoutDiscovery,
  buildScoutDispatchPayload,
};
