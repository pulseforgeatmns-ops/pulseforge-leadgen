'use strict';

/**
 * AUDIT-003 — ScoutDiscoveryExecutor owns the Discovery stage (prospect_discovery).
 * Delegates to MissionExecutor / prospect_discovery capability — never advisory prose.
 */

const { BUILTIN_IDS } = require('../../capabilities/types');
const { MISSION_STATUS, REVIEW_ACTIONS } = require('../types');

const EXECUTOR_ID = 'ScoutDiscoveryExecutor';

/**
 * @param {object} input
 * @param {object} input.mission
 * @param {import('../MissionEngine').MissionEngine} input.missionEngine
 * @param {string} [input.operatorId]
 * @param {string} [input.message]
 * @returns {Promise<object>}
 */
async function executeScoutDiscovery(input) {
  const { mission, missionEngine, operatorId, message } = input;
  if (!mission || !missionEngine) {
    throw new Error('ScoutDiscoveryExecutor requires mission and missionEngine');
  }

  const missionId = mission.id;
  const steps = (mission.plan && mission.plan.steps) || [];
  const discoveryIdx = steps.findIndex(
    (s) =>
      s.stageId === 'prospect_discovery' ||
      s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      s.capabilityId === 'prospect_discovery'
  );

  const payload = buildScoutDispatchPayload(mission, message);

  if (
    mission.status === MISSION_STATUS.PLANNING ||
    mission.status === MISSION_STATUS.REQUESTED
  ) {
    const updated = await missionEngine.executor.execute(missionId);
    return {
      executorId: EXECUTOR_ID,
      success: true,
      outcome: 'executed',
      mission: updated,
      scoutPayload: payload,
      invocation: { attempted: true, skipped: false },
    };
  }

  if (discoveryIdx >= 0) {
    const resetSteps = steps.map((s, idx) => {
      if (idx > discoveryIdx) {
        return { ...s, status: 'queued', error: undefined };
      }
      if (idx === discoveryIdx) {
        return { ...s, status: 'queued', error: undefined };
      }
      return s.status === 'completed'
        ? s
        : { ...s, status: 'completed', error: undefined };
    });

    await missionEngine.store.update({
      id: missionId,
      status: MISSION_STATUS.EXECUTING,
      plan: { ...mission.plan, steps: resetSteps },
      review: null,
    });

    const updated = await missionEngine.executor.execute(missionId);
    return {
      executorId: EXECUTOR_ID,
      success: true,
      outcome: 'discovery_re_executed',
      mission: updated,
      scoutPayload: payload,
      invocation: { attempted: true, skipped: false },
    };
  }

  const updated = await missionEngine.review({
    missionId,
    action: REVIEW_ACTIONS.RUN_AGAIN,
    actor: operatorId || null,
  });

  return {
    executorId: EXECUTOR_ID,
    success: true,
    outcome: 'run_again',
    mission: updated,
    scoutPayload: payload,
    invocation: { attempted: true, skipped: false },
  };
}

/**
 * @param {object} mission
 * @param {string} [message]
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

module.exports = {
  EXECUTOR_ID,
  executeScoutDiscovery,
  buildScoutDispatchPayload,
};
