'use strict';

/**
 * AUDIT-003 — Stage → executor registry.
 * Maps Mission stages to stage-level executors (not generic MissionExecutor).
 */

const EXECUTOR_IDS = Object.freeze({
  SCOUT_DISCOVERY: 'ScoutDiscoveryExecutor',
  RECOMMENDATION_ENGINE: 'RecommendationEngine',
});

/** Stage id → executor id */
const STAGE_EXECUTOR_MAP = Object.freeze({
  prospect_discovery: EXECUTOR_IDS.SCOUT_DISCOVERY,
});

/** Capability id → executor id (when stage id absent on step) */
const CAPABILITY_EXECUTOR_MAP = Object.freeze({
  prospect_discovery: EXECUTOR_IDS.SCOUT_DISCOVERY,
});

/**
 * @param {object} stage
 * @param {string|null} stage.stageId
 * @param {string|null} stage.capabilityId
 * @returns {{ executorId: string|null, selectionReason: string }}
 */
function selectExecutorForStage(stage) {
  const stageId = stage && stage.stageId ? String(stage.stageId) : null;
  const capabilityId = stage && stage.capabilityId ? String(stage.capabilityId) : null;

  if (stageId && STAGE_EXECUTOR_MAP[stageId]) {
    return {
      executorId: STAGE_EXECUTOR_MAP[stageId],
      selectionReason: `stage_registry:${stageId}`,
    };
  }

  if (capabilityId && CAPABILITY_EXECUTOR_MAP[capabilityId]) {
    return {
      executorId: CAPABILITY_EXECUTOR_MAP[capabilityId],
      selectionReason: `capability_registry:${capabilityId}`,
    };
  }

  return {
    executorId: null,
    selectionReason: 'no_executor_registered',
  };
}

function isRegisteredExecutor(executorId) {
  return Object.values(EXECUTOR_IDS).includes(executorId);
}

module.exports = {
  EXECUTOR_IDS,
  STAGE_EXECUTOR_MAP,
  CAPABILITY_EXECUTOR_MAP,
  selectExecutorForStage,
  isRegisteredExecutor,
};
