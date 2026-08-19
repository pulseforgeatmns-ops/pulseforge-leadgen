'use strict';

/**
 * AUDIT-003 — Stage resolution → executor selection → invocation.
 * Answers: which executor owns this stage? Was it selected? Invoked? If not, why?
 */

const { getStage } = require('./StageLibrary');
const { BUILTIN_IDS } = require('../capabilities/types');
const {
  selectExecutorForStage,
  EXECUTOR_IDS,
} = require('./stageExecutors/StageExecutorRegistry');
const {
  executeScoutDiscovery,
} = require('./stageExecutors/ScoutDiscoveryExecutor');
const {
  executeRecommendationFallback,
} = require('./stageExecutors/RecommendationEngineExecutor');
const {
  logMissionStage,
  logMissionExecutorSelected,
  logMissionExecutorInvoked,
  logMissionExecutorResult,
  logMissionExecutorFallback,
} = require('./MissionStageAudit');

/**
 * Resolve the active stage for a Mission.
 * @param {object} mission
 * @returns {{ stageId: string|null, stageName: string|null, capabilityId: string|null, source: string, confidence: number }}
 */
function resolveCurrentStage(mission) {
  const steps = (mission && mission.plan && mission.plan.steps) || [];

  const executing = steps.find(
    (s) => s.status === 'running' || s.status === 'executing'
  );
  if (executing) {
    const def = getStage(executing.stageId);
    return {
      stageId: executing.stageId || null,
      stageName: executing.stageLabel || (def && def.name) || executing.name || null,
      capabilityId: executing.capabilityId || null,
      source: 'plan_step_executing',
      confidence: 0.95,
    };
  }

  const pending = steps.find(
    (s) =>
      s.status === 'queued' ||
      s.status === 'stale' ||
      s.status === 'failed' ||
      s.status === 'blocked'
  );
  if (pending) {
    const def = getStage(pending.stageId);
    return {
      stageId: pending.stageId || null,
      stageName: pending.stageLabel || (def && def.name) || pending.name || null,
      capabilityId: pending.capabilityId || null,
      source: 'plan_step_pending',
      confidence: 0.92,
    };
  }

  if (mission.progress && mission.progress.currentCapabilityId) {
    const capId = mission.progress.currentCapabilityId;
    const step = steps.find((s) => s.capabilityId === capId) || null;
    const def = step && step.stageId ? getStage(step.stageId) : null;
    return {
      stageId: (step && step.stageId) || null,
      stageName:
        mission.progress.currentStage ||
        (def && def.name) ||
        (step && step.name) ||
        null,
      capabilityId: capId,
      source: 'progress_current_capability',
      confidence: 0.88,
    };
  }

  if (mission.status === 'planning' && steps.length) {
    const first = steps[0];
    const def = getStage(first.stageId);
    return {
      stageId: first.stageId || null,
      stageName: first.stageLabel || (def && def.name) || first.name || null,
      capabilityId: first.capabilityId || null,
      source: 'plan_first_step',
      confidence: 0.85,
    };
  }

  const stageLabel = String(
    (mission.progress && mission.progress.currentStage) || ''
  );
  if (/discover/i.test(stageLabel)) {
    return {
      stageId: 'prospect_discovery',
      stageName: 'Discovery',
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      source: 'progress_stage_label',
      confidence: 0.78,
    };
  }

  return {
    stageId: null,
    stageName: stageLabel || null,
    capabilityId: null,
    source: 'unresolved',
    confidence: 0,
  };
}

/**
 * @param {object} input
 * @param {object} input.mission
 * @param {import('./MissionEngine').MissionEngine} input.missionEngine
 * @param {string} [input.operatorId]
 * @param {string} [input.message]
 * @returns {Promise<object>}
 */
async function executeCurrentStage(input) {
  const { mission, missionEngine, operatorId, message } = input;
  const stage = resolveCurrentStage(mission);

  logMissionStage({
    missionId: mission.id,
    stage: stage.stageName || stage.stageId,
    stageId: stage.stageId,
    stageName: stage.stageName,
    stageSource: stage.source,
    stageConfidence: stage.confidence,
    capabilityId: stage.capabilityId,
  });

  const { executorId, selectionReason } = selectExecutorForStage(stage);

  logMissionExecutorSelected({
    missionId: mission.id,
    stage: stage.stageName || stage.stageId,
    executor: executorId,
    selectionReason,
  });

  if (!executorId) {
    logMissionExecutorFallback({
      missionId: mission.id,
      stage: stage.stageName || stage.stageId,
      reason: selectionReason,
      selected: EXECUTOR_IDS.RECOMMENDATION_ENGINE,
    });

    const fallback = await executeRecommendationFallback({
      mission,
      stage,
      fallbackReason: selectionReason,
    });

    logMissionExecutorResult({
      missionId: mission.id,
      stage: stage.stageName || stage.stageId,
      executor: EXECUTOR_IDS.RECOMMENDATION_ENGINE,
      outcome: fallback.outcome,
      success: fallback.success,
    });

    return {
      stage,
      executorId: null,
      selectionReason,
      fallback: true,
      result: fallback,
    };
  }

  logMissionExecutorInvoked({
    missionId: mission.id,
    stage: stage.stageName || stage.stageId,
    executor: executorId,
  });

  let result;
  try {
    if (executorId === EXECUTOR_IDS.SCOUT_DISCOVERY) {
      result = await executeScoutDiscovery({
        mission,
        missionEngine,
        operatorId,
        message,
      });
    } else {
      logMissionExecutorFallback({
        missionId: mission.id,
        stage: stage.stageName || stage.stageId,
        reason: `unknown_executor:${executorId}`,
        selected: EXECUTOR_IDS.RECOMMENDATION_ENGINE,
      });
      result = await executeRecommendationFallback({
        mission,
        stage,
        fallbackReason: `unknown_executor:${executorId}`,
      });
    }
  } catch (err) {
    logMissionExecutorResult({
      missionId: mission.id,
      stage: stage.stageName || stage.stageId,
      executor: executorId,
      outcome: 'exception',
      success: false,
      error: err.message || String(err),
    });
    throw err;
  }

  logMissionExecutorResult({
    missionId: mission.id,
    stage: stage.stageName || stage.stageId,
    executor: result.executorId || executorId,
    outcome: result.outcome,
    success: result.success,
    scoutPayload: result.scoutPayload || null,
  });

  return {
    stage,
    executorId,
    selectionReason,
    fallback: Boolean(result.advisory),
    result,
  };
}

function createStageExecutionOrchestrator(deps = {}) {
  return {
    resolveCurrentStage,
    executeCurrentStage,
  };
}

module.exports = {
  resolveCurrentStage,
  executeCurrentStage,
  createStageExecutionOrchestrator,
};
