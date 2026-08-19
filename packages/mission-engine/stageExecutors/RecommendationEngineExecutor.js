'use strict';

/**
 * AUDIT-003 — Explicit advisory fallback when no stage executor is registered.
 * Never silent: always logs MISSION_EXECUTOR_FALLBACK before returning.
 */

const EXECUTOR_ID = 'RecommendationEngine';

/**
 * @param {object} input
 * @param {object} input.mission
 * @param {object} [input.stage]
 * @param {string} [input.fallbackReason]
 * @returns {Promise<object>}
 */
async function executeRecommendationFallback(input) {
  const { mission, stage, fallbackReason } = input;
  const stageName =
    (stage && (stage.stageName || stage.stageId)) || 'unknown';

  return {
    executorId: EXECUTOR_ID,
    success: false,
    outcome: 'fallback_advisory',
    advisory: true,
    mission,
    fallbackReason: fallbackReason || 'No executor registered',
    summary: [
      `Stage ${stageName} has no registered stage executor.`,
      'Returning advisory response instead of Scout execution.',
      'Register ScoutDiscoveryExecutor for Discovery (prospect_discovery) to execute.',
    ].join(' '),
    invocation: { attempted: false, skipped: true, reason: fallbackReason },
  };
}

module.exports = {
  EXECUTOR_ID,
  executeRecommendationFallback,
};
