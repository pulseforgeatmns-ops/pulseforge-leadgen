'use strict';

const { OUTCOME_RESULTS, LIFECYCLE } = require('./OutcomeTypes');
const { buildCalibrationReport } = require('./CalibrationReport');
const { buildStrategyPerformance } = require('./StrategyPerformance');
const { detectDrift } = require('./DriftDetector');

/**
 * Internal Intelligence Review dashboard (SPEC-013).
 * Pulseforge's own command deck — not customer-facing.
 *
 * Recommendation Success
 *   ↓
 * Strategy Performance
 *   ↓
 * Confidence Calibration
 *   ↓
 * Operator Behavior
 *   ↓
 * System Drift
 */

/**
 * @param {object} input
 * @param {object[]} input.records
 * @param {object} [input.operatorQuality]
 * @param {object} [input.driftOptions]
 */
function buildReviewDashboard(input = {}) {
  const records = input.records || [];
  const calibration = buildCalibrationReport({ records });
  const strategies = buildStrategyPerformance({ records });
  const drift = detectDrift({
    records,
    operatorQuality: input.operatorQuality,
    ...(input.driftOptions || {}),
  });

  const successful = records.filter(
    (r) => r.outcome === OUTCOME_RESULTS.SUCCESSFUL
  );
  const unsuccessful = records.filter(
    (r) => r.outcome === OUTCOME_RESULTS.UNSUCCESSFUL
  );
  const inconclusive = records.filter(
    (r) => r.outcome === OUTCOME_RESULTS.INCONCLUSIVE
  );
  const executed = records.filter((r) => r.executed);
  const observed = records.filter((r) => r.outcome != null);
  const decisive = successful.length + unsuccessful.length;

  const recommendationSuccess = {
    generated: records.length,
    reviewed: records.filter(
      (r) => stageAtLeast(r.lifecycle, LIFECYCLE.REVIEWED)
    ).length,
    approved: records.filter(
      (r) => stageAtLeast(r.lifecycle, LIFECYCLE.APPROVED)
    ).length,
    executed: executed.length,
    observed: observed.length,
    successful: successful.length,
    unsuccessful: unsuccessful.length,
    inconclusive: inconclusive.length,
    successRate:
      decisive === 0 ? null : round3(successful.length / decisive),
    executionRate:
      records.length === 0
        ? null
        : round3(executed.length / records.length),
  };

  const operatorBehavior = input.operatorQuality
    ? {
        recommendationAcceptanceRate:
          input.operatorQuality.recommendationAcceptanceRate,
        averageInvestigationDepth:
          input.operatorQuality.averageInvestigationDepth,
        averageTimeToDecisionMs:
          input.operatorQuality.averageTimeToDecisionMs,
        maxUsage: input.operatorQuality.maxUsage,
        averageTrustScore: input.operatorQuality.averageTrustScore,
        totals: input.operatorQuality.totals || null,
        source: 'operator_quality',
      }
    : {
        source: 'unavailable',
        note: 'Pass operatorQuality from OperatorEngine.quality() when available',
      };

  return {
    generatedAt: new Date().toISOString(),
    internal: true,
    customerFacing: false,
    mutatesReasoning: false,
    mutatesConfidence: false,
    sections: {
      recommendationSuccess,
      strategyPerformance: strategies,
      confidenceCalibration: calibration,
      operatorBehavior,
      systemDrift: drift,
    },
  };
}

function stageAtLeast(lifecycle, stage) {
  const order = [
    LIFECYCLE.GENERATED,
    LIFECYCLE.REVIEWED,
    LIFECYCLE.APPROVED,
    LIFECYCLE.EXECUTED,
    LIFECYCLE.OBSERVED,
    LIFECYCLE.SUCCESSFUL,
    LIFECYCLE.UNSUCCESSFUL,
    LIFECYCLE.INCONCLUSIVE,
  ];
  const a = order.indexOf(lifecycle);
  const b = order.indexOf(stage);
  if (a < 0 || b < 0) return false;
  // Terminal outcomes count as past observed
  if (
    lifecycle === LIFECYCLE.SUCCESSFUL ||
    lifecycle === LIFECYCLE.UNSUCCESSFUL ||
    lifecycle === LIFECYCLE.INCONCLUSIVE
  ) {
    return b <= order.indexOf(LIFECYCLE.OBSERVED);
  }
  return a >= b;
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

module.exports = { buildReviewDashboard };
