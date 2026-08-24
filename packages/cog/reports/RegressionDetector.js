'use strict';

/**
 * Regression detection — surfaces score and failure regressions across COG runs.
 */

const REGRESSION_THRESHOLD = 0.5;

/**
 * @param {import('../types').CogRunResult} current
 * @param {import('../types').CogRunResult|null} baseline
 * @param {object} [options]
 * @returns {object}
 */
function detectRegressions(current, baseline, options = {}) {
  const threshold = options.threshold ?? REGRESSION_THRESHOLD;

  if (!baseline) {
    return {
      hasRegression: false,
      baselineRunId: null,
      message: 'No baseline run available for comparison',
      domainRegressions: [],
      overallRegression: null,
      newFailures: [],
    };
  }

  const domainRegressions = [];
  const newFailures = [];

  for (const domain of current.domains) {
    const baseDomain = baseline.domains.find(d => d.domainId === domain.domainId);
    if (!baseDomain) continue;

    if (domain.score !== null && baseDomain.score !== null) {
      const delta = domain.score - baseDomain.score;
      if (delta < -threshold) {
        domainRegressions.push({
          domainId: domain.domainId,
          baselineScore: baseDomain.score,
          currentScore: domain.score,
          delta,
          type: 'score_regression',
        });
      }
    }

    const baseFailureCodes = new Set(baseDomain.failures.map(f => f.code));
    for (const failure of domain.failures) {
      if (!baseFailureCodes.has(failure.code)) {
        newFailures.push({
          domainId: domain.domainId,
          failure,
          type: 'new_failure',
        });
      }
    }

    const basePassed = baseDomain.behaviorResults.filter(r => r.passed).length;
    const currentPassed = domain.behaviorResults.filter(r => r.passed).length;
    if (basePassed > currentPassed) {
      domainRegressions.push({
        domainId: domain.domainId,
        baselinePassed: basePassed,
        currentPassed,
        delta: currentPassed - basePassed,
        type: 'behavior_regression',
      });
    }
  }

  let overallRegression = null;
  if (current.overallScore !== null && baseline.overallScore !== null) {
    const delta = current.overallScore - baseline.overallScore;
    if (delta < -threshold) {
      overallRegression = {
        baselineScore: baseline.overallScore,
        currentScore: current.overallScore,
        delta,
      };
    }
  }

  const hasRegression = domainRegressions.length > 0
    || newFailures.length > 0
    || overallRegression !== null;

  return {
    hasRegression,
    baselineRunId: baseline.runId,
    baselineStartedAt: baseline.startedAt,
    domainRegressions,
    overallRegression,
    newFailures,
    message: hasRegression
      ? `Regression detected vs run ${baseline.runId}`
      : 'No regression detected',
  };
}

module.exports = {
  REGRESSION_THRESHOLD,
  detectRegressions,
};
