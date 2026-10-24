'use strict';

const { round } = require('../../reasoning/ReasoningTypes');
const { BRIEFING_PERFORMANCE_TARGET_MS } = require('../BriefingTypes');

/**
 * Briefing build metrics — timing and assembly counts only.
 */
function buildMetricsSection(input) {
  const contexts = input.contexts || [];
  const executionTimeMs = round(Number(input.executionTimeMs) || 0);

  let memoryLookups = Number(input.memoryLookups) || 0;
  let queryCount = Number(input.queryCount) || 0;
  if (!input.memoryLookups) {
    memoryLookups = contexts.reduce(
      (n, c) => n + (Number(c.memoryLookups) || 0),
      0
    );
  }
  if (!input.queryCount) {
    queryCount = Number(input.baseQueryCount) || 0;
  }

  const recommendationCount = contexts.filter((c) => c.recommendation).length;
  const strategyIds = new Set();
  for (const c of contexts) {
    for (const s of (c.latest && c.latest.strategyResults) || []) {
      if (s.strategy) strategyIds.add(s.strategy);
    }
  }

  return {
    buildTimeMs: executionTimeMs,
    withinTarget: executionTimeMs <= BRIEFING_PERFORMANCE_TARGET_MS,
    performanceTargetMs: BRIEFING_PERFORMANCE_TARGET_MS,
    queryCount,
    recommendationCount,
    memoryLookups,
    strategyCount: strategyIds.size,
    companyCount: contexts.length,
    changeEventCount: contexts.reduce(
      (n, c) => n + (c.changes || []).length,
      0
    ),
    watchAlertCount: contexts.reduce(
      (n, c) => n + (c.triggeredWatches || []).length,
      0
    ),
    period: input.window ? input.window.period : null,
  };
}

module.exports = {
  buildMetricsSection,
};
