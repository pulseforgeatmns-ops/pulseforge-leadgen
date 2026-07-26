'use strict';

const { round } = require('../../reasoning/ReasoningTypes');
const { CHANGE_TYPES } = require('../../memory/snapshots/MemoryTypes');

/**
 * Executive summary — high-level tenant state from assembled contexts.
 * Never invents metrics; counts existing company/memory signals.
 */
function buildExecutiveSummary(input) {
  const contexts = input.contexts || [];
  const window = input.window;
  const withMemory = contexts.filter((c) => c.latest);
  const withRec = contexts.filter((c) => c.recommendation);

  const priorityOpportunities = withRec.filter(
    (c) =>
      c.recommendation.priority === 'critical' ||
      c.recommendation.priority === 'high' ||
      c.recommendation.type === 'pursue'
  ).length;

  const newDecisionMakers = countChangeType(
    contexts,
    CHANGE_TYPES.NEW_DECISION_MAKER
  );
  const newHiring = countChangeType(contexts, CHANGE_TYPES.NEW_HIRING_SIGNAL);
  const newContradictions = countChangeType(
    contexts,
    CHANGE_TYPES.NEW_CONTRADICTION
  );

  const opportunityUp = countChangeType(
    contexts,
    CHANGE_TYPES.NEW_OPPORTUNITY_SIGNAL
  );
  const scoreUp = countChangeType(contexts, CHANGE_TYPES.SCORE_INCREASED);
  const scoreDown = countChangeType(contexts, CHANGE_TYPES.SCORE_DECREASED);
  const confUp = countChangeType(contexts, CHANGE_TYPES.CONFIDENCE_INCREASED);
  const confDown = countChangeType(contexts, CHANGE_TYPES.CONFIDENCE_DECREASED);

  const scores = withMemory.map((c) => c.latest.score);
  const confidences = withMemory.map((c) => c.latest.confidence);

  const trendUp = contexts.filter((c) => c.trend && c.trend.score === 'up').length;
  const trendDown = contexts.filter(
    (c) => c.trend && c.trend.score === 'down'
  ).length;

  return {
    period: window.period,
    asOf: window.asOf,
    windowStart: window.start,
    windowEnd: window.end,
    companiesMonitored: contexts.length,
    companiesWithMemory: withMemory.length,
    companiesWithRecommendations: withRec.length,
    priorityOpportunities,
    newDecisionMakers,
    newHiringSignals: newHiring,
    newContradictions,
    opportunityTrend: {
      newOpportunitySignals: opportunityUp,
      scoreIncreased: scoreUp,
      scoreDecreased: scoreDown,
      companiesTrendingUp: trendUp,
      companiesTrendingDown: trendDown,
    },
    confidenceTrend: {
      confidenceIncreased: confUp,
      confidenceDecreased: confDown,
      averageConfidence: average(confidences),
    },
    averageScore: average(scores),
    watchAlertsTriggered: contexts.reduce(
      (n, c) => n + (c.triggeredWatches || []).length,
      0
    ),
    changeEventCount: contexts.reduce((n, c) => n + (c.changes || []).length, 0),
  };
}

function countChangeType(contexts, type) {
  let n = 0;
  for (const c of contexts) {
    for (const ch of c.changes || []) {
      if (ch.type === type) n += 1;
    }
  }
  return n;
}

function average(nums) {
  if (!nums || nums.length === 0) return null;
  const sum = nums.reduce((a, b) => a + Number(b), 0);
  return round(sum / nums.length);
}

module.exports = {
  buildExecutiveSummary,
};
