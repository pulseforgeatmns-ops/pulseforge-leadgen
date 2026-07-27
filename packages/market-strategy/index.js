'use strict';

const { createReasoningRuntime } = require('@pulseforge/reasoning-runtime');
const {
  MarketStrategyPack,
  createMarketStrategyPack,
} = require('./MarketStrategyPack');
const {
  MarketContextProvider,
  createMarketContextProvider,
} = require('./MarketContextProvider');
const {
  ResearchRecommendationProvider,
  createResearchRecommendationProvider,
} = require('./ResearchRecommendationProvider');
const {
  createMarketStrategyRegistry,
  aggregateMarketScores,
  findMarketAnalogs,
} = require('./registry');
const {
  MARKET_CLAIM_TYPES,
  RESEARCH_ACTIONS,
  FORBIDDEN_ACTIONS,
  DEFAULT_MARKET_WEIGHTS,
} = require('./types');

/**
 * Factory wiring for SPEC-016 acceptance criteria:
 *
 * const runtime = createMarketReasoningRuntime();
 * await runtime.evaluate({ subjectId: 'BTC' });
 */
function createMarketReasoningRuntime(deps = {}) {
  const recommendationProvider =
    deps.recommendationProvider || createResearchRecommendationProvider();
  const strategyPack =
    deps.strategyPack ||
    createMarketStrategyPack({
      registry: deps.registry || createMarketStrategyRegistry(),
      aggregator: deps.aggregator || aggregateMarketScores,
      recommendationProvider,
      analogFinder: deps.analogFinder || findMarketAnalogs,
    });
  const contextProvider =
    deps.contextProvider || createMarketContextProvider();

  return createReasoningRuntime({
    strategyPack,
    contextProvider,
    recommendationProvider,
    memory: deps.memory || null,
    performanceTargetMs: deps.performanceTargetMs,
    buildReport: deps.buildReport,
  });
}

module.exports = {
  createReasoningRuntime,
  createMarketReasoningRuntime,
  MarketStrategyPack,
  createMarketStrategyPack,
  MarketContextProvider,
  createMarketContextProvider,
  ResearchRecommendationProvider,
  createResearchRecommendationProvider,
  createMarketStrategyRegistry,
  aggregateMarketScores,
  findMarketAnalogs,
  MARKET_CLAIM_TYPES,
  RESEARCH_ACTIONS,
  FORBIDDEN_ACTIONS,
  DEFAULT_MARKET_WEIGHTS,
};
