'use strict';

const { createKnowledgeRuntime } = require('../knowledge');
const { ReasoningEngine, createReasoningEngine } = require('./reasoning/ReasoningEngine');
const { ReasoningContextBuilder } = require('./context/ReasoningContextBuilder');
const {
  StrategyRegistry,
  createDefaultStrategyRegistry,
} = require('./strategies/StrategyRegistry');
const {
  OpportunityStrategy,
  EngagementStrategy,
  RelationshipStrategy,
  DecisionMakerStrategy,
  OverflowStrategy,
  TechnologyStrategy,
  RiskStrategy,
} = require('./strategies');
const { ScoreAggregator } = require('./aggregation/ScoreAggregator');
const { RecommendationBuilder } = require('./recommendations/RecommendationBuilder');
const { ExplanationEngine } = require('./explanations/ExplanationEngine');
const { buildReasoningReport } = require('./reports/ReasoningReport');
const {
  STRATEGY_IDS,
  DEFAULT_STRATEGY_WEIGHTS,
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  PERFORMANCE_TARGET_MS,
} = require('./reasoning/ReasoningTypes');
const {
  MemoryEngine,
  createMemoryEngine,
  SnapshotEngine,
  DiffEngine,
  ChangeDetector,
  TimelineBuilder,
  WatchRegistry,
  RecommendationEvolution,
  TemporalExplanationEngine,
  InMemorySnapshotRepository,
  SerializingSnapshotRepository,
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
} = require('./memory');

/**
 * Create a Max reasoning + memory runtime.
 * Runtime agents remain unwired — library entrypoint only.
 *
 * @param {object} [options]
 * @param {import('../knowledge/services/KnowledgeService').KnowledgeService} [options.knowledge]
 * @param {boolean} [options.withSync=false]
 * @param {import('./memory/snapshots/SnapshotRepository').SnapshotRepository} [options.snapshotRepository]
 */
function createMaxReasoningRuntime(options = {}) {
  let knowledge = options.knowledge;
  let runtime = null;
  if (!knowledge) {
    runtime = createKnowledgeRuntime({
      withSync: options.withSync === true,
      startIngestor: options.startIngestor !== false,
    });
    knowledge = runtime.knowledge;
  }
  const engine = createReasoningEngine({ knowledge });
  const memory = createMemoryEngine({
    reasoningEngine: engine,
    repository: options.snapshotRepository,
  });
  return {
    knowledge,
    engine,
    memory,
    runtime,
    evaluate: (input) => engine.evaluate(input),
    remember: (input) => memory.remember(input),
  };
}

module.exports = {
  createMaxReasoningRuntime,
  createReasoningEngine,
  createMemoryEngine,
  ReasoningEngine,
  ReasoningContextBuilder,
  StrategyRegistry,
  createDefaultStrategyRegistry,
  OpportunityStrategy,
  EngagementStrategy,
  RelationshipStrategy,
  DecisionMakerStrategy,
  OverflowStrategy,
  TechnologyStrategy,
  RiskStrategy,
  ScoreAggregator,
  RecommendationBuilder,
  ExplanationEngine,
  buildReasoningReport,
  MemoryEngine,
  SnapshotEngine,
  DiffEngine,
  ChangeDetector,
  TimelineBuilder,
  WatchRegistry,
  RecommendationEvolution,
  TemporalExplanationEngine,
  InMemorySnapshotRepository,
  SerializingSnapshotRepository,
  STRATEGY_IDS,
  DEFAULT_STRATEGY_WEIGHTS,
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  PERFORMANCE_TARGET_MS,
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
};
