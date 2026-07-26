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
const {
  BriefingEngine,
  createBriefingEngine,
  BriefingBuilder,
  DigestBuilder,
  Prioritizer,
  PresentationAdapter,
  StructuredPresentationAdapter,
  MarkdownPresentationAdapter,
  createPresentationAdapter,
  BRIEFING_PERIODS,
  BRIEFING_SECTIONS,
  BRIEFING_PERFORMANCE_TARGET_MS,
  RISK_KIND,
} = require('./briefing');
const {
  PolicyEngine,
  createPolicyEngine,
  TenantPolicyStore,
  PolicyAuditLog,
  createDefaultRuleRegistry,
  ConfidenceRule,
  ContradictionRule,
  TenantPolicyRule,
  RiskRule,
  CooldownRule,
  ContactRule,
  EvidenceFreshnessRule,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  RULE_IDS,
  DEFAULT_TENANT_POLICY,
  canAutonomousExecute,
  approvalRequired,
} = require('./policy');
const {
  CommandDeckComposer,
  createCommandDeckComposer,
  CARD_TYPES,
  ACTION_TYPES,
  COMMAND_DECK_PERFORMANCE_TARGET_MS,
  buildIntelligenceCard,
  buildBriefingId,
  EMPTY_CATALOG,
} = require('./commandDeck');

/**
 * Create a Max reasoning + memory + briefing + policy + command-deck runtime.
 * Runtime agents remain unwired — library entrypoint only.
 *
 * @param {object} [options]
 * @param {import('../knowledge/services/KnowledgeService').KnowledgeService} [options.knowledge]
 * @param {boolean} [options.withSync=false]
 * @param {import('./memory/snapshots/SnapshotRepository').SnapshotRepository} [options.snapshotRepository]
 * @param {object} [options.tenantPolicies] - map of tenantId → policy config
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
  const briefing = createBriefingEngine({ knowledge, memory });
  const policy = createPolicyEngine();
  if (options.tenantPolicies && typeof options.tenantPolicies === 'object') {
    for (const tenantId of Object.keys(options.tenantPolicies).sort()) {
      policy.configureTenant(tenantId, options.tenantPolicies[tenantId]);
    }
  }
  const commandDeck = createCommandDeckComposer({ briefing, policy });
  return {
    knowledge,
    engine,
    memory,
    briefing,
    policy,
    commandDeck,
    runtime,
    evaluate: (input) => engine.evaluate(input),
    remember: (input) => memory.remember(input),
    brief: (input) => briefing.brief(input),
    decide: (input) => policy.evaluate(input),
    compose: (input) => commandDeck.compose(input),
  };
}

module.exports = {
  createMaxReasoningRuntime,
  createReasoningEngine,
  createMemoryEngine,
  createBriefingEngine,
  createPolicyEngine,
  createCommandDeckComposer,
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
  BriefingEngine,
  BriefingBuilder,
  DigestBuilder,
  Prioritizer,
  PresentationAdapter,
  StructuredPresentationAdapter,
  MarkdownPresentationAdapter,
  createPresentationAdapter,
  PolicyEngine,
  TenantPolicyStore,
  PolicyAuditLog,
  CommandDeckComposer,
  createDefaultRuleRegistry,
  ConfidenceRule,
  ContradictionRule,
  TenantPolicyRule,
  RiskRule,
  CooldownRule,
  ContactRule,
  EvidenceFreshnessRule,
  canAutonomousExecute,
  approvalRequired,
  STRATEGY_IDS,
  DEFAULT_STRATEGY_WEIGHTS,
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  PERFORMANCE_TARGET_MS,
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
  BRIEFING_PERIODS,
  BRIEFING_SECTIONS,
  BRIEFING_PERFORMANCE_TARGET_MS,
  RISK_KIND,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  RULE_IDS,
  DEFAULT_TENANT_POLICY,
  CARD_TYPES,
  ACTION_TYPES,
  COMMAND_DECK_PERFORMANCE_TARGET_MS,
  buildIntelligenceCard,
  buildBriefingId,
  EMPTY_CATALOG,
};
