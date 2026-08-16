'use strict';

const { createKnowledgeRuntime } = require('../knowledge');
const {
  ReasoningEngine,
  createReasoningEngine,
  createCrmReasoningRuntime,
} = require('./reasoning/ReasoningEngine');
const { ReasoningContextBuilder } = require('./context/ReasoningContextBuilder');
const {
  createReasoningRuntime,
  CRMStrategyPack,
  createCRMStrategyPack,
  CRMContextProvider,
  createCRMContextProvider,
  NextBestActionProvider,
  createNextBestActionProvider,
  assertStrategyPack,
  assertContextProvider,
  assertRecommendationProvider,
} = require('../reasoning-runtime');
const {
  StrategyRegistry,
  createDefaultStrategyRegistry,
  createCRMStrategyRegistry,
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
const {
  WorkspaceEngine,
  createWorkspaceEngine,
  PAGE_TYPES,
  SessionStore,
  PresentationEngine,
  composeResponse,
  buildOpeningState,
  buildSuggestions,
  normalizeContext,
  EXECUTION_DOMAINS,
  selectExecutionDomain,
  isMissionDomain,
} = require('./workspace');
const {
  createSpecialistDelegationService,
  createMemoryStore: createSpecialistDelegationMemoryStore,
  CONTRACT_OBJECTIVE,
  AUTHORITY_LEVELS,
} = require('./specialistDelegation');
const {
  IntelligenceComposer,
  createIntelligenceComposer,
  RelatedIntelligenceBuilder,
  RecommendationDetailComposer,
  CompanyIntelligenceComposer,
  NAV_TYPES,
  TRAIL_KINDS,
  buildNavRef,
  parseRecommendationId,
  buildRecommendationId,
  pushTrail,
  popTrailTo,
  focusFromTrail,
} = require('./intelligence');
const {
  LiveLoopEngine,
  createLiveLoopEngine,
  LIFECYCLE,
  EVENT_TYPES,
  MATERIAL_EVENT_TYPES,
  ENTITY_KINDS,
  SEVERITY,
  buildIntelligenceEvent,
  encodeCursor,
  decodeCursor,
  isMaterialEvent,
  toNotifications,
  buildAwareness,
} = require('./live');
const {
  OperatorEngine,
  createOperatorEngine,
  INTERACTION_TYPES,
  OUTCOMES,
  SECTION_IDS,
  DOMINANCE,
  INTENT_TAGS,
  buildInteractionEvent,
  scoreTrust,
  buildAdaptivePresentation,
  buildQualityDashboard,
} = require('./operator');
const {
  OutcomeEngine,
  createOutcomeEngine,
  LIFECYCLE: OUTCOME_LIFECYCLE,
  OUTCOME_RESULTS,
  CONFIDENCE_BANDS,
  STRATEGY_IDS: OUTCOME_STRATEGY_IDS,
  buildRecommendationOutcome,
  canTransitionLifecycle,
  bandForConfidence,
  buildCalibrationReport,
  buildStrategyPerformance,
  detectDrift,
  buildReviewDashboard,
} = require('./outcome');

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
  const workspace = createWorkspaceEngine({
    anthropic: options.anthropic,
    disableLlm: options.disableLlm,
    model: options.workspaceModel,
    missionEngine: options.missionEngine || null,
    missionsEnabled: options.missionsEnabled,
  });
  const intelligence = createIntelligenceComposer({
    knowledge,
    memory,
    policy,
  });
  const live = createLiveLoopEngine({
    confidenceThreshold: options.confidenceThreshold,
  });
  const operator = createOperatorEngine();
  const outcome = createOutcomeEngine({
    getOperatorQuality: (tenantId) => operator.quality(tenantId),
  });

  async function compose(input) {
    const model = await commandDeck.compose(input);
    const observed = live.observeDeck({
      tenantId: String(input.tenantId),
      model,
    });
    const withLive = live.withLive(model, observed);
    // SPEC-012: adaptive presentation only — never mutates intelligence facts
    const decorated = operator.decorate(withLive, String(input.tenantId));
    // SPEC-013: register Generated recommendations for outcome tracking (no model mutation)
    outcome.observeGenerated(decorated, String(input.tenantId));
    return decorated;
  }

  function openWorkspace(context, openOptions) {
    const opened = workspace.open(context, openOptions);
    const session = workspace.sessions.get(opened.sessionId);
    if (session) {
      session.liveCursor = live.store.cursor();
      session.liveSeq = live.store.seq;
    }
    const tenantId = String(context.tenantId || '');
    const entityId = context.recommendationId || context.companyId || null;
    // On open: recent entity motion (not limited to session start)
    const recent = entityId
      ? live.timeline({
          tenantId,
          entityId: String(entityId),
          limit: 8,
        })
      : null;
    const awareness = buildAwareness({
      events: (recent && recent.events) || [],
      entityLabel:
        (context.selectedEntity && context.selectedEntity.name) || null,
    });

    // SPEC-012: record Ask Max open + personalize suggestion chips
    if (tenantId) {
      operator.track({
        type: INTERACTION_TYPES.ASKED_MAX,
        tenantId,
        recommendationId: context.recommendationId || null,
        companyId: context.companyId || null,
        payload: {
          context: context.page || 'command-deck',
          open: true,
        },
      });
    }
    const suggestions = operator.suggestions(opened.context, tenantId);

    return {
      ...opened,
      suggestions,
      awareness,
    };
  }

  async function askWorkspace(input) {
    const result = await workspace.ask(input);
    const session = workspace.sessions.get(result.sessionId);
    const tenantId = String(
      (result.context && result.context.tenantId) ||
        (input.context && input.context.tenantId) ||
        ''
    );
    const sinceSeq =
      session && session.liveSeq != null ? Number(session.liveSeq) : 0;
    const awareness = live.awareness({
      tenantId,
      sinceSeq,
      openedAt: session && session.createdAt,
      entityId:
        (result.context &&
          (result.context.recommendationId || result.context.companyId)) ||
        null,
      entityLabel:
        (result.context &&
          result.context.selectedEntity &&
          result.context.selectedEntity.name) ||
        null,
    });
    if (awareness.headline && result.prose) {
      result.prose = `${awareness.headline}\n\n${result.prose}`;
    }
    result.awareness = awareness;

    // SPEC-012: learn from the question; refresh personalized chips.
    // Preserve activeWorkContext and response-level packet-review hints so
    // desk / canary workflows keep workflow-aware chips instead of falling
    // back to briefing defaults after personalization.
    if (tenantId && input.question) {
      operator.track({
        type: INTERACTION_TYPES.ASKED_MAX,
        tenantId,
        recommendationId:
          (result.context && result.context.recommendationId) || null,
        companyId: (result.context && result.context.companyId) || null,
        payload: { question: String(input.question) },
      });
      const suggestionContext = {
        ...((result.context && typeof result.context === 'object'
          ? result.context
          : null) ||
          (input.context && typeof input.context === 'object'
            ? input.context
            : {})),
      };
      const awc =
        suggestionContext.activeWorkContext ||
        (session && session.activeWorkContext) ||
        (session &&
          session.context &&
          session.context.activeWorkContext) ||
        null;
      if (awc) suggestionContext.activeWorkContext = awc;

      const meta =
        (result.structured &&
          result.structured.metadata &&
          typeof result.structured.metadata === 'object'
          ? result.structured.metadata
          : null) ||
        (result.metadata && typeof result.metadata === 'object'
          ? result.metadata
          : null) ||
        {};
      suggestionContext.metadata = meta;
      if (meta.outputKind != null) suggestionContext.outputKind = meta.outputKind;
      if (meta.lastOutputKind != null) {
        suggestionContext.lastOutputKind = meta.lastOutputKind;
      } else if (meta.outputKind != null) {
        suggestionContext.lastOutputKind = meta.outputKind;
      }
      if (meta.contextHints) suggestionContext.contextHints = meta.contextHints;
      if (meta.packetReviewContext) {
        suggestionContext.packetReviewContext = meta.packetReviewContext;
      }

      suggestionContext.latestQuestion = String(input.question);
      result.suggestions = operator.suggestions(suggestionContext, tenantId);
    }
    return result;
  }

  return {
    knowledge,
    engine,
    memory,
    briefing,
    policy,
    commandDeck,
    workspace,
    intelligence,
    live,
    operator,
    outcome,
    runtime,
    missionEngine: options.missionEngine || null,
    evaluate: (input) => engine.evaluate(input),
    remember: (input) => memory.remember(input),
    brief: (input) => briefing.brief(input),
    decide: (input) => policy.evaluate(input),
    compose,
    composeRecommendation: (input) =>
      intelligence.composeRecommendation(input),
    composeCompany: (input) => intelligence.composeCompany(input),
    openWorkspace,
    askWorkspace,
    liveSince: (input) => live.liveSince(input),
    liveTimeline: (input) => live.timeline(input),
    liveAwareness: (input) => live.awareness(input),
    trackOperator: (input) => operator.track(input),
    operatorOutcome: (input) => operator.setOutcome(input),
    operatorLearning: (tenantId, recommendationId) =>
      operator.getLearning(tenantId, recommendationId),
    operatorQuality: (tenantId) => operator.quality(tenantId),
    recordOutcome: (input) => outcome.record(input),
    outcomeLifecycle: (input) => outcome.transition(input),
    outcomeGet: (tenantId, recommendationId) =>
      outcome.get(tenantId, recommendationId),
    outcomeCalibration: (tenantId) => outcome.calibration(tenantId),
    outcomeStrategies: (tenantId) => outcome.strategies(tenantId),
    outcomeDrift: (tenantId, options) => outcome.drift(tenantId, options),
    outcomeReview: (tenantId) => outcome.review(tenantId),
  };
}

module.exports = {
  createMaxReasoningRuntime,
  createReasoningEngine,
  createCrmReasoningRuntime,
  createReasoningRuntime,
  CRMStrategyPack,
  createCRMStrategyPack,
  CRMContextProvider,
  createCRMContextProvider,
  NextBestActionProvider,
  createNextBestActionProvider,
  assertStrategyPack,
  assertContextProvider,
  assertRecommendationProvider,
  createMemoryEngine,
  createBriefingEngine,
  createPolicyEngine,
  createCommandDeckComposer,
  createIntelligenceComposer,
  IntelligenceComposer,
  RelatedIntelligenceBuilder,
  RecommendationDetailComposer,
  CompanyIntelligenceComposer,
  NAV_TYPES,
  TRAIL_KINDS,
  buildNavRef,
  parseRecommendationId,
  buildRecommendationId,
  pushTrail,
  popTrailTo,
  focusFromTrail,
  ReasoningEngine,
  ReasoningContextBuilder,
  StrategyRegistry,
  createDefaultStrategyRegistry,
  createCRMStrategyRegistry,
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
  WorkspaceEngine,
  createWorkspaceEngine,
  createSpecialistDelegationService,
  createSpecialistDelegationMemoryStore,
  CONTRACT_OBJECTIVE,
  AUTHORITY_LEVELS,
  PAGE_TYPES,
  SessionStore,
  PresentationEngine,
  composeResponse,
  buildOpeningState,
  buildSuggestions,
  normalizeContext,
  EXECUTION_DOMAINS,
  selectExecutionDomain,
  isMissionDomain,
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
  LiveLoopEngine,
  createLiveLoopEngine,
  LIFECYCLE,
  EVENT_TYPES,
  MATERIAL_EVENT_TYPES,
  ENTITY_KINDS,
  SEVERITY,
  buildIntelligenceEvent,
  encodeCursor,
  decodeCursor,
  isMaterialEvent,
  toNotifications,
  buildAwareness,
  OperatorEngine,
  createOperatorEngine,
  INTERACTION_TYPES,
  OUTCOMES,
  SECTION_IDS,
  DOMINANCE,
  INTENT_TAGS,
  buildInteractionEvent,
  scoreTrust,
  buildAdaptivePresentation,
  buildQualityDashboard,
  OutcomeEngine,
  createOutcomeEngine,
  OUTCOME_LIFECYCLE,
  OUTCOME_RESULTS,
  CONFIDENCE_BANDS,
  OUTCOME_STRATEGY_IDS,
  buildRecommendationOutcome,
  canTransitionLifecycle,
  bandForConfidence,
  buildCalibrationReport,
  buildStrategyPerformance,
  detectDrift,
  buildReviewDashboard,
};
