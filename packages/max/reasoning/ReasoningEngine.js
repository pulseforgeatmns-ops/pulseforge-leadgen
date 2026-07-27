'use strict';

const {
  createReasoningRuntime,
  createCRMStrategyPack,
  createCRMContextProvider,
  createNextBestActionProvider,
  DEFAULT_PERFORMANCE_TARGET_MS,
} = require('../../reasoning-runtime');
const { ReasoningContextBuilder } = require('../context/ReasoningContextBuilder');
const { createDefaultStrategyRegistry } = require('../strategies/StrategyRegistry');
const { ScoreAggregator } = require('../aggregation/ScoreAggregator');
const { RecommendationBuilder } = require('../recommendations/RecommendationBuilder');
const { ExplanationEngine } = require('../explanations/ExplanationEngine');
const { buildReasoningReport } = require('../reports/ReasoningReport');
const { PERFORMANCE_TARGET_MS, round } = require('./ReasoningTypes');

/**
 * Max Reasoning Engine — deterministic recommendations from Knowledge Graph evidence.
 *
 * SPEC-015A: orchestration is domain-neutral (`ReasoningRuntime`).
 * CRM meaning lives in `CRMStrategyPack` + CRM providers (default wiring).
 *
 * Architecture:
 *   Operator → Max → ReasoningEngine → ReasoningRuntime → StrategyPack
 *                                      → Knowledge Query Engine (via context provider)
 *
 * No LLM. No repository access. Strategies never query or mutate context.
 */
class ReasoningEngine {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../context/ReasoningContextBuilder').ReasoningContextBuilder} [deps.contextBuilder]
   * @param {import('../strategies/StrategyRegistry').StrategyRegistry} [deps.registry]
   * @param {ScoreAggregator} [deps.aggregator]
   * @param {RecommendationBuilder} [deps.recommendationBuilder]
   * @param {ExplanationEngine} [deps.explanationEngine]
   * @param {import('../../reasoning-runtime').ReasoningRuntime} [deps.runtime]
   * @param {object} [deps.memory]
   * @param {(input: object) => object[]|Promise<object[]>} [deps.analogFinder]
   */
  constructor(deps) {
    if (!deps || !deps.knowledge) {
      throw new Error('ReasoningEngine requires knowledge (KnowledgeService)');
    }
    this._knowledge = deps.knowledge;
    this._contextBuilder =
      deps.contextBuilder || new ReasoningContextBuilder({ knowledge: deps.knowledge });
    this._registry = deps.registry || createDefaultStrategyRegistry();
    this._aggregator = deps.aggregator || new ScoreAggregator();
    this._recommendationBuilder = deps.recommendationBuilder || new RecommendationBuilder();
    this._explanationEngine = deps.explanationEngine || new ExplanationEngine();
    this._memory = deps.memory || null;

    this._runtime =
      deps.runtime ||
      createCrmReasoningRuntime({
        contextBuilder: this._contextBuilder,
        registry: this._registry,
        aggregator: this._aggregator,
        recommendationBuilder: this._recommendationBuilder,
        explanationEngine: this._explanationEngine,
        memory: this._memory,
        analogFinder: deps.analogFinder,
        performanceTargetMs: PERFORMANCE_TARGET_MS,
      });
  }

  /** @returns {import('../strategies/StrategyRegistry').StrategyRegistry} */
  get registry() {
    return this._registry;
  }

  /** @returns {import('../../reasoning-runtime').ReasoningRuntime} */
  get runtime() {
    return this._runtime;
  }

  /**
   * Evaluate a subject and produce a structured recommendation + full report.
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} [input.companyId]
   * @param {string} [input.subjectId] - domain-neutral alias for companyId (SPEC-015A / audit R1)
   * @param {string} [input.asOf]
   * @returns {Promise<{
   *   recommendation: import('./ReasoningTypes').Recommendation,
   *   explanation: object,
   *   report: object,
   * }>}
   */
  async evaluate(input) {
    const normalized = normalizeEvaluateInput(input);
    const out = await this._runtime.evaluate(normalized);

    return {
      recommendation: out.recommendation,
      explanation: out.explanation,
      report: out.report,
      meta: {
        executionTimeMs: round(out.meta.executionTimeMs),
        withinTarget: out.meta.withinTarget,
        performanceTargetMs: out.meta.performanceTargetMs,
      },
    };
  }
}

/**
 * Wire CRM collaborators into a ReasoningRuntime (SPEC-015A default).
 *
 * @param {object} deps
 */
function createCrmReasoningRuntime(deps) {
  const recommendationProvider = createNextBestActionProvider({
    builder: deps.recommendationBuilder,
  });
  const contextProvider = createCRMContextProvider({
    builder: deps.contextBuilder,
  });
  const strategyPack = createCRMStrategyPack({
    registry: deps.registry,
    aggregator: deps.aggregator,
    recommendationProvider,
    explanationEngine: deps.explanationEngine,
    analogFinder: deps.analogFinder,
  });

  return createReasoningRuntime({
    strategyPack,
    contextProvider,
    recommendationProvider,
    memory: deps.memory || null,
    performanceTargetMs:
      deps.performanceTargetMs == null
        ? DEFAULT_PERFORMANCE_TARGET_MS
        : deps.performanceTargetMs,
    buildReport: ({ context, ranked, recommendation, explanation, executionTimeMs, pack }) => {
      const session = pack.getSession ? pack.getSession() : null;
      return buildReasoningReport({
        context,
        strategyResults: (session && session.strategyResults) || [],
        aggregated: ranked,
        recommendation,
        explanation,
        strategyTimings: (session && session.strategyTimings) || {},
        executionTimeMs,
      });
    },
  });
}

/**
 * @param {object} input
 */
function normalizeEvaluateInput(input) {
  if (!input || typeof input !== 'object') {
    throw new Error('evaluate requires an input object');
  }
  const companyId = input.companyId || input.subjectId;
  if (!input.tenantId || !companyId) {
    throw new Error('evaluate requires tenantId and companyId (or subjectId)');
  }
  return {
    ...input,
    tenantId: String(input.tenantId),
    companyId: String(companyId),
    subjectId: String(companyId),
  };
}

/**
 * Create a reasoning engine bound to a KnowledgeService instance.
 * @param {object} options
 * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} options.knowledge
 */
function createReasoningEngine(options) {
  return new ReasoningEngine(options);
}

module.exports = {
  ReasoningEngine,
  createReasoningEngine,
  createCrmReasoningRuntime,
  normalizeEvaluateInput,
};
