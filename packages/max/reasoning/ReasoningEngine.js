'use strict';

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
 * Architecture:
 *   Operator → Max → ReasoningEngine → Knowledge Query Engine (via KnowledgeService)
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
  }

  /** @returns {import('../strategies/StrategyRegistry').StrategyRegistry} */
  get registry() {
    return this._registry;
  }

  /**
   * Evaluate a company and produce a structured recommendation + full report.
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {string} [input.asOf]
   * @returns {Promise<{
   *   recommendation: import('./ReasoningTypes').Recommendation,
   *   explanation: object,
   *   report: object,
   * }>}
   */
  async evaluate(input) {
    const started = process.hrtime.bigint();

    const context = await this._contextBuilder.build({
      tenantId: input.tenantId,
      companyId: input.companyId,
      asOf: input.asOf,
    });

    const { results: strategyResults, timings: strategyTimings } =
      this._registry.evaluateAll(context);

    const aggregated = this._aggregator.aggregate(strategyResults);

    const recommendation = this._recommendationBuilder.build({
      context,
      strategyResults,
      aggregated,
    });

    const explanation = this._explanationEngine.explain({
      recommendation,
      context,
      strategyResults,
    });

    const executionTimeMs = Number(process.hrtime.bigint() - started) / 1e6;
    const report = buildReasoningReport({
      context,
      strategyResults,
      aggregated,
      recommendation,
      explanation,
      strategyTimings,
      executionTimeMs,
    });

    return {
      recommendation,
      explanation,
      report,
      meta: {
        executionTimeMs: round(executionTimeMs),
        withinTarget: executionTimeMs <= PERFORMANCE_TARGET_MS,
        performanceTargetMs: PERFORMANCE_TARGET_MS,
      },
    };
  }
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
};
