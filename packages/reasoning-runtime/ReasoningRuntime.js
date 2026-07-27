'use strict';

const { assertStrategyPack } = require('./interfaces/StrategyPack');
const { assertContextProvider } = require('./interfaces/ContextProvider');
const {
  assertRecommendationProvider,
} = require('./interfaces/RecommendationProvider');

const DEFAULT_PERFORMANCE_TARGET_MS = 500;

/**
 * Domain-neutral reasoning orchestration.
 *
 * Invokes injected StrategyPack / ContextProvider / RecommendationProvider.
 * Never branches on domain type. Never constructs domain context.
 */
class ReasoningRuntime {
  /**
   * @param {object} deps
   * @param {import('./interfaces/StrategyPack').StrategyPack} deps.strategyPack
   * @param {import('./interfaces/ContextProvider').ContextProvider} deps.contextProvider
   * @param {import('./interfaces/RecommendationProvider').RecommendationProvider} deps.recommendationProvider
   * @param {object} [deps.memory] - optional memory surface (history/analogs)
   * @param {number} [deps.performanceTargetMs]
   * @param {(input: object) => object} [deps.buildReport] - optional report builder
   * @param {() => string} [deps.clock] - injectable ISO clock (replay determinism)
   */
  constructor(deps) {
    if (!deps || typeof deps !== 'object') {
      throw new Error('ReasoningRuntime requires dependency injection');
    }
    assertStrategyPack(deps.strategyPack);
    assertContextProvider(deps.contextProvider);
    assertRecommendationProvider(deps.recommendationProvider);

    this._pack = deps.strategyPack;
    this._contextProvider = deps.contextProvider;
    this._recommendationProvider = deps.recommendationProvider;
    this._memory = deps.memory || null;
    this._performanceTargetMs =
      deps.performanceTargetMs == null
        ? DEFAULT_PERFORMANCE_TARGET_MS
        : Number(deps.performanceTargetMs);
    this._buildReport =
      typeof deps.buildReport === 'function' ? deps.buildReport : null;
    this._clock =
      typeof deps.clock === 'function' ? deps.clock : () => new Date().toISOString();
  }

  /** @returns {import('./interfaces/StrategyPack').StrategyPack} */
  get strategyPack() {
    return this._pack;
  }

  /** @returns {import('./interfaces/ContextProvider').ContextProvider} */
  get contextProvider() {
    return this._contextProvider;
  }

  /** @returns {import('./interfaces/RecommendationProvider').RecommendationProvider} */
  get recommendationProvider() {
    return this._recommendationProvider;
  }

  /**
   * Execute a full reasoning pass via the injected pack.
   *
   * @param {object} input - opaque to the runtime (interpreted by providers/pack)
   * @returns {Promise<{
   *   recommendation: object,
   *   explanation: object,
   *   report: object|null,
   *   analogs: object[],
   *   trace: object,
   *   meta: object,
   * }>}
   */
  async evaluate(input) {
    const started = process.hrtime.bigint();
    const trace = {
      steps: [],
      packId: this._pack.id,
      domain: this._pack.domain,
    };

    const now = () => this._clock();

    const context = await this._contextProvider.build(input);
    trace.steps.push({ step: 'context', at: now() });

    await this._pack.initialize({
      input,
      context,
      memory: this._memory,
      recommendationProvider: this._recommendationProvider,
    });
    trace.steps.push({ step: 'initialize', at: now() });

    const evidence = await this._pack.buildEvidence();
    trace.steps.push({
      step: 'buildEvidence',
      at: now(),
      count: countOf(evidence),
    });

    const claims = await this._pack.buildClaims();
    trace.steps.push({
      step: 'buildClaims',
      at: now(),
      count: countOf(claims),
    });

    const analogs = (await this._pack.findHistoricalAnalogs()) || [];
    trace.steps.push({
      step: 'findHistoricalAnalogs',
      at: now(),
      count: Array.isArray(analogs) ? analogs.length : 0,
    });

    const ranked = await this._pack.rankClaims();
    trace.steps.push({ step: 'rankClaims', at: now() });

    const recommendation = await this._pack.generateRecommendations();
    trace.steps.push({ step: 'generateRecommendations', at: now() });

    const explanation = await this._pack.explain();
    trace.steps.push({ step: 'explain', at: now() });

    const confidenceChanges = extractConfidenceChanges(explanation, ranked);
    const executionTimeMs = Number(process.hrtime.bigint() - started) / 1e6;

    const explainability = {
      evidenceUsed: evidence,
      claimsEvaluated: claims,
      historicalAnalogs: analogs,
      confidenceChanges,
      reasoningTrace: trace,
    };

    let report = null;
    if (this._buildReport) {
      report = this._buildReport({
        input,
        context,
        evidence,
        claims,
        analogs,
        ranked,
        recommendation,
        explanation,
        explainability,
        executionTimeMs,
        pack: this._pack,
      });
    }

    return {
      recommendation,
      explanation,
      report,
      analogs,
      ranked,
      context,
      evidence,
      claims,
      explainability,
      trace,
      meta: {
        executionTimeMs: round(executionTimeMs),
        withinTarget: executionTimeMs <= this._performanceTargetMs,
        performanceTargetMs: this._performanceTargetMs,
        packId: this._pack.id,
        domain: this._pack.domain,
      },
    };
  }
}

/**
 * @param {object} options
 * @returns {ReasoningRuntime}
 */
function createReasoningRuntime(options) {
  return new ReasoningRuntime(options);
}

/**
 * @param {*} value
 */
function countOf(value) {
  if (Array.isArray(value)) return value.length;
  if (value && typeof value === 'object' && Array.isArray(value.items)) {
    return value.items.length;
  }
  if (value && typeof value === 'object' && Array.isArray(value.results)) {
    return value.results.length;
  }
  return value == null ? 0 : 1;
}

/**
 * @param {object} explanation
 * @param {object} ranked
 */
function extractConfidenceChanges(explanation, ranked) {
  if (explanation && Array.isArray(explanation.confidenceChanges)) {
    return explanation.confidenceChanges;
  }
  if (ranked && ranked.confidence != null) {
    return [
      {
        field: 'aggregated.confidence',
        value: ranked.confidence,
      },
    ];
  }
  return [];
}

function round(n) {
  return Math.round(Number(n) * 1000) / 1000;
}

module.exports = {
  ReasoningRuntime,
  createReasoningRuntime,
  DEFAULT_PERFORMANCE_TARGET_MS,
};
