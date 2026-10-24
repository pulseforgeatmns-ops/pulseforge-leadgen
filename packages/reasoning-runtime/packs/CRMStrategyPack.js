'use strict';

const { assertStrategyPack } = require('../interfaces/StrategyPack');

/**
 * CRM Strategy Pack — encapsulates existing CRM reasoning meaning.
 *
 * All CRM collaborators are injected. This pack does not import Max modules
 * directly, so the runtime package stays free of hard CRM coupling while still
 * hosting the CRM pack contract required by SPEC-015A.
 *
 * Behavior is identical to the pre-decoupling ReasoningEngine path when wired
 * with the production CRM collaborators from packages/max.
 */
class CRMStrategyPack {
  /**
   * @param {object} deps
   * @param {object} deps.registry - StrategyRegistry with evaluateAll(context)
   * @param {object} deps.aggregator - ScoreAggregator with aggregate(results)
   * @param {import('../interfaces/RecommendationProvider').RecommendationProvider} deps.recommendationProvider
   * @param {object} deps.explanationEngine - ExplanationEngine with explain(input)
   * @param {(input: object) => object[]} [deps.analogFinder] - optional memory analogs
   * @param {string} [deps.id]
   * @param {string} [deps.domain]
   */
  constructor(deps) {
    if (!deps || typeof deps !== 'object') {
      throw new Error('CRMStrategyPack requires dependency injection');
    }
    if (!deps.registry || typeof deps.registry.evaluateAll !== 'function') {
      throw new Error('CRMStrategyPack requires registry.evaluateAll');
    }
    if (!deps.aggregator || typeof deps.aggregator.aggregate !== 'function') {
      throw new Error('CRMStrategyPack requires aggregator.aggregate');
    }
    if (
      !deps.recommendationProvider ||
      typeof deps.recommendationProvider.generate !== 'function'
    ) {
      throw new Error('CRMStrategyPack requires recommendationProvider.generate');
    }
    if (
      !deps.explanationEngine ||
      typeof deps.explanationEngine.explain !== 'function'
    ) {
      throw new Error('CRMStrategyPack requires explanationEngine.explain');
    }

    this.id = deps.id || 'crm';
    this.domain = deps.domain || 'crm';
    this._registry = deps.registry;
    this._aggregator = deps.aggregator;
    this._recommendationProvider = deps.recommendationProvider;
    this._explanationEngine = deps.explanationEngine;
    this._analogFinder =
      typeof deps.analogFinder === 'function' ? deps.analogFinder : null;

    /** @type {object|null} */
    this._session = null;

    assertStrategyPack(this);
  }

  /**
   * @param {object} input
   * @param {object} input.context
   * @param {object} [input.input]
   * @param {object} [input.memory]
   * @param {object} [input.recommendationProvider]
   */
  initialize(input) {
    if (!input || !input.context) {
      throw new Error('CRMStrategyPack.initialize requires context');
    }
    this._session = {
      input: input.input || null,
      context: input.context,
      memory: input.memory || null,
      recommendationProvider:
        input.recommendationProvider || this._recommendationProvider,
      evidence: null,
      claims: null,
      strategyResults: null,
      strategyTimings: null,
      analogs: null,
      ranked: null,
      recommendation: null,
      explanation: null,
    };
    return this._session;
  }

  buildEvidence() {
    const session = requireSession(this);
    const evidence = (session.context.evidence || []).slice();
    session.evidence = evidence;
    return evidence;
  }

  buildClaims() {
    const session = requireSession(this);
    const { results, timings } = this._registry.evaluateAll(session.context);
    session.strategyResults = results;
    session.strategyTimings = timings;
    // Strategy observations + graph claims both surface as claim material.
    const graphClaims = (session.context.claims || []).slice();
    const observationClaims = (results || []).flatMap((r) => r.claims || []);
    session.claims = {
      graph: graphClaims,
      observations: observationClaims,
      results,
    };
    return session.claims;
  }

  async findHistoricalAnalogs() {
    const session = requireSession(this);
    let analogs = [];
    if (this._analogFinder) {
      analogs = (await this._analogFinder({
        input: session.input,
        context: session.context,
        memory: session.memory,
        strategyResults: session.strategyResults,
      })) || [];
    }
    session.analogs = Array.isArray(analogs) ? analogs : [];
    return session.analogs;
  }

  rankClaims() {
    const session = requireSession(this);
    if (!session.strategyResults) {
      throw new Error('CRMStrategyPack.rankClaims requires buildClaims first');
    }
    const ranked = this._aggregator.aggregate(session.strategyResults);
    session.ranked = ranked;
    return ranked;
  }

  generateRecommendations() {
    const session = requireSession(this);
    if (!session.ranked) {
      throw new Error(
        'CRMStrategyPack.generateRecommendations requires rankClaims first'
      );
    }
    const provider =
      session.recommendationProvider || this._recommendationProvider;
    const recommendation = provider.generate({
      context: session.context,
      strategyResults: session.strategyResults,
      aggregated: session.ranked,
      claims: session.claims,
      evidence: session.evidence,
      analogs: session.analogs,
    });
    session.recommendation = recommendation;
    return recommendation;
  }

  explain() {
    const session = requireSession(this);
    if (!session.recommendation) {
      throw new Error('CRMStrategyPack.explain requires generateRecommendations first');
    }
    const base = this._explanationEngine.explain({
      recommendation: session.recommendation,
      context: session.context,
      strategyResults: session.strategyResults,
    });
    // Required explainability surface (SPEC-015A) — additive fields only
    const explanation = {
      ...base,
      historicalAnalogs: session.analogs || [],
      confidenceChanges: [
        {
          field: 'aggregated.confidence',
          value: session.ranked ? session.ranked.confidence : null,
        },
        {
          field: 'recommendation.confidence',
          value: session.recommendation.confidence,
        },
      ],
      reasoningTrace: {
        packId: this.id,
        domain: this.domain,
        strategyIds: (session.strategyResults || []).map((r) => r.strategy),
        strategyTimings: session.strategyTimings || {},
      },
    };
    session.explanation = explanation;
    return explanation;
  }

  /** @returns {object|null} */
  getSession() {
    return this._session;
  }
}

/**
 * @param {CRMStrategyPack} pack
 */
function requireSession(pack) {
  if (!pack._session) {
    throw new Error('CRMStrategyPack requires initialize() before other steps');
  }
  return pack._session;
}

/**
 * @param {object} deps
 * @returns {CRMStrategyPack}
 */
function createCRMStrategyPack(deps) {
  return new CRMStrategyPack(deps);
}

module.exports = {
  CRMStrategyPack,
  createCRMStrategyPack,
};
