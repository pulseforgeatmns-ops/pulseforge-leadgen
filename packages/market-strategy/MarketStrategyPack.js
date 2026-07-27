'use strict';

const { assertStrategyPack } = require('@pulseforge/reasoning-runtime');
const {
  createMarketStrategyRegistry,
  aggregateMarketScores,
  findMarketAnalogs,
} = require('./registry');

/**
 * Market Strategy Pack — research-only financial observation reasoning (SPEC-016).
 *
 * Consumes public runtime interfaces only. No execution logic.
 */
class MarketStrategyPack {
  /**
   * @param {object} deps
   * @param {import('./registry').MarketStrategyRegistry} [deps.registry]
   * @param {(results: import('./types').MarketStrategyResult[]) => object} [deps.aggregator]
   * @param {import('./ResearchRecommendationProvider').ResearchRecommendationProvider} deps.recommendationProvider
   * @param {(input: object) => import('./types').HistoricalAnalog[]|Promise<import('./types').HistoricalAnalog[]>} [deps.analogFinder]
   * @param {string} [deps.id]
   * @param {string} [deps.domain]
   */
  constructor(deps) {
    if (!deps || typeof deps !== 'object') {
      throw new Error('MarketStrategyPack requires dependency injection');
    }
    if (
      !deps.recommendationProvider ||
      typeof deps.recommendationProvider.generate !== 'function'
    ) {
      throw new Error('MarketStrategyPack requires recommendationProvider.generate');
    }

    this.id = deps.id || 'market';
    this.domain = deps.domain || 'market';
    this._registry = deps.registry || createMarketStrategyRegistry();
    this._aggregator = deps.aggregator || aggregateMarketScores;
    this._recommendationProvider = deps.recommendationProvider;
    this._analogFinder =
      typeof deps.analogFinder === 'function' ? deps.analogFinder : findMarketAnalogs;

    /** @type {object|null} */
    this._session = null;

    assertStrategyPack(this);
  }

  /**
   * @param {object} input
   */
  initialize(input) {
    if (!input || !input.context) {
      throw new Error('MarketStrategyPack.initialize requires context');
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

    const graphClaims = (session.context.claims || []).slice();
    const observationClaims = (results || []).flatMap((r) => r.claims || []);
    const derivedClaims = (results || [])
      .filter((r) => (r.claims || []).length > 0)
      .map((r) => ({
        id: `claim:${r.strategy}`,
        statement: r.summary,
        claimType: r.strategy,
        confidence: r.confidence,
        status: 'active',
      }));

    session.claims = {
      graph: graphClaims,
      observations: observationClaims,
      derived: derivedClaims,
      results,
    };
    return session.claims;
  }

  async findHistoricalAnalogs() {
    const session = requireSession(this);
    const analogs = await this._analogFinder({
      input: session.input,
      context: session.context,
      memory: session.memory,
      strategyResults: session.strategyResults,
      evidence: session.evidence,
      claims: session.claims,
    });
    session.analogs = Array.isArray(analogs) ? analogs : [];
    return session.analogs;
  }

  rankClaims() {
    const session = requireSession(this);
    if (!session.strategyResults) {
      throw new Error('MarketStrategyPack.rankClaims requires buildClaims first');
    }
    const ranked = this._aggregator(session.strategyResults);
    session.ranked = ranked;
    return ranked;
  }

  generateRecommendations() {
    const session = requireSession(this);
    if (!session.ranked) {
      throw new Error(
        'MarketStrategyPack.generateRecommendations requires rankClaims first'
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
      throw new Error('MarketStrategyPack.explain requires generateRecommendations first');
    }

    const supportingEvidence = (session.strategyResults || []).flatMap(
      (r) => r.supportingEvidence || []
    );
    const contradictingEvidence = (session.strategyResults || []).flatMap(
      (r) => r.contradictingEvidence || []
    );
    const activeClaims = (session.claims?.derived || []).map((c) => ({
      id: c.id,
      statement: c.statement,
      claimType: c.claimType,
      confidence: c.confidence,
    }));

    const explanation = {
      recommendationId: session.recommendation.id,
      subjectId: session.recommendation.subject.id,
      score: session.recommendation.score,
      confidence: session.recommendation.confidence,
      claims: activeClaims,
      supportingEvidence: dedupeById(supportingEvidence),
      contradictingEvidence: dedupeById(contradictingEvidence),
      historicalAnalogs: session.analogs || [],
      reasoningTrace: {
        packId: this.id,
        domain: this.domain,
        strategyIds: (session.strategyResults || []).map((r) => r.strategy),
        strategyTimings: session.strategyTimings || {},
        steps: [
          'context',
          'buildEvidence',
          'buildClaims',
          'findHistoricalAnalogs',
          'rankClaims',
          'generateRecommendations',
          'explain',
        ],
      },
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
    };

    session.explanation = explanation;
    return explanation;
  }

  getSession() {
    return this._session;
  }
}

/**
 * @param {MarketStrategyPack} pack
 */
function requireSession(pack) {
  if (!pack._session) {
    throw new Error('MarketStrategyPack requires initialize() before other steps');
  }
  return pack._session;
}

/**
 * @param {Array<{ id: string }>} items
 */
function dedupeById(items) {
  const seen = new Set();
  return items.filter((item) => {
    if (seen.has(item.id)) return false;
    seen.add(item.id);
    return true;
  });
}

/**
 * @param {object} deps
 * @returns {MarketStrategyPack}
 */
function createMarketStrategyPack(deps) {
  return new MarketStrategyPack(deps);
}

module.exports = {
  MarketStrategyPack,
  createMarketStrategyPack,
};
