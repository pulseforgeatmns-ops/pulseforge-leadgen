'use strict';

const {
  MARKET_CLAIM_TYPES,
  DEFAULT_MARKET_WEIGHTS,
  evidenceRef,
  strategyResult,
  clamp,
  round,
} = require('./types');

/**
 * @typedef {object} MarketStrategy
 * @property {string} id
 * @property {string} name
 * @property {(context: import('./types').MarketContext) => import('./types').MarketStrategyResult} evaluate
 */

class MarketStrategyRegistry {
  constructor() {
    /** @type {Map<string, MarketStrategy>} */
    this._strategies = new Map();
  }

  /**
   * @param {MarketStrategy} strategy
   */
  register(strategy) {
    assertMarketStrategy(strategy);
    if (this._strategies.has(strategy.id)) {
      throw new Error(`Market strategy already registered: ${strategy.id}`);
    }
    this._strategies.set(strategy.id, strategy);
    return this;
  }

  /**
   * @param {string} id
   */
  get(id) {
    return this._strategies.get(id) || null;
  }

  list() {
    return [...this._strategies.values()].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    );
  }

  ids() {
    return this.list().map((s) => s.id);
  }

  /**
   * @param {import('./types').MarketContext} context
   */
  evaluateAll(context) {
    if (!context || typeof context !== 'object') {
      throw new Error('evaluateAll requires a MarketContext');
    }
    const results = [];
    /** @type {Record<string, number>} */
    const timings = {};
    for (const strategy of this.list()) {
      const start = process.hrtime.bigint();
      const result = strategy.evaluate(context);
      timings[strategy.id] = Number(process.hrtime.bigint() - start) / 1e6;
      if (!result || result.strategy !== strategy.id) {
        throw new Error(
          `Strategy ${strategy.id} returned invalid result (strategy field mismatch)`
        );
      }
      if (Object.prototype.hasOwnProperty.call(result, 'recommendedAction')) {
        throw new Error(`Strategy ${strategy.id} must not produce recommendations`);
      }
      results.push(result);
    }
    return { results, timings };
  }
}

/**
 * @param {MarketStrategy} strategy
 */
function assertMarketStrategy(strategy) {
  if (!strategy || typeof strategy !== 'object') {
    throw new Error('Market strategy must be an object');
  }
  if (!strategy.id) throw new Error('Market strategy requires id');
  if (!strategy.name) throw new Error('Market strategy requires name');
  if (typeof strategy.evaluate !== 'function') {
    throw new Error(`Market strategy ${strategy.id} requires evaluate(context)`);
  }
}

/**
 * @param {import('./types').MarketContext} context
 * @param {string} prefix
 */
function evidenceByType(context, prefix) {
  return (context.evidence || []).filter((e) =>
    String(e.summary).toLowerCase().startsWith(prefix)
  );
}

const MomentumContinuationStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION,
  name: 'Momentum Continuation',
  evaluate(context) {
    const changePct = Number(context.metrics.changePct) || 0;
    const vol1h = Number(context.metrics.volume_1h) || 0;
    const supporting = [];
    const contradicting = [];
    const snapshot = evidenceByType(context, 'snapshot:');
    const volume = evidenceByType(context, 'volume:');

    if (changePct > 1.5) {
      supporting.push(
        ...(snapshot.length ? snapshot : []).map((e) => e),
        evidenceRef({
          id: 'metric:changePct',
          kind: 'metric',
          summary: `changePct=${changePct}`,
          confidence: 0.8,
        })
      );
    } else if (changePct < -0.5) {
      contradicting.push(
        evidenceRef({
          id: 'metric:changePct',
          kind: 'metric',
          summary: `changePct=${changePct}`,
          confidence: 0.7,
        })
      );
    }
    if (vol1h > 1e9) {
      supporting.push(...volume);
    }

    const scoreDelta =
      Math.min(80, Math.max(0, changePct) * 12) +
      (vol1h > 1e9 ? 15 : 0) -
      (changePct < 0 ? 20 : 0);

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION,
      scoreDelta,
      confidence: supporting.length ? 65 + Math.min(25, supporting.length * 8) : 25,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION}`] : [],
      summary: `momentum_continuation:change=${changePct}:vol1h=${vol1h > 0}`,
    });
  },
});

const MomentumExhaustionStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.MOMENTUM_EXHAUSTION,
  name: 'Momentum Exhaustion',
  evaluate(context) {
    const changePct = Math.abs(Number(context.metrics.changePct) || 0);
    const vol1h = Number(context.metrics.volume_1h) || 0;
    const vol24h = Number(context.metrics.volume24h) || 0;
    const supporting = [];
    const contradicting = [];

    const highMoveLowVolume =
      changePct > 2 && vol1h > 0 && vol24h > 0 && vol1h < vol24h / 20;

    if (highMoveLowVolume) {
      supporting.push(
        evidenceRef({
          id: 'metric:exhaustion',
          kind: 'metric',
          summary: `exhaustion:change=${changePct}:vol_ratio_low`,
          confidence: 0.72,
        })
      );
    } else if (changePct > 3 && vol1h > vol24h / 10) {
      contradicting.push(
        evidenceRef({
          id: 'metric:volume_still_strong',
          kind: 'metric',
          summary: 'volume_still_supports_move',
          confidence: 0.65,
        })
      );
    }

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.MOMENTUM_EXHAUSTION,
      scoreDelta: highMoveLowVolume ? 45 : -10,
      confidence: highMoveLowVolume ? 70 : 30,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.MOMENTUM_EXHAUSTION}`] : [],
      summary: `momentum_exhaustion:signal=${highMoveLowVolume}`,
    });
  },
});

const ElevatedVolatilityStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.ELEVATED_VOLATILITY,
  name: 'Elevated Volatility',
  evaluate(context) {
    const vol =
      Number(context.metrics.volatility_realized_24h) ||
      Number(context.metrics.volatility_realized) ||
      0;
    const threshold = 0.5;
    const supporting = [];
    const contradicting = [];
    const volEvidence = evidenceByType(context, 'volatility:');

    if (vol >= threshold) {
      supporting.push(...volEvidence);
    } else if (vol > 0) {
      contradicting.push(
        evidenceRef({
          id: 'metric:vol_below_threshold',
          kind: 'metric',
          summary: `volatility=${vol}<${threshold}`,
          confidence: 0.6,
        })
      );
    }

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.ELEVATED_VOLATILITY,
      scoreDelta: vol >= threshold ? Math.min(70, vol * 80) : -15,
      confidence: vol >= threshold ? 75 : 35,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.ELEVATED_VOLATILITY}`] : [],
      summary: `elevated_volatility:vol=${vol}`,
    });
  },
});

const MeanReversionStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.MEAN_REVERSION,
  name: 'Mean Reversion',
  evaluate(context) {
    const changePct = Number(context.metrics.changePct) || 0;
    const vol = Number(context.metrics.volatility_realized_24h) || 0;
    const supporting = [];
    const contradicting = [];

    const extended = Math.abs(changePct) > 3;
    const elevatedVol = vol > 0.55;

    if (extended && elevatedVol) {
      supporting.push(
        evidenceRef({
          id: 'metric:mean_reversion_setup',
          kind: 'metric',
          summary: `extended_move:change=${changePct}:vol=${vol}`,
          confidence: 0.68,
        })
      );
    } else if (Math.abs(changePct) < 1) {
      contradicting.push(
        evidenceRef({
          id: 'metric:no_extension',
          kind: 'metric',
          summary: 'price_near_mean',
          confidence: 0.55,
        })
      );
    }

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.MEAN_REVERSION,
      scoreDelta: extended && elevatedVol ? 40 : extended ? 15 : -5,
      confidence: extended ? 60 : 30,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.MEAN_REVERSION}`] : [],
      summary: `mean_reversion:extended=${extended}`,
    });
  },
});

const RegimeTransitionStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.REGIME_TRANSITION,
  name: 'Regime Transition',
  evaluate(context) {
    const vol = Number(context.metrics.volatility_realized_24h) || 0;
    const newsCount = Number(context.metrics.newsCount) || 0;
    const economicSurprise = Number(context.metrics.economicSurprise);
    const supporting = [];
    const contradicting = [];

    const sessionShift = context.session && context.session.status === 'open';
    const macroShift = Number.isFinite(economicSurprise) && Math.abs(economicSurprise) > 0.1;
    const volSpike = vol > 0.6;

    if ((sessionShift && volSpike) || macroShift) {
      supporting.push(
        evidenceRef({
          id: 'metric:regime_shift',
          kind: 'metric',
          summary: `regime_shift:vol=${vol}:macro=${macroShift}`,
          confidence: 0.7,
        })
      );
    }
    if (newsCount > 0 && volSpike) {
      supporting.push(
        ...evidenceByType(context, 'news:').slice(0, 2)
      );
    }
    if (!supporting.length) {
      contradicting.push(
        evidenceRef({
          id: 'metric:stable_regime',
          kind: 'metric',
          summary: 'no_regime_transition_signals',
          confidence: 0.5,
        })
      );
    }

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.REGIME_TRANSITION,
      scoreDelta: supporting.length ? 50 : -8,
      confidence: supporting.length ? 68 : 28,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.REGIME_TRANSITION}`] : [],
      summary: `regime_transition:signals=${supporting.length}`,
    });
  },
});

const NewsDrivenExpansionStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.NEWS_DRIVEN_EXPANSION,
  name: 'News-Driven Expansion',
  evaluate(context) {
    const newsCount = Number(context.metrics.newsCount) || 0;
    const sentiment = Number(context.metrics.avgNewsSentiment) || 0;
    const newsEvidence = evidenceByType(context, 'news:');
    const supporting = [];
    const contradicting = [];

    if (newsCount > 0 && sentiment > 0.2) {
      supporting.push(...newsEvidence);
    } else if (newsCount > 0 && sentiment < -0.2) {
      contradicting.push(...newsEvidence);
    }

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.NEWS_DRIVEN_EXPANSION,
      scoreDelta:
        newsCount > 0 && sentiment > 0.2
          ? Math.min(60, sentiment * 100 + newsCount * 10)
          : newsCount > 0
            ? -15
            : 0,
      confidence: newsCount > 0 ? 55 + Math.min(30, newsCount * 10) : 20,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.NEWS_DRIVEN_EXPANSION}`] : [],
      summary: `news_driven_expansion:count=${newsCount}:sentiment=${sentiment}`,
    });
  },
});

const LiquidityContractionStrategy = Object.freeze({
  id: MARKET_CLAIM_TYPES.LIQUIDITY_CONTRACTION,
  name: 'Liquidity Contraction',
  evaluate(context) {
    const vol1h = Number(context.metrics.volume_1h) || 0;
    const vol24h = Number(context.metrics.volume24h) || 0;
    const supporting = [];
    const contradicting = [];

    const contracted = vol24h > 0 && vol1h > 0 && vol1h < vol24h / 30;

    if (contracted) {
      supporting.push(
        evidenceRef({
          id: 'metric:liquidity_contraction',
          kind: 'metric',
          summary: `liquidity_contracted:vol1h=${vol1h}:vol24h=${vol24h}`,
          confidence: 0.66,
        })
      );
    } else if (vol1h > vol24h / 10) {
      contradicting.push(
        evidenceRef({
          id: 'metric:liquidity_adequate',
          kind: 'metric',
          summary: 'hourly_volume_healthy',
          confidence: 0.6,
        })
      );
    }

    return strategyResult({
      strategy: MARKET_CLAIM_TYPES.LIQUIDITY_CONTRACTION,
      scoreDelta: contracted ? 42 : -5,
      confidence: contracted ? 62 : 30,
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: supporting.length ? [`claim:${MARKET_CLAIM_TYPES.LIQUIDITY_CONTRACTION}`] : [],
      summary: `liquidity_contraction:contracted=${contracted}`,
    });
  },
});

/**
 * Create registry with all seven market claim strategies (SPEC-016).
 * @returns {MarketStrategyRegistry}
 */
function createMarketStrategyRegistry() {
  const registry = new MarketStrategyRegistry();
  registry
    .register(MomentumContinuationStrategy)
    .register(MomentumExhaustionStrategy)
    .register(ElevatedVolatilityStrategy)
    .register(MeanReversionStrategy)
    .register(RegimeTransitionStrategy)
    .register(NewsDrivenExpansionStrategy)
    .register(LiquidityContractionStrategy);
  return registry;
}

/**
 * Weighted score aggregation for market strategies.
 * @param {import('./types').MarketStrategyResult[]} strategyResults
 * @param {Record<string, number>} [weights]
 */
function aggregateMarketScores(strategyResults, weights = DEFAULT_MARKET_WEIGHTS) {
  const results = strategyResults || [];
  const byStrategy = Object.fromEntries(results.map((r) => [r.strategy, r]));
  const normalizedScores = {};
  const weightedContributions = {};
  let weightedSum = 0;
  let weightTotal = 0;
  let confidenceWeightedSum = 0;
  let confidenceWeightTotal = 0;

  for (const [strategyId, weight] of Object.entries(weights)) {
    const result = byStrategy[strategyId];
    const delta = result ? clamp(result.scoreDelta, -100, 100) : 0;
    const normalized = round((delta + 100) / 2);
    normalizedScores[strategyId] = normalized;
    const contribution = weight * normalized;
    weightedContributions[strategyId] = round(contribution);
    weightedSum += contribution;
    weightTotal += weight;
    const conf = result ? clamp(result.confidence, 0, 100) : 0;
    confidenceWeightedSum += weight * conf;
    confidenceWeightTotal += weight;
  }

  return {
    score: round(weightTotal === 0 ? 0 : weightedSum / weightTotal),
    confidence: round(
      confidenceWeightTotal === 0 ? 0 : confidenceWeightedSum / confidenceWeightTotal
    ),
    normalizedScores,
    weightedContributions,
    byStrategy,
    weights: { ...weights },
  };
}

/**
 * Heuristic historical analog search (no ML).
 * @param {object} input
 * @param {import('./types').MarketContext} input.context
 * @param {import('./types').MarketStrategyResult[]} [input.strategyResults]
 * @param {object} [input.memory]
 */
function findMarketAnalogs(input) {
  const { context, strategyResults, memory } = input;
  const candidates =
    (memory && Array.isArray(memory.analogs) ? memory.analogs : null) ||
    defaultAnalogCandidates(context.subjectId);

  const activeClaims = (strategyResults || [])
    .filter((r) => (r.claims || []).length > 0)
    .map((r) => r.strategy);

  const changePct = Number(context.metrics.changePct) || 0;
  const vol = Number(context.metrics.volatility_realized_24h) || 0;

  return candidates
    .map((candidate) => {
      const claimOverlap = (candidate.claims || []).filter((c) =>
        activeClaims.includes(c)
      ).length;
      const changeDiff = Math.abs((candidate.changePct || 0) - changePct);
      const volDiff = Math.abs((candidate.volatility || 0) - vol);
      const similarityScore = round(
        Math.max(
          0,
          100 -
            changeDiff * 8 -
            volDiff * 40 +
            claimOverlap * 12
        ),
        1
      );
      return {
        id: candidate.id,
        similarityScore,
        timestamp: candidate.timestamp,
        supportingClaims: (candidate.claims || []).filter((c) =>
          activeClaims.includes(c)
        ),
      };
    })
    .filter((a) => a.similarityScore >= 40)
    .sort((a, b) => b.similarityScore - a.similarityScore)
    .slice(0, 5);
}

/**
 * @param {string} subjectId
 */
function defaultAnalogCandidates(subjectId) {
  // Fixed calendar timestamps — deterministic for replay (SPEC-018).
  return [
    {
      id: `analog:${subjectId}:2024-03-12`,
      timestamp: '2024-03-12T00:00:00.000Z',
      changePct: 2.1,
      volatility: 0.58,
      claims: [MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION, MARKET_CLAIM_TYPES.ELEVATED_VOLATILITY],
    },
    {
      id: `analog:${subjectId}:2024-01-10`,
      timestamp: '2024-01-10T00:00:00.000Z',
      changePct: 3.5,
      volatility: 0.72,
      claims: [MARKET_CLAIM_TYPES.MOMENTUM_EXHAUSTION, MARKET_CLAIM_TYPES.MEAN_REVERSION],
    },
    {
      id: `analog:${subjectId}:2023-11-02`,
      timestamp: '2023-11-02T00:00:00.000Z',
      changePct: -1.2,
      volatility: 0.45,
      claims: [MARKET_CLAIM_TYPES.LIQUIDITY_CONTRACTION],
    },
  ];
}

module.exports = {
  MarketStrategyRegistry,
  createMarketStrategyRegistry,
  aggregateMarketScores,
  findMarketAnalogs,
  MomentumContinuationStrategy,
  MomentumExhaustionStrategy,
  ElevatedVolatilityStrategy,
  MeanReversionStrategy,
  RegimeTransitionStrategy,
  NewsDrivenExpansionStrategy,
  LiquidityContractionStrategy,
};
