'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createReasoningRuntime } = require('@pulseforge/reasoning-runtime');
const {
  createMarketReasoningRuntime,
  createMarketStrategyPack,
  createMarketContextProvider,
  createResearchRecommendationProvider,
  createMarketStrategyRegistry,
  MARKET_CLAIM_TYPES,
  RESEARCH_ACTIONS,
  FORBIDDEN_ACTIONS,
} = require('..');

const BTC_OBSERVATIONS = [
  {
    type: 'market_snapshot',
    asset: 'BTC',
    price: 68000,
    volume24h: 30000000000,
    changePct: 2.8,
    timestamp: '2026-07-26T15:00:00.000Z',
  },
  {
    type: 'price_tick',
    asset: 'BTC',
    price: 68000,
    venue: 'coinbase',
    timestamp: '2026-07-26T15:00:01.000Z',
  },
  {
    type: 'volume_update',
    asset: 'BTC',
    volume: 2100000000,
    window: '1h',
    timestamp: '2026-07-26T15:00:02.000Z',
  },
  {
    type: 'volatility_observation',
    asset: 'BTC',
    value: 0.65,
    measure: 'realized_24h',
    timestamp: '2026-07-26T15:00:03.000Z',
  },
  {
    type: 'news_event',
    headline: 'BTC institutional inflows accelerate',
    symbols: ['BTC'],
    sentiment: 0.4,
    timestamp: '2026-07-26T14:55:00.000Z',
  },
  {
    type: 'economic_release',
    series: 'CPI',
    actual: 3.2,
    forecast: 3.0,
    prior: 3.1,
    timestamp: '2026-07-26T13:30:00.000Z',
  },
  {
    type: 'market_session',
    session: 'us_regular',
    status: 'open',
    timestamp: '2026-07-26T14:30:00.000Z',
  },
];

describe('MarketContextProvider', () => {
  it('normalizes all supported observation types without reasoning', () => {
    const provider = createMarketContextProvider();
    const context = provider.build({
      subjectId: 'BTC',
      observations: BTC_OBSERVATIONS,
    });

    assert.equal(context.subjectId, 'BTC');
    assert.equal(context.asset.symbol, 'BTC');
    assert.equal(context.observations.length, 7);
    assert.equal(context.evidence.length, 7);
    assert.ok(context.metrics.changePct > 0);
    assert.ok(context.metrics.volatility_realized_24h > 0);
    assert.equal(context.session.status, 'open');
    assert.ok(context.builtAt);
    // Provider must not attach ranked claims or confidence scores to context
    assert.equal(context.claims.length, 0);
  });

  it('provides default fixture observations when none supplied', () => {
    const provider = createMarketContextProvider();
    const context = provider.build({ subjectId: 'ETH' });
    assert.ok(context.observations.length >= 5);
    assert.ok(context.evidence.length >= 5);
  });
});

describe('MarketStrategyPack + registry', () => {
  it('builds claims from seven market strategies', () => {
    const registry = createMarketStrategyRegistry();
    assert.equal(registry.ids().length, 7);
    assert.ok(registry.ids().includes(MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION));

    const provider = createMarketContextProvider();
    const context = provider.build({ subjectId: 'BTC', observations: BTC_OBSERVATIONS });
    const { results } = registry.evaluateAll(context);
    assert.equal(results.length, 7);
    for (const r of results) {
      assert.ok(r.strategy);
      assert.ok(typeof r.scoreDelta === 'number');
      assert.ok(typeof r.confidence === 'number');
      assert.ok(!Object.prototype.hasOwnProperty.call(r, 'recommendedAction'));
    }
  });
});

describe('ResearchRecommendationProvider', () => {
  it('returns research actions only — never execution vocabulary', () => {
    const provider = createResearchRecommendationProvider();
    const rec = provider.generate({
      context: {
        subjectId: 'BTC',
        asset: { id: 'BTC', symbol: 'BTC', name: 'BTC' },
        evidence: [],
        claims: [],
        observations: [],
        metrics: {},
        session: null,
        builtAt: new Date().toISOString(),
        repositoryType: 'test',
      },
      strategyResults: [],
      aggregated: { score: 50, confidence: 50, normalizedScores: {} },
      analogs: [],
    });

    const action = String(rec.recommendedAction).toLowerCase();
    for (const forbidden of FORBIDDEN_ACTIONS) {
      assert.ok(
        !action.includes(forbidden),
        `recommendedAction must not contain "${forbidden}"`
      );
    }
    assert.ok(Object.values(RESEARCH_ACTIONS).includes(rec.recommendedAction));
  });
});

describe('Market reasoning runtime (SPEC-016 acceptance)', () => {
  it('evaluate({ subjectId: "BTC" }) returns full explainable research output', async () => {
    const recommendationProvider = createResearchRecommendationProvider();
    const strategyPack = createMarketStrategyPack({ recommendationProvider });
    const contextProvider = createMarketContextProvider();
    const runtime = createReasoningRuntime({
      strategyPack,
      contextProvider,
      recommendationProvider,
    });

    const out = await runtime.evaluate({ subjectId: 'BTC' });

    // Claims
    assert.ok(out.claims);
    assert.ok(Array.isArray(out.claims.results));
    assert.equal(out.claims.results.length, 7);

    // Evidence
    assert.ok(Array.isArray(out.evidence));
    assert.ok(out.evidence.length > 0);

    // Historical analogs
    assert.ok(Array.isArray(out.analogs));
    assert.ok(out.analogs.length > 0);
    assert.ok(out.analogs[0].id);
    assert.ok(typeof out.analogs[0].similarityScore === 'number');
    assert.ok(out.analogs[0].timestamp);
    assert.ok(Array.isArray(out.analogs[0].supportingClaims));

    // Confidence
    assert.ok(out.ranked);
    assert.ok(typeof out.ranked.confidence === 'number');
    assert.ok(out.recommendation.confidence > 0);

    // Reasoning trace
    assert.ok(out.explanation.reasoningTrace);
    assert.equal(out.explanation.reasoningTrace.packId, 'market');
    assert.equal(out.explanation.reasoningTrace.domain, 'market');
    assert.ok(out.trace.steps.length >= 7);

    // Research recommendations
    assert.ok(out.recommendation);
    assert.ok(Object.values(RESEARCH_ACTIONS).includes(out.recommendation.recommendedAction));

    // Explainability surface
    assert.ok(Array.isArray(out.explanation.claims));
    assert.ok(Array.isArray(out.explanation.supportingEvidence));
    assert.ok(Array.isArray(out.explanation.contradictingEvidence));
    assert.ok(Array.isArray(out.explanation.historicalAnalogs));
    assert.ok(out.explanation.confidence != null || out.recommendation.confidence != null);
    assert.ok(out.explanation.reasoningTrace);

    assert.equal(out.meta.packId, 'market');
    assert.equal(out.meta.domain, 'market');
  });

  it('createMarketReasoningRuntime factory wires acceptance path', async () => {
    const runtime = createMarketReasoningRuntime();
    const out = await runtime.evaluate({ subjectId: 'BTC' });
    assert.ok(out.claims);
    assert.ok(out.evidence.length > 0);
    assert.ok(out.analogs.length > 0);
    assert.ok(out.recommendation);
    assert.ok(out.explanation.reasoningTrace);
  });

  it('analog lookup executes with custom observations', async () => {
    const runtime = createMarketReasoningRuntime();
    const out = await runtime.evaluate({
      subjectId: 'BTC',
      observations: BTC_OBSERVATIONS,
    });
    const withClaims = out.analogs.filter((a) => a.supportingClaims.length > 0);
    assert.ok(withClaims.length > 0 || out.analogs.length > 0);
  });
});
