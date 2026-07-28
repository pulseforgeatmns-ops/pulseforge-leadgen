'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createEvidenceLab,
  createExperiment,
  LAB_RULES,
} = require('..');
const {
  createMarketStrategyPack,
  createResearchRecommendationProvider,
} = require('@pulseforge/market-strategy');

const START = '2026-07-26T13:00:00.000Z';
const END = '2026-07-26T16:00:00.000Z';

const BTC_OBSERVATIONS = [
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
  {
    type: 'news_event',
    headline: 'BTC institutional inflows accelerate',
    symbols: ['BTC'],
    sentiment: 0.4,
    timestamp: '2026-07-26T14:55:00.000Z',
  },
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
];

function baseSeed(overrides = {}) {
  return {
    subjectId: 'BTC',
    startTime: START,
    endTime: END,
    ontology: 'market',
    strategyPack: 'market',
    observations: BTC_OBSERVATIONS,
    ...overrides,
  };
}

describe('SPEC-019 Laboratory rules', () => {
  it('exports guiding rules', () => {
    assert.ok(LAB_RULES.ASK_ONLY);
    assert.ok(LAB_RULES.NO_PRODUCTION_MUTATION);
    assert.ok(LAB_RULES.NOT_PAPER_TRADING);
    assert.ok(LAB_RULES.ISOLATED_EXPERIMENTS);
  });
});

describe('SPEC-019 Experiment isolation', () => {
  it('never mutates production and copy-on-write removes/injects', () => {
    const parent = createExperiment(baseSeed({ name: 'baseline' }));
    assert.equal(parent.mutatesProduction, false);
    assert.equal(parent.isIsolated, true);

    const baselineCount = parent.getObservations().length;
    assert.equal(baselineCount, BTC_OBSERVATIONS.length);

    const targetId = parent.getObservations()[0].id;
    const removed = parent.withRemoved(targetId);
    assert.equal(removed.parentId, parent.id);
    assert.equal(removed.getObservations().length, baselineCount - 1);
    assert.equal(parent.getObservations().length, baselineCount);
    assert.ok(removed.removedObservationIds().includes(targetId));

    const injected = parent.withInjected({
      type: 'news_event',
      headline: 'counterfactual shock',
      symbols: ['BTC'],
      sentiment: -0.8,
      timestamp: '2026-07-26T15:30:00.000Z',
    });
    assert.equal(injected.getObservations().length, baselineCount + 1);
    assert.equal(parent.getObservations().length, baselineCount);
  });
});

describe('SPEC-019 EvidenceLab capabilities', () => {
  it('runs an isolated experiment without production mutation flags', async () => {
    const lab = createEvidenceLab();
    const exp = lab.createExperiment(baseSeed());
    const result = await lab.run(exp);

    assert.equal(result.isolated, true);
    assert.equal(result.mutatesProduction, false);
    assert.equal(result.subjectId, 'BTC');
    assert.ok(result.confidence != null);
    assert.ok(Array.isArray(result.recommendations));
    assert.ok(result.recommendations.length > 0);
    assert.ok(result.versions.ontology.includes('market'));
  });

  it('compareReplay surfaces side-by-side diffs for what-if removals', async () => {
    const lab = createEvidenceLab();
    const baseline = lab.createExperiment(baseSeed({ name: 'full-history' }));
    const obsId = baseline.getObservations().find((o) => o.observationType === 'news_event').id;
    const withoutNews = lab.removeObservation({
      experiment: baseline,
      observationId: obsId,
      name: 'without-news',
    });

    assert.equal(withoutNews.getObservations().length, baseline.getObservations().length - 1);

    const comparison = await lab.compareReplay({
      left: baseline,
      right: withoutNews,
      name: 'news-ablation',
    });

    assert.equal(comparison.name, 'news-ablation');
    assert.ok(comparison.sideBySide);
    assert.ok('confidence' in comparison.sideBySide);
    assert.ok('recommendation' in comparison.sideBySide);
    assert.equal(typeof comparison.identical, 'boolean');
  });

  it('removeObservation({ run: true }) executes the counterfactual', async () => {
    const lab = createEvidenceLab();
    const baseline = lab.createExperiment(baseSeed());
    const obsId = baseline.getObservations()[0].id;
    const out = await lab.removeObservation({
      experiment: baseline,
      observationId: obsId,
      run: true,
    });
    assert.equal(out.mutation, 'removeObservation');
    assert.equal(out.result.mutatesProduction, false);
    assert.equal(
      out.result.observations.length,
      baseline.getObservations().length - 1
    );
  });

  it('injectObservation adds a counterfactual observation', async () => {
    const lab = createEvidenceLab();
    const baseline = lab.createExperiment(baseSeed());
    const child = lab.injectObservation({
      experiment: baseline,
      observation: {
        type: 'volatility_observation',
        asset: 'BTC',
        value: 0.95,
        measure: 'realized_24h',
        timestamp: '2026-07-26T15:45:00.000Z',
      },
    });
    assert.equal(child.getObservations().length, baseline.getObservations().length + 1);

    const ran = await lab.injectObservation({
      experiment: baseline,
      observation: {
        type: 'news_event',
        headline: 'injected',
        symbols: ['BTC'],
        sentiment: 0.1,
        timestamp: '2026-07-26T15:50:00.000Z',
      },
      run: true,
    });
    assert.equal(ran.mutation, 'injectObservation');
    assert.ok(ran.result.observations.length > baseline.getObservations().length);
  });

  it('findAnalogs returns historical situations', async () => {
    const lab = createEvidenceLab();
    const out = await lab.findAnalogs(baseSeed({ name: 'analogs' }));
    assert.equal(out.subjectId, 'BTC');
    assert.equal(out.mutatesProduction, false);
    assert.ok(Array.isArray(out.analogs));
    assert.ok(out.analogs.length > 0);
    assert.ok(out.analogs[0].id);
  });

  it('findAnalogs honors injected analogFinder', async () => {
    const lab = createEvidenceLab({
      analogFinder: async () => [{ id: 'analog:custom', similarityScore: 88 }],
    });
    const out = await lab.findAnalogs(baseSeed());
    assert.equal(out.source, 'analogFinder');
    assert.equal(out.analogs[0].id, 'analog:custom');
  });

  it('compareStrategies diffs two strategy packs over the same history', async () => {
    const lab = createEvidenceLab();
    const leftPack = createMarketStrategyPack({
      id: 'market-left',
      version: '1',
      recommendationProvider: createResearchRecommendationProvider({ id: 'rec-left' }),
    });
    // Right pack forces gather_more_evidence via empty analogs + low confidence path:
    // wrap recommendation provider to rewrite action.
    const baseProvider = createResearchRecommendationProvider({ id: 'rec-right' });
    const rightProvider = {
      id: 'rec-right-forced',
      generate(input) {
        const rec = baseProvider.generate(input);
        return {
          ...rec,
          recommendedAction: 'gather_more_evidence',
          id: `forced:${rec.id}`,
        };
      },
    };
    const rightPack = createMarketStrategyPack({
      id: 'market-right',
      version: '2',
      recommendationProvider: rightProvider,
    });

    const comparison = await lab.compareStrategies({
      ...baseSeed(),
      left: leftPack,
      right: rightPack,
      name: 'pack-a-vs-b',
    });

    assert.equal(comparison.kind, 'compareStrategies');
    assert.equal(comparison.name, 'pack-a-vs-b');
    assert.ok(comparison.sideBySide.recommendation);
    assert.equal(
      comparison.observationDiff.leftOnly.length +
        comparison.observationDiff.rightOnly.length,
      0
    );
  });

  it('compareOntologies diffs ontology version labels over the same history', async () => {
    const lab = createEvidenceLab();
    const comparison = await lab.compareOntologies({
      ...baseSeed(),
      left: { id: 'market', version: '1.0.0' },
      right: { id: 'market', version: '2.0.0' },
      name: 'ontology-v1-v2',
    });

    assert.equal(comparison.kind, 'compareOntologies');
    assert.equal(comparison.leftOntology, 'market@1.0.0');
    assert.equal(comparison.rightOntology, 'market@2.0.0');
    assert.ok(comparison.sideBySide.versions);
    assert.equal(comparison.identical, false);
  });

  it('observationsForClaim returns claim evidence surface', async () => {
    const lab = createEvidenceLab();
    const result = await lab.run(baseSeed());
    const claimId =
      (result.claims &&
        result.claims.derived &&
        result.claims.derived[0] &&
        result.claims.derived[0].id) ||
      (result.claims &&
        result.claims.results &&
        result.claims.results[0] &&
        result.claims.results[0].strategy);

    assert.ok(claimId, 'expected at least one claim from market replay');
    const evidence = await lab.observationsForClaim({ result, claimId });
    assert.equal(evidence.claimId, claimId);
    assert.ok(Array.isArray(evidence.supporting));
    assert.ok(evidence.contradicting);
  });

  it('ScenarioRunner refuses production write options', async () => {
    const lab = createEvidenceLab();
    const exp = lab.createExperiment(baseSeed());
    await assert.rejects(
      () => lab.runner.run(exp, { persist: true }),
      /refuses persist/
    );
  });

  it('lab.query runs domain-neutral EQL against an experiment result (SPEC-020)', async () => {
    const lab = createEvidenceLab();
    const result = await lab.run(baseSeed());

    const found = await lab.query(
      `
      FIND Claims
      WHERE subject = "BTC"
      AND confidence > 0.1
      ORDER BY confidence DESC
    `,
      { result }
    );

    assert.equal(found.kind, 'FIND');
    assert.equal(found.mutatesProduction, false);
    assert.ok(found.count >= 1);

    const replayed = await lab.query(
      `
      REPLAY
      FROM "2026-07-26T13:00:00.000Z"
      TO "2026-07-26T16:00:00.000Z"
    `,
      { result }
    );
    assert.equal(replayed.kind, 'REPLAY');
    assert.ok(replayed.count >= 1);
  });

  it('lab.compareCalibration and lab.replayWithCalibration (SPEC-021)', async () => {
    const lab = createEvidenceLab();
    const left = {
      claims: [{ id: 'momentum_continuation', confidence: 0.82, strategyPack: 'market' }],
      outcomes: [
        { claimId: 'momentum_continuation', verdict: 'correct', strategyPack: 'market' },
        { claimId: 'momentum_continuation', verdict: 'correct', strategyPack: 'market' },
        { claimId: 'momentum_continuation', verdict: 'incorrect', strategyPack: 'market' },
      ],
      strategyPack: 'market',
    };
    const right = {
      claims: [{ id: 'momentum_continuation', confidence: 0.82, strategyPack: 'market' }],
      outcomes: [
        { claimId: 'momentum_continuation', verdict: 'correct', strategyPack: 'market' },
        { claimId: 'momentum_continuation', verdict: 'incorrect', strategyPack: 'market' },
        { claimId: 'momentum_continuation', verdict: 'incorrect', strategyPack: 'market' },
      ],
      strategyPack: 'market',
    };

    const comparison = lab.compareCalibration({ left, right });
    assert.equal(comparison.kind, 'compareCalibration');
    assert.equal(comparison.mutatesHistory, false);
    assert.equal(comparison.mutatesReplay, false);
    assert.equal(comparison.mutatesRuntime, false);
    assert.ok(comparison.leftAccuracy.accuracy > comparison.rightAccuracy.accuracy);

    const overlay = await lab.replayWithCalibration({
      experiment: baseSeed(),
      outcomes: [
        {
          claimId: 'momentum_continuation',
          verdict: 'correct',
          strategyPack: 'market',
        },
        {
          claimId: 'momentum_continuation',
          verdict: 'incorrect',
          strategyPack: 'market',
        },
      ],
      strategyPack: 'market',
    });
    assert.equal(overlay.kind, 'replayWithCalibration');
    assert.equal(overlay.mutatesReplay, false);
    assert.equal(overlay.mutatesRuntime, false);
    assert.ok(overlay.replay);
    assert.ok(overlay.learning);

    const shown = await lab.query(
      `SHOW Calibration FOR Claim("momentum_continuation")`
    );
    assert.equal(shown.kind, 'SHOW');
  });

  it('lab.findTrades / compareWinningTrades / compareLosingTrades (SPEC-044)', async () => {
    const { createCaptureEngine } = require('@pulseforge/trade-capture');
    const capture = createCaptureEngine({ runExtractorsSync: true });
    const png = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
      'base64'
    );
    await capture.capture({
      screenshot: png,
      answers: {
        result: 'Win',
        direction: 'Long',
        hypothesis: 'Velocity',
        confidence: 4,
      },
    });
    await capture.capture({
      screenshot: Buffer.from('loss-shot'),
      answers: {
        result: 'Loss',
        direction: 'Short',
        hypothesis: 'Velocity',
        confidence: 2,
      },
    });

    const lab = createEvidenceLab({ tradeCapture: capture });
    lab.ingestTrades(capture);

    const velocity = lab.findTrades({ hypothesis: 'Velocity' });
    assert.equal(velocity.length, 2);

    const wins = lab.compareWinningTrades({ hypothesis: 'Velocity' });
    assert.equal(wins.kind, 'compareWinningTrades');
    assert.equal(wins.count, 1);

    const losses = lab.compareLosingTrades({ hypothesis: 'Velocity' });
    assert.equal(losses.kind, 'compareLosingTrades');
    assert.equal(losses.count, 1);

    const found = await lab.query(`FIND Trades WHERE hypothesis = "Velocity"`);
    assert.equal(found.count, 2);

    const compared = await lab.query(`COMPARE WinningTrades WITH LosingTrades`);
    assert.equal(compared.rows[0].left.count, 1);
    assert.equal(compared.rows[0].right.count, 1);
  });

  it('lab trade intelligence helpers (SPEC-046)', async () => {
    const { createCaptureEngine } = require('@pulseforge/trade-capture');
    const { createTradeIntelligenceEngine } = require('@pulseforge/trade-intelligence');
    const capture = createCaptureEngine({ runExtractorsSync: true });
    const png = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
      'base64'
    );

    for (let i = 0; i < 6; i += 1) {
      await capture.capture({
        screenshot: i % 2 === 0 ? png : Buffer.from(`intel-${i}`),
        answers: {
          result: i < 4 ? 'Win' : 'Loss',
          direction: 'Long',
          hypothesis: i < 4 ? 'Velocity' : 'Breakout',
          confidence: 2 + (i % 4),
        },
        opts: { entryTime: `2026-07-28T14:${String(30 + i).padStart(2, '0')}:00.000Z` },
      });
    }

    const intel = createTradeIntelligenceEngine({ captureEngine: capture });
    const lab = createEvidenceLab({ tradeCapture: capture, tradeIntelligence: intel });
    lab.ingestTradeIntelligence(intel);

    const patterns = lab.discoverTradePatterns();
    assert.equal(patterns.kind, 'discoverTradePatterns');
    assert.ok(patterns.count >= 1);

    const strategies = lab.compareTradeStrategies();
    assert.equal(strategies.kind, 'compareTradeStrategies');

    const windows = lab.compareTimeWindows();
    assert.equal(windows.kind, 'compareTimeWindows');

    const bands = lab.compareConfidenceBands();
    assert.equal(bands.kind, 'compareConfidenceBands');

    const daily = await lab.query('SHOW DailyReview FOR Today');
    assert.equal(daily.count, 1);

    const recs = await lab.query('SHOW Recommendations');
    assert.ok(recs.count >= 0);
  });
});
