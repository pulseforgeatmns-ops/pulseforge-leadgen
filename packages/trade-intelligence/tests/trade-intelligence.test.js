'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createTradeIntelligenceEngine,
  createTradeAnalyzer,
  INTELLIGENCE_RULES,
  FINDING_TYPES,
  RUNTIME_VERSION,
} = require('..');

const TINY_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
  'base64'
);

function makeTrade(overrides = {}) {
  const base = {
    id: overrides.id || `trade-${Math.random().toString(36).slice(2, 8)}`,
    entryTime: overrides.entryTime || '2026-07-28T14:30:00.000Z',
    capturedAt: overrides.capturedAt || '2026-07-28T14:34:11.000Z',
    direction: 'Long',
    hypothesis: 'Velocity',
    confidence: 4,
    result: 'Win',
    screenshotId: 'shot-1',
    observationId: 'obs-1',
    symbol: 'BTC',
    timeframe: '5m',
    currentPrice: 65050,
    vwap: 65000,
    atr: 120,
    volume: 1000,
  };
  return { ...base, ...overrides };
}

function seedTrades() {
  const trades = [];
  for (let i = 0; i < 8; i += 1) {
    trades.push(
      makeTrade({
        id: `win-vel-${i}`,
        result: 'Win',
        hypothesis: 'Velocity',
        confidence: i % 2 === 0 ? 4 : 3,
        entryTime: `2026-07-28T14:${String(30 + i).padStart(2, '0')}:00.000Z`,
        currentPrice: 65100,
        vwap: 65000,
      })
    );
  }
  for (let i = 0; i < 4; i += 1) {
    trades.push(
      makeTrade({
        id: `loss-brk-${i}`,
        result: 'Loss',
        hypothesis: 'Breakout',
        confidence: 5,
        entryTime: `2026-07-28T15:${String(10 + i).padStart(2, '0')}:00.000Z`,
        atr: 250,
      })
    );
  }
  for (let i = 0; i < 3; i += 1) {
    trades.push(
      makeTrade({
        id: `win-low-${i}`,
        result: 'Win',
        hypothesis: 'Pullback',
        confidence: 2,
        entryTime: `2026-07-27T16:${String(20 + i).padStart(2, '0')}:00.000Z`,
      })
    );
  }
  return trades;
}

describe('SPEC-046 Trade Intelligence rules', () => {
  it('exports guiding rules', () => {
    assert.ok(INTELLIGENCE_RULES.EVIDENCE_FIRST);
    assert.ok(INTELLIGENCE_RULES.NO_EXECUTION);
    assert.ok(INTELLIGENCE_RULES.FINDINGS_IMMUTABLE);
    assert.equal(RUNTIME_VERSION, 'trade-intelligence@1.0.0');
  });
});

describe('SPEC-046 TradeAnalyzer', () => {
  it('computes session stats', () => {
    const analyzer = createTradeAnalyzer();
    const stats = analyzer.analyze(seedTrades());
    assert.equal(stats.trades, 15);
    assert.ok(stats.winRate > 0.5);
    assert.ok(stats.averageConfidence != null);
    assert.ok(stats.averageHoldTime);
  });
});

describe('SPEC-046 TradeIntelligenceEngine', () => {
  it('generates daily review from evidence', () => {
    const intel = createTradeIntelligenceEngine({ trades: seedTrades() });
    const daily = intel.generateDailyReview({ day: '2026-07-28' });

    assert.equal(daily.kind, 'daily_review');
    assert.equal(daily.title, "Today's Session");
    assert.ok(daily.trades >= 12);
    assert.ok(daily.winRate != null);
    assert.equal(daily.bestPerformingHypothesis.hypothesis, 'Velocity');
    assert.equal(daily.worstPerformingHypothesis.hypothesis, 'Breakout');
    assert.ok(daily.largestMistake);
    assert.equal(daily.immutable, true);
  });

  it('generates weekly review without manual input', () => {
    const intel = createTradeIntelligenceEngine({ trades: seedTrades() });
    const weekly = intel.generateWeeklyReview({
      from: '2026-07-21T00:00:00.000Z',
      to: '2026-07-29T00:00:00.000Z',
    });

    assert.equal(weekly.kind, 'weekly_review');
    assert.equal(weekly.title, 'This Week');
    assert.ok(weekly.trades >= 15);
    assert.ok(weekly.mostProfitableSetup);
    assert.ok(weekly.weakestSetup);
    assert.ok(weekly.mostCommonError);
  });

  it('discovers patterns and calibration findings', () => {
    const intel = createTradeIntelligenceEngine({ trades: seedTrades() });
    const result = intel.analyze();

    assert.ok(result.patterns >= 3);
    assert.ok(result.calibration.bands.length >= 1);
    assert.ok(result.findings.length >= result.patterns);
    assert.equal(result.executesTrades, false);

    for (const finding of result.findings) {
      assert.equal(finding.immutable, true);
      assert.ok(finding.createdAt);
      assert.equal(finding.runtimeVersion, RUNTIME_VERSION);
    }
  });

  it('produces explainable recommendations with evidence', () => {
    const intel = createTradeIntelligenceEngine({ trades: seedTrades() });
    intel.analyze();
    const recs = intel.getRecommendations();

    assert.ok(recs.length >= 1);
    const rec = recs[0];
    assert.ok(rec.title);
    assert.ok(rec.summary);
    assert.ok(rec.sampleSize >= 1);
    assert.ok(Array.isArray(rec.supportingEvidence));
    assert.ok(Array.isArray(rec.replayRefs));
    assert.equal(rec.explainable, true);
    assert.equal(rec.immutable, true);
  });

  it('reviewTrade is reproducible through replay refs', () => {
    const trades = seedTrades();
    const intel = createTradeIntelligenceEngine({ trades });
    const review = intel.reviewTrade('win-vel-0');

    assert.equal(review.trade.id, 'win-vel-0');
    assert.ok(review.replayRef);
    assert.equal(review.reproducible, true);
    assert.ok(review.finding.replayRefs.includes('win-vel-0'));
  });

  it('finds similar trades by hypothesis and metadata', () => {
    const trades = seedTrades();
    const intel = createTradeIntelligenceEngine({ trades });
    const similar = intel.findSimilarTrades('win-vel-0', { limit: 5 });

    assert.ok(similar.length >= 1);
    assert.ok(similar.every((t) => t.hypothesis === 'Velocity'));
    assert.ok(similar[0].similarityScore > 0);
  });

  it('compareWeek surfaces week-over-week deltas', () => {
    const intel = createTradeIntelligenceEngine({ trades: seedTrades() });
    const comparison = intel.compareWeek({
      currentEnd: '2026-07-29T00:00:00.000Z',
    });

    assert.equal(comparison.kind, 'compareWeek');
    assert.ok(comparison.left);
    assert.ok(comparison.right);
    assert.equal(comparison.reproducible, true);
  });

  it('hypothesis performance tracks independently', () => {
    const intel = createTradeIntelligenceEngine({ trades: seedTrades() });
    const perf = intel.hypothesisPerformance();

    const velocity = perf.find((p) => p.hypothesis === 'Velocity');
    const breakout = perf.find((p) => p.hypothesis === 'Breakout');
    assert.ok(velocity.winRate > breakout.winRate);
    assert.ok(velocity.trades >= 8);
    assert.ok(['Improving', 'Stable', 'Degrading', 'Insufficient data'].includes(velocity.trend));
  });
});

describe('SPEC-046 integration with trade-capture', () => {
  it('analyzes trades from capture engine', async () => {
    const { createCaptureEngine } = require('@pulseforge/trade-capture');
    const capture = createCaptureEngine({ runExtractorsSync: true });

    for (let i = 0; i < 6; i += 1) {
      await capture.capture({
        screenshot: i % 2 === 0 ? TINY_PNG : Buffer.from(`shot-${i}`),
        answers: {
          result: i < 4 ? 'Win' : 'Loss',
          direction: 'Long',
          hypothesis: i < 4 ? 'Velocity' : 'Breakout',
          confidence: 3 + (i % 3),
        },
        opts: {
          subjectId: 'BTC',
          entryTime: `2026-07-28T14:${String(30 + i).padStart(2, '0')}:00.000Z`,
        },
      });
    }

    const intel = createTradeIntelligenceEngine({ captureEngine: capture });
    const daily = intel.generateDailyReview({ day: '2026-07-28' });
    assert.ok(daily.trades >= 6);
    assert.ok(intel.getRecommendations().length >= 0);
  });
});
