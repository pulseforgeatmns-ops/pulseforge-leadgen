'use strict';

/**
 * Outcome Intelligence tests — SPEC-013 / ADR-008.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  LIFECYCLE,
  OUTCOME_RESULTS,
  buildRecommendationOutcome,
  canTransitionLifecycle,
  bandForConfidence,
  createOutcomeEngine,
  buildCalibrationReport,
  buildStrategyPerformance,
  detectDrift,
  buildReviewDashboard,
} = require('../index');
const { createMaxReasoningRuntime } = require('../../index');

describe('RecommendationOutcome model', () => {
  it('builds a frozen outcome record', () => {
    const row = buildRecommendationOutcome({
      recommendationId: 'rec:10:a',
      tenantId: '10',
      confidenceAtRecommendation: 92,
      strategyId: 'overflow',
      lifecycle: LIFECYCLE.GENERATED,
    });
    assert.equal(row.recommendationId, 'rec:10:a');
    assert.equal(row.confidenceBand, '90+');
    assert.equal(row.executed, false);
    assert.equal(row.outcome, null);
    assert.throws(() => {
      row.outcome = 'successful';
    });
  });

  it('maps confidence into bands', () => {
    assert.equal(bandForConfidence(95), '90+');
    assert.equal(bandForConfidence(85), '80-89');
    assert.equal(bandForConfidence(72), '70-79');
    assert.equal(bandForConfidence(40), '<60');
  });
});

describe('Outcome lifecycle', () => {
  it('tracks Generated → … → Successful', () => {
    const engine = createOutcomeEngine();
    engine.record({
      tenantId: '10',
      recommendationId: 'rec:1',
      lifecycle: LIFECYCLE.GENERATED,
      confidenceAtRecommendation: 88,
      strategyId: 'relationship',
    });

    const steps = [
      LIFECYCLE.REVIEWED,
      LIFECYCLE.APPROVED,
      LIFECYCLE.EXECUTED,
      LIFECYCLE.OBSERVED,
      LIFECYCLE.SUCCESSFUL,
    ];
    for (const step of steps) {
      engine.transition({
        tenantId: '10',
        recommendationId: 'rec:1',
        lifecycle: step,
        confidenceAtOutcome: 88,
      });
    }

    const row = engine.get('10', 'rec:1');
    assert.equal(row.lifecycle, LIFECYCLE.SUCCESSFUL);
    assert.equal(row.outcome, OUTCOME_RESULTS.SUCCESSFUL);
    assert.equal(row.executed, true);
    assert.ok(row.observedAt);
  });

  it('rejects invalid jumps without force', () => {
    assert.equal(
      canTransitionLifecycle(LIFECYCLE.GENERATED, LIFECYCLE.SUCCESSFUL),
      false
    );
    const engine = createOutcomeEngine();
    engine.record({
      tenantId: '10',
      recommendationId: 'rec:x',
      lifecycle: LIFECYCLE.GENERATED,
      confidenceAtRecommendation: 70,
    });
    assert.throws(() =>
      engine.transition({
        tenantId: '10',
        recommendationId: 'rec:x',
        lifecycle: LIFECYCLE.SUCCESSFUL,
      })
    );
  });

  it('supports Unsuccessful and Inconclusive terminals', () => {
    const engine = createOutcomeEngine();
    for (const [id, terminal] of [
      ['a', LIFECYCLE.UNSUCCESSFUL],
      ['b', LIFECYCLE.INCONCLUSIVE],
    ]) {
      engine.record({
        tenantId: '10',
        recommendationId: id,
        lifecycle: LIFECYCLE.GENERATED,
        confidenceAtRecommendation: 75,
      });
      for (const step of [
        LIFECYCLE.REVIEWED,
        LIFECYCLE.APPROVED,
        LIFECYCLE.EXECUTED,
        LIFECYCLE.OBSERVED,
        terminal,
      ]) {
        engine.transition({
          tenantId: '10',
          recommendationId: id,
          lifecycle: step,
        });
      }
      assert.equal(engine.get('10', id).outcome, terminal);
    }
  });
});

describe('Confidence calibration', () => {
  it('reports empirical success by band without mutating confidence', () => {
    const records = [
      buildRecommendationOutcome({
        recommendationId: '1',
        tenantId: '10',
        confidenceAtRecommendation: 92,
        lifecycle: LIFECYCLE.SUCCESSFUL,
        outcome: OUTCOME_RESULTS.SUCCESSFUL,
        executed: true,
        observedAt: '2026-07-26T12:00:00.000Z',
      }),
      buildRecommendationOutcome({
        recommendationId: '2',
        tenantId: '10',
        confidenceAtRecommendation: 94,
        lifecycle: LIFECYCLE.SUCCESSFUL,
        outcome: OUTCOME_RESULTS.SUCCESSFUL,
        executed: true,
        observedAt: '2026-07-26T12:00:00.000Z',
      }),
      buildRecommendationOutcome({
        recommendationId: '3',
        tenantId: '10',
        confidenceAtRecommendation: 91,
        lifecycle: LIFECYCLE.UNSUCCESSFUL,
        outcome: OUTCOME_RESULTS.UNSUCCESSFUL,
        executed: true,
        observedAt: '2026-07-26T12:00:00.000Z',
      }),
      buildRecommendationOutcome({
        recommendationId: '4',
        tenantId: '10',
        confidenceAtRecommendation: 72,
        lifecycle: LIFECYCLE.SUCCESSFUL,
        outcome: OUTCOME_RESULTS.SUCCESSFUL,
        executed: true,
        promotedFromWatch: true,
        observedAt: '2026-07-26T12:00:00.000Z',
      }),
    ];

    const report = buildCalibrationReport({ records });
    assert.equal(report.mutatesConfidence, false);
    assert.equal(report.customerFacing, false);
    const high = report.bands.find((b) => b.band === '90+');
    assert.equal(high.observed, 3);
    assert.equal(high.successRate, round3(2 / 3));
    assert.ok(/88%|67%|Historically/.test(report.narrative));
    assert.equal(
      report.midConfidencePromotion.promotedFromWatch.successful,
      1
    );
  });
});

describe('Strategy performance', () => {
  it('tracks precision / recall / success / lead time', () => {
    const engine = createOutcomeEngine();
    engine.record({
      tenantId: '10',
      recommendationId: 'o1',
      strategyId: 'overflow',
      lifecycle: LIFECYCLE.GENERATED,
      confidenceAtRecommendation: 80,
      generatedAt: '2026-07-20T10:00:00.000Z',
    });
    advanceTo(engine, '10', 'o1', LIFECYCLE.SUCCESSFUL, {
      executedAt: '2026-07-22T10:00:00.000Z',
    });

    engine.record({
      tenantId: '10',
      recommendationId: 'o2',
      strategyId: 'overflow',
      lifecycle: LIFECYCLE.GENERATED,
      confidenceAtRecommendation: 78,
      promotedFromWatch: true,
    });
    advanceTo(engine, '10', 'o2', LIFECYCLE.UNSUCCESSFUL);

    engine.record({
      tenantId: '10',
      recommendationId: 'r1',
      strategyId: 'relationship',
      lifecycle: LIFECYCLE.GENERATED,
      confidenceAtRecommendation: 85,
    });
    advanceTo(engine, '10', 'r1', LIFECYCLE.SUCCESSFUL);

    const perf = buildStrategyPerformance({
      records: engine.store.listForTenant('10'),
    });
    const overflow = perf.strategies.find((s) => s.strategyId === 'overflow');
    assert.equal(overflow.generated, 2);
    assert.equal(overflow.successful, 1);
    assert.equal(overflow.unsuccessful, 1);
    assert.equal(overflow.precision, 0.5);
    assert.ok(overflow.promotionRate != null);
    assert.equal(perf.mutatesReasoning, false);

    const relationship = perf.strategies.find(
      (s) => s.strategyId === 'relationship'
    );
    assert.equal(relationship.recommendationSuccessRate, 1);
  });
});

describe('Drift detection', () => {
  it('alerts when recent success drops vs baseline', () => {
    const now = Date.parse('2026-07-26T12:00:00.000Z');
    const records = [];

    // Baseline window: 8–30 days ago — mostly successful
    for (let i = 0; i < 5; i++) {
      records.push(
        buildRecommendationOutcome({
          recommendationId: `base-${i}`,
          tenantId: '10',
          strategyId: 'overflow',
          confidenceAtRecommendation: 90,
          lifecycle: LIFECYCLE.SUCCESSFUL,
          outcome: OUTCOME_RESULTS.SUCCESSFUL,
          executed: true,
          observedAt: new Date(now - (10 + i) * 86400000).toISOString(),
          generatedAt: new Date(now - (12 + i) * 86400000).toISOString(),
        })
      );
    }

    // Recent window: mostly unsuccessful
    for (let i = 0; i < 5; i++) {
      records.push(
        buildRecommendationOutcome({
          recommendationId: `recent-${i}`,
          tenantId: '10',
          strategyId: 'overflow',
          confidenceAtRecommendation: 90,
          lifecycle: LIFECYCLE.UNSUCCESSFUL,
          outcome: OUTCOME_RESULTS.UNSUCCESSFUL,
          executed: true,
          observedAt: new Date(now - i * 86400000).toISOString(),
          generatedAt: new Date(now - (i + 1) * 86400000).toISOString(),
        })
      );
    }

    const drift = detectDrift({ records, now, minSample: 3 });
    assert.ok(drift.alertCount >= 1);
    assert.ok(
      drift.alerts.some((a) => a.type === 'strategy_underperforming')
    );
    assert.equal(drift.customerFacing, false);
  });

  it('flags falling operator acceptance when quality is provided', () => {
    const drift = detectDrift({
      records: [],
      operatorQuality: {
        recommendationAcceptanceRate: 0.2,
        totals: { decided: 10 },
      },
      minSample: 3,
    });
    assert.ok(
      drift.alerts.some((a) => a.type === 'recommendation_acceptance_falling')
    );
  });
});

describe('Internal review dashboard', () => {
  it('assembles Recommendation Success → … → System Drift', () => {
    const engine = createOutcomeEngine({
      getOperatorQuality: () => ({
        recommendationAcceptanceRate: 0.6,
        maxUsage: 4,
        totals: { decided: 5 },
      }),
    });
    engine.record({
      tenantId: '10',
      recommendationId: 'rec:dash',
      strategyId: 'opportunity',
      lifecycle: LIFECYCLE.GENERATED,
      confidenceAtRecommendation: 91,
    });
    advanceTo(engine, '10', 'rec:dash', LIFECYCLE.SUCCESSFUL);

    const review = engine.review('10');
    assert.equal(review.internal, true);
    assert.equal(review.customerFacing, false);
    assert.equal(review.mutatesReasoning, false);
    assert.equal(review.mutatesConfidence, false);
    assert.ok(review.sections.recommendationSuccess);
    assert.ok(review.sections.strategyPerformance);
    assert.ok(review.sections.confidenceCalibration);
    assert.ok(review.sections.operatorBehavior);
    assert.ok(review.sections.systemDrift);
    assert.equal(review.sections.recommendationSuccess.successful, 1);
    assert.equal(
      review.sections.operatorBehavior.recommendationAcceptanceRate,
      0.6
    );
  });
});

describe('observeGenerated does not mutate the deck', () => {
  it('registers cards without changing confidence or cards', () => {
    const engine = createOutcomeEngine();
    const model = {
      highestLeverageAction: {
        id: 'rec:hla',
        confidence: 93,
        strategyId: 'opportunity',
      },
      priorityQueue: [
        { id: 'rec:pq1', confidence: 81, strategyId: 'relationship' },
      ],
      watchAlerts: [{ id: 'rec:w1', confidence: 70, strategyId: 'overflow' }],
      cards: [],
      meta: { tenantId: '10' },
    };
    const before = JSON.stringify(model);
    const rows = engine.observeGenerated(model, '10');
    assert.equal(JSON.stringify(model), before);
    assert.equal(rows.length, 3);
    assert.equal(engine.get('10', 'rec:hla').lifecycle, LIFECYCLE.GENERATED);
    assert.equal(engine.get('10', 'rec:hla').confidenceAtRecommendation, 93);
  });
});

describe('Runtime wiring leaves deterministic compose facts intact', () => {
  it('compose observation is additive and does not rewrite confidence', async () => {
    const max = createMaxReasoningRuntime({
      withSync: false,
      startIngestor: false,
      disableLlm: true,
    });
    assert.ok(max.outcome);
    const deck = await max.compose({ tenantId: '10' });
    assert.ok(deck);
    // Outcome layer must not attach customer-facing accuracy fields
    assert.equal(deck.outcomeCalibration, undefined);
    assert.equal(deck.outcomeReview, undefined);
    const review = max.outcomeReview('10');
    assert.equal(review.customerFacing, false);
    assert.equal(review.mutatesReasoning, false);
  });
});

function advanceTo(engine, tenantId, recommendationId, terminal, extra = {}) {
  for (const step of [
    LIFECYCLE.REVIEWED,
    LIFECYCLE.APPROVED,
    LIFECYCLE.EXECUTED,
    LIFECYCLE.OBSERVED,
    terminal,
  ]) {
    engine.transition({
      tenantId,
      recommendationId,
      lifecycle: step,
      ...extra,
    });
  }
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}
