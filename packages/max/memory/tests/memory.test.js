'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMemoryEngine,
  TimelineBuilder,
  WatchRegistry,
  WATCH_OPS,
  TREND_DIRECTIONS,
  InMemorySnapshotRepository,
  SnapshotEngine,
} = require('..');
const { snapshot } = require('./helpers');

function fakeEvaluation(snap) {
  return {
    recommendation: snap.recommendation,
    explanation: { recommendationId: snap.recommendation.id },
    report: {
      strategyResults: snap.strategyResults,
      context: { builtAt: snap.timestamp },
      normalizedScores: {},
    },
  };
}

describe('History ordering + trend detection', () => {
  it('timeline is Monday → Wednesday → Friday ordered', async () => {
    const memory = createMemoryEngine();
    const days = [
      { timestamp: '2026-07-20T12:00:00.000Z', score: 71, confidence: 40 }, // Mon
      { timestamp: '2026-07-22T12:00:00.000Z', score: 78, confidence: 48 }, // Wed
      { timestamp: '2026-07-24T12:00:00.000Z', score: 82, confidence: 55 }, // Fri
    ];
    for (const day of days) {
      const s = snapshot(day);
      await memory.remember({
        tenantId: s.tenantId,
        companyId: s.companyId,
        timestamp: day.timestamp,
        evaluation: fakeEvaluation(s),
      });
    }

    const { timeline } = await memory.history('10', 'co-1');
    assert.equal(timeline.length, 3);
    assert.deepEqual(
      timeline.map((t) => t.timestamp),
      days.map((d) => d.timestamp)
    );
    assert.equal(timeline[2].scoreDelta, 4);
    assert.ok(timeline[2].changes.length >= 0);

    const scores = await memory.scoreHistory('10', 'co-1');
    assert.deepEqual(
      scores.map((s) => s.score),
      [71, 78, 82]
    );
    const conf = await memory.confidenceHistory('10', 'co-1');
    assert.deepEqual(
      conf.map((c) => c.confidence),
      [40, 48, 55]
    );

    const trend = await memory.trend('10', 'co-1');
    assert.equal(trend.score, TREND_DIRECTIONS.UP);
    assert.equal(trend.confidence, TREND_DIRECTIONS.UP);
    assert.equal(trend.scoreDeltaTotal, 11);
  });

  it('TimelineBuilder alone preserves order', () => {
    const rows = [
      snapshot({ id: 'c', timestamp: '2026-07-24T12:00:00.000Z', score: 82 }),
      snapshot({ id: 'a', timestamp: '2026-07-20T12:00:00.000Z', score: 71 }),
      snapshot({ id: 'b', timestamp: '2026-07-22T12:00:00.000Z', score: 78 }),
    ];
    const timeline = new TimelineBuilder().build(rows);
    assert.deepEqual(
      timeline.map((t) => t.timestamp),
      [
        '2026-07-20T12:00:00.000Z',
        '2026-07-22T12:00:00.000Z',
        '2026-07-24T12:00:00.000Z',
      ]
    );
  });
});

describe('Memory queries', () => {
  it('whatChanged / whyChanged / evolve work', async () => {
    const memory = createMemoryEngine();
    const s1 = snapshot({
      timestamp: '2026-07-20T12:00:00.000Z',
      score: 71,
      confidence: 40,
      claims: ['c1'],
      evidence: ['e1'],
    });
    const s2 = snapshot({
      timestamp: '2026-07-22T12:00:00.000Z',
      score: 82,
      confidence: 52,
      claims: ['c1', 'c2'],
      evidence: ['e1', 'e2'],
      priority: 'critical',
    });
    await memory.remember({
      tenantId: '10',
      companyId: 'co-1',
      timestamp: s1.timestamp,
      evaluation: fakeEvaluation(s1),
    });
    await memory.remember({
      tenantId: '10',
      companyId: 'co-1',
      timestamp: s2.timestamp,
      evaluation: fakeEvaluation(s2),
    });

    const changed = await memory.whatChanged({ tenantId: '10', companyId: 'co-1' });
    assert.equal(changed.scoreDelta, 11);
    assert.deepEqual(changed.newClaims, ['c2']);
    assert.ok(changed.changes.some((c) => c.type === 'score_increased'));

    const why = await memory.whyChanged({ tenantId: '10', companyId: 'co-1' });
    assert.ok(why.chain.why);
    assert.ok(why.chain.evidence);
    assert.ok(why.chain.history);
    assert.ok(why.chain.change);
    assert.ok(why.chain.reason);
    assert.equal(why.change.score.delta, 11);

    const evolution = await memory.evolve('10', 'co-1');
    assert.equal(evolution.history.length, 2);
    assert.ok(evolution.trend);
    assert.ok(Array.isArray(evolution.reason));
    assert.ok(evolution.forecast);
    assert.equal(evolution.forecast.kind, 'linear_extrapolation');
  });
});

describe('Watch registration', () => {
  it('registers watches and detects score delta > 10 without notifying', async () => {
    const memory = createMemoryEngine();
    const watch = memory.watch({
      tenantId: '10',
      targetType: 'company',
      targetId: 'co-1',
      condition: { op: WATCH_OPS.DELTA_ABS_GT, field: 'score', value: 10 },
    });
    assert.ok(watch.id);

    const s1 = snapshot({ timestamp: '2026-07-20T12:00:00.000Z', score: 71 });
    const s2 = snapshot({ timestamp: '2026-07-22T12:00:00.000Z', score: 82 });
    await memory.remember({
      tenantId: '10',
      companyId: 'co-1',
      timestamp: s1.timestamp,
      evaluation: fakeEvaluation(s1),
    });
    const second = await memory.remember({
      tenantId: '10',
      companyId: 'co-1',
      timestamp: s2.timestamp,
      evaluation: fakeEvaluation(s2),
    });
    assert.ok(second.triggeredWatches.some((t) => t.watchId === watch.id));
    assert.equal(second.triggeredWatches[0].scoreDelta, 11);
  });

  it('claim confidence watch triggers when meta.claimConfidences exceeds threshold', () => {
    const registry = new WatchRegistry();
    registry.register({
      id: 'w-claim',
      tenantId: '10',
      targetType: 'claim',
      targetId: 'claim-overflow',
      condition: { op: WATCH_OPS.VALUE_GTE, field: 'claimConfidence', value: 0.8 },
    });
    const from = snapshot({ claims: ['claim-overflow'], claimConfidences: { 'claim-overflow': 0.5 } });
    const to = snapshot({
      id: 'snap-2',
      timestamp: '2026-07-22T12:00:00.000Z',
      claims: ['claim-overflow'],
      claimConfidences: { 'claim-overflow': 0.85 },
    });
    const { DiffEngine } = require('..');
    const diff = new DiffEngine().diff(from, to);
    const triggered = registry.evaluate({
      diff,
      changes: [],
      toSnapshot: to,
      fromSnapshot: from,
    });
    assert.equal(triggered.length, 1);
    assert.equal(triggered[0].watchId, 'w-claim');
  });
});

describe('Determinism (memory)', () => {
  it('same snapshot sequence yields same diffs/fingerprints', async () => {
    async function run() {
      const repo = new InMemorySnapshotRepository();
      const engine = new SnapshotEngine({ repository: repo });
      const memory = createMemoryEngine({ repository: repo, snapshotEngine: engine });
      for (const [score, ts] of [
        [71, '2026-07-20T12:00:00.000Z'],
        [82, '2026-07-22T12:00:00.000Z'],
      ]) {
        const s = snapshot({ score, timestamp: ts, claims: score === 71 ? ['c1'] : ['c1', 'c2'] });
        await memory.remember({
          tenantId: '10',
          companyId: 'co-1',
          timestamp: ts,
          evaluation: fakeEvaluation(s),
        });
      }
      const changed = await memory.whatChanged({ tenantId: '10', companyId: 'co-1' });
      return {
        scoreDelta: changed.scoreDelta,
        newClaims: changed.newClaims,
        fingerprint: changed.diff.fingerprint,
        changeTypes: changed.changes.map((c) => c.type).sort(),
      };
    }

    assert.deepEqual(await run(), await run());
  });
});
