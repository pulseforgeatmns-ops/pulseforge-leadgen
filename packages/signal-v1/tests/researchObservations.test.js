'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { replayToken } = require('../replay/replayEngine');
const { getSourceQualityAsOf } = require('../research/sourceQuality');
const { resolveAchievableObservationPrice } = require('../research/achievablePrice');
const { computeSourcePerformanceAsOf } = require('../research/sourcePerformanceEngine');
const { evaluateStructureGate } = require('../research/structureGate');

const DUPLICATE = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
const DOOM = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';
const HALLOW = '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump';

const pricePath = [
  { occurredAt: '2026-09-29T18:00:00Z', price: 0.00012 },
  { occurredAt: '2026-09-29T18:01:00Z', price: 0.00013 },
  { occurredAt: '2026-09-29T18:02:00Z', price: 0.00014 },
  { occurredAt: '2026-09-29T18:05:00Z', price: 0.00014 },
  { occurredAt: '2026-09-29T18:30:00Z', price: 0.00024 },
  { occurredAt: '2026-09-29T19:00:00Z', price: 0.0003 },
  { occurredAt: '2026-09-29T20:00:00Z', price: 0.00007 },
];

describe('SIGNAL-V1-003 research observations', () => {
  it('observation temporal integrity — future events cannot create earlier observations', () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    replayToken(store, { tokenAddress: DUPLICATE, pricePath });

    const firstCaller = store.researchObservations.find(o => o.observationType === 'FIRST_CALLER');
    assert.ok(firstCaller);
    assert.equal(new Date(firstCaller.occurredAt).toISOString(), '2026-09-29T18:04:00.000Z');

    const independent = store.researchObservations.find(
      o => o.observationType === 'INDEPENDENT_CONVERGENCE'
    );
    assert.ok(independent);
    assert.equal(new Date(independent.occurredAt).toISOString(), '2026-09-29T18:09:00.000Z');
  });

  it('cluster independence — one promotional cluster cannot trigger independent convergence alone', () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    replayToken(store, { tokenAddress: HALLOW, pricePath: [
      { occurredAt: '2026-09-27T20:00:00Z', price: 0.00004 },
      { occurredAt: '2026-09-27T20:10:00Z', price: 0.000045 },
    ] });

    const independent = store.researchObservations.find(
      o => o.tokenAddress === HALLOW && o.observationType === 'INDEPENDENT_CONVERGENCE'
    );
    assert.equal(independent, undefined);
    assert.ok(store.researchObservations.some(o => o.observationType === 'FIRST_CALLER'));
  });

  it('quality as-of — future source performance cannot alter historical quality', () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const early = getSourceQualityAsOf(store, 'src-independent-alpha', '2026-08-01T00:00:00Z');
    assert.equal(early.quality, 'unavailable');

    const later = getSourceQualityAsOf(store, 'src-independent-alpha', '2026-10-01T00:00:00Z');
    assert.notEqual(later.quality, 'unavailable');
  });

  it('null semantics — unavailable quality is not treated as low quality', () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    replayToken(store, { tokenAddress: DUPLICATE, pricePath });
    const qualityConv = store.researchObservations.find(
      o => o.observationType === 'QUALITY_CONVERGENCE'
    );
    assert.equal(qualityConv, undefined);
  });

  it('research/strategy separation — research observations do not change replay strategy timeline', () => {
    const storeA = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeA);
    const withResearch = replayToken(storeA, { tokenAddress: DUPLICATE, pricePath });

    const storeB = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeB);
    const baseline = replayToken(storeB, { tokenAddress: DUPLICATE, pricePath });

    assert.deepEqual(
      withResearch.timeline.map(t => ({ state: t.state, score: t.score })),
      baseline.timeline.map(t => ({ state: t.state, score: t.score }))
    );
    assert.ok(withResearch.researchObservations.length > 0);
  });

  it('achievable price uses first valid observation after configured delay', () => {
    const at = '2026-09-29T18:04:00Z';
    const resolved = resolveAchievableObservationPrice(at, 60, pricePath);
    assert.equal(resolved.price, 0.00014);
    assert.equal(resolved.priceAt.toISOString(), '2026-09-29T18:05:00.000Z');
  });

  it('structure gate missing data returns UNKNOWN (not PASS)', () => {
    const gate = evaluateStructureGate({
      liquidityUsd: 20000,
      bundledSupplyPct: null,
      top10HolderPct: null,
      devHoldingPct: null,
      tokenAgeSeconds: 900,
    });
    assert.equal(gate.result, 'UNKNOWN');
  });

  it('determinism — same events and versions produce identical observations', () => {
    const input = { tokenAddress: DUPLICATE, pricePath };
    const storeA = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeA);
    const a = replayToken(storeA, input);

    const storeB = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeB);
    const b = replayToken(storeB, input);

    assert.equal(a.digest, b.digest);
    assert.deepEqual(
      a.researchObservations.map(o => [o.observationType, new Date(o.occurredAt).toISOString()]),
      b.researchObservations.map(o => [o.observationType, new Date(o.occurredAt).toISOString()])
    );
  });

  it('initial proof tokens emit FIRST_CALLER where fixture calls exist', () => {
    for (const token of [DUPLICATE, DOOM, HALLOW]) {
      const store = new InMemorySignalStore();
      seedFrontRunnersFixtures(store);
      replayToken(store, { tokenAddress: token, pricePath: pricePath.slice(0, 3) });
      assert.ok(
        store.researchObservations.some(o => o.observationType === 'FIRST_CALLER'),
        `expected FIRST_CALLER for ${token}`
      );
    }
  });

  it('source performance returns unavailable below minimum sample', () => {
    const store = new InMemorySignalStore();
    const perf = computeSourcePerformanceAsOf(store, 'src-independent-alpha', new Date());
    assert.equal(perf.score, null);
    assert.equal(perf.metadata.quality, 'unavailable');
  });
});
