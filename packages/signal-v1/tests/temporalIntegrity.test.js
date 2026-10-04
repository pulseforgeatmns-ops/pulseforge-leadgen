'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { buildFeatureSnapshot } = require('../features/featureEngine');

describe('Signal V1 temporal integrity', () => {
  it('future events cannot affect historical snapshots', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const token = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
    const earlyAt = new Date('2026-09-29T18:09:00Z');

    const early = await buildFeatureSnapshot(store, token, earlyAt);
    assert.equal(early.features.whaleDistributionDetected, false);
    assert.equal(early.features.profitableWalletSellCount, 0);
    assert.equal(early.features.independentClusterCount, 2);

    const late = await buildFeatureSnapshot(store, token, new Date('2026-09-29T19:30:00Z'));
    assert.equal(late.features.whaleDistributionDetected, true);
    assert.ok(late.features.profitableWalletSellCount >= 1);
  });

  it('future market observations cannot affect feature snapshot at T', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const token = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
    const t = new Date('2026-09-29T18:10:00Z');

    store.insertMarketObservation({
      tokenAddress: token,
      occurredAt: new Date('2026-09-29T18:05:00Z'),
      priceUsd: 0.0001,
      intervalSeconds: 60,
      provider: 'test',
    });
    store.insertMarketObservation({
      tokenAddress: token,
      occurredAt: new Date('2026-09-29T19:00:00Z'),
      priceUsd: 0.001,
      intervalSeconds: 60,
      provider: 'test',
    });

    const snap = await buildFeatureSnapshot(store, token, t);
    assert.equal(snap.features.priceUsd, 0.0001);
  });
});
