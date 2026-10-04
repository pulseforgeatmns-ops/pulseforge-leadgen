'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { buildFeatureSnapshot } = require('../features/featureEngine');

describe('Signal V1 temporal integrity', () => {
  it('future events cannot affect historical snapshots', () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const token = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
    const earlyAt = new Date('2026-09-29T18:09:00Z');

    const early = buildFeatureSnapshot(store, token, earlyAt);
    assert.equal(early.features.whaleDistributionDetected, false);
    assert.equal(early.features.profitableWalletSellCount, 0);
    assert.equal(early.features.independentClusterCount, 2);

    const late = buildFeatureSnapshot(store, token, new Date('2026-09-29T19:30:00Z'));
    assert.equal(late.features.whaleDistributionDetected, true);
    assert.ok(late.features.profitableWalletSellCount >= 1);
  });
});
