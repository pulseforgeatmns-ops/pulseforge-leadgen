'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { replayToken } = require('../replay/replayEngine');

describe('Signal V1 replay determinism', () => {
  it('same events and versions produce identical replay digest', async () => {
    const token = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
    const input = {
      tokenAddress: token,
      featureVersion: 'signal-features-v1',
      strategyVersion: 'signal-strategy-v1',
      replaceExisting: false,
      pricePath: [
        { occurredAt: '2026-09-29T18:00:00Z', priceUsd: 0.00012, intervalSeconds: 60 },
        { occurredAt: '2026-09-29T18:30:00Z', priceUsd: 0.00024, intervalSeconds: 60 },
        { occurredAt: '2026-09-29T19:00:00Z', priceUsd: 0.0003, intervalSeconds: 60 },
        { occurredAt: '2026-09-29T20:00:00Z', priceUsd: 0.00007, intervalSeconds: 60 },
      ],
    };

    const storeA = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeA);
    const a = await replayToken(storeA, input);

    const storeB = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeB);
    const b = await replayToken(storeB, input);

    assert.equal(a.digest, b.digest);
    assert.deepEqual(
      a.timeline.map(s => ({ state: s.state, score: s.score })),
      b.timeline.map(s => ({ state: s.state, score: s.score }))
    );
  });
});
