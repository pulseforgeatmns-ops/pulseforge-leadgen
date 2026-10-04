'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { replayToken } = require('../replay/replayEngine');

describe('Signal V1 replay determinism', () => {
  it('same events and versions produce identical replay digest', () => {
    const token = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
    const input = {
      tokenAddress: token,
      featureVersion: 'signal-features-v1',
      strategyVersion: 'signal-strategy-v1',
      pricePath: [
        { occurredAt: '2026-09-29T18:00:00Z', price: 0.00012 },
        { occurredAt: '2026-09-29T18:30:00Z', price: 0.00024 },
        { occurredAt: '2026-09-29T19:00:00Z', price: 0.0003 },
        { occurredAt: '2026-09-29T20:00:00Z', price: 0.00007 },
      ],
    };

    const storeA = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeA);
    const a = replayToken(storeA, input);

    const storeB = new InMemorySignalStore();
    seedFrontRunnersFixtures(storeB);
    const b = replayToken(storeB, input);

    assert.equal(a.digest, b.digest);
    assert.deepEqual(
      a.timeline.map(s => ({ state: s.state, score: s.score })),
      b.timeline.map(s => ({ state: s.state, score: s.score }))
    );
  });
});
