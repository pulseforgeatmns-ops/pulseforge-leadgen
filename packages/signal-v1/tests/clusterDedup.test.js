'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { calculateConvergence } = require('../features/convergence');

describe('Signal V1 cluster deduplication', () => {
  it('five sources in one cluster count as one independent confirmation', () => {
    const store = new InMemorySignalStore();
    const token = 'TokenClusterTest1111111111111111111111111111111111';
    const cluster = 'cluster-promo';
    const t0 = new Date('2026-10-01T12:00:00Z');

    for (let i = 0; i < 5; i += 1) {
      store.upsertSource({ id: `src-${i}`, name: `src-${i}`, sourceType: 'telegram', active: true });
      store.addClusterMember(`src-${i}`, cluster);
      store.insertEvent({
        tokenAddress: token,
        eventType: 'CALL',
        occurredAt: new Date(t0.getTime() + i * 60000),
        observedAt: new Date(t0.getTime() + i * 60000),
        sourceType: 'telegram',
        sourceId: `src-${i}`,
        sourceClusterId: cluster,
        payload: {},
      });
    }

    const result = calculateConvergence({
      store,
      tokenAddress: token,
      evaluatedAt: new Date('2026-10-01T12:10:00Z'),
      windowMinutes: 15,
    });

    assert.equal(result.rawSourceCount, 5);
    assert.equal(result.independentClusterCount, 1);
  });
});
