'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { ingestHistoricalMarketData } = require('../ingestion/ingestHistoricalMarketData');

describe('Signal V1 idempotent ingestion', () => {
  it('duplicate provider observations are not inserted twice', async () => {
    const store = new InMemorySignalStore();
    store.upsertToken({ tokenAddress: 'Token1111111111111111111111111111111111111', chain: 'solana' });

    const provider = {
      async getHistoricalPrices() {
        return [
          {
            tokenAddress: 'Token1111111111111111111111111111111111111',
            occurredAt: new Date('2026-09-29T18:00:00Z'),
            priceUsd: 0.5,
            intervalSeconds: 60,
            provider: 'mock',
          },
        ];
      },
    };

    const first = await ingestHistoricalMarketData(store, provider, {
      tokenAddress: 'Token1111111111111111111111111111111111111',
      startTime: '2026-09-29T17:00:00Z',
      endTime: '2026-09-29T19:00:00Z',
    });
    const second = await ingestHistoricalMarketData(store, provider, {
      tokenAddress: 'Token1111111111111111111111111111111111111',
      startTime: '2026-09-29T17:00:00Z',
      endTime: '2026-09-29T19:00:00Z',
    });

    assert.equal(first.inserted, 1);
    assert.equal(second.inserted, 0);
    assert.equal(second.duplicates, 1);
    assert.equal(store.marketObservations.length, 1);
  });
});
