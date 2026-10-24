'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { ingestHistoricalMarketData } = require('../ingestion/ingestHistoricalMarketData');
const { replayToken } = require('../replay/replayEngine');
const { resolveResearchWindow } = require('../fixtures/researchWindows');
const {
  validateHistoricalCoverage,
  HISTORICAL_DATA_UNAVAILABLE,
} = require('../market/historicalCoverage');

describe('Signal V1 historical window integrity (SIGNAL-V1-002A)', () => {
  it('resolveResearchWindow uses per-token anchors (DOOM Aug, Hallow Sep 29)', () => {
    const doom = resolveResearchWindow('Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump');
    assert.ok(doom.startTime.startsWith('2026-08-04'));
    assert.ok(doom.endTime.startsWith('2026-08-05'));

    const hallow = resolveResearchWindow('6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump');
    assert.equal(hallow.startTime, '2026-09-29T21:43:00.000Z');
    assert.equal(hallow.endTime, '2026-09-30T22:43:00.000Z');
  });

  it('provider receives expected historical timestamps for ingest', async () => {
    const store = new InMemorySignalStore();
    store.upsertToken({
      tokenAddress: 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump',
      chain: 'solana',
    });
    const window = resolveResearchWindow('Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump');
    let seenStart;
    let seenEnd;
    const provider = {
      async getHistoricalPrices(_token, start, end) {
        seenStart = new Date(start).toISOString();
        seenEnd = new Date(end).toISOString();
        return [];
      },
    };
    await ingestHistoricalMarketData(store, provider, {
      tokenAddress: 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump',
      startTime: window.startTime,
      endTime: window.endTime,
      decisionAnchor: window.anchor,
    });
    assert.equal(seenStart, window.startTime);
    assert.equal(seenEnd, window.endTime);
  });

  it('wrong-period provider data (September) for August request => UNAVAILABLE, no persist', async () => {
    const store = new InMemorySignalStore();
    const token = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';
    store.upsertToken({ tokenAddress: token, chain: 'solana' });
    const provider = {
      async getHistoricalPrices() {
        return [
          {
            tokenAddress: token,
            occurredAt: new Date('2026-09-29T18:00:00Z'),
            priceUsd: 0.01,
            intervalSeconds: 60,
            provider: 'mock',
          },
        ];
      },
    };
    const stats = await ingestHistoricalMarketData(store, provider, {
      tokenAddress: token,
      startTime: '2026-08-04T04:27:00.000Z',
      endTime: '2026-08-05T05:27:00.000Z',
      decisionAnchor: '2026-08-04T05:27:00.000Z',
    });
    assert.equal(stats.historicalDataStatus, 'UNAVAILABLE');
    assert.equal(stats.error, HISTORICAL_DATA_UNAVAILABLE);
    assert.equal(stats.inserted, 0);
    assert.equal(store.marketObservations.length, 0);
  });

  it('partial overlap within requested window => PARTIAL and persists overlapping rows', async () => {
    const store = new InMemorySignalStore();
    const token = '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump';
    store.upsertToken({ tokenAddress: token, chain: 'solana' });
    const provider = {
      async getHistoricalPrices() {
        return [
          {
            tokenAddress: token,
            occurredAt: new Date('2026-09-29T21:55:00.000Z'),
            priceUsd: 0.01,
            intervalSeconds: 60,
            provider: 'mock',
          },
          {
            tokenAddress: token,
            occurredAt: new Date('2026-09-30T18:00:00.000Z'),
            priceUsd: 0.02,
            intervalSeconds: 60,
            provider: 'mock',
          },
        ];
      },
    };
    const stats = await ingestHistoricalMarketData(store, provider, {
      tokenAddress: token,
      startTime: '2026-09-29T21:43:00.000Z',
      endTime: '2026-09-30T22:43:00.000Z',
      decisionAnchor: '2026-09-29T22:43:00.000Z',
    });
    assert.equal(stats.historicalDataStatus, 'PARTIAL');
    assert.ok(stats.coverage.missingLeadingDurationMs > 0);
    assert.equal(stats.inserted, 2);
  });

  it('Hallow regression — Sept 29–30 observations remain usable', async () => {
    const coverage = validateHistoricalCoverage({
      requestedStart: '2026-09-29T21:43:00.000Z',
      requestedEnd: '2026-09-30T22:43:00.000Z',
      observations: [
        { occurredAt: '2026-09-29T22:35:00.000Z' },
        { occurredAt: '2026-09-30T18:00:00.000Z' },
      ],
      decisionAnchor: '2026-09-29T22:43:00.000Z',
    });
    assert.notEqual(coverage.status, 'UNAVAILABLE');
    assert.equal(coverage.hasObservationAtOrAfterDecision, true);
  });

  it('replay without market data at/after decision => INSUFFICIENT_MARKET_DATA', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const token = '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump';
    await store.insertMarketObservation({
      tokenAddress: token,
      occurredAt: new Date('2026-09-29T21:50:00.000Z'),
      priceUsd: 0.001,
      intervalSeconds: 60,
      provider: 'test',
    });
    const result = await replayToken(store, {
      tokenAddress: token,
      startTime: '2026-09-29T21:43:00.000Z',
      endTime: '2026-09-30T22:43:00.000Z',
      decisionAnchor: '2026-09-29T22:43:00.000Z',
      replaceExisting: false,
    });
    assert.equal(result.replayStatus, 'INSUFFICIENT_MARKET_DATA');
    assert.equal(result.timeline.length, 0);
  });

  it('good coverage but no ENTRY => NO_ENTRY', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const token = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';
    await store.insertMarketObservation({
      tokenAddress: token,
      occurredAt: new Date('2026-08-04T05:30:00.000Z'),
      priceUsd: 0.001,
      intervalSeconds: 60,
      provider: 'test',
    });
    const result = await replayToken(store, {
      tokenAddress: token,
      startTime: '2026-08-04T04:27:00.000Z',
      endTime: '2026-08-05T05:27:00.000Z',
      decisionAnchor: '2026-08-04T05:27:00.000Z',
      replaceExisting: false,
    });
    assert.equal(result.replayStatus, 'NO_ENTRY');
    assert.ok(result.timeline.length > 0);
  });

  it('wrong-period persisted observations block replay for DOOM August window', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const token = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';
    await store.insertMarketObservation({
      tokenAddress: token,
      occurredAt: new Date('2026-09-29T18:00:00Z'),
      priceUsd: 0.001,
      intervalSeconds: 60,
      provider: 'test',
    });
    const window = resolveResearchWindow(token);
    const result = await replayToken(store, {
      tokenAddress: token,
      startTime: window.startTime,
      endTime: window.endTime,
      decisionAnchor: window.anchor,
      replaceExisting: false,
    });
    assert.equal(result.replayStatus, 'HISTORICAL_DATA_UNAVAILABLE');
    assert.equal(result.skipped, true);
  });
});
