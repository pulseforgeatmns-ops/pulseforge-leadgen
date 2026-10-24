'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { resolveAchievableEntry } = require('../outcomes/achievableEntry');
const { labelMarketOutcome } = require('../outcomes/marketOutcomes');

describe('Signal V1 missing observations', () => {
  it('does not invent prices when gaps exist', () => {
    const entry = resolveAchievableEntry('2026-09-29T18:00:00Z', 30, [
      { occurredAt: '2026-09-29T18:02:00Z', priceUsd: 1.5, intervalSeconds: 60 },
    ]);
    assert.equal(entry.effectivePrice, 1.5);

    const unresolved = labelMarketOutcome({
      entryPrice: 1.0,
      observedAt: '2026-09-29T18:02:00Z',
      pricePath: [{ occurredAt: '2026-09-29T18:10:00Z', price: 1.05 }],
      config: { horizonHours: 1 },
    });
    assert.equal(unresolved.label, 'UNRESOLVED');
  });
});
