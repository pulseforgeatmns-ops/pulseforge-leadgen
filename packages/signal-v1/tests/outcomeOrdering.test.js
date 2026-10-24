'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { labelMarketOutcome } = require('../outcomes/marketOutcomes');

describe('Signal V1 outcome ordering', () => {
  const observedAt = '2026-09-29T18:00:00Z';
  const p0 = 1.0;

  it('PASS only if +100% before -30%', () => {
    const passFirst = labelMarketOutcome({
      entryPrice: p0,
      observedAt,
      pricePath: [
        { occurredAt: '2026-09-29T18:05:00Z', price: 2.1 },
        { occurredAt: '2026-09-29T18:20:00Z', price: 0.6 },
      ],
    });
    assert.equal(passFirst.label, 'PASS');

    const failFirst = labelMarketOutcome({
      entryPrice: p0,
      observedAt,
      pricePath: [
        { occurredAt: '2026-09-29T18:05:00Z', price: 0.65 },
        { occurredAt: '2026-09-29T18:20:00Z', price: 2.5 },
      ],
    });
    assert.equal(failFirst.label, 'FAIL');
  });

  it('UNRESOLVED when neither threshold hit inside horizon', () => {
    const unresolved = labelMarketOutcome({
      entryPrice: p0,
      observedAt,
      pricePath: [{ occurredAt: '2026-09-29T18:30:00Z', price: 1.1 }],
      config: { horizonHours: 1 },
    });
    assert.equal(unresolved.label, 'UNRESOLVED');
  });
});
