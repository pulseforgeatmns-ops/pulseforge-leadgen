'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { resolveAchievableEntry } = require('../outcomes/achievableEntry');

describe('Signal V1 achievable entry', () => {
  it('uses first observation at or after delayed execution timestamp', () => {
    const observations = [
      { occurredAt: '2026-09-29T18:00:00Z', priceUsd: 1.0, intervalSeconds: 60 },
      { occurredAt: '2026-09-29T18:00:30Z', priceUsd: 1.1, intervalSeconds: 60 },
      { occurredAt: '2026-09-29T18:01:00Z', priceUsd: 1.2, intervalSeconds: 60 },
    ];
    const entry = resolveAchievableEntry('2026-09-29T18:00:00Z', 45, observations);
    assert.ok(entry);
    assert.equal(entry.effectivePrice, 1.2);
    assert.equal(entry.executionDelaySeconds, 45);
  });

  it('never backfills from an earlier observation', () => {
    const observations = [
      { occurredAt: '2026-09-29T18:02:00Z', priceUsd: 2.0, intervalSeconds: 60 },
    ];
    const entry = resolveAchievableEntry('2026-09-29T18:00:00Z', 30, observations);
    assert.equal(entry.effectivePrice, 2.0);
  });
});
