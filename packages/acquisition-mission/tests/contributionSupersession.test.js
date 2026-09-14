'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  isSupersededContribution,
  markContributionSuperseded,
  unwrapSpecialistPayload,
} = require('../ContributionSupersession');

describe('ContributionSupersession — nested production CAPACITY JSONB', () => {
  const oldId = 'contrib_78f321cf-405f-4a76-a2e1-9c0fbeb5696c';
  const newId = 'contrib_bf84cba9-eb76-4ff8-8179-3b0f2e3e99b8';

  function productionNestedCapacity(id, { superseded = false, supersededBy = null } = {}) {
    const inner = {
      capacity: { recommended: 1 },
      queue: { items: [{ prospectId: 'f47ac10b-58cc-4372-a567-0e02b2c3d479' }] },
      governor: { outcome: 'proceed' },
    };
    if (superseded) {
      inner.superseded = true;
      inner.supersededBy = supersededBy;
    }
    return {
      id,
      missionId: 'mission_ad7753b0-6def-441d-bb1a-3764656f5750',
      specialist: 'emmett',
      kind: 'capacity',
      payload: {
        id,
        specialist: 'emmett',
        kind: 'capacity',
        payload: inner,
      },
      at: '2026-09-13T20:00:00.000Z',
    };
  }

  it('detects superseded marker on nested payload.payload.superseded', () => {
    const row = productionNestedCapacity(oldId, { superseded: true, supersededBy: newId });
    assert.equal(isSupersededContribution(row), true);
  });

  it('markContributionSuperseded writes both wrapper and nested payload markers', () => {
    const row = productionNestedCapacity(oldId);
    const marked = markContributionSuperseded(row, newId);
    assert.equal(marked.payload.superseded, true);
    assert.equal(marked.payload.supersededBy, newId);
    assert.equal(marked.payload.payload.superseded, true);
    assert.equal(marked.payload.payload.supersededBy, newId);
    assert.equal(isSupersededContribution(marked), true);
  });

  it('unwrapSpecialistPayload reads queue from nested production CAPACITY rows', () => {
    const row = productionNestedCapacity(oldId);
    const body = unwrapSpecialistPayload(row);
    assert.ok(Array.isArray(body.queue?.items));
    assert.equal(body.queue.items.length, 1);
  });

  it('flat in-memory CAPACITY rows remain supported', () => {
    const flat = {
      id: oldId,
      specialist: 'emmett',
      kind: 'capacity',
      payload: {
        queue: { items: [{ prospectId: 'co-harbor' }] },
      },
      at: '2026-09-13T20:00:00.000Z',
    };
    const marked = markContributionSuperseded(flat, newId);
    assert.equal(marked.payload.superseded, true);
    assert.equal(marked.payload.supersededBy, newId);
    assert.equal(marked.payload.payload, undefined);
    assert.equal(isSupersededContribution(marked), true);
  });
});
