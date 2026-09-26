'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  maybeHandleOperatorOperatingUpdate,
  SEMANTIC,
  isOperatorOperatingUpdate,
} = require('../packages/max/workspace/OperatorOperatingUpdate');

const DAILY_DEBRIEF_TEXT = [
  'Quick operating update: 0 physical visits.',
  'I completed 11 outbound business call attempts as part of the acquisition mission.',
  'PMI W / Jon requested Anchor information and provided a direct email.',
].join(' ');

describe('SPEC-218 daily debrief canonical ingestion', () => {
  it('recognizes activity-count semantics from the AO debrief and preserves mission context', async () => {
    const turn = await maybeHandleOperatorOperatingUpdate({
      question: DAILY_DEBRIEF_TEXT,
      context: {
        tenantId: '10',
        clientId: 10,
        missionId: 'mission-123',
      },
      operatingUpdateOpts: {
        now: '2026-08-31T16:00:00.000-04:00',
        timeZone: 'America/New_York',
        actorId: 'ao-42',
        rebuildOperatorContext: async () => {},
      },
    });

    assert.ok(turn);
    assert.equal(turn.handled, true);
    assert.equal(turn.turnType, 'operating_update');
    assert.equal(isOperatorOperatingUpdate(DAILY_DEBRIEF_TEXT), true);

    const visits = turn.assertions.find((item) => item.predicate === 'physical_visits');
    const calls = turn.assertions.find((item) => item.predicate === 'outbound_call_attempts');

    assert.ok(visits);
    assert.equal(visits.semanticType, SEMANTIC.ACTIVITY_COUNT);
    assert.equal(visits.value, 0);
    assert.ok(calls);
    assert.equal(calls.semanticType, SEMANTIC.ACTIVITY_COUNT);
    assert.equal(calls.value, 11);
    assert.ok(turn.results.some((result) => result?.assertion?.predicate === 'physical_visits'));
  });
});
