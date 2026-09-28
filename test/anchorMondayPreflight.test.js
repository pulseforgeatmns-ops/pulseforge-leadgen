'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { assessOperatingCapacity } = require('../packages/emmett-outbound/OperatingCapacity');
const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
const {
  pickPreflightChecks,
  assertPreflight,
} = require('../scripts/anchorGovernedOperatingDay');

const GRANT_POLICY = {
  dailyCap: 15,
  totalCap: 100,
  spacingMinutes: 60,
  startHour: 9,
  endHour: 17,
  timeZone: 'America/New_York',
  weekdays: [1, 2, 3, 4, 5],
  startsAt: '2026-09-28T13:00:00.000Z',
  expiresAt: '2026-10-23T23:59:59.000Z',
};

const PRE_WINDOW = new Date('2026-09-28T12:51:00.000Z');

test('Monday preflight: Max plans 8/day and 24 buffer before send window with Scout replenishment', async () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 16 },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 82 },
    },
    policy: GRANT_POLICY,
    now: PRE_WINDOW,
    schedule: {
      allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
      minSpacingMinutes: 60,
    },
  });
  assert.equal(operating.dispatchCapacityNow, 0);
  assert.equal(operating.planningDailyCapacity, 8);

  let scoutRan = false;
  const result = await runMaxOutboundControlLoop({
    pool: {},
    now: PRE_WINDOW,
    program: {
      id: 'outbound_monday',
      mode: 'active',
      policy_hash: 'hash',
      source_mission_id: 'mission_source',
      policy: GRANT_POLICY,
    },
    source: {
      id: 'mission_source',
      payload: {
        structuredMission: {
          market: { segment: 'short_term_rental', industry: 'hospitality' },
          geography: { region: 'Greater Manchester', cities: ['Manchester'] },
        },
      },
    },
    store: { event: async () => {} },
    infrastructure: {
      cap: operating.planningDailyCapacity,
      operating,
      snapshot: { sentToday: 0 },
      assessed: {
        governor: { outcome: 'proceed', halt: false },
        health: { score: 82 },
        capacity: { recommended: 16 },
      },
    },
    inventory: {
      clean: [{ id: '1' }, { id: '2' }, { id: '3' }],
      excluded: [],
      scope: {},
      exclusionCounts: {},
    },
    inventoryAfter: {
      clean: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }],
      excluded: [],
      scope: {},
      exclusionCounts: {},
    },
    scoutRamp: async ({ plan }) => {
      scoutRan = true;
      assert.equal(plan.planningDailyCapacity, 8);
      assert.equal(plan.targetInventory, 24);
      assert.equal(plan.shouldReplenish, true);
      return {
        promoted: 1,
        recoveredExisting: 0,
        discoveredQueued: 2,
        admission: {
          sameCompanyCandidatesAttempted: 1,
          alternateContactsResolved: 0,
          alternateContactsVerified: 0,
          alternateContactsAddedToCleanInventory: 0,
        },
      };
    },
  });

  assert.equal(scoutRan, true);
  const capture = pickPreflightChecks(result);
  assert.equal(capture.planningDailyCapacity, 8);
  assert.equal(capture.targetDays, 3);
  assert.equal(capture.targetInventory, 24);
  assert.equal(capture.cleanInventory, 4);
  assert.ok(capture.deficit > 0);
  assert.equal(capture.shouldReplenish, true);
  assert.equal(capture.scoutReplenishmentExecuted, true);
  assert.equal(capture.dispatchCapacityNow, 0);
  assert.equal(assertPreflight({
    ...capture,
    shouldReplenish: true,
    cleanInventory: 3,
    deficit: 21,
    scoutInvoked: true,
  }, { requireScout: true }).length, 0);
});
