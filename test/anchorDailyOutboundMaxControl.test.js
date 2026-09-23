'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildControlPlan,
  missionCandidateReason,
  runMaxOutboundControlLoop,
} = require('../services/maxOutboundControlLoop');
const { startAnchorGovernedScheduler } = require('../services/anchorGovernedScheduler');

test('Max derives a three-day inventory target from the lower of policy and Emmett capacity', () => {
  assert.deepEqual(
    buildControlPlan({
      dailyCap: 5,
      emmettCapacity: 16,
      sentToday: 1,
      cleanInventory: 2,
      targetDays: 3,
    }),
    {
      state: 'critical',
      safeDailyCapacity: 5,
      todayRemaining: 4,
      targetDays: 3,
      targetInventory: 15,
      cleanInventory: 2,
      deficit: 13,
      shouldReplenish: true,
    }
  );

  const emmettBound = buildControlPlan({
    dailyCap: 10,
    emmettCapacity: 3,
    cleanInventory: 9,
    targetDays: 3,
  });
  assert.equal(emmettBound.safeDailyCapacity, 3);
  assert.equal(emmettBound.targetInventory, 9);
  assert.equal(emmettBound.state, 'healthy');
  assert.equal(emmettBound.shouldReplenish, false);
});

test('Max inventory requires confirmed service area and source-mission segment compatibility', () => {
  const scope = { segment: 'short_term_rental', industry: 'hospitality' };
  assert.equal(
    missionCandidateReason({ service_area_match: false, vertical: 'str_manager' }, scope),
    'service_area_not_confirmed'
  );
  assert.equal(
    missionCandidateReason({ service_area_match: true, vertical: 'law_firm' }, scope),
    'mission_segment_mismatch'
  );
  assert.equal(
    missionCandidateReason({ service_area_match: true, vertical: 'str_manager' }, scope),
    null
  );
  assert.equal(
    missionCandidateReason({ service_area_match: true, vertical: 'property_management' }, scope),
    null
  );
});

test('Max invokes Scout for a deficit and records the post-replenishment state without touching send authority', async () => {
  const events = [];
  let scoutPlan = null;
  const program = {
    id: 'outbound_test',
    mode: 'active',
    policy_hash: 'policy_hash',
    source_mission_id: 'mission_source',
    policy: { dailyCap: 5 },
  };
  const source = {
    id: 'mission_source',
    payload: {
      structuredMission: {
        market: { segment: 'short_term_rental', industry: 'hospitality' },
        geography: { region: 'Greater Manchester', cities: ['Manchester'] },
      },
    },
  };
  const result = await runMaxOutboundControlLoop({
    pool: {},
    program,
    source,
    store: {
      event: async (type, key, payload) => events.push({ type, key, payload }),
    },
    infrastructure: {
      cap: 5,
      snapshot: { sentToday: 1 },
      assessed: { governor: { outcome: 'proceed' }, health: { score: 74 } },
    },
    inventory: { clean: [{}, {}], excluded: [], scope: {} },
    inventoryAfter: { clean: Array.from({ length: 15 }, () => ({})), excluded: [], scope: {} },
    scoutRamp: async ({ plan }) => {
      scoutPlan = plan;
      return { promoted: 13, discoveredQueued: 13 };
    },
  });

  assert.equal(scoutPlan.deficit, 13);
  assert.equal(result.plan.state, 'healthy');
  assert.equal(result.plan.cleanInventory, 15);
  assert.equal(result.emmett.safeCapacity, 5);
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'max_outbound_control');
  assert.equal(events[0].payload.scoutInvoked, true);
  assert.equal(events[0].payload.bufferTarget, 15);
});

test('Max observation can be disabled without invoking Scout', async () => {
  let called = false;
  const result = await runMaxOutboundControlLoop({
    pool: {},
    execute: false,
    program: {
      id: 'outbound_test',
      mode: 'active',
      policy_hash: 'policy_hash',
      source_mission_id: 'mission_source',
      policy: { dailyCap: 5 },
    },
    source: { id: 'mission_source', payload: {} },
    store: { event: async () => {} },
    infrastructure: {
      cap: 5,
      snapshot: { sentToday: 0 },
      assessed: { governor: { outcome: 'proceed' }, health: { score: 80 } },
    },
    inventory: { clean: [], excluded: [], scope: {} },
    scoutRamp: async () => { called = true; },
  });
  assert.equal(called, false);
  assert.equal(result.plan.state, 'critical');
  assert.equal(result.plan.deficit, 15);
});

test('governed scheduler dispatches before Max replenishment on the control cycle', async () => {
  const calls = [];
  let callback;
  let releasePoll;
  const cron = {
    poll: async () => {
      calls.push('poll');
      if (!releasePoll) await new Promise(resolve => { releasePoll = resolve; });
    },
    run: async () => {
      calls.push('tick');
      return { sent: 1 };
    },
  };

  const scheduler = startAnchorGovernedScheduler({
    enabled: true,
    maxControlEnabled: true,
    cron,
    maxControl: async () => {
      calls.push('max');
      return { plan: { state: 'replenish' } };
    },
    logger: { log() {}, error() {} },
    setInterval(fn, ms) {
      callback = fn;
      assert.equal(ms, 60000);
      return { unref() {} };
    },
    clearInterval() {},
  });

  await callback();
  assert.deepEqual(calls, ['poll']);
  releasePoll();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(calls, ['poll', 'tick', 'max']);
  scheduler.stop();
});

