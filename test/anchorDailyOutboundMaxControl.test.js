'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildControlPlan,
  confirmedServiceAreaMatch,
  missionCandidateReason,
  runMaxOutboundControlLoop,
  _test: { scoutInput, mapReuseCompanyRows },
} = require('../services/maxOutboundControlLoop');
const { adapters } = require('../services/governedOutboundAdapters');
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

test('confirmedServiceAreaMatch accepts boolean true and non-empty locality strings', () => {
  assert.equal(confirmedServiceAreaMatch(true), true);
  assert.equal(confirmedServiceAreaMatch('Manchester'), true);
  assert.equal(confirmedServiceAreaMatch('Bedford'), true);
  assert.equal(confirmedServiceAreaMatch('Hooksett'), true);
  assert.equal(confirmedServiceAreaMatch(false), false);
  assert.equal(confirmedServiceAreaMatch(null), false);
  assert.equal(confirmedServiceAreaMatch(''), false);
  assert.equal(confirmedServiceAreaMatch('   '), false);
});

test('Max inventory requires confirmed service area and source-mission segment compatibility', () => {
  const scope = { segment: 'short_term_rental', industry: 'hospitality' };
  for (const locality of ['Manchester', 'Bedford', 'Hooksett']) {
    assert.equal(
      missionCandidateReason({ service_area_match: locality, vertical: 'property_manager' }, scope),
      null,
      locality
    );
  }
  assert.equal(
    missionCandidateReason({ service_area_match: null, vertical: 'property_manager' }, scope),
    'service_area_not_confirmed'
  );
  assert.equal(
    missionCandidateReason({ service_area_match: '', vertical: 'property_manager' }, scope),
    'service_area_not_confirmed'
  );
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
  assert.equal(
    missionCandidateReason({ service_area_match: 'Manchester', vertical: 'law_firm' }, scope),
    'mission_segment_mismatch'
  );
});

test('governed outbound adapters expose infrastructure for Max control loop', () => {
  const pool = { query: async () => ({ rows: [] }) };
  const adapterSet = adapters(pool, { infrastructure: async () => ({ cap: 5 }) });
  assert.equal(typeof adapterSet.infrastructure, 'function');
});

test('Max scoutInput carries structured replenishment workflow fields', () => {
  const plan = {
    deficit: 15,
    safeDailyCapacity: 5,
    targetDays: 3,
    targetInventory: 15,
    cleanInventory: 0,
  };
  const input = scoutInput(
    { source_mission_id: 'mission_source' },
    {
      payload: {
        structuredMission: {
          market: { segment: 'short_term_rental' },
          geography: { region: 'Greater Manchester' },
        },
      },
    },
    plan
  );
  assert.equal(input.workflow, 'outbound_inventory_replenishment');
  assert.equal(input.inventoryDeficit, 15);
  assert.equal(input.authority, 'observe');
  assert.match(input.operatorDirection, /Do not contact prospects/);
});

test('Max reuse loader preserves company geography without fabricating missing locations', () => {
  const mapped = mapReuseCompanyRows([
    {
      id: 'company-1',
      name: 'Example STR Manager',
      domain: 'example.com',
      website: 'https://example.com',
      location: 'Bedford, NH',
      vertical: 'str_manager',
      icp_score: 85,
      updated_at: '2026-09-01T00:00:00.000Z',
    },
    {
      id: 'company-2',
      name: 'Unknown Location Co',
      domain: 'unknown.example',
      website: null,
      location: null,
      vertical: null,
      icp_score: null,
      updated_at: null,
    },
  ]);
  assert.equal(mapped[0].location, 'Bedford, NH');
  assert.equal(mapped[1].location, null);
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

