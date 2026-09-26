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

test('Max derives a three-day inventory target from Emmett effective capacity, exposing authorization as a limiter', () => {
  const authorizationBound = buildControlPlan({
    dailyCap: 5,
    emmettCapacity: 16,
    sentToday: 1,
    cleanInventory: 2,
    targetDays: 3,
  });
  assert.equal(authorizationBound.state, 'critical');
  assert.equal(authorizationBound.safeDailyCapacity, 5);
  assert.equal(authorizationBound.dispatchableDailyCapacity, 5);
  assert.equal(authorizationBound.effectiveDailyCapacity, 5);
  assert.equal(authorizationBound.recommendedSafeDailyCapacity, 16);
  assert.equal(authorizationBound.limitingFactor, 'authorization_daily_cap');
  assert.equal(authorizationBound.todayRemaining, 4);
  assert.equal(authorizationBound.targetInventory, 15);
  assert.equal(authorizationBound.cleanInventory, 2);
  assert.equal(authorizationBound.deficit, 13);
  assert.equal(authorizationBound.shouldReplenish, true);
  assert.match(authorizationBound.capacityReason, /authorization limits daily sends to 5/i);

  const emmettBound = buildControlPlan({
    dailyCap: 10,
    emmettCapacity: 3,
    cleanInventory: 9,
    targetDays: 3,
  });
  assert.equal(emmettBound.safeDailyCapacity, 3);
  assert.equal(emmettBound.dispatchableDailyCapacity, 3);
  assert.equal(emmettBound.effectiveDailyCapacity, 3);
  assert.equal(emmettBound.recommendedSafeDailyCapacity, 3);
  assert.equal(emmettBound.limitingFactor, 'deliverability');
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
  const scope = {
    segment: 'short_term_rental',
    industry: 'hospitality',
    cities: ['manchester', 'bedford', 'hooksett', 'goffstown', 'londonderry', 'auburn'],
    region: 'Greater Manchester',
  };
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
    missionCandidateReason({
      service_area_match: 'Bedford',
      company_location: '166 State Rte 101, Bedford, NH 03110',
      vertical: 'str_manager',
    }, scope),
    null
  );
  assert.equal(
    missionCandidateReason({
      service_area_match: 'Henniker',
      company_location: 'Henniker, NH',
      vertical: 'str_manager',
    }, scope),
    'service_area_not_confirmed'
  );
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

test('Max inventory target uses dispatchable capacity when schedule is the bottleneck', () => {
  const plan = buildControlPlan({
    dailyCap: 15,
    emmettCapacity: 16,
    operatingCapacity: {
      recommendedSafeDailyCapacity: 16,
      authorizationLimitedCapacity: 15,
      scheduleLimitedCapacity: 8,
      dispatchableDailyCapacity: 8,
      effectiveDailyCapacity: 15,
      limitingFactor: 'schedule_window_spacing',
      governor: 'proceed',
    },
    cleanInventory: 3,
    targetDays: 3,
  });
  assert.equal(plan.dispatchableDailyCapacity, 8);
  assert.equal(plan.targetInventory, 24);
  assert.equal(plan.deficit, 21);
  assert.equal(plan.limitingFactor, 'schedule_window_spacing');
  assert.equal(plan.state, 'critical');
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
    inventory: {
      clean: [{ prospectId: '1' }, { prospectId: '2' }],
      excluded: [],
      scope: {},
    },
    inventoryAfter: {
      clean: Array.from({ length: 15 }, (_, i) => ({ prospectId: String(i + 1) })),
      excluded: [],
      scope: {},
    },
    scoutRamp: async ({ plan }) => {
      scoutPlan = plan;
      return { promoted: 13, enrichmentPromoted: 13, discoveredQueued: 13 };
    },
  });

  assert.equal(scoutPlan.deficit, 13);
  assert.equal(result.plan.state, 'healthy');
  assert.equal(result.plan.cleanInventory, 15);
  assert.equal(result.emmett.safeCapacity, 5);
  assert.equal(result.emmett.dispatchableDailyCapacity, 5);
  assert.equal(result.emmett.effectiveDailyCapacity, 5);
  assert.equal(result.emmett.recommendedSafeDailyCapacity, 5);
  assert.equal(result.inventoryGrowth.newCleanInventoryAdded, 13);
  assert.equal(result.inventoryGrowth.netCleanInventoryDelta, 13);
  assert.equal(result.plan.effectiveDailyCapacity, 5);
  assert.equal(result.plan.targetInventory, 15);
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'max_outbound_control');
  assert.equal(events[0].payload.scoutInvoked, true);
  assert.equal(events[0].payload.bufferTarget, 15);
});

test('Max keeps replenishing on later cycles until the target is met', async () => {
  const calls = [];
  const program = {
    id: 'outbound_test',
    mode: 'active',
    policy_hash: 'policy_hash',
    source_mission_id: 'mission_source',
    policy: { dailyCap: 12 },
  };
  const source = { id: 'mission_source', payload: {} };
  const infrastructure = {
    cap: 12,
    operating: {
      recommendedSafeDailyCapacity: 12,
      authorizationLimitedCapacity: 12,
      scheduleLimitedCapacity: 12,
      dispatchableDailyCapacity: 12,
      effectiveDailyCapacity: 12,
      limitingFactor: 'deliverability',
      governor: 'proceed',
      healthScore: 84,
      capacityReason: 'Emmett recommends 12.',
    },
    snapshot: { sentToday: 0 },
    assessed: { governor: { outcome: 'proceed' }, health: { score: 84 }, capacity: { recommended: 12 } },
  };

  const first = await runMaxOutboundControlLoop({
    pool: {},
    program,
    source,
    store: { event: async () => {} },
    infrastructure,
    inventory: { clean: Array.from({ length: 10 }, () => ({})), excluded: [], scope: {}, exclusionCounts: {} },
    inventoryAfter: { clean: Array.from({ length: 20 }, () => ({})), excluded: [], scope: {}, exclusionCounts: {} },
    timestamps: { lastSuccessfulReplenishmentAt: null, lastSuccessfulPromotionAt: null },
    funnel: { discovered: 0, fit: 0, admittedToEnrichment: 0, enrichmentPending: 4, promotedVerified: 10, unresolved: 1, permanentlyRejected: 0 },
    scoutRamp: async ({ plan }) => {
      calls.push(plan.deficit);
      return {
        promoted: 10,
        enrichmentPromoted: 10,
        recovered: 0,
        discoveredQueued: 6,
        admission: { discovered: 12, evaluated: 12, fit: 8, admittedToEnrichment: 6, recovered: 0, rejected: {} },
      };
    },
  });
  assert.equal(first.plan.state, 'replenish');
  assert.equal(first.plan.targetInventory, 36);
  assert.equal(first.shouldReplenish === true || first.plan.shouldReplenish, true);
  assert.equal(calls[0], 26);

  const second = await runMaxOutboundControlLoop({
    pool: {},
    program,
    source,
    store: { event: async () => {} },
    infrastructure,
    inventory: { clean: Array.from({ length: 20 }, () => ({})), excluded: [], scope: {}, exclusionCounts: {} },
    inventoryAfter: { clean: Array.from({ length: 36 }, () => ({})), excluded: [], scope: {}, exclusionCounts: {} },
    timestamps: { lastSuccessfulReplenishmentAt: first.lastSuccessfulReplenishmentAt, lastSuccessfulPromotionAt: first.lastSuccessfulPromotionAt },
    funnel: first.funnel,
    scoutRamp: async ({ plan }) => {
      calls.push(plan.deficit);
      return {
        promoted: 16,
        enrichmentPromoted: 16,
        recovered: 0,
        discoveredQueued: 4,
        admission: { discovered: 8, evaluated: 8, fit: 5, admittedToEnrichment: 4, recovered: 0, rejected: {} },
      };
    },
  });
  assert.equal(calls[1], 16);
  assert.equal(second.plan.state, 'healthy');
  assert.equal(second.plan.cleanInventory, 36);
  assert.equal(second.plan.shouldReplenish, false);
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

