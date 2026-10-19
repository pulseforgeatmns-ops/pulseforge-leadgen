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
  const inWindow = new Date('2026-09-28T15:00:00.000Z');
  const authorizationBound = buildControlPlan({
    dailyCap: 5,
    emmettCapacity: 16,
    sentToday: 1,
    cleanInventory: 2,
    targetDays: 3,
    now: inWindow,
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
    now: inWindow,
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
    { source_mission_id: 'mission_source', tenant_id: '10' },
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
    tenant_id: '10',
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
    pool: { query: async sql => { assert.match(sql, /to_regclass/); return { rows: [{ control_table: null }] }; } },
    cycleId: 'moc_10_test',
    program,
    source,
    store: {
      event: async (type, key, payload) => events.push({ type, key, payload }),
    },
    infrastructure: {
      cap: 5,
      snapshot: { sentToday: 1 },
      assessed: { governor: { outcome: 'proceed' }, health: { score: 74 } },
      operating: {
        recommendedSafeDailyCapacity: 5,
        authorizationLimitedCapacity: 5,
        scheduleLimitedCapacity: 5,
        dispatchCapacityNow: 5,
        planningDailyCapacity: 5,
        dispatchableDailyCapacity: 5,
        effectiveDailyCapacity: 5,
        governor: 'proceed',
      },
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
  assert.equal(events[0].payload.cycleId, 'moc_10_test');
  assert.equal(events[0].payload.scoutInvoked, true);
  assert.equal(events[0].payload.bufferTarget, 15);
});

test('Max keeps replenishing on later cycles until the target is met', async () => {
  const calls = [];
  const program = {
    id: 'outbound_test',
    tenant_id: '10',
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
    pool: { query: async sql => { assert.match(sql, /to_regclass/); return { rows: [{ control_table: null }] }; } },
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
    pool: { query: async sql => { assert.match(sql, /to_regclass/); return { rows: [{ control_table: null }] }; } },
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

test('Max persists bounded retry failure counts in the existing control event, including admission fallback', async () => {
  for (const sourceKey of ['verificationRetry', 'admission']) {
    for (const categorized of [false, true]) {
      const events = [];
      const telemetry = { verificationRetryAttempted: 3, verificationRetryFailed: 3,
        verificationRetryValid: 0, verificationRetryLoadFailed: 1,
        ...(categorized ? { verificationRetryFailureReasons: {
          verification_failed: 1, persistence_not_confirmed: 1, verification_exception: 1,
          'fixture-secret@example.test': 100, adapter_unavailable: 'fixture-api-key',
        } } : {}),
      };
      const inventory = { clean: [], excluded: [], scope: {}, exclusionCounts: {} };
      await runMaxOutboundControlLoop({
        pool: { query: async sql => { assert.match(sql, /to_regclass/); return { rows: [{ control_table: null }] }; } },
        tenantId: '13',
        now: new Date('2026-09-29T15:00:00.000Z'),
        program: { id: 'outbound_retry_fixture', tenant_id: '13', mode: 'active',
          policy_hash: 'fixture_policy', source_mission_id: 'fixture_mission', policy: { dailyCap: 1 } },
        source: { id: 'fixture_mission', payload: {} },
        store: { tenantId: '13', event: async (type, key, payload) => events.push({ type, payload }) },
        infrastructure: { cap: 1, snapshot: { sentToday: 0 },
          assessed: { governor: { outcome: 'proceed' }, health: { score: 80 } },
          operating: { planningDailyCapacity: 1, dispatchableDailyCapacity: 1, effectiveDailyCapacity: 1,
            recommendedSafeDailyCapacity: 1, authorizationLimitedCapacity: 1, scheduleLimitedCapacity: 1,
            dispatchCapacityNow: 1, governor: 'proceed' } },
        inventory, inventoryAfter: inventory,
        scoutRamp: async () => ({ [sourceKey]: telemetry }),
      });
      assert.equal(events.length, 1);
      assert.equal(events[0].type, 'max_outbound_control');
      const snapshot = JSON.parse(JSON.stringify(events[0].payload));
      assert.equal(snapshot.verificationRetryAttempted, 3);
      assert.equal(snapshot.verificationRetryFailed, 3);
      assert.equal(snapshot.verificationRetryValid, 0);
      assert.equal(snapshot.verificationRetryLoadFailed, 1);
      assert.equal(snapshot.verificationRetryFailureReasons.verification_failed, categorized ? 1 : 0);
      assert.equal(snapshot.verificationRetryFailureReasons.persistence_not_confirmed, categorized ? 1 : 0);
      assert.equal(snapshot.verificationRetryFailureReasons.verification_exception, categorized ? 1 : 0);
      assert.equal(Object.keys(snapshot.verificationRetryFailureReasons).length, 7);
      assert.equal(Object.values(snapshot.verificationRetryFailureReasons).reduce((sum, n) => sum + n, 0), categorized ? 3 : 0);
      assert.doesNotMatch(JSON.stringify(snapshot), /fixture-secret|fixture-api-key/);
    }
  }
});

test('Max observation can be disabled without invoking Scout', async () => {
  let called = false;
  const result = await runMaxOutboundControlLoop({
    pool: { query: async sql => { assert.match(sql, /to_regclass/); return { rows: [{ control_table: null }] }; } },
    execute: false,
    program: {
      id: 'outbound_test',
      tenant_id: '10',
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

test('inventory buffer never exceeds the remaining reviewed total grant',()=>{
 const plan=buildControlPlan({dailyCap:1,emmettCapacity:4,cleanInventory:2,targetDays:3,totalAttempted:0,policy:{dailyCap:1,totalCap:2},operatingCapacity:{planningDailyCapacity:1,dispatchCapacityNow:0,effectiveDailyCapacity:1}});
 assert.equal(plan.targetInventory,2);assert.equal(plan.shouldReplenish,false);
});

test('explicit restaurant FOH mission accepts evidenced restaurants without widening STR scope', () => {
  const scope = { segment: 'commercial_cleaning', eligibleSubsegments: ['property_manager', 'restaurant_foh'], cities: ['manchester'] };
  const row = { client_id: 10, service_area_match: 'Manchester', company_location: 'Manchester, NH', company_name: 'Elm Bistro', vertical: 'restaurant' };
  assert.equal(missionCandidateReason(row, scope, '10'), null);
  assert.equal(missionCandidateReason(row, { ...scope, eligibleSubsegments: ['short_term_rental'] }, '10'), 'mission_segment_mismatch');
  assert.equal(missionCandidateReason({ ...row, company_location: 'Boston, MA', service_area_match: 'Boston' }, scope, '10'), 'service_area_not_confirmed');
  assert.equal(missionCandidateReason(row, scope, '1'), 'pulseforge_reviewed_offer_required');
});

test('restaurant inventory does not require FOH scope before a facilities assessment', () => {
  const scope = { segment: 'restaurant_foh', cities: ['manchester'] };
  const row = { client_id: 10, service_area_match: 'Manchester', company_location: 'Manchester, NH', company_name: 'Queen City Ghost Kitchen', vertical: 'restaurant' };
  assert.equal(missionCandidateReason(row, scope, '10'), null);
  assert.equal(missionCandidateReason({ ...row, company_name: 'Food Manufacturing Plant' }, scope, '10'), 'contradictory_business_type');
});

test('qualified knowledge cannot bypass restaurant business fit or geography but retains unrelated tenant behavior', async t => {
  const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
  const inventory = require('../services/acquisitionMissionInventory');
  const row = { id: 'restaurant-test', company_id: 'c', client_id: 10,
    company_name: 'Elm Bistro', company_domain: 'elm.example', email: 'owner@elm.example',
    email_verified: true, email_status: 'valid', enrichment_provenance: { email: { source: 'website_email' } },
    vertical: 'restaurant', service_area_match: 'Manchester', company_location: 'Manchester, NH' };
  t.mock.method(inventory, 'loadKnowledgeInventory', async () => [{ ...row, qualificationReason: null }]);
  const pool = { query: async (sql, params) => {
    assert.match(sql, /WHERE p.client_id=\$1/);
    assert.match(sql, /c.client_id=p.client_id/);
    assert.equal(params[0], row.client_id);
    return { rows: [row] };
  } };
  const store = { candidateOwnership: async () => null, suppression: async () => null };
  const source = { payload: { structuredMission: { market: { eligibleSubsegments: ['restaurant_foh'] }, geography: { cities: ['Manchester'] } } } };
  const run = () => loadCleanInventory(pool, store, source, row.client_id, { tenantId: String(row.client_id) });
  assert.equal((await run()).clean.length, 1);
  source.payload.structuredMission.market.eligibleSubsegments = ['short_term_rental'];
  assert.equal((await run()).exclusionCounts.business_fit, 1);
  source.payload.structuredMission.market.eligibleSubsegments = ['restaurant'];
  assert.equal((await run()).clean.length, 1);
  row.company_name = 'Queen City Ghost Kitchen';
  assert.equal((await run()).clean.length, 1);
  row.company_name = 'Food Manufacturing Plant';
  assert.equal((await run()).exclusionCounts.business_fit, 1);
  row.company_name = 'Elm Bistro'; row.company_location = 'Boston, MA'; row.service_area_match = 'Boston';
  assert.equal((await run()).clean.length, 0);
  // Existing knowledge qualification remains authoritative for unrelated segments.
  row.client_id = 13; row.vertical = 'property_manager';
  const unrelated = await run();
  assert.equal(unrelated.exclusionCounts.business_fit, undefined);
  assert.equal(unrelated.exclusionCounts.out_of_scope, undefined);
});
