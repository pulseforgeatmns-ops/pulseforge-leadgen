'use strict';

// Anchor Scout P0: replenishment composition, bounded zero-yield behavior and
// truthful cycle outcomes. Synthetic pools and transports only.
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');

const quiet = { log() {}, warn() {}, error() {} };
const LEGACY_SOURCE = { id: 'mission_anchor_weekend_20261003_approved_market', client_id: 10, objective: 'Anchor acquisition',
  payload: { structuredMission: {
    market: { segment: 'commercial_cleaning_buyers', industry: 'property_and_office_operations',
      eligibleSubsegments: ['property_manager', 'str_manager', 'realtor', 'commercial_office'] },
    geography: { region: 'Greater Manchester', scope: null,
      cities: ['Manchester', 'Hooksett', 'Bedford', 'Auburn', 'Goffstown', 'Londonderry'] },
  } } };

function withEnv(t, values) {
  for (const [key, value] of Object.entries(values)) {
    const prior = process.env[key];
    if (value == null) delete process.env[key]; else process.env[key] = value;
    t.after(() => { if (prior == null) delete process.env[key]; else process.env[key] = prior; });
  }
}

function discoveryPlanFor(source) {
  const { _test: { scoutInput } } = require('../services/maxOutboundControlLoop');
  const { buildBoundedScoutContext } = require('../packages/max/scoutAcquisition/BoundedContext');
  const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
  const { buildDiscoveryPlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
  const program = { id: 'p', tenant_id: '10', source_mission_id: source.id };
  const input = scoutInput(program, source, { deficit: 5 }, { tenantId: '10' });
  const definition = buildAcquisitionSearchDefinition({ ...buildBoundedScoutContext(input), authorizedTenantId: '10' });
  return buildDiscoveryPlan(definition, { enabledSources: ['public_business_data'] });
}

function facilitySource() {
  const { facilityBuyerSuccessorMission } = require('../scripts/activateAnchorFacilityBuyerMission');
  const mission = facilityBuyerSuccessorMission(LEGACY_SOURCE);
  return { id: mission.id, payload: mission };
}

// --- 5/6: composition -------------------------------------------------------

test('brokerage listings filed under the shared realty place type are realtors, not property managers', () => {
  const { resolveReplenishmentVertical } = require('../utils/replenishmentVertical');
  const realty = ['real_estate_agency'];
  assert.equal(resolveReplenishmentVertical({ name: 'Keller Williams Realty Metropolitan', placeTypes: realty,
    snippet: 'Find your next property in Bedford' }), 'realtor');
  assert.equal(resolveReplenishmentVertical({ name: 'Hometown Property Group', placeTypes: realty }), 'realtor');
  assert.equal(resolveReplenishmentVertical({ name: 'Bedford Realty Group', placeTypes: realty,
    snippet: 'property management' }), 'property_manager', 'explicit management evidence still wins');
});

test('relevant commercial property management companies remain eligible under the facility mission', () => {
  const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
  const mission = facilitySource().payload.structuredMission;
  const context = { tenantId: '10', missionSegment: 'managed_facilities', missionSegments: mission.market.eligibleSubsegments,
    missionCities: mission.geography.cities };
  for (const candidate of [
    { name: 'Granite Commercial Property Management', website: 'granite.example', location: 'Nashua, NH', placeTypes: ['real_estate_agency'] },
    { name: 'Cedar Management Group', website: 'cedar.example', location: 'Merrimack, NH', placeTypes: ['real_estate_agency'] },
  ]) {
    const result = evaluateReplenishmentAdmission(candidate, context);
    assert.equal(result.admitted, true, candidate.name);
    assert.equal(result.vertical, 'property_manager');
  }
  const brokerage = evaluateReplenishmentAdmission({ name: 'Keller Williams Realty', website: 'kw.example',
    location: 'Bedford, NH', placeTypes: ['real_estate_agency'], snippet: 'property listings' }, context);
  assert.deepEqual([brokerage.admitted, brokerage.reason], [false, 'segment_mismatch']);
  for (const [name, vertical] of [['Derryfield School', 'school'], ['Acme Manufacturing Company', 'manufacturer']]) {
    const result = evaluateReplenishmentAdmission({ name, website: 'x.example', location: 'Manchester, NH',
      snippet: vertical === 'school' ? 'independent school' : 'manufacturer' }, context);
    assert.deepEqual([result.admitted, result.vertical], [true, vertical]);
  }
});

test('brokerage-heavy approved scope searches realtor categories last; the revised scope never queries them', () => {
  const legacy = discoveryPlanFor(LEGACY_SOURCE);
  const realtorIndex = legacy.workloads.findIndex(w => /real estate|realtor/i.test(w.concept));
  const firstCityWorkloads = legacy.workloads.filter(w => w.city === legacy.cities[0]);
  assert.ok(realtorIndex > 0);
  assert.ok(firstCityWorkloads.slice(-2).every(w => /real estate|realtor/i.test(w.concept)),
    'within each city the one-page budget reaches realtor queries only after every other approved category');
  assert.ok(!legacy.concepts.includes('Property Manager'), 'pure synonym query is not bought twice');

  const revised = discoveryPlanFor(facilitySource());
  assert.equal(revised.cities.length, 8);
  assert.ok(revised.cities.includes('Nashua NH') && revised.cities.includes('Merrimack NH'));
  assert.ok(revised.concepts.every(concept => !/real estate|realtor|vacation|airbnb/i.test(concept)));
  assert.deepEqual(revised.concepts.slice(0, 4), ['private school', 'college campus', 'academy', 'commercial property management']);
});

test('brokerage-heavy inventory never displaces institutional and commercial prospects at first touch', async t => {
  const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
  const inventory = require('../services/acquisitionMissionInventory');
  t.mock.method(inventory, 'loadKnowledgeInventory', async () => []);
  const base = (id, name, vertical) => ({ id, company_id: `c-${id}`, client_id: 10, ao_outreach_review_required: false,
    company_name: name, company_domain: `${id}.example`, email: `owner@${id}.example`, email_verified: true,
    email_status: 'valid', enrichment_provenance: { email: { source: 'website_email' } }, vertical,
    service_area_match: 'Manchester', company_location: 'Manchester, NH' });
  const rows = [
    base('r1', 'Keller Williams Realty', 'realtor'), base('r2', 'Coldwell Banker Office', 'realtor'),
    base('r3', 'RE/MAX Bedford', 'realtor'), base('s1', 'Lakeside Airbnb Co-host', 'str_manager'),
    base('pm1', 'Hilltop Apartments Rental Management', 'property_manager'),
    base('pm2', 'Granite Commercial Property Management', 'property_manager'),
    base('o1', 'Bedford Office Park', 'commercial_office'),
  ];
  const pool = { query: async () => ({ rows: rows.map(row => ({ ...row })) }) };
  const store = { candidateOwnership: async () => null, suppression: async () => null };
  const result = await loadCleanInventory(pool, store, LEGACY_SOURCE, 10, { tenantId: '10' });
  const order = result.clean.map(row => row.prospectId);
  assert.equal(order.length, rows.length, 'deprioritized, not excluded under the still-approved legacy scope');
  assert.deepEqual(order.slice(0, 3), ['pm2', 'o1', 'pm1']);
  assert.deepEqual(order.slice(-3), ['r1', 'r2', 'r3']);

  // Other tenants keep their existing ordering.
  const other = await loadCleanInventory(pool, store, LEGACY_SOURCE, 13, { tenantId: '13' });
  assert.notDeepEqual(other.clean.map(row => row.prospectId).slice(0, 3), ['pm2', 'o1', 'pm1']);
});

test('the revised mission proposal is immutable, retires realtor/STR and covers the full service geography', () => {
  const { facilityBuyerSuccessorMission, CONFIRMATION } = require('../scripts/activateAnchorFacilityBuyerMission');
  const before = JSON.stringify(LEGACY_SOURCE);
  const mission = facilityBuyerSuccessorMission(LEGACY_SOURCE);
  assert.equal(JSON.stringify(LEGACY_SOURCE), before);
  assert.equal(mission.predecessorMissionId, LEGACY_SOURCE.id);
  assert.equal(mission.structuredMission.immutable, true);
  assert.deepEqual(mission.structuredMission.market.eligibleSubsegments, ['managed_facilities', 'restaurant_foh']);
  assert.deepEqual(mission.structuredMission.geography.cities,
    ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack']);
  assert.equal(facilityBuyerSuccessorMission(LEGACY_SOURCE).id, mission.id, 'deterministic identity');
  assert.match(CONFIRMATION, /^anchor-facility-buyer-icp-/);
  const { sourceScope, scoutSearchFingerprint } = require('../services/maxOutboundControlLoop');
  assert.equal(sourceScope({ payload: mission }).commercialCapability, 'commercial_cleaning');
  const program = { tenant_id: '10', source_mission_id: mission.id };
  assert.notEqual(scoutSearchFingerprint(program, { payload: mission }),
    '22d50dc07f997a3a811368a7e8b1de5a3211df5bd555260c3d42c4e780a960b7');
});

// --- 7: bounded zero-yield / exhaustion -------------------------------------

function eventsPool(rows) {
  return { query: async (sql, params) => {
    assert.match(sql, /FROM acquisition_outbound_events/);
    return { rows: rows.slice(0, params[2]) };
  } };
}

test('an exhausted search generation advances without waiting and the final one becomes a durable hold, never a reset', async () => {
  const { loadScoutDiscoveryBackoff } = require('../services/maxOutboundControlLoop');
  const now = new Date('2026-10-28T15:00:00Z');
  const event = (generation, minutesAgo, exhausted = true) => ({ created_at: new Date(+now - minutesAgo * 60_000),
    payload: { scoutSearchGeneration: generation, scoutSearchGenerationExhausted: exhausted, newCleanInventoryAdded: 0 } });

  const advance = await loadScoutDiscoveryBackoff(eventsPool([event(0, 15)]), '10', 'fp', now);
  assert.deepEqual([advance.diversify, advance.searchGeneration, advance.reason], [true, 1, 'search_generation_exhausted_advance']);

  const terminal = await loadScoutDiscoveryBackoff(eventsPool([event(3, 15), event(2, 30), event(1, 45)]), '10', 'fp', now);
  assert.equal(terminal.reason, 'approved_search_plan_exhausted');
  assert.equal(terminal.diversify, false);
  assert.equal(terminal.retryAt, new Date(+now - 15 * 60_000 + 24 * 60 * 60_000).toISOString());

  // Three hours later the old code restarted generation 0 and walked a handled plan again.
  const later = await loadScoutDiscoveryBackoff(eventsPool([event(3, 180), event(2, 195), event(1, 210)]), '10', 'fp', now);
  assert.equal(later.reason, 'approved_search_plan_exhausted');
  // A daily re-observation is allowed; durable memory still forbids rebuying.
  assert.equal(await loadScoutDiscoveryBackoff(eventsPool([event(3, 25 * 60)]), '10', 'fp', now), null);

  const fresh = await loadScoutDiscoveryBackoff(eventsPool([event(0, 15, false)]), '10', 'fp', now);
  assert.equal(fresh, null, 'generation 0 with searchable queries keeps the existing threshold behavior');

  // A productive diversified generation is kept, instead of alternating with an
  // exhausted generation 0 every other cycle.
  const sticky = await loadScoutDiscoveryBackoff(eventsPool([event(1, 15, false)]), '10', 'fp', now);
  assert.deepEqual([sticky.reason, sticky.searchGeneration, sticky.diversify], ['continue_search_generation', 1, true]);
  assert.equal(await loadScoutDiscoveryBackoff(eventsPool([event(1, 15, false)]), '13', 'fp', now), null,
    'other tenants keep their prior generation behavior');
});

test('generation exhaustion is recognized only when nothing was bought, replayed or deferred by this cycle', () => {
  const { _test: { searchGenerationExhausted } } = require('../services/maxOutboundControlLoop');
  const snap = (statuses, extra = {}) => ({ observedQueries: 3, fetchedQueries: 0, replayedQueries: 0,
    continuation: { newPagesReserved: 0, statuses }, ...extra });
  assert.equal(searchGenerationExhausted(snap({ tenant_query_previously_searched: 2, finite_query_exhausted: 1 })), true);
  assert.equal(searchGenerationExhausted(snap({ cycle_page_budget_reached: 2 })), false);
  assert.equal(searchGenerationExhausted(snap({}, { fetchedQueries: 1 })), false);
  assert.equal(searchGenerationExhausted(snap({}, { continuation: { newPagesReserved: 1, statuses: {} } })), false);
  assert.equal(searchGenerationExhausted({ observedQueries: 0 }), false);
  assert.equal(searchGenerationExhausted({ observedQueries: 2, fetchedQueries: 0, replayedQueries: 0 }), false,
    'the legacy non-durable path has no continuation evidence');
});

// --- 7/8/9/10: control-cycle outcomes --------------------------------------

function maxOptions(pool, overrides = {}) {
  const inventory = { clean: [], excluded: [], scope: {}, exclusionCounts: {} };
  const events = [];
  return {
    events,
    options: {
      pool, tenantId: '10', logger: quiet, now: new Date('2026-10-28T16:00:00Z'),
      program: { id: 'local-10', tenant_id: '10', mode: 'active', source_mission_id: 'source-10', policy: { dailyCap: 5 } },
      source: { id: 'source-10', payload: {} },
      store: { tenantId: '10', clientId: 10, event: async (type, key, data) => { events.push({ type, data }); },
        envelope: async () => null },
      inventory, inventoryAfter: inventory, timestamps: {}, funnel: {},
      infrastructure: { cap: 5, snapshot: { sentToday: 0 },
        operating: { planningDailyCapacity: 5, authorizationLimitedCapacity: 5,
          dispatchCapacityNow: 5, scheduleLimitedCapacity: 5, governor: 'proceed' } },
      refillAoBooks: async () => ({ assigned: [] }),
      ...overrides,
    },
  };
}

function recordingPool(controlRow = null) {
  const calls = [];
  return { calls, query: async (sql, params) => {
    calls.push({ sql, params });
    if (/to_regclass\('public.anchor_email_recovery_control'\)/.test(sql)) {
      return { rows: [{ control_table: controlRow ? 'anchor_email_recovery_control' : null }] };
    }
    if (/FROM anchor_email_recovery_control/.test(sql)) return { rows: controlRow ? [controlRow] : [] };
    return { rows: [] };
  } };
}
const holdReasons = pool => pool.calls.filter(call => /scout_replenishment_blocked/.test(call.sql)).map(call => call.params[4]);
const legacyControls = { ANCHOR_RESEARCH_PROVIDERS_PAUSED: null, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: null,
  ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: null, ANCHOR_EMAIL_SCOPED_RESUME_ID: null, ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false' };

test('a completed cycle that admits nothing reports zero yield, not replenishment', async t => {
  withEnv(t, legacyControls);
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool();
  const { options, events } = maxOptions(pool, { scoutRamp: async () => ({ admission: {}, discoveryAttempted: true,
    admissionMeasured: true, discoveredQueued: 0 }) });
  const result = await runMaxOutboundControlLoop(options);
  assert.equal(result.replenishment.status, 'zero_yield');
  assert.equal(result.replenishment.newQualifiedInventory, 0);
  assert.equal(result.replenishment.newQueueAdmissions, 0);
  assert.equal(events.find(e => e.type === 'max_outbound_control').data.replenishment.status, 'zero_yield');

  const { classify, evidenceReferences } = require('../services/governedOutboundStageScheduler');
  const outcome = classify(result, 'control', '10');
  assert.deepEqual(outcome, { status: 'completed', reason: 'replenishment_zero_yield' });
  assert.equal(evidenceReferences(result, 'control').replenishmentStatus, 'zero_yield');

  const grown = maxOptions(recordingPool(), { scoutRamp: async () => ({ admission: {} }),
    inventoryAfter: { clean: [{ prospectId: 'new-1', companyId: 'c1', email: 'a@b.example' }], excluded: [], scope: {}, exclusionCounts: {} } });
  const replenished = await runMaxOutboundControlLoop(grown.options);
  assert.equal(replenished.replenishment.status, 'replenished');
  assert.equal(classify(replenished, 'control', '10').reason, 'replenishment_replenished');
});

test('an exhausted approved search plan raises one operator hold and spends nothing', async t => {
  withEnv(t, legacyControls);
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool();
  const { options } = maxOptions(pool, { scoutRamp: async () => ({ admission: {}, discoveryAttempted: false,
    discovery: { kind: 'backoff', reason: 'approved_search_plan_exhausted' },
    discoveryBackoff: { reason: 'approved_search_plan_exhausted', retryAt: '2026-10-29T15:45:00.000Z' } }) });
  const result = await runMaxOutboundControlLoop(options);
  assert.equal(result.replenishment.status, 'search_plan_exhausted');
  assert.deepEqual(holdReasons(pool), ['scout_approved_search_plan_exhausted']);
  const hold = pool.calls.find(call => /scout_replenishment_blocked/.test(call.sql));
  assert.match(hold.params[1], /no unsearched query left/);
});

test('scheduler failures are durable and can never be read as a replenishment', async () => {
  const { dispatchGovernedOutboundControl, whenControlDispatchSettled, resetControlDispatchForTests }
    = require('../services/governedOutboundControlDispatch');
  resetControlDispatchForTests();
  const logged = [];
  const client = { query: async sql => ({ rows: /pg_try_advisory_lock/.test(sql) ? [{ locked: true }] : [] }), release() {} };
  const pool = { connect: async () => client, query: async (sql, params) => { logged.push({ sql, params }); return { rows: [] }; } };
  const receipt = await dispatchGovernedOutboundControl({ pool, tenantIds: ['10'], logger: quiet,
    runControl: async () => { throw Object.assign(new Error('boom'), { code: 'scout_ramp_crashed' }); } });
  assert.equal(receipt.tenants['10'].status, 'started', 'dispatch only acknowledges the start');
  await whenControlDispatchSettled();
  const [failure] = logged.filter(call => /INSERT INTO agent_log/.test(call.sql));
  assert.ok(failure, 'the crash leaves a durable row');
  assert.match(failure.sql, /'failed'/);
  assert.equal(failure.params[1], 'scout_ramp_crashed');
  assert.equal(failure.params[2], 10);
  assert.equal(JSON.parse(failure.params[0]).replenishment.status, 'failed');

  const { classify } = require('../services/governedOutboundStageScheduler');
  assert.notEqual(classify(null, 'control', '10').status, 'completed');
  assert.notEqual(classify({ halted: 'anchor_research_provider_paused' }, 'control', '10').status, 'completed');
  assert.notEqual(classify(receipt, 'control', '10').status, 'completed', 'a dispatch receipt is not a cycle result');
});

test('Anchor native discovery without durable memory is blocked before any provider work', async t => {
  withEnv(t, { ...legacyControls, SCOUT_DURABLE_CANDIDATES_ENABLED: null, SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: null,
    GOOGLE_PLACES_KEY: 'synthetic-key-never-used' });
  const { defaultScoutRamp, anchorDurableDiscoveryEnabled } = require('../services/maxOutboundControlLoop');
  assert.equal(anchorDurableDiscoveryEnabled('10'), false);
  assert.equal(anchorDurableDiscoveryEnabled('13'), true, 'other tenants keep their existing path');
  const originalFetch = global.fetch;
  let fetches = 0;
  global.fetch = async () => { fetches++; throw new Error('provider must not be reached'); };
  t.after(() => { global.fetch = originalFetch; });
  const events = [];
  const result = await defaultScoutRamp({
    pool: recordingPool(),
    store: { tenantId: '10', clientId: 10, event: async (...args) => events.push(args) },
    program: { id: 'p', tenant_id: '10', source_mission_id: LEGACY_SOURCE.id, policy: {} },
    source: LEGACY_SOURCE, plan: { deficit: 5 }, logger: quiet, skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
    loadDiscoveryBackoff: async () => null,
  });
  assert.equal(fetches, 0);
  assert.equal(result.discoveryAttempted, false);
  assert.deepEqual(result.discovery, { kind: 'blocked', reason: 'scout_discovery_memory_required' });
  assert.ok(events.some(([type, , data]) => type === 'tick_blocked' && data.reason === 'scout_discovery_memory_required'));
});

test('provider pause and recovery kill switch stop every research root with zero provider calls', async t => {
  const { runMaxOutboundControlLoop, defaultScoutRamp } = require('../services/maxOutboundControlLoop');
  const originalFetch = global.fetch;
  let fetches = 0;
  global.fetch = async () => { fetches++; throw new Error('provider must not be reached'); };
  t.after(() => { global.fetch = originalFetch; });

  withEnv(t, { ...legacyControls, ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true', SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true' });
  const paused = recordingPool();
  const { options } = maxOptions(paused);
  const blocked = await runMaxOutboundControlLoop(options);
  assert.equal(blocked.replenishment.status, 'blocked');
  assert.equal(blocked.replenishment.reason, 'anchor_research_provider_paused');
  assert.equal(blocked.scout, null);
  await assert.rejects(defaultScoutRamp({ pool: paused, store: options.store, program: options.program,
    source: LEGACY_SOURCE, plan: { deficit: 5 }, logger: quiet }), { code: 'anchor_research_provider_paused' });

  withEnv(t, { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' });
  const halted = recordingPool({ singleton: true, maintenance_armed: true, manifest: null });
  const run = maxOptions(halted, { scoutRamp: async () => { throw new Error('ramp must not run'); } });
  assert.equal((await runMaxOutboundControlLoop(run.options)).halted, 'anchor_recovery_separate_preparation_approval_required');
  assert.equal(fetches, 0);
});

test('no outbound send path is reachable from replenishment while governed outbound is disabled', async t => {
  withEnv(t, legacyControls);
  const { governedOutboundEnabledForTenant } = require('../services/governedOutboundTenant');
  for (const value of ['false', '', 'TRUE', '1', null]) {
    withEnv(t, { ANCHOR_GOVERNED_OUTBOUND_ENABLED: value });
    assert.equal(governedOutboundEnabledForTenant('10'), false, String(value));
  }
  withEnv(t, { ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false' });
  const sends = [];
  const transport = new Proxy({}, { get: () => async () => { sends.push('send'); throw new Error('no send'); } });
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool();
  const { options, events } = maxOptions(pool, { transport, brevo: transport,
    scoutRamp: async () => ({ admission: {}, discoveredQueued: 3, admissionMeasured: true }),
    inventoryAfter: { clean: [{ prospectId: 'n1', companyId: 'c1', email: 'x@y.example' }], excluded: [], scope: {}, exclusionCounts: {} } });
  const result = await runMaxOutboundControlLoop(options);
  assert.equal(result.replenishment.status, 'replenished');
  assert.equal(events.find(e => e.type === 'max_outbound_control').data.sendingEnabled, false);
  assert.deepEqual(sends, []);
  assert.equal(pool.calls.some(call => /acquisition_outbound_items|brevo|INSERT INTO touchpoints/i.test(call.sql)), false);
});
