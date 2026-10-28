'use strict';

// Anchor Scout P0 end to end on native PostgreSQL: repeated scheduled cycles of
// the actual Max ramp under the revised facility-buyer mission. Synthetic Places
// transport only; external network is denied.
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: { query: async () => ({ rows: [] }) } };
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true,
  exports: { reserveApiCall: async () => 'synthetic', settleApiCall: async () => {} } };

// One Bedford-only slice of the revised mission keeps the plan to 8 queries.
const SEARCH_RESULTS = {
  'private school Bedford NH': ['school-1', 'shared-1'],
  'college campus Bedford NH': ['campus-1'],
  'academy Bedford NH': ['shared-1', 'academy-2'],
  'commercial property management Bedford NH': ['pm-1', 'realty-1', 'realty-2'],
};
const DETAILS = {
  'school-1': ['Bedford Montessori Private School', ['school']],
  'shared-1': ['Derryfield Preparatory School', ['school']],
  'campus-1': ['Granite State College Campus', ['university']],
  'academy-2': ['Bedford Academy', ['school']],
  'pm-1': ['Granite Commercial Property Management', ['real_estate_agency']],
  'realty-1': ['Keller Williams Realty Bedford', ['real_estate_agency']],
  'realty-2': ['Hometown Property Group', ['real_estate_agency']],
};

test('scheduled Anchor cycles buy each approved query once, reuse known businesses and end in an explicit exhaustion', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('scout-anchor-p0-');
  const db = new Pool({ connectionString: pg.connectionString, max: 8 });
  t.after(async () => { await db.end(); await pg.stop(); });
  for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql',
    '2026-10-19-scout-candidate-tickets.sql', '2026-10-27-scout-tenant-search-memory.sql']) {
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8'));
  }
  await db.query(`CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,vertical text,
    location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());`);

  for (const [key, value] of Object.entries({ SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true', SCOUT_DURABLE_CANDIDATES_ENABLED: null,
    ANCHOR_RESEARCH_PROVIDERS_PAUSED: null, ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false' })) {
    const prior = process.env[key];
    if (value == null) delete process.env[key]; else process.env[key] = value;
    t.after(() => { if (prior == null) delete process.env[key]; else process.env[key] = prior; });
  }

  const transport = [];
  const providerPath = require.resolve('../packages/capabilities/discovery/providers/PlacesProvider');
  const previous = require.cache[providerPath]?.exports || require(providerPath);
  require.cache[providerPath].exports = { ...previous, createPlacesProvider: options => previous.createPlacesProvider({ ...options,
    apiKey: 'synthetic', useCache: false, fetchImpl: async input => {
      const url = new URL(input);
      const detail = url.pathname.includes('details');
      const id = url.searchParams.get('place_id');
      const query = url.searchParams.get('query');
      transport.push(detail ? `detail:${id}` : `search:${query}`);
      if (detail) {
        const [name, types] = DETAILS[id];
        return { ok: true, status: 200, json: async () => ({ status: 'OK', result: { place_id: id, name,
          website: `https://${id}.example`, formatted_address: 'Bedford, NH', types } }) };
      }
      const ids = SEARCH_RESULTS[query] || [];
      return { ok: true, status: 200, json: async () => ids.length
        ? { status: 'OK', results: ids.map(place_id => ({ place_id, name: DETAILS[place_id][0] })) }
        : { status: 'ZERO_RESULTS', results: [] } };
    } }) };
  t.after(() => { require.cache[providerPath].exports = previous; });

  const { defaultScoutRamp, _test: { searchGenerationExhausted } } = require('../services/maxOutboundControlLoop');
  const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
  const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
  const { buildBoundedScoutContext } = require('../packages/max/scoutAcquisition/BoundedContext');
  const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
  const { facilityBuyerSuccessorMission } = require('../scripts/activateAnchorFacilityBuyerMission');

  function source(predecessor) {
    const mission = facilityBuyerSuccessorMission({ id: predecessor, client_id: 10, objective: 'Anchor', payload: { structuredMission: {
      market: { segment: 'commercial_cleaning_buyers', eligibleSubsegments: [] }, geography: { region: 'Greater Manchester' } } } });
    mission.structuredMission.market.eligibleSubsegments = ['managed_facilities'];
    mission.structuredMission.geography.cities = ['Bedford'];
    return { id: mission.id, payload: mission };
  }
  // Every cycle uses a fresh pool wrapper and module-level sessions: nothing
  // survives between cycles except PostgreSQL, as after a process restart.
  async function cycle(src) {
    const pool = { connect: db.connect.bind(db),
      query: (sql, p) => /FROM prospects p/.test(sql) ? Promise.resolve({ rows: [] }) : db.query(sql, p) };
    const events = [];
    const store = { tenantId: '10', clientId: 10, candidateOwnership: async () => null, event: async (...args) => events.push(args) };
    const before = transport.length;
    let snapshot = null;
    const result = await defaultScoutRamp({ pool, store,
      program: { id: `program-${src.id}`, tenant_id: '10', source_mission_id: src.id, policy: {} },
      source: src, plan: { deficit: 5 }, skipVerificationRetry: true, logger: { log() {}, warn() {} },
      enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
      loadDiscoveryBackoff: async () => null,
      // The real Max input, bounded context, search definition and coverage plan.
      runDiscovery: async (input, options) => {
        const definition = buildAcquisitionSearchDefinition({ ...buildBoundedScoutContext(input), authorizedTenantId: '10' });
        const outcome = await constructCandidateUniverse({ searchDefinition: definition, forceDiscover: true,
          existing: { companies: [], people: [] },
          adapters: [createPlacesDiscoveryAdapter({ placesProvider: options.placesProvider })],
          persistCompanies: options.persistCompanies, acknowledgeDiscovery: options.acknowledgeDiscovery });
        snapshot = outcome;
        return { kind: 'investigated' };
      } });
    return { result, events, calls: transport.slice(before), snapshot };
  }
  const queued = async () => (await db.query('SELECT company,vertical,domain FROM scout_unenriched ORDER BY id')).rows;

  const missionA = source('mission_anchor_weekend_20261003_approved_market');
  const c1 = await cycle(missionA);
  assert.deepEqual(c1.calls, ['search:private school Bedford NH', 'detail:school-1', 'detail:shared-1'],
    'one page per cycle, highest-priority approved query first');
  assert.equal(c1.result.discoveredQueued, 2);
  assert.equal(c1.result.searchGenerationExhausted, false);

  const c2 = await cycle(missionA);
  assert.deepEqual(c2.calls, ['search:college campus Bedford NH', 'detail:campus-1'], 'searched query is never repeated after restart');

  const c3 = await cycle(missionA);
  assert.deepEqual(c3.calls, ['search:academy Bedford NH', 'detail:academy-2'],
    'a known business from another query costs no detail request');
  assert.equal(c3.result.queryProgression.tenantMemory.detailsReused, 1);
  const afterAcademy = await queued();
  assert.equal(afterAcademy.filter(row => /derryfield/i.test(row.company)).length, 1, 'existing candidate is not duplicated');
  assert.equal(afterAcademy.length, 4);

  const c4 = await cycle(missionA);
  assert.equal(c4.calls[0], 'search:commercial property management Bedford NH');
  const rows = await queued();
  assert.ok(rows.some(row => row.vertical === 'property_manager' && /commercial property management/i.test(row.company)));
  assert.ok(rows.every(row => !/realty|hometown|keller/i.test(row.company)), 'brokerages are not admitted');
  assert.ok(rows.filter(row => ['school', 'campus'].includes(row.vertical)).length >= 3);

  const searched = new Set([...c1.calls, ...c2.calls, ...c3.calls, ...c4.calls].filter(call => call.startsWith('search:')));
  for (let i = 0; i < 4; i++) {
    const next = await cycle(missionA);
    const searches = next.calls.filter(call => call.startsWith('search:'));
    assert.equal(searches.length, 1, 'at most one new paid page per cycle');
    assert.ok(!searched.has(searches[0]));
    searched.add(searches[0]);
  }
  assert.equal(searched.size, 8, 'the whole approved Bedford plan was covered exactly once');

  const exhausted = await cycle(missionA);
  assert.deepEqual(exhausted.calls, [], 'an exhausted plan spends nothing');
  assert.equal(exhausted.result.searchGenerationExhausted, true);
  assert.equal(exhausted.result.discoveredQueued, 0);

  // Mission replacement: a new fingerprint inherits tenant-wide query memory.
  const missionB = source('mission_anchor_replacement_fixture');
  assert.notEqual(missionA.id, missionB.id);
  const replaced = await cycle(missionB);
  assert.deepEqual(replaced.calls, [], 'cross-mission memory survives restart and mission replacement');
  assert.equal(replaced.result.searchGenerationExhausted, true);
  assert.ok(replaced.result.queryProgression.tenantMemory.queriesSkipped >= 8);
  assert.equal((await queued()).length, rows.length, 'no duplicate businesses after replacement');
  assert.equal(typeof searchGenerationExhausted, 'function');
});
