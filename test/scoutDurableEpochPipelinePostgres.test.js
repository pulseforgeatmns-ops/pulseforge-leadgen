'use strict';
require('./helpers/denyExternalProviders.cjs');
// Actual controller/provider/page/ticket/queue path; synthetic HTTP and local PG.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const calls = [], reservations = [];
let prefix = 'first';
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: { query: async sql => {
  assert.match(sql, /places_api_requests|CREATE TABLE|CREATE INDEX|ALTER TABLE/); return { rows: [] };
} } };
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true, exports: {
  reserveApiCall: async input => { reservations.push(input); return 'synthetic'; }, settleApiCall: async () => {},
} };
const providerPath = require.resolve('../packages/capabilities/discovery/providers/PlacesProvider');
const provider = require(providerPath);
require.cache[providerPath].exports = { ...provider, createPlacesProvider: options => provider.createPlacesProvider({
  ...options, apiKey: 'synthetic', fetchImpl: async input => {
    const url = new URL(input); calls.push(url.pathname);
    if (url.pathname.includes('details')) {
      const id = url.searchParams.get('place_id');
      return { ok: true, status: 200, json: async () => ({ status: 'OK', result: {
        place_id: id, name: `Painting Contractor ${id}`, website: `https://${id}.example`,
        formatted_address: 'Manchester, NH', types: ['painter'],
      } }) };
    }
    const token = url.searchParams.get('pagetoken');
    if (!token) assert.equal(url.searchParams.get('query'), 'Painting Contractor Manchester');
    const page = token ? Number(token.split('-').at(-1)) : 0;
    return { ok: true, status: 200, json: async () => ({ status: 'OK',
      results: Array.from({ length: 20 }, (_, i) => ({
        place_id: prefix === 'first' && page === 0 && i === 0 ? 'oldpending5' : `${prefix}${page * 20 + i}`,
        name: `Painter ${i}`,
      })),
      next_page_token: page < 2 ? `private-token-${page + 1}` : null,
    }) };
  },
}) };
const { defaultScoutRamp } = require('../services/maxOutboundControlLoop');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { createPageCheckpointStore } = require('../services/scoutQueryContinuation');
const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const { buildDiscoveryPlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { queryIdentity, PLAN_VERSION } = require('../services/scoutQueryCheckpoints');
const cache = require('../utils/placesQueryCache');
const root = queryIdentity('Painting Contractor Manchester', false);
const definition = { tenantId: '13', segments: ['small_business_owners'], businessNeed: 'business_transformation',
  geography: { label: 'Manchester', cities: ['Manchester'], state: null },
  evidenceRequest: { tenantId: '13', scope: 'market', segment: 'small_business_owners', evidenceType: 'identity',
    discoveryConcept: 'Painting Contractor', geography: { cities: ['Manchester'], state: null } } };
const roots = buildDiscoveryPlan(definition).concepts.map(concept => queryIdentity(`${concept} Manchester`, false));
const hash = value => createHash('sha256').update(value).digest('hex');

test('explicit legacy epoch sustains new same-mission admissions and preserves old responsibility across disable/restart', { timeout: 60000 }, async t => {
  const envNames = ['SCOUT_DURABLE_CANDIDATES_ENABLED', 'SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED', 'SCOUT_LEGACY_COVERAGE_EPOCH_JSON'];
  const old = Object.fromEntries(envNames.map(name => [name, process.env[name]]));
  process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = 'true';
  const instance = await startDisposablePostgres('scout-epoch-pipeline-');
  const db = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 3 });
  t.after(async () => { for (const name of envNames) old[name] == null ? delete process.env[name] : process.env[name] = old[name];
    await closePostgresFixture(db, instance); });
  for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql',
    '2026-10-19-scout-candidate-tickets.sql', '2026-10-19-scout-query-coverage-epochs.sql', '2026-10-20-scout-coverage-grants.sql']) {
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8'));
  }
  await db.query(`CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,
    vertical text,location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());`);
  const pool = { connect: (...args) => db.connect(...args), query: (sql, params) => /FROM prospects p/.test(sql)
    ? Promise.resolve({ rows: [] }) : db.query(sql, params) };
  const events = [], logs = [];
  const store = { tenantId: '13', clientId: 13, candidateOwnership: async () => null, event: async (...args) => events.push(args) };
  let now = Date.parse('2026-10-19T12:00:00.000Z');
  let fingerprint = hash('same-approved-mission');
  let failTransfer = false;
  const tickets = () => createCandidateTicketStore({ pool, tenantId: '13', fingerprint });
  const approve = () => {
    process.env.SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED = 'true';
    process.env.SCOUT_LEGACY_COVERAGE_EPOCH_JSON = JSON.stringify({ version: 1, epoch: 1, approvalRef: 'local-synthetic-only',
      tenantId: '13', fingerprint, rootQueryKeys: [root], notBefore: '2026-10-19T00:00:00.000Z', expiresAt: '2026-10-20T00:00:00.000Z' });
  };
  const stage = (queryKey, candidates, state = 'fetched') => db.query(`INSERT INTO scout_query_checkpoints
    (tenant_id,search_fingerprint,query_key,plan_version,state,candidates,handled_at)
    VALUES('13',$1,$2,$3,$4,$5::jsonb,CASE WHEN $4='handled' THEN now() ELSE NULL END)`,
  [fingerprint, queryKey, PLAN_VERSION, state, JSON.stringify(candidates)]);
  const oldBusiness = id => ({ placeId: id, companyName: `Painting Contractor ${id}`, website: `${id}.example`,
    address: 'Manchester, NH', placeTypes: ['painter'], source: 'google_places', discoveryQuery: 'Painting Contractor Manchester' });
  const run = async () => {
    cache.clearPlacesQueryCache();
    const result = await defaultScoutRamp({ pool, store, program: { id: 'fixture', tenant_id: '13', policy: {} },
      source: { payload: { structuredMission: { market: { segment: 'small_business_owners' },
        geography: { region: 'Manchester', scope: 'local', cities: ['Manchester'] } } } },
      plan: { deficit: 1 }, searchFingerprint: fingerprint, skipVerificationRetry: true,
      enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
      loadDiscoveryBackoff: async () => null,
      createCandidateTicketStore: () => { const original = tickets(); return { ...original, transferQuery: async key => {
        if (failTransfer && key !== root) throw Error('Synthetic crash after raw page stage');
        return original.transferQuery(key);
      } }; },
      createPageCheckpointStore: options => createPageCheckpointStore({ ...options, now: () => now }),
      runDiscovery: async (_input, options) => {
        const universe = await constructCandidateUniverse({ searchDefinition: definition, forceDiscover: true,
          existing: { companies: [], people: [] }, adapters: [createPlacesDiscoveryAdapter({ placesProvider: options.placesProvider })],
          persistCompanies: options.persistCompanies, acknowledgeDiscovery: options.acknowledgeDiscovery });
        if (!failTransfer) assert.deepEqual(universe.discoveryErrors, []);
        return { kind: 'investigated' };
      }, logger: { log: (...args) => logs.push(args), warn() {} },
    });
    now += 6000; return result;
  };
  await t.test('existing fetched legacy payload transfers before a single explicitly allowed fresh epoch', async () => {
    approve();
    assert.equal(roots.length, 21);
    const original = [oldBusiness(`oldpending${roots.indexOf(root)}`)];
    for (let i = 0; i < roots.length; i++) await stage(roots[i], [oldBusiness(`oldpending${i}`)]);
    const first = await run();
    assert.equal(first.discoveredQueued, 21); assert.equal(calls.length, 0);
    assert.deepEqual(first.queryProgression.coverageApprovalManifest.rootQueryKeys, [...roots].sort());
    for (let cycle = 0; cycle < 3; cycle++) {
      const before = calls.length; const result = await run();
      assert.equal(result.discoveredQueued, cycle === 0 ? 19 : 20); assert.equal(calls.length - before, 21);
      assert.equal(result.candidateWork.byState.handled, 20 + (cycle + 1) * 20);
      assert.equal(result.queryProgression.coverageApprovalManifest.fingerprint, fingerprint);
      assert.ok(result.queryProgression.coverageApprovalManifest.rootQueryKeys.every(key => roots.includes(key)));
    }
    const before = calls.length;
    for (let cycle = 0; cycle < 3; cycle++) {
      assert.equal((await run()).discoveredQueued, 0); assert.equal(calls.length, before);
    }
    assert.equal(reservations.length, 63); assert.equal(reservations.filter(row => row.operation === 'text_search').length, 3);
    const saved = (await db.query('SELECT state,candidates FROM scout_query_checkpoints WHERE search_fingerprint=$1 AND query_key=$2', [fingerprint, root])).rows[0];
    assert.equal(saved.state, 'handled'); assert.deepEqual(saved.candidates, original);
    assert.equal((await db.query('SELECT count(*)::int AS n FROM scout_candidate_work WHERE search_fingerprint=$1', [fingerprint])).rows[0].n, 80);
    const unchanged = (await db.query(`SELECT w.attempts FROM scout_candidate_work w JOIN scout_candidate_memberships m
      USING(tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key)
      WHERE w.search_fingerprint=$1 AND m.query_key=$2 AND m.raw_candidate->>'placeId'='oldpending5'`, [fingerprint, root])).rows[0];
    assert.equal(Number(unchanged.attempts), 1, 'a repeated first-page identity adds provenance without repeating candidate handling');
  });
  await t.test('disabled permission replays a staged epoch without HTTP and retains the original backlog scope', async () => {
    fingerprint = hash('same-approved-mission-crash'); prefix = 'second'; approve();
    for (const key of roots) await stage(key, [], 'handled');
    failTransfer = true;
    const before = calls.length;
    await run(); assert.equal(calls.length - before, 21);
    const pending = (await db.query("SELECT query_key,candidates FROM scout_query_checkpoints WHERE search_fingerprint=$1 AND state='fetched'", [fingerprint])).rows;
    assert.equal(pending.length, 1); assert.equal(pending[0].candidates.length, 20);
    delete process.env.SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED; delete process.env.SCOUT_LEGACY_COVERAGE_EPOCH_JSON;
    failTransfer = false;
    const replayed = await run(); assert.equal(replayed.discoveredQueued, 20); assert.equal(calls.length - before, 21);
    assert.equal(replayed.candidateWork.total, 20);
    for (let cycle = 0; cycle < 3; cycle++) {
      const held = await run(); assert.equal(held.discoveredQueued, 0); assert.equal(calls.length - before, 21);
      assert.equal(held.queryProgression.continuation.statuses.coverage_epoch_permission_required, 1);
    }
    await stage(hash('another-original-query'), [oldBusiness('later-old-obligation')]);
    await tickets().transferQuery(hash('another-original-query'));
    process.env.SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED = 'true'; process.env.SCOUT_LEGACY_COVERAGE_EPOCH_JSON = '{invalid';
    const drained = await run(); assert.equal(drained.discoveredQueued, 1); assert.equal(calls.length - before, 21);
    assert.equal(drained.discovery.reason, 'coverage_epoch_config_invalid'); assert.equal(drained.candidateWork.total, 21);
  });
  await t.test('v2 one-page grants renew actual Scout admissions without resetting page or candidate history', async () => {
    fingerprint = hash('synthetic-renewable-grant-mission'); prefix = 'third'; failTransfer = false;
    for (const key of roots) await stage(key, [], 'handled');
    const grant = reference => {
      process.env.SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED = 'true';
      process.env.SCOUT_LEGACY_COVERAGE_EPOCH_JSON = JSON.stringify({ version: 2, epoch: 1,
        approvalRef: reference, tenantId: '13', fingerprint, rootQueryKeys: [root], maxNewPages: 1,
        notBefore: new Date(now - 1000).toISOString(), expiresAt: new Date(now + 10000).toISOString() });
    };
    const before = calls.length;
    grant('synthetic-first-page');
    assert.equal((await run()).discoveredQueued, 20); assert.equal(calls.length - before, 21);
    assert.equal((await run()).discoveredQueued, 0); assert.equal(calls.length - before, 21);
    grant('synthetic-second-page');
    assert.equal((await run()).discoveredQueued, 20); assert.equal(calls.length - before, 42);
    assert.equal((await run()).discoveredQueued, 0); assert.equal(calls.length - before, 42);
    grant('synthetic-third-page'); failTransfer = true;
    await run(); assert.equal(calls.length - before, 63);
    delete process.env.SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED; delete process.env.SCOUT_LEGACY_COVERAGE_EPOCH_JSON;
    failTransfer = false;
    assert.equal((await run()).discoveredQueued, 20); assert.equal(calls.length - before, 63);
    grant('synthetic-cannot-reset');
    assert.equal((await run()).discoveredQueued, 0); assert.equal(calls.length - before, 63);
    const pages = (await db.query('SELECT page_index FROM scout_query_pages WHERE search_fingerprint=$1 ORDER BY page_index', [fingerprint])).rows;
    assert.deepEqual(pages.map(row => row.page_index), [0,1,2]);
    assert.equal((await db.query('SELECT count(*)::int AS count FROM scout_query_coverage_grant_pages WHERE search_fingerprint=$1', [fingerprint])).rows[0].count, 3);
    assert.equal((await db.query('SELECT count(*)::int AS count FROM scout_candidate_work WHERE search_fingerprint=$1', [fingerprint])).rows[0].count, 60);
  });
  assert.ok(!JSON.stringify({ logs, events }).includes('private-token'));
  assert.equal(db.waitingCount, 0); assert.equal(db.idleCount, db.totalCount);
});
