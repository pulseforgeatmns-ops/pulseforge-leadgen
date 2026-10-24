'use strict';
const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
// All persistence and HTTP are synthetic. Never construct a production pool.
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  async query(sql) { assert.match(sql, /places_api_requests|CREATE TABLE|CREATE INDEX|ALTER TABLE/); return { rows: [] }; },
} };
let denyBudget = false;
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true, exports: {
  reserveApiCall: async () => { if (denyBudget) throw Error('budget denied'); return 'fake'; }, settleApiCall: async () => {},
} };
const { createCheckpointStore, createCheckpointSession, queryIdentity } = require('../services/scoutQueryCheckpoints');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const cache = require('../utils/placesQueryCache');

function database() {
  const rows = new Map();
  return { rows, async query(sql, p) {
    const prefix = `${p[0]}|${p[1]}|`;
    if (sql.startsWith('SELECT')) return { rows: rows.has(prefix + p[2]) ? [structuredClone(rows.get(prefix + p[2]))] : [] };
    if (sql.startsWith('INSERT')) {
      if (!rows.has(prefix + p[2])) rows.set(prefix + p[2], { query_key: p[2], state: 'fetched', candidates: JSON.parse(p[4]) });
      return { rows: [] };
    }
    assert.match(sql, /^UPDATE/);
    const changed = [];
    for (const key of p[2]) {
      const row = rows.get(prefix + key);
      if (row) { row.state = 'handled'; row.candidates = []; changed.push({ query_key: key }); }
    }
    return { rows: changed };
  } };
}
function session(db, tenantId = '13', fingerprint = 'approved-v1') {
  return createCheckpointSession({ store: createCheckpointStore({ db, tenantId, fingerprint }) });
}
const request = { tenantId: '13', scope: 'market', segment: 'small_business_owners', evidenceType: 'identity',
  discoveryConcept: 'Cleaning Company', geography: { cities: ['Manchester'], state: 'NH' } };
function fixture(s, status = 'OK') {
  let calls = 0;
  const provider = createPlacesProvider({ queryCheckpointSession: s, apiKey: 'synthetic-key', fetchImpl: async url => {
    calls++;
    const data = String(url).includes('details')
      ? { status, result: { place_id: 'p1', name: 'Fixture Cleaning', website: 'https://fixture.test', formatted_address: 'Manchester NH' } }
      : { status, results: status === 'OK' ? [{ place_id: 'p1', name: 'Fixture Cleaning' }] : [] };
    return { status: 200, ok: true, json: async () => data };
  } });
  return { provider, calls: () => calls };
}
beforeEach(() => { denyBudget = false; cache.clearPlacesQueryCache(); });

test('query identity ignores case/spacing but preserves geography and output contract', () => {
  assert.equal(queryIdentity('Cleaning   Manchester NH', false), queryIdentity('cleaning manchester nh', false));
  assert.notEqual(queryIdentity('Cleaning Manchester NH', false), queryIdentity('Cleaning Manchester TN', false));
  assert.notEqual(queryIdentity('Cleaning Manchester NH', false), queryIdentity('Cleaning Manchester NH', true));
});

test('fetch persists replay unit; restart replays candidates and only durable ack skips next run', async () => {
  const db = database(); const a = session(db); const one = fixture(a);
  const hits = await one.provider.collectEvidence(request);
  assert.equal(hits.length, 1); assert.equal(one.calls(), 2);
  assert.equal(a.snapshot().status, 'pending_query_work');
  cache.clearPlacesQueryCache(); // Simulate process restart, including lost HTTP cache.
  const b = session(db); const two = fixture(b);
  assert.deepEqual(await two.provider.collectEvidence(request), hits); assert.equal(two.calls(), 0);
  assert.deepEqual(await two.provider.collectEvidence(request), hits, 'same-run replay retains candidates');
  assert.equal(b.snapshot().replayedQueries, 1, 'counts are distinct queries');
  await b.acknowledge();
  assert.equal(b.snapshot().status, 'observed_query_set_handled');
  const c = session(db); const three = fixture(c);
  assert.deepEqual(await three.provider.collectEvidence(request), []); assert.equal(three.calls(), 0);
  assert.equal(three.provider.lastExecution.abortReason, 'approved_query_set_handled');
  assert.equal(c.snapshot().handledQueries, 1);
});

test('remaining approved city actually executes while handled city skips', async () => {
  const db = database(); const a = session(db); const first = fixture(a, 'ZERO_RESULTS');
  await first.provider.collectEvidence(request); await a.acknowledge(); cache.clearPlacesQueryCache();
  const b = session(db); const second = fixture(b, 'ZERO_RESULTS');
  await second.provider.collectEvidence({ ...request, geography: { cities: ['Manchester', 'Bedford'], state: 'NH' } });
  assert.equal(second.calls(), 1); assert.equal(b.snapshot().handledQueries, 1);
  assert.equal(b.snapshot().pendingAcknowledgements, 1); assert.equal(b.snapshot().status, 'pending_query_work');
});

for (const status of ['REQUEST_DENIED', 'OVER_QUERY_LIMIT']) test(`${status} never stages or claims exhaustion`, async () => {
  const db = database(); const s = session(db); const f = fixture(s, status);
  await f.provider.collectEvidence(request);
  assert.equal(db.rows.size, 0); assert.equal(s.snapshot().status, 'pending_query_work');
  assert.equal(s.snapshot().unresolvedQueries, 1);
});

test('budget and missing schema fail before network and remain unfinished', async () => {
  const db = database(); const s = session(db); const f = fixture(s); denyBudget = true;
  await f.provider.collectEvidence(request);
  assert.equal(f.calls(), 0); assert.equal(db.rows.size, 0); assert.equal(s.snapshot().status, 'pending_query_work');
  const broken = session({ query: async () => { throw Error('missing relation'); } }); const g = fixture(broken);
  await assert.rejects(g.provider.collectEvidence(request), /missing relation/); assert.equal(g.calls(), 0);
  assert.equal(broken.snapshot().status, 'pending_query_work');
  assert.equal(broken.snapshot().handledQueries, 0);
});

test('tenant and mission are isolated; cross-tenant request is rejected before network', async () => {
  const db = database(); const a = session(db); const f = fixture(a, 'ZERO_RESULTS');
  await f.provider.collectEvidence(request); await a.acknowledge();
  const b = session(db, '17'); const g = fixture(b);
  await assert.rejects(g.provider.collectEvidence(request), /tenant mismatch/); assert.equal(g.calls(), 0);
  assert.equal((await b.lookup('Cleaning Company Manchester NH', false)).candidates, null);
  assert.equal((await session(db, '13', 'new-mission').lookup('Cleaning Company Manchester NH', false)).candidates, null);
});

test('acknowledgement failure leaves session pending; later retry is safe', async () => {
  const db = database(); const s = session(db); const f = fixture(s, 'ZERO_RESULTS');
  await f.provider.collectEvidence(request);
  const original = db.query; db.query = async (sql, p) => sql.startsWith('UPDATE') ? { rows: [] } : original(sql, p);
  await assert.rejects(s.acknowledge(), /not confirmed/); assert.equal(s.snapshot().pendingAcknowledgements, 1);
  db.query = original; await s.acknowledge(); assert.equal(s.snapshot().handledQueries, 1);
});

const searchDefinition = { tenantId: '13', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
  segments: ['property_management'], businessNeed: 'commercial_cleaning' };
for (const fail of [true, false]) test(`CandidateUniverse empty ingestion ${fail ? 'failure preserves replay' : 'success acknowledges after persistence'}`, async () => {
  const db = database(); const s = session(db); const f = fixture(s, 'ZERO_RESULTS');
  let persisted = 0; let acknowledged = 0;
  await constructCandidateUniverse({ searchDefinition, forceDiscover: true,
    existing: { companies: [], people: [] }, adapters: [createPlacesDiscoveryAdapter({ placesProvider: f.provider })],
    persistCompanies: async ({ companies }) => { persisted++; assert.equal(companies.length, 0); if (fail) throw Error('interrupted ingestion'); return { checkpointComplete: true }; },
    acknowledgeDiscovery: async () => { assert.equal(persisted, 1); acknowledged++; await s.acknowledge(); },
  });
  assert.equal(persisted, 1); assert.equal(acknowledged, fail ? 0 : 1);
  assert.ok(s.snapshot().observedQueries > 0);
  assert.equal(s.snapshot().status, fail ? 'pending_query_work' : 'observed_query_set_handled');
});

test('overlapping query payloads stay self-contained without repeat detail calls', async () => {
  const db = database(); const s = session(db); const f = fixture(s);
  await f.provider.collectEvidence({ ...request, geography: { cities: ['Manchester', 'Bedford'], state: 'NH' } });
  assert.equal(f.calls(), 3, 'two searches and one detail request');
  assert.equal(db.rows.size, 2);
  assert.ok([...db.rows.values()].every(row => row.candidates.length === 1));
  cache.clearPlacesQueryCache();
  const restarted = session(db); const next = fixture(restarted);
  const candidates = await next.provider.collectEvidence({ ...request, geography: { cities: ['Bedford'], state: 'NH' } });
  assert.equal(candidates.length, 1); assert.equal(next.calls(), 0);
});

test('positive candidate ingestion failure preserves fetched payload for recovery', async () => {
  const db = database(); const s = session(db); const f = fixture(s);
  let persisted = 0; let acknowledged = 0;
  const args = { searchDefinition, forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [createPlacesDiscoveryAdapter({ placesProvider: f.provider })],
    persistCompanies: async ({ companies }) => { persisted++; assert.ok(companies.length); throw Error('partial SQL interruption'); },
    acknowledgeDiscovery: async () => { acknowledged++; await s.acknowledge(); },
  };
  await constructCandidateUniverse(args);
  assert.equal(persisted, 1); assert.equal(acknowledged, 0);
  assert.ok([...db.rows.values()].every(row => row.state === 'fetched'));
});

test('discovery denial prevents acknowledgement even if ingestion resolves', async () => {
  const db = database(); const s = session(db); const f = fixture(s, 'REQUEST_DENIED');
  let acknowledged = 0;
  await constructCandidateUniverse({ searchDefinition, forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [createPlacesDiscoveryAdapter({ placesProvider: f.provider })],
    persistCompanies: async () => {}, acknowledgeDiscovery: async () => { acknowledged++; },
  });
  assert.equal(acknowledged, 0); assert.equal(s.snapshot().status, 'pending_query_work');
});

test('real acquisition facade carries provider and ingestion acknowledgement dependencies', async () => {
  const { runAcquisitionIntelligenceLoop, createMemoryAcquisitionState } = require('../services/scoutAcquisitionIntelligence');
  const { createMemoryStore, createSpecialistDelegationService } = require('../services/specialistDelegation');
  const db = database(); const s = session(db); const f = fixture(s, 'ZERO_RESULTS');
  let persisted = 0; let acknowledged = 0;
  const result = await runAcquisitionIntelligenceLoop({
    authorizedTenantId: '13', tenantId: '13', authority: 'observe', force: true,
    workflow: 'outbound_inventory_replenishment', inventoryDeficit: 1,
    question: 'Find property management companies in Manchester NH.',
    targetContext: { geography: 'Manchester NH', segments: ['property_management'], businessType: 'commercial_cleaning' },
    businessContext: { serviceGeography: 'Manchester NH', preferredSegments: ['property_management'], commercialCapability: 'commercial_cleaning' },
  }, {
    delegationService: createSpecialistDelegationService({ store: createMemoryStore() }),
    aoStore: createMemoryAcquisitionState(), companies: [], people: [], enablePlaces: true, placesProvider: f.provider,
    persistCompanies: async () => { persisted++; return { checkpointComplete: true }; },
    acknowledgeDiscovery: async () => { assert.ok(persisted); acknowledged++; await s.acknowledge(); },
  });
  assert.equal(result.delegated, true); assert.ok(f.calls() > 0);
  assert.equal(persisted, 1); assert.equal(acknowledged, 1);
  assert.equal(s.snapshot().status, 'observed_query_set_handled');
});

for (const outcome of [undefined, { checkpointComplete: false }]) test(`resolved ingestion without explicit completion cannot acknowledge (${JSON.stringify(outcome)})`, async () => {
  const db = database(); const s = session(db); const f = fixture(s, 'ZERO_RESULTS');
  let acknowledged = 0;
  await constructCandidateUniverse({ searchDefinition, forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [createPlacesDiscoveryAdapter({ placesProvider: f.provider })],
    persistCompanies: async () => outcome,
    acknowledgeDiscovery: async () => { acknowledged++; await s.acknowledge(); },
  });
  assert.equal(acknowledged, 0); assert.equal(s.snapshot().status, 'pending_query_work');
});
