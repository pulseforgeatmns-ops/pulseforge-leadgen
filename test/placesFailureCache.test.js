'use strict';

const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
// The real provider/client/cache run against only synthetic HTTP and a fake DB.
// No production pool is constructed and no provider or database is contacted.
const rows = [];
const fakeDb = { async query(sql, values) {
  if (/INSERT INTO places_api_requests/.test(sql)) rows.push({
    tenant: values[4], endpoint: values[8], httpStatus: values[13], googleStatus: values[14],
    cacheHit: values[21], cacheMiss: values[22], cacheAge: values[23],
    businessesReturned: values[28], businessesAccepted: values[29], candidatesCreated: values[30],
    serialized: JSON.stringify(values),
  });
  else assert.match(sql, /CREATE TABLE|CREATE INDEX|ALTER TABLE/);
  return { rows: [] };
} };
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: fakeDb };
const { legacyTextSearch, legacyPlaceDetails } = require('../utils/placesApi');
const { withPlacesContext } = require('../utils/placesCostAttribution');
const cache = require('../utils/placesQueryCache');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const SECRET = 'synthetic-secret-never-log';
const RAW = 'synthetic-raw-provider-payload-never-log';
function response(status, extra = {}) {
  return { status: 200, ok: true, json: async () => ({ status, ...extra }) };
}
function denied() {
  return response('REQUEST_DENIED', { error_message: `${SECRET} ${RAW}`,
    results: [{ name: RAW }], result: { name: RAW, website: RAW } });
}
function key(query) { return cache.buildCacheKey({ endpoint: 'text_search', normalizedQuery: query }); }
function assertSafe(value) {
  const text = JSON.stringify(value);
  assert.ok(!text.includes(SECRET)); assert.ok(!text.includes(RAW));
  assert.ok(!text.includes('error_message'));
}
beforeEach(() => { rows.length = 0; cache.clearPlacesQueryCache(); });

test('fresh HTTP200 denial records once despite deferred yield, and repeats remain failure-held', async () => {
  let fetches = 0;
  const params = { query: 'Synthetic denial company', apiKey: SECRET, deferRecord: true,
    yield: { businessesReturned: 99, candidatesCreated: 99 },
    fetchImpl: async () => { fetches++; return denied(); } };
  for (let i = 0; i < 2; i++) {
    const result = await legacyTextSearch(params, { db: fakeDb });
    assert.equal(result.ok, false); assert.equal(result.applicationFailure, true);
    assert.deepEqual(result.data, { status: 'REQUEST_DENIED' });
    assert.equal(result.commitRecord, undefined); assertSafe(result);
  }
  assert.equal(fetches, 1); assert.equal(rows.length, 2);
  assert.equal(rows[0].cacheMiss, true); assert.equal(rows[1].cacheHit, true);
  for (const row of rows) {
    assert.equal(row.googleStatus, 'REQUEST_DENIED'); assert.equal(row.businessesReturned, 0);
    assert.equal(row.businessesAccepted, 0); assert.equal(row.candidatesCreated, 0); assertSafe(row);
  }
  const held = cache.lookup(key(params.query));
  assert.equal(held.hit, true); assert.equal(held.payload, undefined);
  assert.deepEqual(held.failure, { httpStatus: 200, googleStatus: 'REQUEST_DENIED' }); assertSafe(held);
});

test('legacy detail denial is also safe, recorded once and held without another request', async () => {
  let fetches = 0;
  const params = { placeId: 'synthetic-place', fields: 'website,name', apiKey: SECRET,
    deferRecord: true, fetchImpl: async () => { fetches++; return denied(); } };
  for (let i = 0; i < 2; i++) {
    const result = await legacyPlaceDetails(params, { db: fakeDb });
    assert.equal(result.ok, false); assert.equal(result.data.result, undefined);
    assert.equal(result.commitRecord, undefined); assertSafe(result);
  }
  assert.equal(fetches, 1); assert.equal(rows.length, 2);
  assert.ok(rows.every(row => row.endpoint === 'place_details' && row.businessesReturned === 0));
  assertSafe(rows);
});

test('old poisoned result is quarantined without refetch, TTL extension or raw payload retention', async () => {
  const originalNow = Date.now; let now = 100000;
  Date.now = () => now;
  try {
    const query = 'Synthetic legacy poison';
    cache.storeResult(key(query), { httpStatus: 200, googleStatus: 'REQUEST_DENIED', ok: true,
      data: { status: 'REQUEST_DENIED', error_message: SECRET, results: [{ name: RAW }] } }, 'text_search');
    now += 500;
    const result = await legacyTextSearch({ query, fetchImpl: async () => { throw new Error('must not refetch'); } }, { db: fakeDb });
    assert.equal(result.ok, false); assertSafe(result);
    const held = cache.lookup(key(query));
    assert.equal(held.age, 500); assert.equal(held.payload, undefined); assertSafe(held);
    now += 250;
    await legacyTextSearch({ query, fetchImpl: async () => { throw new Error('must not refetch'); } }, { db: fakeDb });
    assert.equal(cache.lookup(key(query)).age, 750);
    now = 100000 + cache.DEFAULT_TTL_MS;
    assert.equal(cache.lookup(key(query)).hit, true);
    now++;
    assert.equal(cache.lookup(key(query)).expired, true);
  } finally { Date.now = originalNow; }
});

for (const status of ['OK', 'ZERO_RESULTS']) test(`${status} keeps ordinary caching and deferred success attribution`, async () => {
  let fetches = 0;
  const params = { query: `Synthetic ${status}`, deferRecord: true, fetchImpl: async () => {
    fetches++; return response(status, { results: status === 'OK' ? [{ name: 'Synthetic valid business' }] : [] });
  } };
  const result = await legacyTextSearch(params, { db: fakeDb });
  assert.equal(result.ok, true); assert.equal(rows.length, 0);
  await result.commitRecord({ businessesAccepted: status === 'OK' ? 1 : 0 });
  const cached = await legacyTextSearch(params, { db: fakeDb });
  assert.equal(cached.ok, true); assert.deepEqual(cached.data, result.data);
  assert.equal(fetches, 1); assert.equal(rows.length, 2); assert.equal(rows[1].cacheHit, true);
});

for (const status of ['OVER_QUERY_LIMIT', 'INVALID_REQUEST', 'UNKNOWN_ERROR', 'untrusted ' + SECRET, null]) {
  test(`non-success application status ${String(status).split(' ')[0]} cannot become business evidence`, async () => {
    let fetches = 0;
    const params = { query: 'Synthetic other failure', deferRecord: true,
      fetchImpl: async () => { fetches++; return response(status, { error_message: SECRET, results: [{ name: RAW }] }); } };
    const one = await legacyTextSearch(params, { db: fakeDb });
    const two = await legacyTextSearch(params, { db: fakeDb });
    assert.equal(one.ok, false); assert.equal(two.ok, false); assert.equal(fetches, 1);
    assert.equal(rows.length, 2); assertSafe(one); assertSafe(two); assertSafe(rows);
    if (!status || status.startsWith('untrusted')) assert.equal(one.googleStatus, 'UNKNOWN_STATUS');
  });
}

test('HTTP failures keep the prior uncached request behavior', async () => {
  let fetches = 0;
  for (let i = 0; i < 2; i++) await legacyTextSearch({ query: 'Synthetic HTTP failure',
    fetchImpl: async () => { fetches++; return { status: 403, ok: false, json: async () => ({ status: 'REQUEST_DENIED' }) }; } }, { db: fakeDb });
  assert.equal(fetches, 2); assert.equal(cache.cacheStats().entries, 0); assert.equal(rows.length, 2);
});

test('explicit useCache=false is honored without creating a failure hold', async () => {
  let fetches = 0;
  for (let i = 0; i < 2; i++) await legacyTextSearch({ query: 'Synthetic bypass', useCache: false,
    fetchImpl: async () => { fetches++; return denied(); } }, { db: fakeDb });
  assert.equal(fetches, 2); assert.equal(cache.cacheStats().entries, 0); assert.equal(rows.length, 2);
});

test('explicit uncached diagnostics keep response inspection but never persist raw failure details', async () => {
  const result = await legacyTextSearch({ query: 'Synthetic explicit diagnostic', useCache: false,
    fetchImpl: async () => denied() }, { db: fakeDb });
  assert.equal(result.ok, false); assert.equal(result.data.error_message, `${SECRET} ${RAW}`);
  assert.equal(cache.cacheStats().entries, 0); assert.equal(rows.length, 1); assertSafe(rows);
});

test('actual provider exposes a safe denied status, records fresh failure and never retries denial', async () => {
  let fetches = 0;
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async () => { fetches++; return denied(); } });
  for (let i = 0; i < 2; i++) {
    assert.deepEqual(await provider.search({ industry: 'Synthetic service', location: 'Synthetic city' }), []);
    assert.equal(provider.lastExecution.errors[0].code, 'google_places_status_REQUEST_DENIED');
    assert.equal(provider.lastExecution.totals.retries, 0); assertSafe(provider.lastExecution);
  }
  assert.equal(fetches, 1); assert.equal(rows.length, 2); assertSafe(rows);
});

test('retryable quota status retains existing two-retry ceiling without extra upstream calls', async () => {
  let fetches = 0;
  const provider = createPlacesProvider({ apiKey: 'synthetic', fetchImpl: async () => { fetches++; return response('OVER_QUERY_LIMIT'); } });
  assert.deepEqual(await provider.search({ industry: 'Synthetic quota', location: 'Synthetic city' }), []);
  assert.equal(provider.lastExecution.totals.retries, 2); assert.equal(provider.lastExecution.queries.length, 3);
  assert.equal(fetches, 1); assert.equal(rows.length, 3);
  assert.equal(provider.lastExecution.errors[0].code, 'google_places_status_OVER_QUERY_LIMIT');
});

test('public sourcing adapter preserves the safe denial reason for fresh and cached failures', async () => {
  const { createScoutPlacesSearchProvider } = require('../services/scoutPublicSourcing');
  let fetches = 0;
  const provider = createScoutPlacesSearchProvider({ apiKey: SECRET,
    fetchImpl: async () => { fetches++; return denied(); } });
  for (let i = 0; i < 2; i++) await assert.rejects(
    () => provider.search({ query: 'Synthetic public source', limit: 100 }),
    error => { assert.equal(error.message, 'google_places_status_REQUEST_DENIED'); assertSafe(error.message); return true; }
  );
  assert.equal(fetches, 1); assert.equal(rows.length, 2); assertSafe(rows);
});

test('public sourcing fails closed with no candidates or downstream work after denial', async () => {
  const { createScoutPlacesSearchProvider, sourceScoutCandidatesFromPublicSources } = require('../services/scoutPublicSourcing');
  let fetches = 0;
  const searchProvider = createScoutPlacesSearchProvider({ apiKey: SECRET,
    fetchImpl: async () => { fetches++; return denied(); } });
  const result = await sourceScoutCandidatesFromPublicSources({
    workRequest: { clientId: 13, targetSegment: 'Property managers', marketBounds: 'Manchester NH' },
    opts: { searchProvider },
  });
  assert.equal(result.ok, false); assert.equal(result.error, 'google_places_status_REQUEST_DENIED');
  assert.deepEqual(result.candidates, []); assert.equal(result.crmWritesMade, false);
  assert.equal(result.outreachCopyGenerated, false); assert.equal(result.accountChangesMade, false);
  assert.equal(fetches, 1); assert.equal(rows.length, 1); assert.equal(rows[0].tenant, 13);
  assertSafe(result); assertSafe(rows);
});

test('denied cache receipts retain each invoking tenant context without adding provider calls', async () => {
  let fetches = 0;
  for (const tenantId of ['13', '10', '17', '1']) await withPlacesContext({ tenantId }, () => legacyTextSearch({
    query: 'Synthetic shared failure', fetchImpl: async () => { fetches++; return denied(); },
  }, { db: fakeDb }));
  assert.deepEqual(rows.map(row => row.tenant), [13, 10, 17, 1]); assert.equal(fetches, 1);
  assertSafe(rows);
});

test('provider result limit remains twenty and successful details stay available', async () => {
  let fetches = 0;
  const provider = createPlacesProvider({ apiKey: 'synthetic', fetchImpl: async url => {
    fetches++;
    if (new URL(url).pathname.includes('/details/')) return response('OK', {
      result: { name: 'Synthetic company', website: 'https://synthetic.example', place_id: new URL(url).searchParams.get('place_id') },
    });
    return response('OK', { results: Array.from({ length: 25 }, (_, i) => ({ name: `Synthetic ${i}`, place_id: `synthetic-${i}` })) });
  } });
  const businesses = await provider.search({ industry: 'Synthetic limit', location: 'Synthetic city', limit: 100 });
  assert.equal(businesses.length, 20); assert.equal(fetches, 21); assert.equal(rows.length, 21);
});
