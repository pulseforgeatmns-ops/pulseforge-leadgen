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
// Provider adapters use the default spend-control dependency. Keep their budget
// reservations synthetic too; the real budget contract has its own test suite.
const spendPath = require.resolve('../utils/apiSpendControl');
const reservations = [];
const settlements = [];
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true, exports: {
  reserveApiCall: async request => { reservations.push(request); return `synthetic-${reservations.length}`; },
  settleApiCall: async (id, result) => { settlements.push({ id, result }); },
} };
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
function key(query, apiKey) { return cache.buildCacheKey({ endpoint: 'text_search', query, apiKey }); }
function assertSafe(value) {
  const text = JSON.stringify(value);
  assert.ok(!text.includes(SECRET)); assert.ok(!text.includes(RAW));
  assert.ok(!text.includes('error_message'));
}
beforeEach(() => { rows.length = 0; reservations.length = 0; settlements.length = 0; cache.clearPlacesQueryCache(); });

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
  assert.equal(reservations.length, 1); assert.equal(settlements.length, 1);
  const held = cache.lookup(key(params.query, params.apiKey));
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

for (const endpoint of ['search', 'details']) test(`overlapping ${endpoint} denials preserve one reservation and safe per-tenant reuse receipts`, async () => {
  let fetches = 0;
  const params = { query: 'Synthetic concurrent denial', placeId: 'synthetic-shared-place',
    fields: 'name,website', apiKey: SECRET, deferRecord: true,
    yield: { businessesReturned: 99, candidatesCreated: 99 },
    fetchImpl: async () => { fetches++; return denied(); } };
  const request = endpoint === 'search' ? legacyTextSearch : legacyPlaceDetails;
  const results = await Promise.all([13, 10, 17, 1].map(tenantId =>
    withPlacesContext({ tenantId }, () => request(params, { db: fakeDb }))));
  assert.equal(fetches, 1); assert.equal(reservations.length, 1); assert.equal(settlements.length, 1);
  assert.equal(rows.length, 4); assert.equal(new Set(rows.map(row => row.tenant)).size, 4);
  assert.equal(rows.filter(row => row.cacheHit).length, 3);
  assert.equal(rows.filter(row => row.cacheMiss).length, 1);
  for (const result of results) {
    assert.equal(result.ok, false); assert.equal(result.applicationFailure, true);
    assert.deepEqual(result.data, { status: 'REQUEST_DENIED' });
    assert.equal(result.commitRecord, undefined); assertSafe(result);
  }
  assert.ok(rows.every(row => row.businessesReturned === 0 && row.candidatesCreated === 0));
  assertSafe(rows);
  await request(params, { db: fakeDb });
  assert.equal(fetches, 1); assert.equal(reservations.length, 1);
  assert.equal(rows.length, 5); assert.equal(rows[4].cacheHit, true);
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

test('detail denial is visible as provider failure rather than an empty successful search', async () => {
  let fetches = 0;
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url => {
    fetches++;
    if (String(url).includes('/details/')) return denied();
    return response('OK', { results: [{ place_id: 'synthetic-detail-denial', name: 'Synthetic business' }] });
  } });
  for (let i = 0; i < 2; i++) {
    assert.deepEqual(await provider.search({ industry: 'Synthetic details', location: 'Synthetic city' }), []);
    assert.equal(provider.lastExecution.errors[0]?.code, 'google_places_status_REQUEST_DENIED');
    assert.equal(provider.lastExecution.errors[0]?.endpoint, 'place_details');
    assert.equal(provider.lastExecution.totals.retries, 0);
    assertSafe(provider.lastExecution);
  }
  assert.equal(fetches, 2, 'both search and denied details must reuse their cache');
});

test('successful details without website remain a normal rejected hit, not a provider failure', async () => {
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url =>
    String(url).includes('/details/')
      ? response('OK', { result: { name: 'Synthetic business', formatted_phone_number: 'synthetic-phone' } })
      : response('OK', { results: [{ place_id: 'synthetic-no-site', name: 'Synthetic business' }] })
  });
  assert.deepEqual(await provider.search({ industry: 'Synthetic no website', location: 'Synthetic city' }), []);
  assert.deepEqual(provider.lastExecution.errors, []);
});

test('partial details failures preserve successful website and business phone without inventing a person', async () => {
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url => {
    const u = new URL(url);
    if (!u.pathname.includes('/details/')) return response('OK', { results: [
      { place_id: 'synthetic-good', name: 'Synthetic good' }, { place_id: 'synthetic-bad', name: 'Synthetic bad' },
    ] });
    return u.searchParams.get('place_id') === 'synthetic-bad' ? denied()
      : response('OK', { result: { name: 'Synthetic good', website: 'https://example.test', formatted_phone_number: 'synthetic-phone' } });
  } });
  const hits = await provider.search({ industry: 'Synthetic mixed', location: 'Synthetic city' });
  assert.equal(hits.length, 1);
  assert.equal(hits[0].website, 'example.test');
  assert.equal(hits[0].phone, 'synthetic-phone');
  assert.equal(hits[0].email, undefined);
  assert.equal(hits[0].people, undefined);
  assert.equal(provider.lastExecution.errors[0].endpoint, 'place_details');
  assertSafe(provider.lastExecution);
});

test('detail authorization denial stops remaining details, pages, and generated queries in this invocation', async () => {
  const calls = [];
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url => {
    calls.push(String(url));
    if (String(url).includes('/details/')) return denied();
    return response('OK', { results: [{ place_id: 'first' }, { place_id: 'must-not-fetch' }], next_page_token: 'must-not-page' });
  } });
  const evidenceRequest = { segment: 'property_management', evidenceType: 'contact_path',
    geography: { cities: ['Manchester', 'Concord'], state: 'NH' } };
  for (let i = 0; i < 2; i++) {
    assert.deepEqual(await provider.collectEvidence(evidenceRequest), []);
    assert.equal(provider.lastExecution.queries.length, 1);
    assert.equal(provider.lastExecution.errors[0].endpoint, 'place_details');
  }
  assert.equal(calls.length, 2, 'one search + one details call; cached rerun makes no upstream requests');
});

test('Scout adapter and evidence contract expose detail denial as failed, not successful empty discovery', async () => {
  const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
  const { normalizeProviderReport } = require('../packages/scout/coverage/ProviderEvidenceContract');
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url =>
    String(url).includes('/details/') ? denied()
      : response('OK', { results: [{ place_id: 'synthetic-propagation', name: 'Synthetic business' }] }) });
  const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });
  const report = await adapter.discover({ tenantId: '13',
    geography: { cities: ['Manchester'], state: 'NH', label: 'Manchester NH' },
    segments: ['property_management'],
    evidenceRequest: { segment: 'property_management', evidenceType: 'contact_path',
      geography: { cities: ['Manchester'], state: 'NH' } } });
  const normalized = normalizeProviderReport(report, { providerId: 'google_maps', evidenceType: 'contact_path' });
  assert.equal(normalized.status, 'failed');
  assert.equal(normalized.error, 'REQUEST_DENIED');
  assert.deepEqual(normalized.evidenceProduced, []);
  assert.equal(normalized.mappedCandidateCount, 0);
  assert.equal(normalized.rawResultCount, 1);
  assert.equal(normalized.execution.errors[0].endpoint, 'place_details');
  assertSafe(normalized);
});

test('legacy Scout discovery stops segments on detail denial and exposes safe execution failure', async () => {
  const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
  const { normalizeProviderReport } = require('../packages/scout/coverage/ProviderEvidenceContract');
  let fetches = 0;
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url => {
    fetches++;
    return String(url).includes('/details/') ? denied()
      : response('OK', { results: [{ place_id: 'legacy-denial', name: 'Synthetic business' }] });
  } });
  const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });
  for (let attempt = 0; attempt < 2; attempt++) {
    const report = await adapter.discover({ tenantId: '13', geography: { label: 'Synthetic city' },
      segments: ['first_segment', 'second_segment'] });
    const normalized = normalizeProviderReport(report, { providerId: 'google_maps', evidenceType: 'contact_path' });
    assert.equal(report.coverage.queries, 1);
    assert.equal(normalized.status, 'failed');
    assert.equal(normalized.error, 'REQUEST_DENIED');
    assert.equal(normalized.rawResultCount, 1);
    assert.equal(normalized.execution.errors[0].endpoint, 'place_details');
    assert.deepEqual(normalized.candidates, []);
    assertSafe(report); assertSafe(normalized);
  }
  assert.equal(fetches, 2, 'only first search and first detail; repeat is failure-held');
});

test('legacy Scout discovery preserves earlier valid candidates and aggregates execution before denial', async () => {
  const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
  const { normalizeProviderReport } = require('../packages/scout/coverage/ProviderEvidenceContract');
  const calls = [];
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async url => {
    const u = new URL(url); calls.push(u.pathname);
    if (!u.pathname.includes('/details/')) {
      const id = u.searchParams.get('query').includes('first segment') ? 'legacy-good' : 'legacy-bad';
      return response('OK', { results: [{ place_id: id, name: 'Synthetic business' }] });
    }
    return u.searchParams.get('place_id') === 'legacy-good'
      ? response('OK', { result: { name: 'Synthetic good', website: 'https://example.test', formatted_phone_number: 'synthetic-phone' } })
      : denied();
  } });
  const report = await createPlacesDiscoveryAdapter({ placesProvider: provider }).discover({
    tenantId: '13', geography: { label: 'Synthetic city' }, segments: ['first_segment', 'second_segment', 'third_segment'] });
  assert.equal(calls.length, 4);
  assert.equal(report.coverage.queries, 2);
  assert.equal(report.execution.queries.length, 2);
  assert.equal(report.execution.totals.results, 2);
  assert.equal(report.execution.totals.retries, 0);
  assert.equal(report.candidates.length, 1);
  assert.equal(report.candidates[0].website, 'example.test');
  assert.equal(report.candidates[0].phone, 'synthetic-phone');
  const normalized = normalizeProviderReport(report, { providerId: 'google_maps', evidenceType: 'contact_path' });
  assert.equal(normalized.status, 'failed');
  assert.equal(normalized.mappedCandidateCount, 1);
  assert.equal(normalized.rawResultCount, 2);
  assertSafe(report); assertSafe(normalized);
});

test('legacy Scout transport failure exposes only fixed diagnostics, never thrown provider secrets', async () => {
  const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
  const { normalizeProviderReport } = require('../packages/scout/coverage/ProviderEvidenceContract');
  const privateText = `${SECRET} ${RAW} person@example.test https://provider.test/?key=${SECRET}`;
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async () => { throw new Error(privateText); } });
  const report = await createPlacesDiscoveryAdapter({ placesProvider: provider }).discover({
    tenantId: '13', geography: { label: 'Synthetic city' }, segments: ['first_segment'] });
  const normalized = normalizeProviderReport(report, { providerId: 'google_maps', evidenceType: 'contact_path' });
  assert.equal(normalized.status, 'failed');
  assert.equal(normalized.error, 'google_places_transport_failed');
  assert.equal(report.execution.queries[0].googleError, 'google_places_transport_failed');
  assertSafe(report); assertSafe(normalized);
  assert.ok(!JSON.stringify([report, normalized]).includes('person@example.test'));
  assert.ok(!JSON.stringify([report, normalized]).includes('provider.test'));
});

for (const status of ['OK', 'ZERO_RESULTS']) test(`legacy Scout ${status} ignores arbitrary error_message metadata`, async () => {
  const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
  const { normalizeProviderReport } = require('../packages/scout/coverage/ProviderEvidenceContract');
  const provider = createPlacesProvider({ apiKey: SECRET, fetchImpl: async () => response(status, {
    results: [], error_message: `${SECRET} ${RAW} person@example.test https://provider.test/private`,
  }) });
  const report = await createPlacesDiscoveryAdapter({ placesProvider: provider }).discover({
    tenantId: '13', geography: { label: 'Synthetic city' }, segments: ['first_segment'] });
  const normalized = normalizeProviderReport(report, { providerId: 'google_maps', evidenceType: 'contact_path' });
  assert.equal(normalized.status, 'empty');
  assert.deepEqual(report.errors, []);
  assert.equal(report.execution.queries[0].googleError, null);
  assertSafe(report); assertSafe(normalized);
  assert.ok(!JSON.stringify([report, normalized]).includes('person@example.test'));
  assert.ok(!JSON.stringify([report, normalized]).includes('provider.test'));
});
