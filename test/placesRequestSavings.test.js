'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { legacyTextSearch, legacyPlaceDetails } = require('../utils/placesApi');
const { clearPlacesQueryCache, buildCacheKey } = require('../utils/placesQueryCache');

function telemetry() {
  const rows = [], reservations = [], settlements = [];
  const db = { async query(sql, params) {
    if (/INSERT INTO places_api_requests/.test(sql)) rows.push(params);
    return { rows: [] };
  } };
  return { rows, reservations, settlements, deps: {
    db,
    reserveApiCall: async request => { reservations.push(request); return `request-${reservations.length}`; },
    settleApiCall: async (id, result) => { settlements.push({ id, result }); },
  } };
}
const response = data => ({ status: 200, ok: true, json: async () => data });

describe('Places request savings', () => {
  beforeEach(clearPlacesQueryCache);

  it('twenty overlapping searches use one paid request and nineteen reuse records', async () => {
    const t = telemetry();
    let release, started, networkCalls = 0;
    const began = new Promise(resolve => { started = resolve; });
    const gate = new Promise(resolve => { release = resolve; });
    const fetchImpl = async () => { networkCalls++; started(); await gate;
      return response({ status: 'OK', results: [{ place_id: 'one' }] }); };
    const calls = Array.from({ length: 20 }, (_, i) => legacyTextSearch({
      query: 'warehouses Test City NH', apiKey: 'test-account', fetchImpl,
      record: { caller: `caller-${i}`, tenantId: i + 1 },
    }, t.deps));
    await began;
    release();
    const results = await Promise.all(calls);
    assert.equal(networkCalls, 1);
    assert.equal(t.reservations.length, 1);
    assert.equal(t.settlements.length, 1);
    assert.ok(results.every(r => r.data.results[0].place_id === 'one'));
    assert.equal(t.rows.filter(row => row[21] === false).length, 1);
    assert.equal(t.rows.filter(row => row[21] === true).length, 19);
    assert.equal(new Set(t.rows.map(row => row[4])).size, 20);
  });

  it('overlapping details share a request but different fields and accounts remain separate', async () => {
    const t = telemetry();
    let release, started, networkCalls = 0;
    const began = new Promise(resolve => { started = resolve; });
    const gate = new Promise(resolve => { release = resolve; });
    const fetchImpl = async () => { networkCalls++; started(); await gate;
      return response({ status: 'OK', result: { place_id: 'one', website: 'https://example.com' } }); };
    const common = { placeId: 'one', fields: 'website,formatted_phone_number', apiKey: 'test-account', fetchImpl };
    const calls = [legacyPlaceDetails(common, t.deps), legacyPlaceDetails({ ...common, fields: 'formatted_phone_number,website' }, t.deps),
      legacyPlaceDetails({ ...common, fields: 'website' }, t.deps),
      legacyPlaceDetails({ ...common, apiKey: 'different-test-account' }, t.deps)];
    await began;
    release();
    await Promise.all(calls);
    assert.equal(networkCalls, 3);
    assert.equal(t.reservations.length, 3);
    assert.equal(t.rows.filter(row => row[21] === false).length, 3);
    assert.equal(t.rows.filter(row => row[21] === true).length, 1);
    const key = buildCacheKey({ endpoint: 'text_search', query: 'test', apiKey: 'test-account', pageToken: 'private-page-token' });
    assert.ok(!key.includes('test-account') && !key.includes('private-page-token'));
  });

  it('pagination and state geography retain distinct responses', async () => {
    const t = telemetry();
    let networkCalls = 0;
    const fetchImpl = async url => { networkCalls++; const u = new URL(url);
      return response({ status: 'OK', results: [{ place_id: u.searchParams.get('pagetoken') || u.searchParams.get('query') }] }); };
    const common = { query: 'law firms Test City NH', apiKey: 'test-account', fetchImpl };
    const first = await legacyTextSearch(common, t.deps);
    const second = await legacyTextSearch({ ...common, pageToken: 'second-page' }, t.deps);
    const differentState = await legacyTextSearch({ ...common, query: 'law firms Test City MA' }, t.deps);
    assert.equal(first.data.results[0].place_id, common.query);
    assert.equal(second.data.results[0].place_id, 'second-page');
    assert.equal(differentState.data.results[0].place_id, 'law firms Test City MA');
    await legacyTextSearch({ ...common, pageToken: 'second-page' }, t.deps);
    assert.equal(networkCalls, 3);
    assert.equal(t.reservations.length, 3);
  });

  it('shared failures retain one uncertain reservation and a later request can retry', async () => {
    const t = telemetry();
    let networkCalls = 0;
    const fetchImpl = async () => { if (++networkCalls === 1) throw new Error('temporary transport failure');
      return response({ status: 'OK', results: [] }); };
    const args = { query: 'warehouses Test City NH', apiKey: 'test-account', fetchImpl };
    const results = await Promise.allSettled([legacyTextSearch(args, t.deps), legacyTextSearch(args, t.deps)]);
    assert.ok(results.every(r => r.status === 'rejected'));
    assert.equal(t.reservations.length, 1);
    assert.equal(t.settlements[0].result.status, 'uncertain');
    assert.equal(t.rows.filter(row => row[21] === false).length, 1);
    await legacyTextSearch(args, t.deps);
    assert.equal(networkCalls, 2);
    assert.equal(t.reservations.length, 2);
  });

  it('quota errors stay failure-held until TTL expiry, then permit a fresh request', async () => {
    const t = telemetry();
    let calls = 0;
    const fetchImpl = async () => response(++calls === 1 ? { status: 'OVER_QUERY_LIMIT' } : { status: 'OK', results: [{ place_id: 'recovered' }] });
    const args = { query: 'warehouses Test City NH', apiKey: 'test-account', fetchImpl };
    const originalNow = Date.now;
    let now = 100000;
    Date.now = () => now;
    try {
      await legacyTextSearch(args, t.deps);
      const held = await legacyTextSearch(args, t.deps);
      assert.equal(held.ok, false);
      assert.equal(held.applicationFailure, true);
      assert.deepEqual(held.data, { status: 'OVER_QUERY_LIMIT' });
      assert.equal(calls, 1);
      assert.equal(t.reservations.length, 1);
      now += require('../utils/placesQueryCache').DEFAULT_TTL_MS + 1;
      const result = await legacyTextSearch(args, t.deps);
      assert.equal(result.data.results[0].place_id, 'recovered');
      assert.equal(calls, 2);
      assert.equal(t.reservations.length, 2);
    } finally { Date.now = originalNow; }
  });

  it('explicit cache bypass keeps separate paid attempts', async () => {
    const t = telemetry();
    let calls = 0;
    const fetchImpl = async () => { calls++; return response({ status: 'OK', results: [] }); };
    const args = { query: 'warehouses Test City NH', apiKey: 'test-account', fetchImpl, useCache: false };
    await Promise.all([legacyTextSearch(args, t.deps), legacyTextSearch(args, t.deps)]);
    assert.equal(calls, 2);
    assert.equal(t.reservations.length, 2);
  });

  it('a denied shared reservation never reaches the network or creates paid attribution rows', async () => {
    const t = telemetry();
    let calls = 0, reservations = 0;
    t.deps.reserveApiCall = async () => { reservations++; throw Object.assign(new Error('daily request limit'), { code: 'api_daily_request_limit' }); };
    const args = { query: 'warehouses Test City NH', apiKey: 'test-account', fetchImpl: async () => { calls++; return response({ status: 'OK', results: [] }); } };
    const results = await Promise.allSettled([legacyTextSearch(args, t.deps), legacyTextSearch(args, t.deps)]);
    assert.ok(results.every(r => r.status === 'rejected' && r.reason.apiUsageNoNetwork === true));
    assert.equal(reservations, 1);
    assert.equal(calls, 0);
    assert.equal(t.rows.length, 0);
    assert.equal(t.settlements.length, 0);
  });
});

function providerWith(placesApi) {
  const filename = path.resolve(__dirname, '../packages/capabilities/discovery/providers/PlacesProvider.js');
  const mod = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { module: mod, exports: mod.exports, URL, process: { env: {} },
    require(specifier) {
      if (specifier === '../../../../utils/anchorResearchProviderPause') return require('../utils/anchorResearchProviderPause');
      if (specifier.endsWith('MarketHypothesisRegistry')) return { resolveMarketHypothesisBySegmentKey: () => null, expandSearchStrategies: () => [], SEARCH_SOURCES: {} };
      if (specifier.endsWith('EvidenceRequirements')) return { INVESTIGATIVE_EVIDENCE: { IDENTITY: 'identity' } };
      if (specifier.endsWith('placesCostAttribution')) return { PLACES_FEATURES: { DISCOVERY: 'discovery' } };
      if (specifier.endsWith('placesApi')) return placesApi;
      throw new Error(`Unexpected provider dependency: ${specifier}`);
    },
  }, { filename });
  return mod.exports.createPlacesProvider({ apiKey: 'test-account', fetchImpl: async () => {} });
}

describe('Discovery detail fields', () => {
  it('stops pagination once the requested eligible prospect count is satisfied', async () => {
    let searches = 0, details = 0;
    const provider = providerWith({
      legacyTextSearch: async () => { searches++; return { ok: true, httpStatus: 200, data: { status: 'OK', next_page_token: 'another-page', results: [{ place_id: 'one' }, { place_id: 'two' }] } }; },
      legacyPlaceDetails: async () => { details++; return { ok: true, data: { status: 'OK', result: { website: 'https://example.com' } } }; },
    });
    const result = await provider.search({ industry: 'warehouses', location: 'Test City NH', limit: 1 });
    assert.equal(result.length, 1);
    assert.equal(searches, 1);
    assert.equal(details, 1);
  });

  it('preserves the search rating, website, phone, address, and identity without requesting Atmosphere data', async () => {
    let detailFields;
    const hit = { place_id: 'one', name: 'Test Business', formatted_address: 'Test City NH', types: ['warehouse'], rating: 4.8 };
    const provider = providerWith({
      legacyTextSearch: async () => ({ ok: true, httpStatus: 200, data: { status: 'OK', results: [hit] } }),
      legacyPlaceDetails: async args => { detailFields = args.fields.split(','); return { ok: true, data: { status: 'OK', result: { website: 'https://example.com', formatted_phone_number: '555-0100' } } }; },
    });
    const results = await provider.search({ industry: 'warehouses', location: 'Test City NH' });
    assert.equal(results.length, 1);
    assert.equal(results[0].googleRating, 4.8);
    assert.equal(results[0].website, 'example.com');
    assert.equal(results[0].phone, '555-0100');
    assert.equal(results[0].address, 'Test City NH');
    assert.equal(results[0].placeId, 'one');
    assert.equal(results[0].companyName, 'Test Business');
    assert.ok(detailFields.includes('website') && detailFields.includes('formatted_phone_number'));
    assert.ok(!detailFields.includes('rating') && !detailFields.includes('reviews'));
  });

  it('looks up a rejected place only once across overlapping evidence searches', async () => {
    let details = 0;
    const provider = providerWith({
      legacyTextSearch: async () => ({ ok: true, httpStatus: 200, data: { status: 'OK', results: [{ place_id: 'no-website' }, {}] } }),
      legacyPlaceDetails: async () => { details++; return { ok: true, data: { status: 'OK', result: { name: 'Test Business' } } }; },
    });
    const result = await provider.collectEvidence({ segment: 'warehouse', evidenceType: 'website', geography: { cities: ['Test A', 'Test B'], state: 'NH' } });
    assert.equal(result.length, 0);
    assert.equal(details, 1);
  });
});
