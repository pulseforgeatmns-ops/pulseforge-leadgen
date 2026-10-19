'use strict';

// Independent SPEC-265 caller-to-leaf regressions. Actual provider module;
// synthetic HTTP boundaries only, no database, credentials, or external calls.
const { test, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const guard = require('../utils/anchorResearchProviderPause');
const flag = 'ANCHOR_RESEARCH_PROVIDERS_PAUSED';
const original = process.env[flag];
const sourceRoot = process.env.RESEARCH_PAUSE_SOURCE_ROOT || path.join(__dirname, '..');
afterEach(() => {
  if (original === undefined) delete process.env[flag];
  else process.env[flag] = original;
});

function fixture({ afterSearch, withHit = false, checkpoint = false } = {}) {
  const file = path.join(sourceRoot, 'packages/capabilities/discovery/providers/PlacesProvider.js');
  const nativeRequire = createRequire(file);
  const module = { exports: {} };
  const calls = { search: 0, details: 0, receipts: 0, staged: 0 };
  const traced = data => ({ ok: true, httpStatus: 200, googleStatus: data.status, data,
    commitRecord: async () => { calls.receipts++; } });
  const leaves = {
    legacyTextSearch: async () => {
      guard.assertResearchProviderAllowed('google_places');
      calls.search++;
      await afterSearch?.();
      return traced({ status: withHit ? 'OK' : 'ZERO_RESULTS', results: withHit
        ? [{ place_id: 'synthetic-place', name: 'Synthetic office' }] : [] });
    },
    legacyPlaceDetails: async () => {
      guard.assertResearchProviderAllowed('google_places');
      calls.details++;
      return traced({ status: 'OK', result: { name: 'Synthetic office', website: 'https://synthetic.test' } });
    },
  };
  vm.runInNewContext(fs.readFileSync(file, 'utf8'), {
    module, exports: module.exports, process, URL, Date, console,
    require(name) {
      if (name.endsWith('/anchorResearchProviderPause')) return guard;
      if (name.endsWith('/placesApi')) return leaves;
      if (name.endsWith('/placesCostAttribution')) return { PLACES_FEATURES: { DISCOVERY: 'Discovery' } };
      return nativeRequire(name);
    },
  }, { filename: file });
  const queryCheckpointSession = checkpoint ? {
    assertTenant() {},
    lookup: async () => ({ key: 'synthetic-query' }),
    stage: async () => { calls.staged++; return []; },
  } : undefined;
  const provider = module.exports.createPlacesProvider({ apiKey: 'synthetic',
    fetchImpl: () => assert.fail('Only synthetic traced leaves may run'), queryCheckpointSession });
  const request = tenantId => ({ tenantId, segment: 'property_management', evidenceType: 'identity',
    geography: { cities: ['Manchester'], state: 'NH' } });
  return { provider, calls, request };
}

test('actual canonical Places provider preserves explicit nonAnchor evidence and legacy search while armed', async () => {
  process.env[flag] = 'true';
  const f = fixture();
  assert.equal((await f.provider.collectEvidence(f.request('13'))).length, 0);
  assert.equal((await f.provider.search({ tenantId: '13', industry: 'synthetic business', location: 'Manchester NH' })).length, 0);
  assert.ok(f.calls.search >= 2);
  assert.equal(f.calls.details, 0);
  assert.equal(guard.hasResearchProviderScope(), false, 'provider scope must not leak to unrelated work');
});

test('actual canonical Places provider blocks Anchor and unknown research before traced leaf handoff', async () => {
  process.env[flag] = 'true';
  const f = fixture();
  await assert.rejects(f.provider.collectEvidence(f.request(10)), { code: 'anchor_research_provider_paused' });
  await assert.rejects(f.provider.search({ tenantId: 10, industry: 'synthetic', location: 'Manchester NH' }),
    { code: 'anchor_research_provider_paused' });
  await assert.rejects(f.provider.collectEvidence(f.request(undefined)), { code: 'research_provider_tenant_required' });
  assert.deepEqual(f.calls, { search: 0, details: 0, receipts: 0, staged: 0 });
});

test('canonical search-to-details interruption retains original tenant and never stages a false candidate outcome', async () => {
  delete process.env[flag];
  let request;
  const f = fixture({ checkpoint: true, withHit: true, afterSearch: async () => {
    request.tenantId = '13';
    process.env[flag] = 'true';
  } });
  request = f.request('10');
  await assert.rejects(f.provider.collectEvidence(request), { code: 'anchor_research_provider_paused' });
  assert.deepEqual(f.calls, { search: 1, details: 0, receipts: 0, staged: 0 });
  assert.equal(guard.hasResearchProviderScope(), false);
});

test('unarmed canonical provider retains contextless legacy calls and rejects conflicting bound authority when armed', async () => {
  delete process.env[flag];
  const f = fixture();
  await f.provider.collectEvidence(f.request(undefined));
  assert.ok(f.calls.search > 0);
  const before = f.calls.search;
  process.env[flag] = 'true';
  await assert.rejects(guard.withResearchProviderScope(13,
    () => f.provider.collectEvidence(f.request(10))), { code: 'research_provider_tenant_mismatch' });
  assert.equal(f.calls.search, before);
});
