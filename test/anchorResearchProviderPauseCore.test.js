'use strict';
const { test, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const guard = require('../utils/anchorResearchProviderPause');
const flag = 'ANCHOR_RESEARCH_PROVIDERS_PAUSED';
const initial = process.env[flag];
afterEach(() => { if (initial === undefined) delete process.env[flag]; else process.env[flag] = initial; });
const pause = () => { process.env[flag] = 'true'; };
const unpause = () => { delete process.env[flag]; };
const sourceRoot = process.env.RESEARCH_PAUSE_SOURCE_ROOT || path.join(__dirname, '..');
function load(file, stubs = {}) {
  const filename = path.join(sourceRoot, file), mod = { exports: {} }, real = createRequire(path.join(__dirname, '..', file));
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { module: mod, exports: mod.exports,
    require: name => name.includes('anchorResearchProviderPause') ? guard : Object.hasOwn(stubs, name) ? stubs[name] : real(name),
    process, URL, AbortController, AbortSignal, setTimeout, clearTimeout, console: { log() {}, warn() {}, error() {} }, Buffer,
  }, { filename });
  return mod.exports;
}
function placesFixture() {
  let calls = 0, reserves = 0; const records = [], settled = [];
  const attribution = require('../utils/placesCostAttribution');
  const api = load('utils/placesApi.js', {
    axios: { get: async () => { calls++; return { status: 200, data: { status: 'OK', results: [] } }; } },
    './placesCostAttribution': { ...attribution, recordPlacesRequest: async row => records.push(row) },
  });
  const deps = { reserveApiCall: async () => { reserves++; return 'synthetic-reservation'; },
    settleApiCall: async (_, outcome) => settled.push(outcome) };
  const params = { query: 'synthetic business', apiKey: 'synthetic', useCache: false,
    fetchImpl: async () => { calls++; return { status: 200, json: async () => ({ status: 'OK', results: [] }) }; },
    record: { feature: 'Discovery', caller: 'synthetic-research' } };
  return { api, deps, params, records, settled, counts: () => ({ calls, reserves }) };
}

test('default-off unknown scope keeps its behavior and helper imports no DB/network module', () => {
  unpause(); let called = 0;
  assert.equal(guard.withResearchProviderScope(undefined, () => ++called), 1);
  const code = fs.readFileSync(require.resolve('../utils/anchorResearchProviderPause'), 'utf8');
  const deps = []; vm.runInNewContext(code, { module: { exports: {} }, process: { env: {} }, require(name) {
    deps.push(name); assert.equal(name, 'node:async_hooks'); return require(name);
  } });
  assert.deepEqual(deps, ['node:async_hooks']);
});
test('armed tenant10, malformed, absent and conflicting research scopes reject safely', () => {
  pause();
  for (const tenant of [10, '10']) assert.throws(() => guard.withResearchProviderScope(tenant, () => assert.fail('entered')), /anchor_research_provider_paused/);
  for (const tenant of [undefined, '', '13tail', 0, {}, ' 13']) assert.throws(() => guard.withResearchProviderScope(tenant, () => assert.fail('entered')), /research_provider_tenant_required/);
  assert.throws(() => guard.withResearchProviderScope(13, () => guard.withResearchProviderScope(10, () => {})), /tenant_mismatch/);
  assert.equal(guard.withResearchProviderScope(13, () => 'same'), 'same');
});
test('concurrent tenant scopes stay isolated when pause is armed after an await', async () => {
  unpause(); let release; const wait = new Promise(resolve => { release = resolve; }); const calls = [];
  const run = tenant => guard.withResearchProviderScope(tenant, async () => { await wait; guard.assertResearchProviderAllowed('synthetic'); calls.push(tenant); });
  const anchor = run(10), other = run(13); pause(); release();
  const outcomes = await Promise.allSettled([anchor, other]);
  assert.equal(outcomes[0].reason.code, 'anchor_research_provider_paused'); assert.equal(outcomes[1].status, 'fulfilled'); assert.deepEqual(calls, [13]);
});
test('actual Places transport stops after reservation wait without false request attribution', async () => {
  unpause(); const f = placesFixture();
  f.deps.reserveApiCall = async () => { pause(); return 'held-reservation'; };
  await assert.rejects(guard.withResearchProviderScope(10, () => f.api.legacyTextSearch(f.params, f.deps)), /anchor_research_provider_paused/);
  assert.equal(f.counts().calls, 0); assert.equal(f.records.length, 0); assert.equal(f.settled.length, 0, 'existing reservation remains held; no refund or false provider outcome');
});
test('Places record cannot substitute a different tenant for bound authority', async () => {
  pause(); const f = placesFixture(); f.params.record.tenantId = 10;
  await assert.rejects(guard.withResearchProviderScope(13, () => f.api.legacyTextSearch(f.params, f.deps)), /tenant_mismatch/);
  assert.deepEqual(f.counts(), { calls: 0, reserves: 0 });
});
test('explicit nonAnchor research and unmarked field-route geocoding still dispatch', async () => {
  pause(); const f = placesFixture();
  await guard.withResearchProviderScope(13, () => f.api.legacyTextSearch(f.params, f.deps));
  await f.api.geocodeAddress({ address: 'synthetic address', apiKey: 'synthetic', record: { feature: 'Geocode', tenantId: 10 } }, f.deps);
  assert.deepEqual(f.counts(), { calls: 2, reserves: 2 });
});
test('Bouncer rejects pause before HTTP, cache and false verification log', async () => {
  const calls = [], logs = []; const oldEnabled = process.env.BOUNCER_ENABLED, oldKey = process.env.BOUNCER_API_KEY;
  process.env.BOUNCER_ENABLED = 'true'; process.env.BOUNCER_API_KEY = 'synthetic';
  try {
    const verifier = load('utils/emailVerifier.js', { '../db': { query: async () => { throw new Error('unexpected DB'); } }, './emailValidation': { validateEmail: async () => { throw new Error('unexpected MX'); } } });
    verifier._test.setTestHooks({ fetchImpl: async () => { calls.push('http'); return { ok: true, json: async () => ({ status: 'deliverable' }) }; }, logImpl: async data => logs.push(data) });
    pause(); await assert.rejects(verifier.verifyEmail('fresh@synthetic.example', { tenantId: 10 }), /anchor_research_provider_paused/);
    assert.equal(calls.length, 0); assert.equal(logs.length, 0);
    const result = await verifier.verifyEmail('fresh@synthetic.example', { tenantId: 13 });
    assert.equal(result.valid, true); assert.equal(calls.length, 1);
  } finally { if (oldEnabled === undefined) delete process.env.BOUNCER_ENABLED; else process.env.BOUNCER_ENABLED = oldEnabled; if (oldKey === undefined) delete process.env.BOUNCER_API_KEY; else process.env.BOUNCER_API_KEY = oldKey; }
});
test('SerpAPI control rejection escapes normal provider-error conversion', async () => {
  let calls = 0; const old = process.env.SERPAPI_KEY; process.env.SERPAPI_KEY = 'synthetic';
  try { const api = load('lib/serpapi.js', { axios: { get: async () => { calls++; return { data: { organic_results: [] } }; } } });
    pause(); await assert.rejects(api.searchSerpApi({ query: 'synthetic' }), /research_provider_tenant_required/); assert.equal(calls, 0);
    await guard.withResearchProviderScope(13, () => api.searchSerpApi({ query: 'synthetic' })); assert.equal(calls, 1);
  } finally { if (old === undefined) delete process.env.SERPAPI_KEY; else process.env.SERPAPI_KEY = old; }
});

function prospeoFixture({ onSlot, onPost, onLog } = {}) {
  const source = fs.readFileSync(path.join(sourceRoot, 'leadgen.js'), 'utf8');
  const start = source.indexOf('async function callProspeoSearchPerson('), end = source.indexOf('\n// ', source.indexOf('async function enrichWithProspeo(', start));
  assert.ok(start >= 0 && end > start); const mod = { exports: {} }; const counts = { posts: 0, quota: 0, recorded: 0 };
  const context = { module: mod, process, ...guard, PROSPEO_API_KEY: 'synthetic', PROSPEO_RATE_LIMIT_BACKOFF_MS: [0, 0, 0],
    awaitProspeoSlot: async () => onSlot?.(), checkProspeoQuota: async () => { counts.quota++; return { ok: true }; }, recordProspeoCall: async () => { counts.recorded++; },
    axios: { post: async () => { counts.posts++; return onPost ? onPost() : { data: { results: [] } }; } },
    getProspeoTitleIncludes: () => ['owner'], normalizeProspeoPersonRow: row => row,
    isProspeoRateLimited: error => error.response?.status === 429, trip429() {}, logScoutRun: async () => onLog?.(),
    setTimeout: fn => { fn(); }, console: { warn() {} },
  };
  vm.runInNewContext(source.slice(start, end) + '\nmodule.exports={enrichWithProspeo,searchProspeoContactsForDomain};', context);
  return { ...mod.exports, counts };
}
test('Prospeo handoff rechecks pause after throttle and does not become empty provider result', async () => {
  unpause(); const old = process.env.PROSPEO_ENABLED; process.env.PROSPEO_ENABLED = 'true';
  try { const f = prospeoFixture({ onSlot: pause });
    await assert.rejects(guard.withResearchProviderScope(10, () => f.enrichWithProspeo('synthetic.example')), /anchor_research_provider_paused/);
    assert.equal(f.counts.posts, 0); assert.equal(f.counts.recorded, 1, 'already reserved work is not refunded');
  } finally { if (old === undefined) delete process.env.PROSPEO_ENABLED; else process.env.PROSPEO_ENABLED = old; }
});
test('Prospeo retries stop before second HTTP after arming during existing backoff', async () => {
  unpause(); const old = process.env.PROSPEO_ENABLED; process.env.PROSPEO_ENABLED = 'true';
  try { const f = prospeoFixture({ onPost: () => { throw Object.assign(new Error('synthetic429'), { response: { status: 429 } }); }, onLog: pause });
    await assert.rejects(guard.withResearchProviderScope(10, () => f.enrichWithProspeo('synthetic.example')), /anchor_research_provider_paused/);
    assert.equal(f.counts.posts, 1); assert.equal(f.counts.recorded, 1);
  } finally { if (old === undefined) delete process.env.PROSPEO_ENABLED; else process.env.PROSPEO_ENABLED = old; }
});

test('real SEC blocked control cannot become an allowed fixture fallback after executor entry', async () => {
  unpause();
  const { runScoutForAmoMission } = load('packages/max/workspace/ScoutDiscoveryExecutor.js');
  let entered = 0, fallback = 0;
  const mission = { id: 'synthetic-sec-pause', tenantId: '10', clientId: 10,
    objective: 'Find synthetic commercial offices.', stage: 'discover', status: 'Discovering',
    structuredMission: { immutable: true, objective: 'Find synthetic commercial offices.',
      market: { segment: 'law_firm', industry: 'legal', buyer: 'office manager', label: 'Law firms' },
      geography: { region: 'Manchester NH', cities: ['Manchester'] }, constraints: [],
      evidence: { requireDecisionMaker: true }, successMetric: { qualifiedLeads: 1 } } };
  for (const treatErrorsAsBlocked of [true, false]) {
    unpause();
    await assert.rejects(runScoutForAmoMission(mission, {
    treatErrorsAsBlocked, allowFixtureFallback: true,
    fixtureScoutDiscoveryResult: () => { fallback++; return { fabricated: true }; },
    discoverImpl: async () => { entered++; pause(); guard.assertResearchProviderAllowed('google_places'); },
  }), { code: 'anchor_research_provider_paused' });
  }
  assert.equal(entered, 2); assert.equal(fallback, 0);
});
