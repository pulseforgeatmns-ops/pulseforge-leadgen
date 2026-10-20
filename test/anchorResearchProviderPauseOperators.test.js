'use strict';

const { test, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const control = require('../utils/anchorResearchProviderPause');
const root = path.join(__dirname, '..');
const initialEnv = { ...process.env };
afterEach(() => {
  for (const key of Object.keys(process.env)) if (!(key in initialEnv)) delete process.env[key];
  Object.assign(process.env, initialEnv);
});

function load(file, stubs = {}, appended = '') {
  const filename = path.join(root, file);
  const module = { exports: {} };
  const requireStub = name => {
    if (Object.hasOwn(stubs, name)) return stubs[name];
    if (name.endsWith('/anchorResearchProviderPause')) return control;
    if (name === 'dotenv') return { config() {} };
    throw new Error(`Unstubbed dependency ${name} in ${file}`);
  };
  vm.runInNewContext(`(function(require, module, exports) { ${fs.readFileSync(filename, 'utf8')}\n${appended}\n})`,
    { process, console: { log() {}, warn() {}, error() {} }, URL, setTimeout, clearTimeout, Buffer },
    { filename })(requireStub, module, module.exports);
  return module.exports;
}
function diagnostic(onLegacy = async () => ({ status: 'ZERO_RESULTS', results: [] })) {
  const calls = [];
  const service = load('services/scoutPlacesDiagnostic.js', {
    './scoutPublicSourcing': {
      SCOUT_PLACES_ENDPOINT_FAMILY: 'legacy', SCOUT_PLACES_AUTH_STYLE: 'query',
      SCOUT_PLACES_TEXTSEARCH_URL: 'https://maps.googleapis.com/maps/api/place/textsearch/json',
      buildScoutPlacesTextSearchUrl: () => new URL('https://maps.googleapis.com/maps/api/place/textsearch/json'),
      scoutPlacesUrlHostPath: url => ({ host: new URL(url).host, path: new URL(url).pathname }),
    },
    '../utils/placesCostAttribution': { PLACES_FEATURES: { DIAGNOSTIC: 'diagnostic' }, TRIGGER_MODES: { MANUAL: 'manual' } },
    '../utils/placesApi': { legacyTextSearch: async () => {
      control.assertResearchProviderAllowed('google_places');
      calls.push('legacy');
      return { ok: true, httpStatus: 200, data: await onLegacy() };
    } },
  });
  return { calls, run: opts => service.diagnoseScoutPlaces({ apiKey: 'synthetic-test-key', fetchImpl: async () => {
    calls.push('new'); return { ok: true, status: 200, text: async () => '{"places":[]}' };
  }, ...opts }) };
}
function phone({ post = async () => ({ data: { contacts: [], results: [] } }), slot = async () => {}, record = async () => {}, places = async () => ({ data: { places: [] } }) } = {}) {
  return load('phoneEnrich.js', {
    axios: { post }, './utils/prospeoThrottle': { awaitProspeoSlot: slot },
    './utils/prospeoBreaker': { checkProspeoQuota: async () => ({ ok: true }), recordProspeoCall: record, trip429() {} },
    './utils/placesCostAttribution': { PLACES_FEATURES: { CANDIDATE_REFRESH: 'candidate_refresh' } },
    './utils/placesApi': { v1SearchText: places, v1PlaceDetails: places },
  });
}

test('armed diagnostic rejects tenant10, missing and malformed explicit scope before either probe', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  for (const tenant of [10, undefined, '11junk']) {
    const d = diagnostic();
    await assert.rejects(d.run({ client_id: tenant }), error => control.isResearchProviderControlError(error));
    assert.deepEqual(d.calls, []);
  }
});
test('armed non10 diagnostic performs legacy and New comparison under explicit scope', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  const d = diagnostic();
  const result = await d.run({ client_id: 11 });
  assert.equal(result.ok, true);
  assert.deepEqual(d.calls, ['legacy', 'new']);
});
test('diagnostic pause between probes prevents direct New comparison', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const d = diagnostic(async () => {
    process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
    return { status: 'ZERO_RESULTS', results: [] };
  });
  await assert.rejects(d.run({ client_id: 10 }), { code: 'anchor_research_provider_paused' });
  assert.deepEqual(d.calls, ['legacy']);
});
test('armed exported phone enrichment rejects tenant10 and missing scope without provider calls', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  process.env.APOLLO_API_KEY = 'synthetic';
  let calls = 0;
  const p = phone({ post: async () => { calls++; return { data: {} }; } });
  for (const client_id of [10, undefined, '11junk']) {
    await assert.rejects(p.enrichPhoneWaterfall({ client_id, business_name: 'Synthetic' }), error => control.isResearchProviderControlError(error));
  }
  assert.equal(calls, 0);
});

test('default-off diagnostics retain unscoped comparison; explicit aliases cannot conflict while armed', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const d = diagnostic();
  assert.equal((await d.run()).ok, true);
  assert.deepEqual(d.calls, ['legacy', 'new']);
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  await assert.rejects(d.run({ client_id: 11, tenantId: 12 }), { code: 'research_provider_tenant_mismatch' });
  assert.equal(d.calls.length, 2);
});
test('diagnostic typed legacy errors escape without launching comparison', async () => {
  const d = diagnostic(async () => { throw Object.assign(new Error('pause'), { code: 'anchor_research_provider_paused' }); });
  await assert.rejects(d.run({ client_id: 11 }), { code: 'anchor_research_provider_paused' });
  assert.deepEqual(d.calls, ['legacy']);
});
test('phone captures tenant before awaits and blocks Apollo fallback when armed after Places', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  process.env.GOOGLE_PLACES_KEY = process.env.APOLLO_API_KEY = 'synthetic';
  let paid = 0;
  const lead = { client_id: 10, business_name: 'Synthetic' };
  const p = phone({ places: async () => {
    paid++; lead.client_id = 11; process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
    return { data: { places: [] } };
  }, post: async () => { paid++; return { data: {} }; } });
  await assert.rejects(p.enrichPhoneWaterfall(lead), { code: 'anchor_research_provider_paused' });
  assert.equal(paid, 1);
});
test('phone rechecks Prospeo after throttle wait and never converts pause into failure', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  delete process.env.GOOGLE_PLACES_KEY; delete process.env.APOLLO_API_KEY;
  process.env.PROSPEO_API_KEY = 'synthetic'; process.env.PROSPEO_ENABLED = 'true';
  let paid = 0; let recorded = 0;
  const p = phone({ record: async () => { recorded++; }, slot: async () => { process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true'; },
    post: async () => { paid++; return { data: {} }; } });
  await assert.rejects(p.enrichPhoneWaterfall({ client_id: 10, website: 'https://synthetic.invalid' }), { code: 'anchor_research_provider_paused' });
  assert.equal(paid, 0); assert.equal(recorded, 0);
});
test('phone preserves non10 waterfall and rejects row/options conflicts', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  delete process.env.GOOGLE_PLACES_KEY; process.env.APOLLO_API_KEY = 'synthetic';
  let paid = 0;
  const p = phone({ post: async () => { paid++; return { data: { contacts: [{ phone: '+12025550123' }] } }; } });
  const result = await p.enrichPhoneWaterfall({ client_id: 11 });
  assert.equal(result.phone, '+12025550123'); assert.equal(paid, 1);
  await assert.rejects(p.enrichPhoneWaterfall({ client_id: 11 }, { tenantId: 12 }), { code: 'research_provider_tenant_mismatch' });
  assert.equal(paid, 1);
});

function response() {
  return { statusCode: 200, events: [], setHeader() {}, set() {}, status(value) { this.statusCode = value; return this; },
    json(value) { this.body = value; return this; }, write(value) { this.events.push(JSON.parse(value.slice(6))); }, end() { this.ended = true; } };
}
function searchHandler({ get, delay = async () => {} }) {
  const source = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const route = source.slice(source.indexOf("app.get('/api/search'"), source.indexOf('// CSV export'));
  const helpers = source.slice(source.indexOf('async function searchSerpAPI'), source.indexOf('// ── HELPERS'));
  let handler;
  let resolved = 0;
  vm.runInNewContext(route + '\n' + helpers, { ...control,
    app: { get: (...args) => { handler = args.at(-1); } }, requireAuth() {}, requireRole: () => () => {},
    resolveCieClientId: () => { resolved++; return 11; }, axios: { get }, SERPAPI_KEY: 'synthetic', HUNTER_KEY: 'synthetic',
    SKIP_DOMAINS: [], deduplicate: value => value, extractDomain: value => new URL(value).hostname,
    detectType: () => 'other', scoreLead: () => 100, delay,
  });
  return { run: handler, resolved: () => resolved };
}
function searchResult() { return { data: { organic_results: [{ title: 'Synthetic', link: 'https://synthetic.invalid' }] } }; }

test('actual search route blocks selected tenant10, missing, malformed and conflicting hints', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  let paid = 0;
  const h = searchHandler({ get: async () => { paid++; return searchResult(); } });
  for (const [selected, query] of [[10, {}], [undefined, { client_id: 11 }], ['11junk', {}], [11, { client_id: 12 }], [11, { client_id: 11, clientId: 12 }]]) {
    const res = response();
    await h.run({ user: { role: 'admin' }, session: { active_client_id: selected }, query }, res);
    assert.equal(res.ended, true);
    assert.match(res.events.at(-1).msg, /research_provider/);
  }
  assert.equal(paid, 0);
});
test('actual search route preserves non10 calls and validates selected tenant', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  const calls = [];
  const h = searchHandler({ get: async url => { calls.push(url); return url.includes('serpapi') ? searchResult() : { data: { data: { emails: [] } } }; } });
  const res = response();
  await h.run({ user: { role: 'admin' }, session: { active_client_id: 11 }, query: { max: 1 } }, res);
  assert.equal(calls.length, 2); assert.equal(h.resolved(), 1);
  assert.equal(res.events.at(-1).type, 'results');
});
test('actual search route rejects later pages after flag flip despite request tenant mutation', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const req = { user: { role: 'admin' }, session: { active_client_id: 10 }, query: { max: 20 } };
  let paid = 0;
  const h = searchHandler({ get: async () => { paid++; req.session.active_client_id = 11;
    process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true'; return searchResult(); } });
  const res = response(); await h.run(req, res);
  assert.equal(paid, 1); assert.match(res.events.at(-1).msg, /anchor_research_provider_paused/);
});
test('actual search route keeps default-off missing-context behavior', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  let paid = 0;
  const h = searchHandler({ get: async () => { paid++; return { data: {} }; } });
  const res = response(); await h.run({ query: { max: 1 } }, res);
  assert.equal(paid, 1); assert.equal(res.events.at(-1).type, 'results');
});

function cronHandler(d) {
  const source = fs.readFileSync(path.join(root, 'routes/cron.js'), 'utf8');
  return vm.runInNewContext(source.slice(source.indexOf('async function handleScoutPlacesDiagnostic'),
    source.indexOf('function maxAttentionCronResult')) + '\nhandleScoutPlacesDiagnostic;',
    { ...control, process, console: { error() {} }, diagnoseScoutPlaces: d.run });
}
test('actual special cron requires explicit valid client and preserves auth and non10 comparison', async () => {
  process.env.CRON_SECRET = 'synthetic-secret'; process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  const d = diagnostic(); const handler = cronHandler(d);
  for (const [query, body, expected] of [
    [{}, {}, 400], [{ client_id: 10 }, {}, 423], [{ client_id: '11junk' }, {}, 400],
    [{ client_id: 11 }, { client_id: 12 }, 400], [{ client_id: 11 }, {}, 200],
  ]) {
    const res = response(); await handler({ query: { secret: 'synthetic-secret', ...query }, body }, res);
    assert.equal(res.statusCode, expected);
  }
  const unauthorized = response(); await handler({ query: { client_id: 11 }, body: {} }, unauthorized);
  assert.equal(unauthorized.statusCode, 401); assert.deepEqual(d.calls, ['legacy', 'new']);
});
test('actual special cron retains unscoped default-off behavior', async () => {
  delete process.env.CRON_SECRET; delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const d = diagnostic(); const res = response(); await cronHandler(d)({ query: {}, body: {} }, res);
  assert.equal(res.statusCode, 200); assert.deepEqual(d.calls, ['legacy', 'new']);
});

function importer(post, query = async () => ({ rows: [] })) {
  return load('scripts/importAnchorCallList.js', { axios: { post }, '../db': { query },
    '../leadgen': { scoreCleaningLead: () => ({ total: 50, components: {}, flags: [] }) },
    '../utils/callDispositions': { applyProspectDisposition() {}, resolveCallbackAt() {} },
    '../utils/setterVisibility': { setSetterVisibility() {} },
  });
}
test('known10 import rejects REVIEW_ONLY and apply before DB or paid verification', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  let calls = 0;
  const m = importer(async () => { calls++; }, async () => { calls++; return { rows: [] }; });
  for (const apply of [false, true]) await assert.rejects(m.run({ apply }), { code: 'anchor_research_provider_paused' });
  assert.equal(calls, 0);
});
test('known10 import rechecks alternate Places query and does not swallow control errors', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED; process.env.GOOGLE_PLACES_KEY = 'synthetic';
  let paid = 0;
  const m = importer(async () => { paid++; process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true'; return { data: { places: [] } }; });
  await assert.rejects(m.run({ firm: 'Porter Tax Preparation' }), { code: 'anchor_research_provider_paused' });
  assert.equal(paid, 1);
});

function diagnosticHttpHandler(d) {
  const source = fs.readFileSync(path.join(root, 'routes/clientIntelligence.js'), 'utf8');
  const routeStart = source.lastIndexOf('router.get(', source.indexOf("  '/api/v1/scout/places-diagnostic'"));
  const route = source.slice(routeStart, source.indexOf('/**\n * SPEC-077', routeStart));
  let handler;
  vm.runInNewContext(route, { ...control,
    router: { get: (...args) => { handler = args.at(-1); } }, requireInternal: [],
    diagnoseScoutPlaces: d.run, parseBool: (value, defaultValue) => value == null ? defaultValue : value === 'true',
    noStore() {}, resolveCieClientId(req, hint) {
      const id = req.user?.client_id ?? req.session?.active_client_id;
      if (hint != null && Number(hint) !== Number(id)) throw Object.assign(new Error('tenant_mismatch'), { status: 403 });
      return id;
    }, sendError: (res, err) => res.status(err.status || 500).json({ error: err.message }),
  });
  return handler;
}
test('actual HTTP diagnostic uses authenticated selected tenant, rejects hints and preserves non10', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  const d = diagnostic(); const handler = diagnosticHttpHandler(d);
  for (const [selected, query, expected] of [
    [10, {}, 423], [undefined, { client_id: 11 }, 400], ['11junk', {}, 400],
    [11, { client_id: 12 }, 403], [11, { client_id: 11, clientId: 12 }, 400], [11, {}, 200],
  ]) {
    const res = response();
    await handler({ user: { role: 'admin' }, session: { active_client_id: selected }, query }, res);
    assert.equal(res.statusCode, expected);
  }
  assert.deepEqual(d.calls, ['legacy', 'new']);
});
test('actual HTTP diagnostic retains default-off missing selection', async () => {
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const d = diagnostic(); const res = response();
  await diagnosticHttpHandler(d)({ query: {}, user: { role: 'admin' } }, res);
  assert.equal(res.statusCode, 200); assert.deepEqual(d.calls, ['legacy', 'new']);
});
test('diagnostic CLI accepts explicit --client_id in both forms without silently defaulting', () => {
  const source = fs.readFileSync(path.join(root, 'scripts/diagnoseScoutPlaces.js'), 'utf8');
  const parse = vm.runInNewContext(source.slice(source.indexOf('function parseArgs'), source.indexOf('function printHelp')) + '\nparseArgs;');
  assert.equal(parse([]).client_id, undefined);
  assert.equal(parse(['--client_id=11']).client_id, '11');
  assert.equal(parse(['--client_id', '10']).client_id, '10');
  assert.equal(parse(['--client_id=11junk']).client_id, '11junk');
  assert.equal(parse(['--client_id']).client_id, '');
});
test('phone CLI carries explicit tenant and never infers it from business text', async () => {
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
  delete process.env.GOOGLE_PLACES_KEY; process.env.APOLLO_API_KEY = 'synthetic';
  let paid = 0;
  const p = load('phoneEnrich.js', {
    axios: { post: async () => { paid++; return { data: { contacts: [{ phone: '+12025550123' }] } }; } },
    './utils/prospeoThrottle': { awaitProspeoSlot: async () => {} },
    './utils/prospeoBreaker': { checkProspeoQuota: async () => ({ ok: false }), recordProspeoCall() {}, trip429() {} },
    './utils/placesCostAttribution': { PLACES_FEATURES: {} }, './utils/placesApi': {},
  }, 'module.exports.runCli = runCli;');
  const originalArgv = process.argv;
  try {
    process.argv = ['node', 'phoneEnrich.js', 'Anchor', '11'];
    await assert.rejects(p.runCli(), { code: 'research_provider_tenant_required' });
    process.argv = ['node', 'phoneEnrich.js', '--client_id', '10', 'Synthetic'];
    await assert.rejects(p.runCli(), { code: 'anchor_research_provider_paused' });
    process.argv = ['node', 'phoneEnrich.js', '--client_id=11', '--client_id'];
    await assert.rejects(p.runCli(), { code: 'research_provider_tenant_required' });
    process.argv = ['node', 'phoneEnrich.js', '--client_id=11', '--client_id=12', 'Synthetic'];
    await assert.rejects(p.runCli(), { code: 'research_provider_tenant_mismatch' });
    process.argv = ['node', 'phoneEnrich.js', '--client_id=11', 'Synthetic'];
    await p.runCli(); assert.equal(paid, 1);
  } finally { process.argv = originalArgv; }
});
