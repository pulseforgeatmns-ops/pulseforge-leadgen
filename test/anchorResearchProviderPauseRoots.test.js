'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const pause = require('../utils/anchorResearchProviderPause');

// Actual root modules, synthetic dependencies, no database/provider connections.
function load(relative, overrides = {}, globals = {}) {
  const filename = path.join(__dirname, '..', relative);
  const nativeRequire = createRequire(filename);
  const module = { exports: {} };
  const deps = {
    dotenv: { config() {} },
    ...overrides,
  };
  const requireStub = name => Object.hasOwn(deps, name) ? deps[name] : nativeRequire(name);
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module, exports: module.exports, require: requireStub, process,
    console: { log() {}, error() {}, warn() {} }, setTimeout, clearTimeout, setInterval,
    URL, Date, Buffer, ...globals,
  }, { filename });
  return module.exports;
}

async function withFlag(value, fn) {
  const prior = process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  if (value == null) delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  else process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = value;
  try { return await fn(); }
  finally {
    if (prior == null) delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
    else process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = prior;
  }
}

async function withEnvironment(values, fn) {
  const prior = Object.fromEntries(Object.keys(values).map(key => [key, process.env[key]]));
  for (const [key, value] of Object.entries(values)) {
    if (value == null) delete process.env[key]; else process.env[key] = value;
  }
  try { return await fn(); }
  finally {
    for (const [key, value] of Object.entries(prior)) {
      if (value == null) delete process.env[key]; else process.env[key] = value;
    }
  }
}

const rejectsPause = fn => assert.rejects(fn, { code: 'anchor_research_provider_paused' });
const row = (client_id = 10) => ({ client_id, id: 'p1', prospect_id: 'p1', company_id: 'c1',
  company: 'Example', company_name: 'Example', domain: 'firm.example', email: 'jane@firm.example' });

test('armed scheduler stops before schema setup, reads, promotion and counters', async () => withFlag('true', async () => {
  const seen = [];
  const db = { query: async sql => { seen.push(sql); return { rows: [] }; } };
  const scheduler = load('scoutUnenrichedEnrichmentAgent.js', {
    './db': db,
    './utils/scoutUnenrichedSchema': { ensureScoutUnenrichedTable: async () => seen.push('schema') },
    './utils/emailVerificationSchema': { ensureEmailVerificationColumns: async () => seen.push('schema') },
    './scripts/promoteUnenriched': { promoteRecord: async () => seen.push('provider') },
  });
  await rejectsPause(() => scheduler.run({ client_id: 10, db }));
  // Only the durable governed-block record is written; no schema, read or promotion.
  assert.equal(seen.length, 1);
  assert.match(seen[0], /INSERT INTO agent_log[\s\S]*'skipped'/);
}));

test('armed promotion rejects before configuration, enrichment, verification or writes', async () => withFlag('true', async () => {
  const seen = [];
  const service = load('scripts/promoteUnenriched.js', { '../db': {}, '../leadgen': {} });
  await rejectsPause(() => service.promoteRecord(row(), {
    loadClientConfig: async () => { seen.push('config'); return {}; },
    enrich: async () => { seen.push('provider'); return null; },
    db: { query: async () => { seen.push('write'); return { rows: [] }; } },
  }));
  assert.deepEqual(seen, []);
}));

test('armed tiered row root stops before website/provider or candidate mutation, including dry run', async () => withFlag('true', async () => {
  const service = load('tieredEnrichmentAgent.js', { './db': {} });
  const candidate = row();
  const before = JSON.stringify(candidate);
  await rejectsPause(() => service._test.processProspect(candidate, { dryRun: true, bucketAOnly: true }));
  assert.equal(JSON.stringify(candidate), before);
}));

test('tiered run blocks before schema ensure and rejects absent operator tenant while armed', async () => withFlag('true', async () => {
  const prior = process.env.ACTIVE_CLIENT_ID;
  delete process.env.ACTIVE_CLIENT_ID;
  try {
    let setup = 0;
    const service = load('tieredEnrichmentAgent.js', {
      './db': {},
      './utils/tieredEnrichmentSchema': { ensureTieredEnrichmentSchema: async () => { setup++; } },
    });
    await rejectsPause(() => service.run({ client_id: 10 }));
    await assert.rejects(() => service.run(), { code: 'research_provider_tenant_required' });
    assert.equal(setup, 0);
  } finally {
    if (prior == null) delete process.env.ACTIVE_CLIENT_ID; else process.env.ACTIVE_CLIENT_ID = prior;
  }
}));

test('armed mission enrichment helper stops before remediation and provider fallbacks', async () => withFlag('true', async () => {
  const seen = [];
  const service = load('scripts/lib/anchorMissionBoundEnrichment.js', {
    '../../leadgen': {},
    '../../tieredEnrichmentAgent': {},
    '../../utils/crmEmailProvenance': {
      persistableEmailSource: () => null,
      remediateTaintedCrmEmail: async () => { seen.push('remediation'); return {}; },
    },
  });
  await rejectsPause(() => service.enrichProspectRow(row(), {
    dryRun: true,
    processProspect: async () => { seen.push('tiered'); return { errors: [] }; },
    configureScoringContext: async () => seen.push('config'),
    runEnrichmentChain: async () => { seen.push('provider'); return {}; },
  }));
  assert.deepEqual(seen, []);
}));

test('armed retry service stops before loading candidates or writing failed verification', async () => withFlag('true', async () => {
  const seen = [];
  const service = load('services/emailVerificationRecovery.js', { '../leadgen': {} });
  await rejectsPause(() => service.retryUnverifiedEmails({ query: async () => { seen.push('query'); return { rows: [] }; } }, { clientId: 10 }));
  assert.deepEqual(seen, []);
}));

test('armed same-company recovery preserves counters before any provider or database work', async () => withFlag('true', async () => {
  const seen = [];
  const service = load('services/sameCompanyContactRecovery.js', { '../leadgen': { normalizeDomain: value => value } });
  const telemetry = service.emptyAlternateContactTelemetry();
  const before = JSON.stringify(telemetry);
  await rejectsPause(() => service.attemptSameCompanyAlternateRecovery({ tenantId: '10' }, {
    query: async () => { seen.push('query'); return { rows: [] }; },
  }, { company: row(), telemetry, ownership: { kind: 'same_company_different_contact', companyId: 'c1' } }));
  assert.deepEqual(seen, []);
  assert.equal(JSON.stringify(telemetry), before);
}));

test('armed mission website discovery binds tenantId and CRM row and blocks dry-run transport', async () => withFlag('true', async () => {
  const seen = [];
  const service = load('packages/max/workspace/MissionBoundWebsiteDiscovery.js', {
    '../../../utils/placesApi': {
      legacyPlaceDetails: async () => { seen.push('details'); return {}; },
      legacyTextSearch: async () => { seen.push('search'); return {}; },
    },
  });
  await rejectsPause(() => service.discoverMissionBoundWebsite({ company: 'Example' }, { tenantId: 10, apiKey: 'fixture', dryRun: true }));
  assert.deepEqual(seen, []);
}));

function schedulerFixture(records, promote) {
  const seen = [];
  const db = { query: async (sql, params) => {
    seen.push({ sql, params });
    return { rows: /SELECT \*/.test(sql) ? records : [] };
  } };
  const service = load('scoutUnenrichedEnrichmentAgent.js', {
    './db': db,
    './utils/scoutUnenrichedSchema': { ensureScoutUnenrichedTable: async () => {} },
    './utils/emailVerificationSchema': { ensureEmailVerificationColumns: async () => {} },
    './scripts/promoteUnenriched': { promoteRecord: promote },
  });
  return { seen, db, service };
}

test('mid-cycle pause preserves completed scheduler work but does not consume rejected row attempt', async () => withFlag(undefined, async () => {
  const records = [row(), { ...row(), id: 'p2' }];
  const fixture = schedulerFixture(records, async record => {
    if (record.id === 'p1') return { promoted: true, prospectId: 'existing-result' };
    process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
    pause.assertResearchProviderAllowed('hunter');
  });
  await rejectsPause(() => fixture.service.run({ client_id: 10, db: fixture.db }));
  const updates = fixture.seen.filter(call => /UPDATE scout_unenriched/.test(call.sql));
  assert.equal(updates.length, 1);
  assert.equal(updates[0].params[0], 'p1');
  assert.equal(fixture.seen.some(call => /automatic retry failed/.test(call.sql)), false);
}));

test('scheduler remains unchanged for default-off Anchor and armed non-Anchor', async () => {
  for (const [flag, tenant] of [[undefined, 10], ['false', 10], ['true', 13]]) {
    await withFlag(flag, async () => {
      let calls = 0;
      const fixture = schedulerFixture([row(tenant)], async () => {
        pause.assertResearchProviderAllowed('hunter'); calls++;
        return { promoted: true, prospectId: 'p1' };
      });
      const result = await fixture.service.run({ client_id: tenant, db: fixture.db });
      assert.equal(result.promoted, 1);
      assert.equal(calls, 1);
      assert.equal(fixture.seen.filter(call => /UPDATE scout_unenriched/.test(call.sql)).length, 1);
    });
  }
});

test('scheduler rejects conflicting root and row tenants without attempts', async () => withFlag('true', async () => {
  const fixture = schedulerFixture([row(10)], async () => { throw new Error('unexpected provider'); });
  await assert.rejects(() => fixture.service.run({ client_id: 13, db: fixture.db }), { code: 'research_provider_tenant_mismatch' });
  assert.equal(fixture.seen.some(call => /UPDATE/.test(call.sql)), false);
  await assert.rejects(() => fixture.service.run({ client_id: 13, clientId: 14, db: fixture.db }), { code: 'research_provider_tenant_mismatch' });
}));

test('retry interruption propagates typed control instead of false verification failures or DNC writes', async () => withFlag(undefined, async () => {
  const writes = [];
  const service = load('services/emailVerificationRecovery.js', { '../leadgen': {} });
  const db = { query: async (sql, params) => {
    if (/UPDATE/.test(sql)) { writes.push(params); return { rowCount: 1 }; }
    return { rows: [row()] };
  } };
  await rejectsPause(() => service.retryUnverifiedEmails(db, { clientId: 10, verify: async () => {
    process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
    pause.assertResearchProviderAllowed('bouncer');
  } }));
  assert.deepEqual(writes, []);
}));

test('promotion authority survives row mutation across configuration await', async () => withFlag(undefined, async () => {
  const seen = [];
  const candidate = row();
  const service = load('scripts/promoteUnenriched.js', { '../db': {}, '../leadgen': {} });
  await rejectsPause(() => service.promoteRecord(candidate, {
    loadClientConfig: async () => {
      candidate.client_id = 13;
      process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
      return {};
    },
    enrich: async () => { pause.assertResearchProviderAllowed('prospeo'); seen.push('paid'); return null; },
    db: { query: async () => { seen.push('write'); return { rows: [] }; } },
  }));
  assert.deepEqual(seen, []);
}));

test('scheduler retries same-day pause after unarming and never overlaps ticks', async () => withFlag('true', async () => {
  const ticks = [];
  let queries = 0;
  let blockRecords = 0;
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  class ScheduledDate extends Date {
    constructor(...args) { super(...(args.length ? args : ['2026-10-19T22:00:00.000Z'])); }
  }
  const service = load('scoutUnenrichedEnrichmentAgent.js', {
    './db': { query: async sql => {
      if (/'skipped'/.test(sql)) { blockRecords++; return { rows: [] }; }
      queries++; await gate; return { rows: [] };
    } },
    './utils/scoutUnenrichedSchema': { ensureScoutUnenrichedTable: async () => {} },
    './utils/emailVerificationSchema': { ensureEmailVerificationColumns: async () => {} },
    './scripts/promoteUnenriched': {},
  }, {
    Date: ScheduledDate,
    setTimeout: fn => { ticks.push(fn); return { unref() {} }; },
    setInterval: fn => { ticks.push(fn); return { unref() {} }; },
  });
  service.startAnchorUnenrichedEnrichmentScheduler();
  await ticks[0]();
  assert.equal(queries, 0);
  assert.equal(blockRecords, 1, 'paused tick leaves one durable governed-block record');
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const resumed = ticks[1]();
  assert.equal(ticks[1](), undefined, 'a running retry excludes duplicate ticks');
  release();
  await resumed;
  assert.equal(queries, 2, 'one selection and one completed-run log');
  assert.equal(ticks[1](), undefined, 'completed run retains daily latch');
}));

test('scheduler ordinary failure still consumes the daily schedule', async () => withFlag(undefined, async () => {
  const ticks = [];
  let setup = 0;
  class ScheduledDate extends Date {
    constructor(...args) { super(...(args.length ? args : ['2026-10-19T22:00:00.000Z'])); }
  }
  const service = load('scoutUnenrichedEnrichmentAgent.js', {
    './db': {},
    './utils/scoutUnenrichedSchema': { ensureScoutUnenrichedTable: async () => { setup++; throw new Error('ordinary fixture failure'); } },
    './utils/emailVerificationSchema': {},
    './scripts/promoteUnenriched': {},
  }, {
    Date: ScheduledDate,
    setTimeout: fn => { ticks.push(fn); return { unref() {} }; },
    setInterval: fn => { ticks.push(fn); return { unref() {} }; },
  });
  service.startAnchorUnenrichedEnrichmentScheduler();
  await ticks[0]();
  assert.equal(ticks[1](), undefined);
  assert.equal(setup, 1);
}));

test('same-company direct Hunter transport rechecks pause and rethrows instead of returning provider error', async () => withFlag(undefined, async () => {
  const prior = process.env.HUNTER_API_KEY;
  process.env.HUNTER_API_KEY = 'fixture-only';
  try {
    let paid = 0;
    const service = load('services/sameCompanyContactRecovery.js', {
      '../leadgen': {},
      axios: { get: async () => { paid++; return { data: { data: { emails: [] } } }; } },
    });
    await rejectsPause(() => pause.withResearchProviderScope(10, () => service._test.resolveAlternateContacts({
      domain: 'firm.example', sources: {
        website: async () => {
          process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
          return { contacts: [] };
        },
        prospeo: async () => ({ contacts: [] }),
      },
    })));
    assert.equal(paid, 0);
    await pause.withResearchProviderScope(13, () => service._test.resolveAlternateContacts({
      domain: 'firm.example', sources: {
        website: async () => ({ contacts: [] }), prospeo: async () => ({ contacts: [] }),
      },
    }));
    assert.equal(paid, 1);
  } finally {
    if (prior == null) delete process.env.HUNTER_API_KEY;
    else process.env.HUNTER_API_KEY = prior;
  }
}));

test('tiered interruption escapes candidate persistence and retains captured row authority', async () => withFlag(undefined, async () => {
  const priorEnabled = process.env.BOUNCER_ENABLED;
  const priorKey = process.env.BOUNCER_API_KEY;
  process.env.BOUNCER_ENABLED = 'true';
  process.env.BOUNCER_API_KEY = 'fixture-only';
  try {
    let writes = 0;
    const service = load('tieredEnrichmentAgent.js', { './db': { query: async () => { writes++; return { rows: [] }; } } });
    const candidate = { ...row(), email: '', first_name: '', last_name: '' };
    await rejectsPause(() => service._test.processProspect(candidate, {
      fetchDelayMs: 0,
      fetchPage: async url => {
        candidate.client_id = 13;
        process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
        return { ok: true, status: 200, url, text: '<html><a href="mailto:jane@firm.example">Jane Smith, Owner</a></html>' };
      },
      verifyEmail: async () => { pause.assertResearchProviderAllowed('bouncer'); throw new Error('unexpected provider'); },
    }));
    assert.equal(writes, 0);
  } finally {
    if (priorEnabled == null) delete process.env.BOUNCER_ENABLED; else process.env.BOUNCER_ENABLED = priorEnabled;
    if (priorKey == null) delete process.env.BOUNCER_API_KEY; else process.env.BOUNCER_API_KEY = priorKey;
  }
}));

test('mission website helper cross-checks explicit and row tenants and requires unknown scope', async () => withFlag('true', async () => {
  let calls = 0;
  const service = load('packages/max/workspace/MissionBoundWebsiteDiscovery.js', {
    '../../../utils/placesApi': {
      legacyPlaceDetails: async () => { calls++; return {}; },
      legacyTextSearch: async () => { calls++; return {}; },
    },
  });
  await assert.rejects(() => service.discoverMissionBoundWebsite({}, { tenantId: 13, crmRow: { client_id: 14 } }), { code: 'research_provider_tenant_mismatch' });
  await assert.rejects(() => service.discoverMissionBoundWebsite({}, { apiKey: 'fixture' }), { code: 'research_provider_tenant_required' });
  await assert.rejects(() => service.fetchPlaceDetailsWebsite('place-id', { apiKey: 'fixture' }), { code: 'research_provider_tenant_required' });
  assert.equal(calls, 0);
  const result = await service.discoverMissionBoundWebsite({ company: 'Example' }, { tenantId: 13, crmRow: { client_id: 13 }, apiKey: 'fixture' });
  assert.equal(result.action, 'manual_review');
  assert.equal(calls, 1);
}));

test('mission website fallback stops when pause arms after first paid request', async () => withFlag(undefined, async () => {
  const calls = [];
  const service = load('packages/max/workspace/MissionBoundWebsiteDiscovery.js', {
    '../../../utils/placesApi': {
      legacyPlaceDetails: async () => {
        pause.assertResearchProviderAllowed('places'); calls.push('details');
        process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
        return { googleStatus: 'OK', data: { result: {} } };
      },
      legacyTextSearch: async () => { pause.assertResearchProviderAllowed('places'); calls.push('search'); return {}; },
    },
  });
  await rejectsPause(() => service.discoverMissionBoundWebsite({ company: 'Example', placeId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU' }, { tenantId: 10, apiKey: 'fixture' }));
  assert.deepEqual(calls, ['details']);
}));

test('mixed-tenant backfill retains row identity and rejects Anchor without false DNC mutation', async () => withFlag('true', async () => {
  const priorArgv = process.argv;
  process.argv = ['node', 'synthetic-backfill'];
  try {
    const paid = [];
    const writes = [];
    const service = load('scripts/backfill_email_verification.js', {
      '../db': { query: async (sql, params) => {
        if (/SELECT/.test(sql)) {
          assert.match(sql, /SELECT id, client_id, email/);
          return { rows: [{ ...row(13), email: 'nonanchor@example.test' }, row(10)] };
        }
        writes.push(params); return { rowCount: 1 };
      } },
      '../utils/emailVerifier': { verifyEmail: async email => {
        pause.assertResearchProviderAllowed('bouncer'); paid.push(email);
        return { valid: true, method: 'bouncer', status: 'valid' };
      } },
    });
    await rejectsPause(() => service.run());
    assert.deepEqual(paid, ['nonanchor@example.test']);
    assert.equal(writes.length, 1);
    assert.equal(writes[0][4], false);
  } finally { process.argv = priorArgv; }
}));

test('explicit malformed backfill identity is rejected before query while armed', async () => withFlag('true', async () => {
  const priorArgv = process.argv;
  process.argv = ['node', 'synthetic-backfill', '--client_id=10invalid'];
  try {
    let queries = 0;
    const service = load('scripts/backfill_email_verification.js', {
      '../db': { query: async () => { queries++; return { rows: [] }; } },
      '../utils/emailVerifier': {},
    });
    await assert.rejects(() => service.run(), { code: 'research_provider_tenant_required' });
    assert.equal(queries, 0);
  } finally { process.argv = priorArgv; }
}));

test('Babrun first-ten fixed-tenant root retains paid helper access while Anchor is paused', async () => withFlag('true', () => withEnvironment({
  DATABASE_URL: 'postgresql://fixture.invalid/offline', ACTIVE_CLIENT_ID: '10',
}, async () => {
  const calls = [];
  const target = { akId: 'fixture', domain: 'firm.example', company: 'Fixture Firm', founder: 'Maya Chen' };
  const service = load('scripts/resolveBabrunFirstTenContacts.js', {
    '../db': {},
    '../leadgen': { runEnrichmentChain: async () => {
      pause.assertResearchProviderAllowed('hunter'); pause.assertResearchProviderAllowed('hunter', 13); calls.push('hunter'); return {};
    } },
    './lib/babrunContactResolution': {
      TENANT_ID: '13', CLIENT_ID: 13, BABRUN_CONTACT_TARGETS: [target], CONTACT_FINAL_STATE: { VERIFIED_FOUNDER_EMAIL: 'VERIFIED_FOUNDER_EMAIL' },
      loadProspectByAkId: async () => ({ client_id: 13 }),
      providerCandidateFromEnrichment: () => null,
      resolveTarget: async () => { pause.assertResearchProviderAllowed('bouncer'); pause.assertResearchProviderAllowed('bouncer', 13); calls.push('bouncer'); return { target, finalState: 'UNRESOLVED' }; },
      persistContactResolution: async () => ({ dryRun: true }),
      formatReportRow: () => ({}), summarizeResults: () => ({ UNRESOLVED: 1 }),
    },
  });
  const result = await service.run({ confirmProduction: true, dryRun: true, akIds: ['fixture'] });
  assert.equal(result.clientId, 13);
  assert.deepEqual(calls, ['hunter', 'bouncer']);
})));

test('Babrun exported resolution helper binds fixed tenant 13 and cross-checks supplied CRM tenant', async () => withFlag('true', async () => {
  let verified = 0;
  const service = load('scripts/lib/babrunContactResolution.js', {
    '../../tieredEnrichmentAgent': { _test: { scrapeWebsite: async () => ({ pages: [] }) } },
    '../../utils/emailVerifier': { verifyEmail: async () => {
      pause.assertResearchProviderAllowed('bouncer'); pause.assertResearchProviderAllowed('bouncer', 13); verified++;
      return { valid: true, status: 'valid', method: 'bouncer' };
    } },
  }, {
    AbortController,
    fetch: async () => ({ ok: true, status: 200, text: async () => '<html>Fixture Firm <a href="mailto:maya@firm.example">Maya Chen</a></html>' }),
  });
  const target = { domain: 'firm.example', company: 'Fixture Firm', founder: 'Maya Chen' };
  const result = await service.resolveTarget(target, { prospect: { client_id: 13 } });
  assert.ok(result.evaluated.length > 0);
  assert.ok(verified > 0);
  await assert.rejects(() => service.resolveTarget(target, { prospect: { client_id: 14 } }), { code: 'research_provider_tenant_mismatch' });
}));

test('web cohort discovery roots retain explicit non-Anchor identity without mutable global authority', async () => withFlag('true', () => withEnvironment({
  GOOGLE_PLACES_KEY: 'fixture-only', SERPAPI_KEY: 'fixture-only', ACTIVE_CLIENT_ID: '10',
}, async () => {
  for (const [file, method] of [
    ['scripts/lib/webCohort002.js', 'discoverCandidatesViaScout'],
    ['scripts/lib/webCohort003.js', 'discoverStratifiedCandidates'],
  ]) {
    let searches = 0;
    let configured = 0;
    const search = async () => { pause.assertResearchProviderAllowed('fixture'); pause.assertResearchProviderAllowed('fixture', 13); searches++; return []; };
    const service = load(file, {
      '../../leadgen': {
        configureScoringContext: async options => { assert.equal(options.client_id, 13); configured++; },
        getSearchQueriesForTarget: () => ['fixture'],
        searchGooglePlaces: search,
        _test: { searchGoogle: search },
      },
    }, { setTimeout: fn => { fn(); return 0; } });
    const result = await service[method](13, { targetSize: 1 });
    assert.equal(result.candidates.length, 0);
    assert.ok(searches > 0);
    const before = configured;
    await rejectsPause(() => service[method](10, { targetSize: 1 }));
    await assert.rejects(() => service[method](null, { targetSize: 1 }), { code: 'research_provider_tenant_required' });
    assert.equal(configured, before);
  }
})));

test('Babrun cohort Places root retains tenant-13 search/detail access and propagates both control catches', async () => withFlag('true', async () => {
  const service = load('scripts/lib/babrunCohort002.js', {
    '../operationalizeAcquisitionProspects': { BABRUN_FIRST_TEN: [] },
  });
  const dedupe = { companies: new Set(), domains: new Set(), akIds: new Set(), founders: new Set() };
  let searches = 0;
  let details = 0;
  const result = await service.discoverViaPlaces('fixture-only', dedupe, 1, {
    textSearch: async () => {
      pause.assertResearchProviderAllowed('places'); pause.assertResearchProviderAllowed('places', 13);
      searches++;
      return { ok: true, data: { results: [{ place_id: 'fixture' }] } };
    },
    placeDetails: async () => {
      pause.assertResearchProviderAllowed('places'); pause.assertResearchProviderAllowed('places', 13);
      details++;
      return { ok: true, data: { result: {} } };
    },
  });
  assert.equal(result.length, 0);
  assert.ok(searches > 0);
  assert.equal(details, searches);
  const control = () => { throw Object.assign(new Error('synthetic control'), { code: 'research_provider_tenant_mismatch' }); };
  await assert.rejects(() => service.discoverViaPlaces('fixture-only', dedupe, 1, { textSearch: control }), { code: 'research_provider_tenant_mismatch' });
  await assert.rejects(() => service.discoverViaPlaces('fixture-only', dedupe, 1, {
    textSearch: async () => ({ ok: true, data: { results: [{ place_id: 'fixture' }] } }),
    placeDetails: control,
  }), { code: 'research_provider_tenant_mismatch' });
}));
