'use strict';

// Execute actual standard Scout function bodies with every I/O boundary injected.
// Never import leadgen/db/app startup or permit a provider/network call.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const evidence = require('../../utils/babrunScoutContactEvidence');
const source = fs.readFileSync(path.join(__dirname, '../../leadgen.js'), 'utf8');
const saveBody = source.slice(source.indexOf('async function saveToDatabase('), source.indexOf('\nasync function ensureSetterQueueColumns('));
const waterfall = source.slice(source.indexOf('  // 3. Enrich with email waterfall'), source.indexOf("  if (CONFIG.vertical === 'probate_attorney')"));
const projection = source.slice(source.indexOf('  // 4. Fill missing fields'), source.indexOf('  const postEnrichmentAccepted = []'));
assert.ok(saveBody.startsWith('async function saveToDatabase('));
assert.ok(waterfall.includes('applyScoutEnrichment('));
assert.ok(projection.includes('scoutContactEvidence'));
const noop = async () => {};
const verificationReceipt = {
  emailVerified: true, emailStatus: 'valid', emailVerificationMethod: 'fixture_verifier',
  verifiedAt: '2026-10-14T00:00:00.000Z', verifierCheckedAt: '2026-10-14T00:00:00.000Z',
  verifierResponse: { status: 'deliverable', fixture: true }, doNotContact: false,
};
const leadFixture = () => ({ company: 'Fixture Painting', url: 'https://fixturepainting.example',
  address: 'Denver, CO', source: ['google_places'], score: 80, places_locality: 'Denver' });
const ownerFixture = () => ({ email: 'ada@fixturepainting.example', contact: 'Ada Lovelace',
  title: 'Owner', sourceUrl: 'https://fixturepainting.example/about' });

async function runScoutWaterfall(lead, { clientId = 13, prospeo = null, hunter = ownerFixture(), scraped = null } = {}) {
  const calls = [], scrapeOptions = [];
  const context = { ...evidence, leads: [lead], CONFIG: { clientId, vertical: 'painting' },
    console: { log() {} }, process: { stdout: { write() {} } }, setTimeout: fn => fn(),
    getClientConfig: async () => ({}), normalizeDomain: v => new URL(v).hostname,
    enrichWithProspeo: async () => { calls.push('prospeo'); return prospeo; },
    enrichWithHunter: async () => { calls.push('hunter'); return hunter; },
    scrapeWebsiteEmail: async (domain, options) => { calls.push('scraped'); scrapeOptions.push(options); return scraped; },
    recordEmailEnrichmentMethod: noop, detectType: () => 'local_business', scoreLead: l => l.score,
  };
  vm.createContext(context);
  await vm.runInContext(`(async () => { ${waterfall}\n${projection}\nreturn leads; })()`, context);
  return { lead: context.leads[0], calls, scrapeOptions };
}

function loadScoutSave({ db, clientId = 13, verify = async () => verificationReceipt,
  exclusion = null, companyId = 1, overrides = {} } = {}) {
  const skips = [], writes = [];
  const context = { ...evidence, module: { exports: {} },
    console: { log() {}, error(...args) { throw new Error(args.join(' ')); } },
    CONFIG: { clientId, location: 'United States', vertical: 'painting' }, CLIENT_CONFIG: {}, pool: db,
    ensureSetterQueueColumns: noop, ensureCompanyColumns: noop, ensureEmailVerificationColumns: noop,
    ensureScoutUnenrichedTable: noop, ensureScoutSkipLogTable: noop,
    normalizeDomain: value => new URL(value).hostname,
    scoutDiscoveryMethod: () => 'google_places', establishBusinessIdentity: () => ({ established: true }),
    validateProspect: () => true, configuredServiceAreas: () => [],
    matchServiceAreaFromLocation: () => 'United States', matchServiceAreaLocality: () => 'United States',
    resolveScoutContactCandidate: lead => ({ insertTarget: 'prospect', email: lead.email }),
    shouldExcludeProspect: async () => ({ excluded: Boolean(exclusion), reason: exclusion }),
    resolveEmailVerification: verify, logExcludedProspect: noop, logScoutRun: noop,
    sanitizeFirstName: value => value, isCleaningBuyerProfile: () => false,
    isWebDesignProfile: () => false, isStudioSubstralScoutProfile: () => false,
    getScoutPreferredChannel: () => 'email', findOrCreateCompany: async () => companyId,
    safeIngestScoutLifecycleSignal: noop, safeWriteProspect: value => writes.push(value),
    OPERATIONAL_EVENTS: { CONTACT_DISCOVERED: 'fixture' }, recordScoutBaseline: noop,
    setSetterVisibility: async () => ({ setter_visible: false }),
    incrementBreakdown: (counts, reason) => { counts[reason] = (counts[reason] || 0) + 1; },
    SCOUT_SKIP_REASONS: { DUPLICATE: 'duplicate', INSERT_CONFLICT: 'insert_conflict', DB_ERROR: 'db_error' },
    persistScoutSkip: async (...args) => skips.push(args), ...overrides,
  };
  vm.runInNewContext(saveBody + '\nmodule.exports = saveToDatabase;', context);
  return { save: context.module.exports, skips, writes };
}

function recordingDb({ duplicate = false, conflict = false } = {}) {
  const calls = [], rows = [];
  return { calls, rows, query: async (sql, params) => {
    calls.push({ sql, params });
    if (/INSERT INTO prospects/.test(sql)) {
      if (conflict) return { rows: [], rowCount: 0 };
      const columns = sql.match(/INSERT INTO prospects\s*\(([\s\S]*?)\)\s*VALUES/)[1].split(',').map(x => x.trim());
      assert.equal(columns.length, params.length);
      const row = Object.fromEntries(columns.map((key, i) => [key,
        ['acquisition_metadata', 'enrichment_provenance', 'verifier_response'].includes(key) ? JSON.parse(params[i]) : params[i]]));
      rows.push(row); return { rows: [{ id: 1 }], rowCount: 1 };
    }
    if (/SELECT p.id/.test(sql)) return { rows: duplicate ? [{ id: 99 }] : [] };
    if (/UPDATE prospects\s+SET setter_status/.test(sql)) return { rows: [] };
    throw new Error('Unexpected fixture SQL');
  } };
}
module.exports = { runScoutWaterfall, loadScoutSave, recordingDb, verificationReceipt, leadFixture, ownerFixture };
