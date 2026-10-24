'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createFakePool } = require('./helpers/scoutEvidenceCursorFakePool');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true,
  exports: { query: async () => { throw Error('Unexpected shared database access'); } } };
const recoveryPath = require.resolve('../services/sameCompanyContactRecovery');
const recovery = require(recoveryPath);
let recoveryCalls = [];
require.cache[recoveryPath].exports = { ...recovery, attemptSameCompanyAlternateRecovery: async (_store, _pool, input) => {
  recoveryCalls.push({ name: input.company.name, tenantId: input.tenantId });
  return { ok: false, reason: 'no_alternate_contact_found', telemetry: recovery.emptyAlternateContactTelemetry() };
} };
const { _test: { persistDiscoveredCompanies }, defaultScoutRamp } = require('../services/maxOutboundControlLoop');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const { createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { candidateKey, createBusinessEvidenceCursorStore } = require('../services/scoutBusinessEvidenceCursor');

function candidate(i, overrides = {}) {
  return { id: `p${i}`, tenantId: '10', name: `Fixture ${i}`, website: `https://fixture${i}.example`,
    location: 'Manchester, NH', address: 'Manchester, NH',
    industry: 'Property Management Manchester NH', discoveryQuery: 'Property Management Manchester NH',
    discoveryConcept: 'Property Management', ...overrides };
}
const context = { tenantId: '10', scoutContext: { authorizedTenantId: '10', clientId: 10,
  scope: { eligibleSubsegments: ['property_manager'] }, allowedCities: ['manchester'], investigateBusinessEvidence: true } };
const unusableProspect = { id: 'canonical', company_id: 'existing-company', email: 'old@fixture.example',
  email_verified: false, email_status: 'unknown', do_not_contact: false, name: 'Existing', domain: 'fixture.example' };
function fixture({ sameCompanies = new Set(), ownership = null, insert = null } = {}) {
  const queued = new Map(); const observations = [];
  const pool = createFakePool({ onQuery: async (sql, p) => {
    observations.push({ sql, params: p });
    if (/FROM prospects p/.test(sql)) {
      assert.equal(p[3], 10);
      return { rows: ownership ? ownership(p[2]) : sameCompanies.has(p[2]) ? [unusableProspect] : [] };
    }
    if (/INSERT INTO scout_unenriched/.test(sql)) {
      if (insert) await insert(p);
      assert.equal(p[7], 10);
      if (queued.has(p[2])) return { rowCount: 0, rows: [] };
      queued.set(p[2], { id: `q${queued.size}`, client_id: 10, eligibility_bucket: 'attempt_limit_hold' });
      return { rowCount: 1, rows: [{ id: queued.get(p[2]).id }] };
    }
    if (/FROM scout_unenriched u/.test(sql)) return { rows: [queued.get(p[1])].filter(Boolean) };
    throw Error(`Unexpected persistence SQL: ${sql}`);
  } });
  const store = { tenantId: '10', clientId: 10, candidateOwnership: async () => null,
    suppression: async () => null, one: async () => null };
  return { pool, store, queued, observations,
    run: rows => createBusinessEvidenceCursorStore({ pool, tenantId: '10', fingerprint: 'fixture-mission' })
      .withPass(evidenceCursor => persistDiscoveredCompanies(pool, store, {
        ...context, companies: rows, scoutContext: { ...context.scoutContext, evidenceCursor },
      })),
  };
}
function website(t, fn) {
  const file = require.resolve('../services/scoutWebsiteBusinessEvidence');
  const original = require.cache[file];
  require.cache[file] = { id: file, filename: file, loaded: true, exports: { acquireBusinessEvidence: fn } };
  t.after(() => { if (original) require.cache[file] = original; else delete require.cache[file]; });
}

test('real persistence reaches 41 clear identities in three capped passes without discarding payload or changing input order', async t => {
  const rows = Array.from({ length: 41 }, (_, i) => candidate(i));
  rows.push(candidate(0, { id: 'alias0', website: 'https://WWW.FIXTURE0.EXAMPLE/about' }));
  const original = structuredClone(rows); const all = new Set(); let visited = [];
  website(t, async company => { visited.push(candidateKey(company)); return null; });
  const f = fixture();
  for (let pass = 1; pass <= 3; pass++) {
    visited = []; const result = await f.run(rows);
    assert.equal(visited.length, 20); assert.equal(new Set(visited).size, 20);
    assert.deepEqual(visited, [...new Set(rows.map(candidateKey))].filter(key => visited.includes(key)));
    visited.forEach(key => all.add(key));
    assert.equal(result.result.checkpointComplete, false);
    assert.equal(result.result.admission.websiteInvestigations, 20);
    assert.equal(result.progression.sequence, pass);
  }
  assert.equal(all.size, 41); assert.deepEqual(rows, original); assert.equal(f.queued.size, 0);
  assert.ok(f.observations.every(row => !/UPDATE|DELETE/.test(row.sql)));
});

test('ownership becoming same-company after preselection blocks website and recovery work', async t => {
  let ownershipReads = 0; let sites = 0; recoveryCalls = [];
  const f = fixture({ ownership: () => ++ownershipReads === 1 ? [] : [unusableProspect] });
  website(t, async () => { sites++; throw Error('must not investigate changed ownership'); });
  const result = await f.run([candidate(1)]);
  assert.equal(ownershipReads, 2); assert.equal(sites, 0); assert.deepEqual(recoveryCalls, []);
  assert.equal(result.result.checkpointComplete, false);
  assert.equal(result.progression.investigated, 0);
});

test('unknown/failed ownership or conflicting store bindings never become clear cursor work', async t => {
  let sites = 0; website(t, async () => { sites++; return null; });
  const f = fixture({ ownership: () => { throw Error('ownership unavailable'); } });
  await assert.rejects(f.run([candidate(1)]), /ownership unavailable/);
  assert.equal(f.pool.events.size, 0);
  const g = fixture(); g.store.tenantId = '13';
  await assert.rejects(g.run([candidate(1)]), /binding mismatch/);
  assert.equal(g.observations.length, 0); assert.equal(sites, 0);
});

test('cursor adds no alternate recovery even within tenant10; already classified recovery retains its normal order', async t => {
  const rows = [candidate(0, { name: 'Ambiguous Existing' }),
    candidate(1, { name: 'Known Existing', description: 'We provide property management services' }), candidate(2)];
  const sameCompanies = new Set(['Ambiguous Existing', 'Known Existing']);
  website(t, async company => ({ candidate: { ...company, description: 'We provide property management services' },
    evidence: { source: 'synthetic' } }));
  recoveryCalls = []; const baseline = fixture({ sameCompanies });
  await persistDiscoveredCompanies(baseline.pool, baseline.store, { ...context, companies: rows });
  assert.deepEqual(recoveryCalls.map(row => row.name), ['Ambiguous Existing', 'Known Existing']);
  recoveryCalls = []; const governed = fixture({ sameCompanies });
  const result = await governed.run(rows);
  assert.deepEqual(recoveryCalls, [{ name: 'Known Existing', tenantId: '10' }]);
  assert.equal(result.result.checkpointComplete, false);
  assert.equal(result.progression.selected, 1);
});

for (const fault of ['insertFailure', 'uncertainCommit']) test(`cursor ${fault} preserves staged checkpoint and withholds acknowledgment`, async t => {
  website(t, async company => ({ candidate: { ...company, description: 'We provide property management services' },
    evidence: { source: 'synthetic' } }));
  const f = fixture(); f.pool.faults[fault] = true;
  let state = 'fetched'; let payload = [candidate(1)]; let acknowledgments = 0;
  const checkpoint = createCheckpointSession({ store: { tenantId: '10',
    load: async () => ({ state, candidates: payload }),
    acknowledge: async () => { acknowledgments++; state = 'handled'; payload = []; },
  } });
  await checkpoint.lookup('Property Management Manchester NH', false);
  const universe = await constructCandidateUniverse({
    searchDefinition: { tenantId: '10', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
      segments: ['property_management'], businessNeed: 'commercial_cleaning' },
    forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [{ id: 'public_business_places', sourceType: 'public_business_data', available: () => true,
      discover: async () => ({ candidates: payload, errors: [] }) }],
    persistCompanies: async input => (await f.run(input.companies)).result,
    acknowledgeDiscovery: () => checkpoint.acknowledge(),
  });
  assert.ok(universe.discoveryErrors.some(error => error.code === 'persist_failed'));
  assert.equal(acknowledgments, 0); assert.equal(state, 'fetched'); assert.equal(payload.length, 1);
  assert.equal(f.queued.size, 1, 'earlier durable ingestion remains safely deduplicated on replay');
  delete f.pool.faults[fault]; const restart = await f.run(payload);
  assert.equal(restart.result.inserted, 0); assert.equal(restart.result.admission.alreadyQueued, 1);
  assert.equal(restart.result.admission.queueMatches.firstFailureCounts.attempt_limit_hold, 1);
  assert.equal(restart.progression.sequence, fault === 'uncertainCommit' ? 2 : 1);
});

test('partial ingestion interruption leaves no cursor receipt and safely replays queue duplicates', async t => {
  website(t, async company => ({ candidate: { ...company, description: 'We provide property management services' },
    evidence: { source: 'synthetic' } }));
  let inserts = 0; let fail = true;
  const f = fixture({ insert: () => { if (++inserts === 2 && fail) throw Error('partial SQL interruption'); } });
  await assert.rejects(f.run([candidate(1), candidate(2)]), /partial SQL interruption/);
  assert.equal(f.pool.events.size, 0); assert.equal(f.queued.size, 1);
  fail = false; const result = await f.run([candidate(1), candidate(2)]);
  assert.equal(result.result.inserted, 1); assert.equal(result.result.admission.alreadyQueued, 1);
  assert.equal(result.progression.sequence, 1);
});

test('governed ramp supplies exact unchanged mission fingerprint and returns confirmed evidence progress', async t => {
  const f = fixture(); website(t, async () => null);
  const fingerprint = 'approved-fingerprint-unchanged';
  const output = await defaultScoutRamp({ pool: f.pool, store: f.store,
    program: { id: 'program', tenant_id: '10', policy: {} },
    source: { payload: { structuredMission: { market: { eligibleSubsegments: ['property_manager'], segment: 'property_manager' },
      geography: { region: 'Manchester NH', cities: ['Manchester'], scope: 'local' } } } },
    plan: { deficit: 1 }, searchFingerprint: fingerprint, skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
    loadDiscoveryBackoff: async () => null,
    createQueryCheckpointSession: () => ({ acknowledge: async () => { throw Error('unexpected acknowledgment'); },
      snapshot: () => ({ status: 'pending_query_work' }) }),
    createBusinessEvidenceCursorStore: options => {
      assert.equal(options.fingerprint, fingerprint); assert.equal(options.tenantId, '10');
      return createBusinessEvidenceCursorStore(options);
    },
    runDiscovery: async (_input, options) => { await options.persistCompanies({ companies: [candidate(1)] }); return {}; },
    logger: { log() {}, warn() {} },
  });
  assert.equal(output.searchFingerprint, fingerprint);
  assert.deepEqual(output.evidenceProgression, { status: 'advanced', selected: 1, investigated: 1, advanced: true, sequence: 1 });
  assert.equal(output.queryProgression.status, 'pending_query_work');
});

test('cursor receipt survives acknowledgment interruption without losing payload or resetting queue attempts', async t => {
  website(t, async company => ({ candidate: { ...company, description: 'We provide property management services' },
    evidence: { source: 'synthetic' } }));
  const f = fixture(); const original = [candidate(1)]; let payload = structuredClone(original);
  let attempts = 0; let fail = true;
  const run = () => constructCandidateUniverse({
    searchDefinition: { tenantId: '10', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
      segments: ['property_management'], businessNeed: 'commercial_cleaning' },
    forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [{ id: 'public_business_places', sourceType: 'public_business_data', available: () => true,
      discover: async () => ({ candidates: payload, errors: [] }) }],
    persistCompanies: async input => (await f.run(input.companies)).result,
    acknowledgeDiscovery: async () => { attempts++; if (fail) throw Error('acknowledgment interrupted'); payload = []; },
  });
  const first = await run();
  assert.ok(first.discoveryErrors.some(error => error.code === 'persist_failed'));
  assert.equal(attempts, 1); assert.deepEqual(payload, original); assert.equal(f.pool.events.size, 1);
  assert.equal(f.queued.size, 1);
  fail = false; const second = await run();
  assert.deepEqual(second.discoveryErrors, []); assert.equal(attempts, 2); assert.deepEqual(payload, []);
  assert.equal(f.pool.events.size, 2); assert.equal(f.queued.size, 1);
  assert.equal([...f.queued.values()][0].eligibility_bucket, 'attempt_limit_hold');
  assert.ok(f.observations.every(row => !/UPDATE|DELETE/.test(row.sql)));
});

for (const fault of ['busy', 'unconfirmed']) test(`governed ramp exposes sanitized ${fault} progress when persistence fails`, async t => {
  const f = fixture(); f.pool.faults[fault] = true; website(t, async () => null);
  const logs = []; const events = []; f.store.event = async (_type, _key, payload) => events.push(payload);
  const fingerprint = 'private-fixture-fingerprint';
  const output = await defaultScoutRamp({ pool: f.pool, store: f.store,
    program: { id: 'program', tenant_id: '10', policy: {} },
    source: { payload: { structuredMission: { market: { eligibleSubsegments: ['property_manager'], segment: 'property_manager' },
      geography: { region: 'Manchester NH', cities: ['Manchester'], scope: 'local' } } } },
    plan: { deficit: 1 }, searchFingerprint: fingerprint, skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
    loadDiscoveryBackoff: async () => null,
    createQueryCheckpointSession: () => ({ acknowledge: async () => { throw Error('unexpected acknowledgment'); },
      snapshot: () => ({ status: 'pending_query_work' }) }),
    runDiscovery: async (_input, options) => {
      // CandidateUniverse reports a persistence error instead of propagating it.
      await assert.rejects(options.persistCompanies({ companies: [candidate(1)] })); return {};
    },
    logger: { log: (...args) => logs.push(args), warn() {} },
  });
  assert.deepEqual(output.evidenceProgression, { status: fault === 'busy' ? 'busy' : 'failed',
    selected: 1, investigated: 1, advanced: false, sequence: null });
  assert.equal(output.admissionMeasured, false);
  assert.equal(output.queryProgression.status, 'pending_query_work');
  const queryEvent = events.find(event => event.evidenceProgression);
  assert.deepEqual(queryEvent.evidenceProgression, output.evidenceProgression);
  const queryLog = logs.find(([label]) => label === '[max-outbound-control] Scout query progression');
  assert.deepEqual(JSON.parse(queryLog[1]).evidenceProgression, output.evidenceProgression);
  assert.ok(!JSON.stringify([...events, ...logs]).includes(fingerprint));
  assert.ok(!JSON.stringify([...events, ...logs]).includes(candidateKey(candidate(1))));
});
