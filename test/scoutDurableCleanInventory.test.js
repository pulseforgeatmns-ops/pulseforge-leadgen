'use strict';
// Offline simulation: real provider/page/ticket/admission/promotion/clean-inventory
// code, real enrichment-worker queue selection and disposable PostgreSQL, with synthetic HTTP, named-owner observations
// and exact-address verifier responses. The components are composed here; the
// full defaultScoutRamp orchestration and actual enrichment waterfall are outside
// this fixture. This is not evidence of live owner yield.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
// PostgreSQL may use loopback TCP; all external transports fail the test even
// if application code catches the thrown error.
let externalAttempts = 0;
const deny = label => { externalAttempts++; throw Error(`Unexpected external transport: ${label}`); };
for (const scheme of ['http', 'https']) {
  require(`node:${scheme}`).request = () => deny(scheme);
  require(`node:${scheme}`).get = () => deny(scheme);
}
globalThis.fetch = () => deny('global.fetch');
require('node:tls').connect = () => deny('tls');
require('node:dgram').createSocket = () => deny('UDP');
const dns = require('node:dns');
for (const name of ['lookup', 'lookupService', 'resolve', 'resolve4', 'resolve6', 'resolveMx', 'resolveTxt',
  'reverse', 'resolveAny', 'resolveCaa', 'resolveCname', 'resolveNaptr', 'resolveNs', 'resolvePtr', 'resolveSoa', 'resolveSrv']) {
  if (typeof dns[name] === 'function') {
    const original = dns[name];
    dns[name] = (host, ...args) => name === 'lookup' && ['127.0.0.1', '::1'].includes(host)
      ? original(host, ...args) : deny(`dns.${name}`);
  }
  if (typeof dns.promises[name] === 'function') {
    const original = dns.promises[name];
    dns.promises[name] = (host, ...args) => name === 'lookup' && ['127.0.0.1', '::1'].includes(host)
      ? original(host, ...args) : deny(`dns.promises.${name}`);
  }
}
const connect = require('node:net').Socket.prototype.connect;
require('node:net').Socket.prototype.connect = function (...args) {
  const input = Array.isArray(args[0]) ? args[0][0] : args[0];
  const host = typeof input === 'object' ? input.host : args[1];
  if (!['127.0.0.1', '::1'].includes(host)) return deny('non-loopback TCP');
  return connect.apply(this, args);
};
process.on('exit', () => { if (externalAttempts) process.exitCode = 1; });
const calls = { places: [], reservations: [], enrich: [], verify: [] };
let localDb = null;
const sharedDb = require.resolve('../db');
require.cache[sharedDb] = { id: sharedDb, filename: sharedDb, loaded: true, exports: { query: async sql => {
  assert.match(sql, /places_api_requests|CREATE TABLE|CREATE INDEX|ALTER TABLE/);
  if (localDb && !/places_api_requests/.test(sql)) return localDb.query(sql);
  return { rows: [] };
} } };
const spend = require.resolve('../utils/apiSpendControl');
require.cache[spend] = { id: spend, filename: spend, loaded: true, exports: {
  reserveApiCall: async row => { calls.reservations.push(row); return 'synthetic-reservation'; },
  settleApiCall: async () => {},
} };
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createCheckpointStore, createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { createPageCheckpointStore } = require('../services/scoutQueryContinuation');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { createCandidateTicketDispatcher } = require('../services/scoutCandidateTicketDispatch');
const { promoteRecord } = require('../scripts/promoteUnenriched');
const { extractWebsiteOwnerCandidates } = require('../utils/websiteOwnerEvidence');
const { run: runEnrichment } = require('../scoutUnenrichedEnrichmentAgent');
const { loadCleanInventory, _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { computeCleanInventoryGrowth } = require('../services/outboundInventory');
const source = { payload: { structuredMission: {
  market: { segment: 'small_business_owners', industry: 'founder_led_smb' },
  geography: { region: 'United States', cities: ['Manchester'], scope: 'local' },
} } };
const request = { tenantId: '13', scope: 'market', segment: 'small_business_owners', evidenceType: 'identity',
  discoveryConcept: 'Painting Contractor', geography: { cities: ['Manchester'], state: 'NH' } };

test('three restarted durable cycles add only independently attributed and exactly verified founder contacts to clean inventory',
  { timeout: 60000 }, async t => {
  const pg = await startDisposablePostgres('scout-clean-flow-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 3 });
  localDb = db;
  t.after(async () => { localDb = null; await closePostgresFixture(db, pg); });
  for (const migration of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql', '2026-10-19-scout-candidate-tickets.sql']) {
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations', migration), 'utf8'));
  }
  await db.query(`CREATE TABLE companies(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id int,name text,
    domain text,website text,industry text,location text,created_at timestamptz,updated_at timestamptz);
    CREATE TABLE prospects(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),company_id uuid,client_id int,
    first_name text,last_name text,email text UNIQUE,phone text,status text,source text,icp_score int,notes text,
    vertical text,service_area_match text,discovery_method text,website_url text,email_verified boolean,
    email_verification_method text,verified_at timestamptz,do_not_contact boolean,email_status text,
    verifier_response jsonb,verifier_checked_at timestamptz,enrichment_provenance jsonb,acquisition_metadata jsonb,
    assigned_ao_id int,closer_id int,last_reply_at timestamptz,last_contacted_at timestamptz,
    updated_at timestamptz DEFAULT now());
    CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,
    vertical text,location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
    CREATE TABLE ao_prospect_tasks(client_id int,prospect_id uuid);
    CREATE TABLE touchpoints(client_id int,prospect_id uuid,action_type text);
    CREATE TABLE agent_log(agent_name text,action text,payload jsonb,status text,error_msg text,ran_at timestamptz,client_id int);
    INSERT INTO scout_unenriched(client_id,company,domain,vertical,enrichment_attempts,last_attempt_at) VALUES
      (13,'Held attempt limit','held-attempt.example','painting',3,now()-interval '2 days'),
      (13,'Held cooldown','held-cooldown.example','painting',1,now()),
      (10,'Held other tenant','held-tenant.example','painting',0,NULL),
      (13,'Held unsupported vertical','held-vertical.example','unknown',0,NULL);`);
  const heldBefore = (await db.query("SELECT * FROM scout_unenriched WHERE domain LIKE 'held-%' ORDER BY id")).rows;
  // No historical analyst/research assets exist in this fixture. All ordinary
  // company/prospect, admission, queue-selector and durable work SQL is executed.
  // Ownership/suppression decisions below are injected; the negatives test the
  // clean loader honoring those decisions, not the production policy SQL.
  const pool = { connect: (...args) => db.connect(...args), query: (sql, params) =>
    /JOIN acquisition_knowledge_objects/.test(sql) ? Promise.resolve({ rows: [] }) : db.query(sql, params) };
  let suppress = null; let owned = null;
  const store = { tenantId: '13', clientId: 13,
    candidateOwnership: async candidate => candidate.prospectId === owned ? 'prior_contact_or_human_owned' : null,
    suppression: async candidate => candidate.prospectId === suppress ? 'suppressed' : null };
  const fingerprint = 'synthetic-approved-painting-mission';
  let clock = Date.now();
  const fetchImpl = async input => {
    const url = new URL(input); calls.places.push(url.pathname);
    if (url.pathname.includes('details')) {
      const id = url.searchParams.get('place_id');
      return { status: 200, ok: true, json: async () => ({ status: 'OK', result: {
        place_id: id, name: `Fixture Painting Contractor ${id}`, website: `https://${id}.example`,
        formatted_address: 'Manchester, NH, United States', types: ['painter'],
      } }) };
    }
    const page = Number((url.searchParams.get('pagetoken') || 'page-0').split('-').at(-1));
    assert.ok(page >= 0 && page <= 2);
    return { status: 200, ok: true, json: async () => ({ status: 'OK',
      results: ['owner', 'employee', 'unverified', 'role'].map(kind => ({
        place_id: `${kind}${page}`, name: `Fixture Painting Contractor ${kind}${page}`,
        formatted_address: 'Manchester, NH, United States', types: ['painter'],
      })), next_page_token: page < 2 ? `opaque-page-${page + 1}` : null,
    }) };
  };
  async function cycle() {
    // Reconstruct sessions/stores and discard provider response cache; durable
    // work is restored from PostgreSQL rather than in-memory processing state.
    require('../utils/placesQueryCache').clearPlacesQueryCache();
    const tickets = createCandidateTicketStore({ pool, tenantId: '13', fingerprint });
    const pages = createPageCheckpointStore({ db: pool, tenantId: '13', fingerprint, now: () => clock });
    await tickets.assertReady(); await pages.assertReady();
    const session = createCheckpointSession({
      store: createCheckpointStore({ db: pool, tenantId: '13', fingerprint }), ticketStore: tickets, pageStore: pages,
    });
    const provider = createPlacesProvider({ apiKey: 'synthetic-key', fetchImpl, queryCheckpointSession: session });
    const candidates = await provider.collectEvidence(request);
    const dispatcher = createCandidateTicketDispatcher({ ticketStore: tickets, tenantId: '13', dependencyKey: 'fixture-dependencies-v1',
      persist: (input, ticketPass) => persistDiscoveredCompanies(pool, store, { ...input, tenantId: '13',
        scoutContext: { ticketPass, scope: { segment: 'small_business_owners', cities: ['Manchester'], region: 'United States' },
          allowedCities: ['Manchester'] } }),
    });
    const admission = await dispatcher.drain({ companies: candidates });
    const enrichment = await runEnrichment({ db: pool, client_id: 13, limit: 5, retryHours: 1, maxAttempts: 3,
      verticals: ['painting'], promote: async (record, { db: selectedDb }) => {
      const expectedEmail = `${record.domain.startsWith('role') ? 'info' : 'maya'}@${record.domain}`;
      const result = await promoteRecord(record, { db: selectedDb,
        loadClientConfig: async () => ({ service_area: ['Manchester'] }),
        enrich: async (domain, role, options) => {
          assert.equal(domain, record.domain); assert.equal(role, 'owner'); assert.equal(options.ownerEvidence, true);
          calls.enrich.push(domain);
          if (domain.startsWith('owner') || domain.startsWith('unverified')) {
            const text = `<html><body><div class="team-member"><h3>Maya Chen</h3><p class="role">Owner</p><a href="mailto:${expectedEmail}">${expectedEmail}</a></div></body></html>`;
            const observations = extractWebsiteOwnerCandidates([{ url: `https://${domain}/about`, text }], domain);
            assert.equal(observations.length, 1);
            assert.equal(observations[0].email, expectedEmail);
            return { ...observations[0], source: ['website_email'] };
          }
          // Deliberately supplied negative observations must not gain ownership
          // merely because they are deliverable and on the official domain.
          return { email: expectedEmail, contact: 'Maya Chen', title: domain.startsWith('employee') ? 'Receptionist' : 'Owner',
            source: ['website_email'], sourceUrl: `https://${domain}/about` };
        },
        verify: async (email, lead) => {
          assert.equal(email, expectedEmail); assert.equal(lead.email, expectedEmail);
          calls.verify.push(email);
          return { emailVerified: !record.domain.startsWith('unverified'),
            emailStatus: record.domain.startsWith('unverified') ? 'unknown' : 'valid',
            emailVerificationMethod: 'synthetic-exact-address-fixture', verifiedAt: new Date(clock).toISOString(),
            verifierCheckedAt: new Date(clock).toISOString(), doNotContact: false };
        },
      });
      assert.equal(result.promoted, true, 'CRM promotion alone does not mean clean inventory');
      return result;
    } });
    clock += 6000;
    return { admission, enrichment, inventory: await loadCleanInventory(pool, store, source, 13), session, provider,
      work: await tickets.snapshot({ dependencyKey: 'fixture-dependencies-v1' }) };
  }
  let before = [];
  for (let index = 0; index < 3; index++) {
    const result = await cycle();
    assert.equal(result.admission.inserted, 4);
    assert.equal(result.enrichment.considered, 4); assert.equal(result.enrichment.promoted, 4);
    assert.equal(result.inventory.clean.length, index + 1);
    assert.equal(computeCleanInventoryGrowth(before, result.inventory.clean).newCleanInventoryAdded, 1);
    assert.ok(result.inventory.clean.every(row => row.contactClassification === 'VERIFIED_FOUNDER_EMAIL'));
    assert.equal(result.inventory.excluded.length, (index + 1) * 3);
    assert.equal(result.work.byState.handled, (index + 1) * 4);
    before = result.inventory.clean;
  }
  assert.equal(calls.places.length, 15); assert.equal(calls.reservations.length, 15);
  assert.equal(calls.enrich.length, 12); assert.equal(calls.verify.length, 12);
  const persisted = (await db.query('SELECT email,email_verified,acquisition_metadata FROM prospects')).rows;
  assert.equal(persisted.filter(row => row.email_verified).length, 9, 'deliverability alone leaves six non-founder contacts excluded');
  assert.equal(persisted.filter(row => row.acquisition_metadata.contactResolution.finalState === 'VERIFIED_FOUNDER_EMAIL').length, 3);
  assert.ok(persisted.filter(row => row.email.includes('@owner')).every(row => row.acquisition_metadata.contactResolution.websiteEvidence?.type === 'html_person_record'));
  assert.equal(externalAttempts, 0);
  const cost = calls.reservations.reduce((sum, row) => sum + row.estimatedUsd, 0);
  assert.ok(Math.abs(cost - 0.42) < 1e-9, 'Places-only synthetic rate inputs; excludes enrichment/verifier costs and actual billing');
  const frozenCounts = JSON.stringify(calls);
  for (let index = 0; index < 3; index++) {
    const result = await cycle();
    assert.equal(result.admission.inserted, 0); assert.equal(result.inventory.clean.length, 3);
    assert.equal(result.enrichment.considered, 0);
    assert.equal(result.provider.lastExecution.abortReason, 'finite_query_exhausted');
    assert.equal(result.work.due, 0);
  }
  assert.equal(JSON.stringify(calls), frozenCounts, 'exhausted cycles do not repeat any provider or enrichment work');
  assert.deepEqual((await db.query("SELECT * FROM scout_unenriched WHERE domain LIKE 'held-%' ORDER BY id")).rows, heldBefore,
    'actual selector preserves attempt-limit, cooldown, vertical and other-tenant holds without enrichment');
  const selected = before[0];
  await db.query('UPDATE prospects SET email=$2 WHERE id=$1', [selected.prospectId, `different@${selected.domain}`]);
  let checked = await loadCleanInventory(pool, store, source, 13);
  assert.equal(checked.clean.length, 2);
  assert.equal(checked.excluded.find(row => row.prospectId === selected.prospectId)?.reason, 'contact_classification_email_changed');
  await db.query('UPDATE prospects SET email=$2 WHERE id=$1', [selected.prospectId, selected.email]);
  suppress = selected.prospectId;
  assert.equal((await loadCleanInventory(pool, store, source, 13)).clean.length, 2);
  suppress = null; owned = selected.prospectId;
  assert.equal((await loadCleanInventory(pool, store, source, 13)).clean.length, 2);
  owned = null;
  await db.query('UPDATE prospects SET do_not_contact=true WHERE id=$1', [selected.prospectId]);
  assert.equal((await loadCleanInventory(pool, store, source, 13)).clean.length, 2);
});
