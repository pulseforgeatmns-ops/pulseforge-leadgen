'use strict';
// SPEC-SCOUT-P0-007 (#1072): Scout must turn an inventory deficit into net-new,
// deduplicated, qualified inventory and hand only that to enrichment. Real
// disposable PostgreSQL with real CRM tables (companies/prospects), the durable
// candidate-ticket ledger and the Scout queue. Synthetic discovery only: no
// Places, website, enrichment provider or production database is reached.
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true,
  exports: { query: async () => { throw Error('Shared database forbidden'); } } };

// Website evidence is the only thing that classifies these synthetic names.
// Count every crawl: a known identity must never be crawled again.
const websitePath = require.resolve('../services/scoutWebsiteBusinessEvidence');
const website = require(websitePath);
const crawled = [];
require.cache[websitePath].exports = { ...website, acquireBusinessEvidence: async (candidate, context, options) => {
  crawled.push(candidate.name);
  return website.acquireBusinessEvidence(candidate, context, { ...options, fetchPage: async url => ({ ok: true, url,
    text: '<html><h1>Local operator</h1><p>We provide residential cleaning services in Manchester.</p></html>' }) });
} };
const recoveryPath = require.resolve('../services/sameCompanyContactRecovery');
const recovery = require(recoveryPath);
require.cache[recoveryPath].exports = { ...recovery, attemptSameCompanyAlternateRecovery: async () => {
  throw Error('Durable recovery must not call the unfenced provider helper');
} };

const { defaultScoutRamp, replenishmentScorecard, _test: { replenishmentOutcome } } = require('../services/maxOutboundControlLoop');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { PLAN_VERSION } = require('../services/scoutQueryCheckpoints');

const TENANT = '13';
const source = { payload: { structuredMission: {
  market: { segment: 'small_business_owners', industry: 'founder_led_smb', capability: 'business_transformation' },
  geography: { region: 'Manchester NH', cities: ['Manchester'], scope: 'local' } } } };
const program = { id: 'p0-007-program', tenant_id: TENANT, policy: {} };
const raw = (id, name, websiteUrl = `https://${id}.example`, extra = {}) => ({ placeId: `place-${id}`, companyName: name,
  website: websiteUrl, address: 'Manchester, NH', industry: 'Small Business Manchester NH',
  discoveryQuery: 'Small Business Manchester NH', discoveryConcept: 'Small Business', ...extra });

test('SPEC-SCOUT-P0-007: deficit → discovery → identity check → qualification → net-new queue → enrichment handoff',
  { timeout: 120000 }, async t => {
    const previous = process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
    process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = 'true';
    const instance = await startDisposablePostgres('scout-p0-007-');
    const db = new Pool({ connectionString: instance.connectionString, max: 4 });
    t.after(async () => { if (previous == null) delete process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
      else process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = previous;
      await db.end(); await instance.stop(); });
    for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-candidate-tickets.sql']) {
      await db.query(fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8'));
    }
    await db.query(`CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,
      domain text,vertical text,location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());
    CREATE TABLE companies(id text PRIMARY KEY,client_id int NOT NULL,name text,domain text,website text,location text);
    CREATE TABLE prospects(id text PRIMARY KEY,client_id int NOT NULL,company_id text REFERENCES companies(id),email text,
      email_verified boolean,email_status text,do_not_contact boolean DEFAULT false,enrichment_provenance jsonb,acquisition_metadata jsonb,
      assigned_ao_id int,closer_id int,last_contacted_at timestamptz,last_reply_at timestamptz,service_area_match text,vertical text,
      updated_at timestamptz DEFAULT now());
    CREATE TABLE ao_prospect_tasks(client_id int,prospect_id text);
    CREATE TABLE touchpoints(client_id int,prospect_id text,action_type text);`);

    // ---- CRM + discovery memory that Scout must recognize as previously known.
    // Existing prospect, contacted three days ago.
    await db.query(`INSERT INTO companies VALUES('c-prospect',13,'Fresh Prospect Co','fresh-prospect.example',NULL,'Manchester, NH')`);
    await db.query(`INSERT INTO prospects(id,client_id,company_id,email,last_contacted_at)
      VALUES('p-prospect',13,'c-prospect','owner@fresh-prospect.example',now()-interval '3 days')`);
    // Existing customer: domain only in a full URL with www/case/path, closer-owned.
    await db.query(`INSERT INTO companies VALUES('c-customer',13,'Fresh Customer LLC',NULL,'HTTPS://WWW.Fresh-Customer.example/home','Manchester, NH')`);
    await db.query(`INSERT INTO prospects(id,client_id,company_id,email,closer_id) VALUES('p-customer',13,'c-customer','ap@fresh-customer.example',5)`);
    // CRM company with no prospect row (import/contactless customer), www-prefixed domain.
    await db.query(`INSERT INTO companies VALUES('c-contactless',13,'Fresh Imported Co','WWW.fresh-imported.example',NULL,NULL)`);
    // Same name, different domain and different town: a different business.
    await db.query(`INSERT INTO companies VALUES('c-twin',13,'Fresh Twin Services','twin-denver.example',NULL,'Denver, CO')`);
    // Already in the enrichment queue and cooling down after an attempt.
    await db.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,location,source,enrichment_attempts,last_attempt_at)
      VALUES(13,'Fresh Queued Co','https://fresh-queued.example','fresh-queued.example','cleaning','Manchester, NH','max_buffer_replenishment',1,now())`);
    // Another tenant's CRM and queue hold this domain; it is still new to tenant 13.
    await db.query(`INSERT INTO companies VALUES('c-other-tenant',1,'Fresh Isolated Co','fresh-isolated.example',NULL,'Manchester, NH')`);
    await db.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,source,enrichment_attempts)
      VALUES(1,'Fresh Isolated Co','https://fresh-isolated.example','fresh-isolated.example','cleaning','max_buffer_replenishment',0)`);

    const events = [];
    const store = { tenantId: TENANT, clientId: 13, candidateOwnership: async () => null,
      suppression: async () => null, one: async () => null, event: async (...args) => events.push(args) };
    const fingerprint = 'p0-007-approved-mission';
    const makeTickets = () => createCandidateTicketStore({ pool: db, tenantId: TENANT, fingerprint });
    async function stage(queryKey, candidates) {
      await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates)
        VALUES($1,$2,$3,$4,'fetched',$5::jsonb)`, [TENANT, fingerprint, queryKey, PLAN_VERSION, JSON.stringify(candidates)]);
      return (await makeTickets().transferQuery(queryKey)).candidates;
    }
    // "Paid" enrichment stand-in: it is handed the queue and records every row
    // it would spend on, then promotes it into the CRM (the durable handoff).
    const enriched = [];
    const enrichment = { run: async ({ client_id: clientId, db: pool, limit }) => {
      const rows = (await pool.query(`SELECT id,company,domain FROM scout_unenriched WHERE client_id=$1
        AND COALESCE(enrichment_attempts,0)=0 ORDER BY id LIMIT $2`, [clientId, limit])).rows;
      for (const row of rows) {
        enriched.push(row.domain);
        await pool.query(`UPDATE scout_unenriched SET enrichment_attempts=1,last_attempt_at=now() WHERE id=$1`, [row.id]);
        await pool.query(`INSERT INTO companies VALUES($1,$2,$3,$4,NULL,'Manchester, NH')`, [`c-q${row.id}`, clientId, row.company, row.domain]);
        await pool.query(`INSERT INTO prospects(id,client_id,company_id,email,email_verified) VALUES($1,$2,$3,$4,true)`,
          [`p-q${row.id}`, clientId, `c-q${row.id}`, `owner@${row.domain}`]);
      }
      return { considered: rows.length, promoted: rows.length, emailResolved: rows.length, emailVerified: rows.length };
    } };
    async function run({ backoff = true, candidates = null } = {}) {
      return defaultScoutRamp({ pool: db, store, program, source, plan: { deficit: 10 },
        searchFingerprint: fingerprint, skipVerificationRetry: true, enrichment, recoverExistingInventory: async () => ({}),
        createCandidateTicketStore: () => makeTickets(), createPageCheckpointStore: () => ({ assertReady: async () => {} }),
        createQueryCheckpointSession: () => ({ acknowledge: async () => {}, snapshot: () => ({ status: 'observed_query_set_handled' }) }),
        loadDiscoveryBackoff: async () => (backoff ? { reason: 'fixture-drain-only', diversify: false } : null),
        runDiscovery: async (_input, options) => { await options.persistCompanies({ companies: candidates || [] }); return { kind: 'investigated' }; },
        logger: { log() {}, warn() {} } });
    }
    const count = async (sql, values = []) => Number((await db.query(sql, values)).rows[0].n);
    const queue13 = () => count('SELECT count(*) n FROM scout_unenriched WHERE client_id=13');
    const tickets = async () => Object.fromEntries((await db.query(`SELECT w.reason, m.raw_candidate->>'companyName' AS name
      FROM scout_candidate_work w JOIN scout_candidate_memberships m USING(tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key)
      WHERE w.tenant_id=$1`, [TENANT])).rows.map(r => [r.name, r.reason]));

    await t.test('one cycle admits only genuinely new businesses and hands only those to enrichment', async () => {
      const queueBefore = await queue13();
      const prospectsBefore = await count('SELECT count(*) n FROM prospects WHERE client_id=13');
      await stage('q1', [
        raw('fresh-prospect', 'Fresh Prospect Co'),                                  // existing CRM prospect
        raw('customer-alt', 'Fresh Customer LLC', 'http://fresh-customer.example/'), // existing customer, alternate URL
        raw('fresh-imported', 'Fresh Imported Co', 'https://fresh-imported.example'), // CRM company without prospects
        raw('fresh-queued', 'Fresh Queued Co', 'https://WWW.Fresh-Queued.example/contact'), // already queued, URL variant
        raw('twin-manchester', 'Fresh Twin Services'),                               // ambiguous name, different business
        raw('fresh-isolated', 'Fresh Isolated Co'),                                  // known only to another tenant
        raw('fresh-new-one', 'Fresh New One'),
        raw('fresh-new-two', 'Fresh New Two'),
      ]);
      const result = await run();
      const reasons = await tickets();
      assert.equal(reasons['Fresh New One'], 'queued');
      assert.equal(reasons['Fresh New Two'], 'queued');
      assert.equal(reasons['Fresh Twin Services'], 'queued', 'same name in another town with another domain stays separate');
      assert.equal(reasons['Fresh Isolated Co'], 'queued', 'another tenant\'s records never suppress this tenant');
      assert.equal(reasons['Fresh Prospect Co'], 'ownership_policy_observed');
      assert.equal(reasons['Fresh Customer LLC'], 'ownership_policy_observed');
      assert.equal(reasons['Fresh Imported Co'], 'ownership_policy_observed');
      assert.equal(reasons['Fresh Queued Co'], 'queue_retry_cooldown', 'held queue work keeps its own clock');

      // Durable deltas, not function returns.
      assert.equal(await queue13() - queueBefore, 4);
      assert.equal(result.discoveredQueued, 4);
      assert.equal(result.admission.netNewQualified, 4);
      assert.equal(result.admission.previouslyKnown, 4);
      assert.deepEqual(new Set(enriched), new Set(['fresh-new-one.example', 'fresh-new-two.example',
        'twin-manchester.example', 'fresh-isolated.example']), 'enrichment spent only on net-new identities');
      assert.equal(await count('SELECT count(*) n FROM prospects WHERE client_id=13') - prospectsBefore, 4,
        'the authorized handoff produced exactly one CRM record per net-new business');
      // No website was crawled for any identity the CRM or queue already knew.
      assert.deepEqual(crawled.sort(), ['Fresh Isolated Co', 'Fresh New One', 'Fresh New Two', 'Fresh Twin Services']);
      assert.equal(await count('SELECT count(*) n FROM scout_unenriched WHERE client_id=1'), 1, 'other tenant queue untouched');

      const scorecard = replenishmentScorecard({ ...result, admissionMeasured: true }, 4);
      assert.equal(scorecard.discovered, 8);
      assert.equal(scorecard.previouslyKnown, 4);
      assert.equal(scorecard.netNewQualified, 4);
      assert.equal(scorecard.newlyQueued, 4);
      assert.equal(scorecard.netNewCleanInventory, 4);
      assert.equal(scorecard.outreachReady, true);
    });

    await t.test('the same businesses returned by another query, after a restart, are known and cost nothing', async () => {
      const queueBefore = await queue13(); const crawls = crawled.length; const spends = enriched.length;
      await stage('q2', [
        raw('fresh-new-one-alt', 'Fresh New One', 'https://www.FRESH-NEW-ONE.example/about'),
        raw('fresh-new-two', 'Fresh New Two'),
      ]);
      const result = await run(); // fresh ticket store + dispatcher = restarted worker
      assert.equal(await queue13(), queueBefore);
      assert.equal(result.discoveredQueued, 0);
      assert.equal(result.admission.netNewQualified, 0);
      // The repeated place keeps its candidate key: its handled ticket is not
      // reprocessed, so replay cannot inflate any count. The alternate URL is a
      // new candidate key, recognized through the CRM record enrichment created.
      assert.equal(result.admission.evaluated, 1);
      assert.equal(result.admission.previouslyKnown, 1, 'now in the CRM after enrichment: previously known');
      assert.equal(crawled.length, crawls, 'no website crawl for a known identity');
      assert.equal(enriched.length, spends, 'no enrichment spend for a known identity');
      const outcome = replenishmentOutcome({ plan: { shouldReplenish: true }, execute: true, scout: result, newCleanInventoryAdded: 0 });
      assert.equal(outcome.status, 'zero_yield', 'a completed cycle with no new inventory is never "replenished"');
    });

    await t.test('a queued-but-not-yet-enriched business is already_queued, not a second admission', async () => {
      await db.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,location,source,enrichment_attempts)
        VALUES(13,'Fresh Waiting Co','https://fresh-waiting.example','fresh-waiting.example','cleaning','Manchester, NH','max_buffer_replenishment',0)`);
      const queueBefore = await queue13(); const crawls = crawled.length;
      await stage('q3', [raw('fresh-waiting', 'Fresh Waiting Co', 'fresh-waiting.example')]);
      // Enrichment off for this step so the queued row stays unenriched.
      const result = await defaultScoutRamp({ pool: db, store, program, source, plan: { deficit: 10 }, searchFingerprint: fingerprint,
        skipVerificationRetry: true, enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
        createCandidateTicketStore: () => makeTickets(), createPageCheckpointStore: () => ({ assertReady: async () => {} }),
        createQueryCheckpointSession: () => ({ acknowledge: async () => {}, snapshot: () => ({}) }),
        loadDiscoveryBackoff: async () => ({ reason: 'fixture-drain-only', diversify: false }),
        runDiscovery: async () => ({ kind: 'investigated' }), logger: { log() {}, warn() {} } });
      assert.equal(await queue13(), queueBefore);
      assert.equal((await tickets())['Fresh Waiting Co'], 'already_queued');
      assert.equal(result.admission.netNewQualified, 0);
      assert.equal(result.admission.alreadyQueued, 1);
      assert.equal(result.admission.fit, 0, 'known before the website evidence that would qualify it');
      assert.equal(crawled.length, crawls);
    });

    await t.test('concurrent workers retain a new business exactly once', async () => {
      const queueBefore = await queue13();
      await stage('q4', [raw('fresh-concurrent', 'Fresh Concurrent Co')]);
      const results = await Promise.all([run(), run()]);
      assert.equal(await queue13() - queueBefore, 1);
      assert.equal(results.reduce((sum, r) => sum + Number(r.admission?.netNewQualified || 0), 0), 1);
      assert.equal(enriched.filter(domain => domain === 'fresh-concurrent.example').length, 1);
    });
  });
