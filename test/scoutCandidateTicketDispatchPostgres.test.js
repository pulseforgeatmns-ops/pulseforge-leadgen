'use strict';
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
const websitePath = require.resolve('../services/scoutWebsiteBusinessEvidence');
const website = require(websitePath);
const calls = { website: [], page: 0, discovery: 0, recovery: 0 };
require.cache[websitePath].exports = { ...website, acquireBusinessEvidence: async (candidate, context, options) => {
  calls.website.push(candidate.name);
  return website.acquireBusinessEvidence(candidate, context, { ...options, fetchPage: async url => {
    calls.page++;
    if (candidate.name.startsWith('Transient')) return {ok:false,status:503,url,text:''};
    return { ok: true, url, text: candidate.name.startsWith('Fresh')
      ? '<html><h1>Local operator</h1><p>We provide residential cleaning services in Manchester.</p></html>'
      : '<html><h1>Local business</h1><p>Contact us to learn more.</p></html>' };
  } });
} };
const recoveryPath = require.resolve('../services/sameCompanyContactRecovery');
const recovery = require(recoveryPath);
require.cache[recoveryPath].exports = { ...recovery, attemptSameCompanyAlternateRecovery: async () => {
  calls.recovery++; throw Error('Durable recovery must not call the unfenced provider helper');
} };
const { defaultScoutRamp } = require('../services/maxOutboundControlLoop');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { claimedCompanies, candidateTicketDependencyKey } = require('../services/scoutCandidateTicketDispatch');
const { PLAN_VERSION } = require('../services/scoutQueryCheckpoints');
const source = { payload: { structuredMission: {
  market: { segment: 'small_business_owners', industry: 'founder_led_smb', capability: 'business_transformation' },
  geography: { region: 'Manchester NH', cities: ['Manchester'], scope: 'local' } } } };
const program = { id: 'fixture-program', tenant_id: '13', policy: {} };
const raw = (id, name = `Unknown ${id}`, extra = {}) => ({ placeId: id, companyName: name,
  website: `https://${id}.example`, address: 'Manchester, NH', industry: 'Small Business Manchester NH',
  discoveryQuery: 'Small Business Manchester NH', discoveryConcept: 'Small Business', ...extra });

// Release gate is deliberately required, rather than silently accepting a skip.
test('real PostgreSQL governed ramp: sustained independent candidate flow, bounded costs and restart', { timeout: 60000 }, async t => {
  const previous = process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
  process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = 'true';
  const instance = await startDisposablePostgres('scout-dispatch-');
  const db = new Pool({ connectionString: instance.connectionString, max: 3 });
  t.after(async () => { if (previous == null) delete process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
    else process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = previous;
    await db.end(); await instance.stop(); });
  for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-candidate-tickets.sql']) {
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8'));
  }
  await db.query(`CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,
    domain text,vertical text,location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());`);
  const ownership = new Map();
  const pool = { connect: (...args) => db.connect(...args), query: async (sql, params) => {
    if (/FROM prospects p/.test(sql)) return { rows: ownership.get(params[2]) || [] };
    return db.query(sql, params);
  } };
  const events = [];
  const store = { tenantId: '13', clientId: 13, candidateOwnership: async () => null,
    suppression: async () => null, one: async () => null, event: async (...args) => events.push(args) };
  const fingerprint = 'approved-local-mission';
  const makeTickets = () => createCandidateTicketStore({ pool, tenantId: '13', fingerprint });
  async function stage(queryKey, candidates) {
    await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates)
      VALUES('13',$1,$2,$3,'fetched',$4::jsonb)`, [fingerprint, queryKey, PLAN_VERSION, JSON.stringify(candidates)]);
    return (await makeTickets().transferQuery(queryKey)).candidates;
  }
  async function run({ candidates = [], backoff = false, discover = null } = {}) {
    return defaultScoutRamp({ pool, store, program, source, plan: { deficit: 1 },
      searchFingerprint: fingerprint, skipVerificationRetry: true,
      enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
      createCandidateTicketStore: () => makeTickets(), createPageCheckpointStore: () => ({ assertReady: async () => {} }),
      createQueryCheckpointSession: ({ ticketStore }) => {
        assert.ok(ticketStore); return { acknowledge: async () => {}, snapshot: () => ({ status: 'observed_query_set_handled' }) };
      },
      loadDiscoveryBackoff: async () => backoff ? { reason: 'fixture-backoff', diversify: false } : null,
      runDiscovery: async (_input, options) => { calls.discovery++; if (discover) return discover(options);
        await options.persistCompanies({ companies: candidates }); return { kind: 'investigated' }; },
      logger: { log() {}, warn() {} },
    });
  }
  const queued = async () => (await db.query('SELECT * FROM scout_unenriched ORDER BY id')).rows;
  await t.test('unresolved21 receive one shared20-site budget, then new distinct eligible businesses keep flowing', async () => {
    const initial = Array.from({ length: 21 }, (_, index) => raw(`unknown${index}`));
    const decorated = await stage('approved-q1', initial);
    const first = await run({ candidates: decorated });
    assert.equal(first.discoveredQueued, 0); assert.equal(first.admission.websiteInvestigations, 20);
    assert.equal(calls.website.length, 20); assert.equal(calls.page, 60);
    assert.equal(first.candidateWork.byReason.unresolved_business_evidence, 20);
    assert.equal(first.candidateWork.byReason.evidence_budget_deferred, 1);
    for (let cycle = 1; cycle <= 3; cycle++) {
      const candidates = await stage(`approved-q${cycle + 1}`, [raw(`fresh${cycle}`, `Fresh ${cycle}`)]);
      const before = calls.website.length;
      const result = await run({ candidates }); // New store/dispatcher every cycle (restart).
      assert.equal(result.discoveredQueued, 1);
      assert.equal((await queued()).length, cycle);
      assert.equal(calls.website.length - before, cycle === 1 ? 2 : 1);
      assert.equal(result.candidateWork.byState.handled, cycle);
      assert.equal(result.candidateWork.byReason.unresolved_business_evidence, 21);
    }
    assert.equal(new Set(calls.website.filter(name => name.startsWith('Unknown'))).size, 21);
    assert.equal(calls.website.filter(name => name.startsWith('Unknown')).length, 21);
    assert.equal(calls.recovery, 0);
    const preserved = (await db.query("SELECT state,candidates FROM scout_query_checkpoints WHERE query_key='approved-q1'")).rows[0];
    assert.equal(preserved.state, 'handled'); assert.deepEqual(preserved.candidates, initial);
    const rows = await queued(); assert.ok(rows.every(row => row.vertical === 'cleaning' && row.enrichment_attempts === 0));
  });
  await t.test('query backoff and discovery failure still drain new durable candidates exactly once', async () => {
    await stage('approved-q5', [raw('fresh4', 'Fresh 4')]);
    const before = calls.discovery; const output = await run({ backoff: true });
    assert.equal(calls.discovery, before); assert.equal(output.discoveredQueued, 1);
    await stage('approved-q6', [raw('fresh5', 'Fresh 5')]);
    assert.equal((await run({ discover: async () => { throw Error('synthetic discovery unavailable'); } })).discoveredQueued, 1);
    const count = calls.website.length;
    const empty = await run({ backoff: true });
    assert.equal(empty.discoveredQueued, 0); assert.equal(calls.website.length, count);
    assert.equal((await queued()).length, 5);
  });
  await t.test('concurrent restarted ramps spend and insert once for new candidate with no pinned pool connection', async () => {
    await stage('approved-q7', [raw('fresh6', 'Fresh 6')]);
    const before = calls.website.length;
    const results = await Promise.all([run({ backoff: true }), run({ backoff: true })]);
    assert.equal(results.reduce((sum, row) => sum + row.discoveredQueued, 0), 1);
    assert.equal(calls.website.length - before, 1); assert.equal((await queued()).length, 6);
    assert.equal(db.waitingCount, 0); assert.equal(db.idleCount, db.totalCount);
  });
  await t.test('unsafe split identity and missing names become explicit holds with complete membership', async () => {
    const observations = [raw('identity', 'A', { id: 'a', website: 'https://a.example' }),
      raw('identity', 'B', { id: 'b', website: 'https://b.example' }), { place_id: 'nameless', website: 'https://nameless.example' }];
    await stage('approved-q8', observations); const before = calls.website.length;
    const result = await run({ backoff: true });
    assert.equal(result.candidateWork.byReason.identity_incomplete, 2); assert.equal(calls.website.length, before);
    await run({ backoff: true }); assert.equal(calls.website.length, before);
  });
  await t.test('existing queue attempt and retry clocks are preserved and independently reconsiderable', async () => {
    await db.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,source,enrichment_attempts,last_attempt_at)
      VALUES(13,'Known Hold','https://knownhold.example','knownhold.example','painting','fixture',6,now());`);
    await stage('approved-q9', [raw('knownhold', 'Known Hold', { placeTypes: ['painter'], description: 'Painting contractor' })]);
    const result = await run({ backoff: true });
    assert.equal(result.candidateWork.byReason.queue_attempt_cooldown, 1);
    const work = (await db.query("SELECT next_attempt_at FROM scout_candidate_work WHERE reason='queue_attempt_cooldown'")).rows[0];
    assert.ok(work.next_attempt_at - Date.now() > 6 * 86400000);
    const row = (await db.query("SELECT enrichment_attempts,last_attempt_at FROM scout_unenriched WHERE company='Known Hold'")).rows[0];
    assert.equal(row.enrichment_attempts, 6); assert.ok(row.last_attempt_at);
  });
  await t.test('mid-tier duplicate tickets retain attempts and wake after one day, not one week', async () => {
    await db.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,source,enrichment_attempts,last_attempt_at)
      VALUES(13,'Daily First','https://dailyfirst.example','dailyfirst.example','painting','fixture',3,now()),
        (13,'Daily Last','https://dailylast.example','dailylast.example','painting','fixture',5,now());`);
    await stage('approved-q9-daily', [raw('dailyfirst', 'Daily First', { placeTypes: ['painter'], description: 'Painting contractor' }),
      raw('dailylast', 'Daily Last', { placeTypes: ['painter'], description: 'Painting contractor' })]);
    const before = (await db.query("SELECT enrichment_attempts,last_attempt_at FROM scout_unenriched WHERE domain IN ('dailyfirst.example','dailylast.example') ORDER BY domain")).rows;
    const result = await run({ backoff: true });
    assert.equal(result.candidateWork.byReason.queue_retry_cooldown, 2);
    const work = (await db.query("SELECT next_attempt_at FROM scout_candidate_work WHERE reason='queue_retry_cooldown'")).rows;
    assert.equal(work.length, 2);
    for (const row of work) {
      const delay = row.next_attempt_at - Date.now();
      assert.ok(delay > 23 * 3600000 && delay <= 24 * 3600000, `daily delay: ${delay}`);
    }
    assert.deepEqual((await db.query("SELECT enrichment_attempts,last_attempt_at FROM scout_unenriched WHERE domain IN ('dailyfirst.example','dailylast.example') ORDER BY domain")).rows, before);
  });
  await t.test('new exact-provider identity evidence repairs a hold without erasing incomplete original observation', async () => {
    await stage('repair1',[{placeId:'repair',website:'https://repair.example'}]);
    await run({backoff:true}); const before=calls.website.length;
    await stage('repair2',[raw('repair','Fresh repaired')]);
    const result=await run({backoff:true}); assert.equal(result.discoveredQueued,1);
    assert.equal(calls.website.length-before,1);
    await stage('repair3',[raw('repair','Fresh repaired',{discoveryQuery:'Different approved query',industry:'Different approved query'})]);
    await run({backoff:true}); assert.equal(calls.website.length-before,1,'query labels are not new evidence');
    assert.equal((await db.query("SELECT count(*)::int AS n FROM scout_candidate_memberships WHERE raw_candidate->>'placeId'='repair'")).rows[0].n,3);
  });
  await t.test('missing geography and conflicting locations across alias tickets never admit or become terminal', async () => {
    await stage('geo1',[raw('missinggeo','Known missing',{address:null,placeTypes:['painter']}),
      raw('geoa','Known alias',{website:'https://geo.example',address:'Boston, MA',placeTypes:['painter']}),
      raw('geob','Known alias',{website:'https://geo.example',address:'Manchester, NH',placeTypes:['painter']})]);
    const before=(await queued()).length; await run({backoff:true}); assert.equal((await queued()).length,before);
    const work=(await db.query("SELECT state,reason FROM scout_candidate_work WHERE candidate_key IN (SELECT candidate_key FROM scout_candidate_memberships WHERE query_key='geo1')")).rows;
    assert.ok(work.every(row=>row.state==='blocked')); assert.ok(work.some(row=>row.reason==='geography_unresolved'));
  });

  await t.test('unchanged successful no-evidence has no timed recrawl; explicit transient failures retry only after cooldown', async () => {
    const before=calls.website.length;
    await db.query("UPDATE scout_candidate_work SET next_attempt_at=now()-interval '30 days' WHERE reason='unresolved_business_evidence'");
    await run({backoff:true}); assert.equal(calls.website.length,before);
    await stage('transient',[raw('transient','Transient outage')]);
    const first=await run({backoff:true}); assert.equal(first.candidateWork.byReason.transient_business_evidence_failure,1);
    const after=calls.website.length; await run({backoff:true});assert.equal(calls.website.length,after);
    const work=(await db.query("SELECT state,next_attempt_at FROM scout_candidate_work WHERE reason='transient_business_evidence_failure'")).rows[0];
    assert.equal(work.state,'deferred');assert.ok(work.next_attempt_at-Date.now()>23*3600000);
    await db.query("UPDATE scout_candidate_work SET next_attempt_at=now()-interval '1 second' WHERE reason='transient_business_evidence_failure'");
    await run({backoff:true});assert.equal(calls.website.length,after+1);
  });

});
