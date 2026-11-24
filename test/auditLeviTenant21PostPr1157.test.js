'use strict';
// Post-PR #1157 tenant-21 audit. Two layers:
//  1. Pure evaluation over fixture sections (PASS and each FAIL path).
//  2. The real production research path (discover -> enrich -> claim ->
//     review-only persist) against a DISPOSABLE PostgreSQL with mocked SODA,
//     then the audit itself, proving it is read-only, tenant-scoped and
//     emits no PII. Never production; all other network is denied.
let allowedPort = null;
const deny = name => function () { throw new Error(`Unexpected network: ${name}`); };
for (const protocol of ['http', 'https']) { const t = require(`node:${protocol}`); t.request = deny(`${protocol}.request`); t.get = deny(`${protocol}.get`); }
require('node:tls').connect = deny('tls.connect');
globalThis.fetch = deny('fetch');
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || (['127.0.0.1', 'localhost', '::1', undefined].includes(o.host) && Number(o.port) === allowedPort);
  if (!local) throw new Error('Unexpected network: net.connect');
  return originalConnect.apply(this, args);
};

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const audit = require('../scripts/auditLeviTenant21PostPr1157');
const { createProductionDeps } = require('../services/leviScoutProductionDeps');
const { runResearchOnlyCycle } = require('../services/leviScoutResearchOnlyExecution');
const { authorityFor } = require('../services/leviScoutProductionRunner');
const { importReviewOnlyContacts } = require('../services/reviewOnlyProspectImportService');
const { NYC_URL, CHICAGO_URL } = require('../services/leviScoutBorrowerDiscovery');

const PII = 'PII_MARKER';
const status = (report, id) => report.checks.find(c => c.id === id)?.status;

test('select-only guard rejects writes and multi-statements', () => {
  for (const sql of Object.values(audit.QUERIES)) assert.doesNotThrow(() => audit.assertSelectOnly(sql));
  for (const bad of ['UPDATE prospects SET x=1', 'SELECT 1; DELETE FROM prospects', 'WITH x AS (DELETE FROM t RETURNING 1) SELECT 1',
    'INSERT INTO agent_log VALUES (1)', 'SELECT 1 FROM t FOR UPDATE; SET x=1']) {
    assert.throws(() => audit.assertSelectOnly(bad), /audit_statement_not_read_only/);
  }
});

test('argument gate requires explicit tenant 21 confirmation', () => {
  assert.throws(() => audit.parseArgs([]), /explicit_tenant_confirmation_required/);
  assert.throws(() => audit.parseArgs(['--tenant', '22', '--confirm-read-only']), /explicit_tenant_confirmation_required/);
  assert.equal(audit.parseArgs(['--tenant', '21', '--confirm-read-only', '--expect-database=pulseforge_production']).expectDatabase, 'pulseforge_production');
});

function fixtureSections(mutate = s => s) {
  const research = i => ({
    id: `r${i}`, client_id: 21, source: 'scout_research_only', first_name: `Ent${i}`, last_name: 'LLC',
    company_name: `Ent${i} LLC`, service_area_match: `${i} N A ST, Chicago, IL`, setter_visible: false,
    ao_outreach_review_required: true, discovery_method: 'open_data_permit', icp_score: 65, notes: 'Review state: qualify first.',
    acquisition_metadata: { importMode: 'review_only', outreachAuthorized: false, verifiedBorrowerIntent: false,
      permitDoesNotEstablishIntent: true, sourceType: 'property_record_signal', bucket: 'research_candidate', reviewState: 'qualify_first',
      city: 'Chicago', cityInference: { source: 'source_url_host', conflict: false },
      sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1'],
      contactResearch: { verifiable: false, fabricated: false, paidProvidersUsed: false,
        lookup: { requests: 4, completeness: 'incomplete', authoritativeZeroMatch: false } } },
  });
  const imported = i => ({ id: `i${i}`, client_id: 21, source: 'review_only_import', first_name: `P${i}`, last_name: 'X',
    company_name: `Co${i}`, setter_visible: false, icp_score: 65, acquisition_metadata: { reviewState: 'qualify_first' } });
  return mutate({
    identity: [{ database: 'pulseforge_production', tx_read_only: 'on' }],
    prospects: [...Array.from({ length: 10 }, (_, i) => research(i)), ...Array.from({ length: 19 }, (_, i) => imported(i))],
    agentLog: [
      { action: 'research_cycle_claim', status: 'success', cycle_id: audit.CLAIMED_CYCLE, rows: 1 },
      { action: 'research_cycle_diagnostic', status: 'completed', cycle_id: audit.CLAIMED_CYCLE, payload_status: 'completed',
        persisted: '10', outreach_attempted: 'false', paid_providers_used: 'false', rows: 1 },
    ],
    briefings: [{ cycle_id: audit.CLAIMED_CYCLE, cycle_status: 'completed', status: 'pending', rows: 1 }],
    claimIndex: [{ unique_claim_indexes: 1 }],
    touchpoints: [], activityLog: [], emailEvents: [], outboundItems: [], scheduledSends: [{ rows: 0 }], sendPlans: [], outreachActions: [],
  });
}

test('clean fixture passes every check', () => {
  const checks = audit.evaluate(fixtureSections(), { expectDatabase: 'pulseforge_production' });
  const failing = checks.filter(c => c.status !== 'PASS');
  assert.deepEqual(failing, []);
});

test('each violation is reported as FAIL', () => {
  const cases = {
    'target.database': s => ({ ...s, identity: [{ database: 'pulseforge_production_migration', tx_read_only: 'on' }] }),
    'gates.setter_visibility': s => { s.prospects[12].setter_visible = true; return s; },
    'research_only.classification': s => { s.prospects[0].acquisition_metadata.outreachAuthorized = true; return s; },
    'soda.incomplete_not_zero_match': s => { s.prospects[1].acquisition_metadata.contactResearch.lookup.authoritativeZeroMatch = true; return s; },
    'soda.request_budget': s => { s.prospects[2].acquisition_metadata.contactResearch.lookup.requests = 5; return s; },
    'soda.no_fabrication_or_paid': s => { s.prospects[3].acquisition_metadata.contactResearch.verifiable = true; return s; },
    'city.no_city_invented_from_conflict': s => {
      s.prospects[4].acquisition_metadata.sourceUrls.push('https://data.cityofnewyork.us/resource/w9ak-ipjd.json');
      return s;
    },
    'cycle.claimed_once': s => { s.agentLog[0].rows = 2; return s; },
    'cycle.no_later_cycle_claimed': s => { s.agentLog.push({ action: 'research_cycle_claim', cycle_id: 'levi-week-2026-11-30', rows: 1 }); return s; },
    'cycle.claim_race_protection': s => ({ ...s, claimIndex: [{ unique_claim_indexes: 0 }] }),
    'outreach.none': s => ({ ...s, emailEvents: [{ event_type: 'sent', rows: 3 }] }),
    'gates.recompute_exposure': s => { Object.assign(s.prospects[13], { icp_score: 80, service_area_match: 'Example', status: 'cold' }); return s; },
    'gates.qualified_contacts': s => { s.prospects[5].acquisition_metadata.bucket = 'qualified_prospect'; return s; },
    'dedup.no_duplicate_identities': s => { s.prospects[20].first_name = 'P0'; s.prospects[20].company_name = 'Co0'; return s; },
    'inventory.checkpoint': s => { s.prospects.pop(); return s; },
  };
  for (const [id, mutate] of Object.entries(cases)) {
    const checks = audit.evaluate(fixtureSections(mutate), { expectDatabase: 'pulseforge_production' });
    assert.equal(checks.find(c => c.id === id).status, 'FAIL', id);
  }
});

test('missing optional outreach tables are UNVERIFIED, never PASS', () => {
  const checks = audit.evaluate(fixtureSections(s => ({ ...s, touchpoints: { unavailable: '42P01' } })));
  assert.equal(checks.find(c => c.id === 'outreach.none').status, 'UNVERIFIED');
});

// ---- disposable PostgreSQL, real research path ----

const SCHEMA = `
  CREATE TABLE clients(id SERIAL PRIMARY KEY, name TEXT, slug TEXT, active BOOLEAN DEFAULT true, enabled_agents TEXT[]);
  CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, name TEXT, location TEXT);
  CREATE TABLE prospects(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, company_id UUID REFERENCES companies(id),
    first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT, linkedin_url TEXT, source TEXT, icp_score INTEGER,
    status TEXT, notes TEXT, vertical TEXT, service_area_match TEXT, setter_visible BOOLEAN DEFAULT false, setter_status TEXT,
    do_not_contact BOOLEAN DEFAULT false, ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false, discovery_method TEXT,
    last_contacted_at TIMESTAMPTZ, acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE agent_log(id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, prospect_id UUID, payload JSONB, status TEXT,
    error_msg TEXT, ran_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER,
    CONSTRAINT agent_log_status_check CHECK (status IN ('success','failed','skipped','pending','completed','posted','in_progress')));
  CREATE TABLE agent_actions(id SERIAL PRIMARY KEY, created_by TEXT, action_type TEXT, title TEXT, description TEXT, payload JSONB,
    status TEXT, executed_at TIMESTAMPTZ, result JSONB, created_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER);
  CREATE TABLE touchpoints(id SERIAL PRIMARY KEY, client_id INTEGER, prospect_id UUID, channel TEXT, action_type TEXT);
  CREATE TABLE activity_log(id SERIAL PRIMARY KEY, client_id INTEGER, lead_id UUID, action_type TEXT);
  CREATE TABLE email_events(event_id TEXT PRIMARY KEY, client_id INTEGER, prospect_id UUID, event_type TEXT, event_at TIMESTAMPTZ);
  CREATE TABLE acquisition_outbound_items(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, prospect_id TEXT, status TEXT NOT NULL DEFAULT 'pending');
  CREATE TABLE tenant_outreach_scheduled_sends(id SERIAL PRIMARY KEY, tenant_id TEXT NOT NULL);
  CREATE TABLE emmett_send_plans(id SERIAL PRIMARY KEY, tenant_id TEXT NOT NULL, client_id INTEGER, status TEXT NOT NULL);
  INSERT INTO clients(id,name,slug,enabled_agents) VALUES (21,'t21','t21','{max}'),(22,'t22','t22','{scout}');`;

function sodaFetch(now) {
  const day = now.toISOString().slice(0, 10);
  const nyc = Array.from({ length: 5 }, (_, i) => ({ owner_s_business_name: `${PII} NYC ${i} HOLDINGS LLC`, job_type: 'ALTERATION',
    first_permit_date: day, job_description: 'Interior rehab of dwelling', job_filing_number: `N${i}`, house_no: `${i}`, street_name: 'MAIN ST', borough: 'BROOKLYN' }));
  const chi = Array.from({ length: 5 }, (_, i) => ({ contact_1_type: 'OWNER', contact_1_name: `${PII} CHI ${i} PROPERTIES LLC`,
    permit_type: 'PERMIT - RENOVATION/ALTERATION', issue_date: day, work_description: 'Renovate', permit_: `C${i}`,
    street_number: `${100 + i}`, street_direction: 'N', street_name: 'STATE', suffix: 'ST' }));
  return async (url) => {
    const u = String(url);
    if (u.startsWith(NYC_URL)) return { ok: true, json: async () => nyc };
    if (u.startsWith(CHICAGO_URL)) return { ok: true, json: async () => chi };
    if (/ezma-pppn|r5kz-chrr/.test(u)) {
      if (decodeURIComponent(u.replace(/\+/g, ' ')).includes('CHI 0 ')) { const e = new Error('aborted due to timeout'); e.name = 'TimeoutError'; throw e; }
      return { ok: true, json: async () => [] };
    }
    throw new Error(`unexpected url ${u}`);
  };
}

test('real research path on disposable PostgreSQL, then read-only audit', async t => {
  const pg = await startDisposablePostgres('levi-post1157-');
  allowedPort = Number(pg.port);
  const db = new Pool({ connectionString: pg.connectionString, max: 4 });
  db.on('error', () => {});
  t.after(async () => { await db.end().catch(() => {}); await pg.stop(); });
  await db.query(SCHEMA);

  // 19 operator review-only imports + noise in another tenant.
  await importReviewOnlyContacts(Array.from({ length: 19 }, (_, i) => ({ name: `${PII} Person ${i}`, company: `${PII} Co ${i}`,
    reviewState: 'qualify_first', market: 'Example', sourceUrl: `https://example.invalid/${i}` })), { clientId: 21, db, actorId: 'test' });
  await db.query(`INSERT INTO prospects(client_id, source, setter_visible, first_name) VALUES (22,'scout',true,'${PII}')`);
  await db.query(`INSERT INTO email_events(event_id, client_id, event_type) VALUES ('e1', 22, 'sent')`);
  await db.query(`INSERT INTO acquisition_outbound_items(id, tenant_id, status) VALUES ('o1', '22', 'sent')`);

  // Real production deps; SODA mocked; the claimed cycle runs once.
  const now = new Date('2026-11-23T03:20:00Z');
  const env = { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' };
  const deps = createProductionDeps({ db, fetch: sodaFetch(now), env, now });
  const first = await runResearchOnlyCycle({ authority: authorityFor({ cycleId: audit.CLAIMED_CYCLE }), env, deps });
  assert.equal(first.status, 'completed');
  assert.equal(first.persisted, 10);
  const replay = await runResearchOnlyCycle({ authority: authorityFor({ cycleId: audit.CLAIMED_CYCLE }), env,
    deps: { ...deps, discover: async () => { throw new Error('must not discover on replay'); } } });
  assert.equal(replay.status, 'replay_blocked');

  const before = JSON.stringify((await db.query(`SELECT
    (SELECT json_agg(p ORDER BY id) FROM prospects p) AS p, (SELECT json_agg(a ORDER BY id) FROM agent_log a) AS a,
    (SELECT json_agg(x ORDER BY id) FROM agent_actions x) AS x`)).rows[0]);
  const client = await db.connect();
  const writes = [];
  const realQuery = client.query.bind(client);
  client.query = (sql, ...rest) => { if (/^\s*(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE)/i.test(sql)) writes.push(sql); return realQuery(sql, ...rest); };
  let report;
  try { report = await audit.collect(client, { expectDatabase: 'postgres' }); } finally { client.release(); }
  const after = JSON.stringify((await db.query(`SELECT
    (SELECT json_agg(p ORDER BY id) FROM prospects p) AS p, (SELECT json_agg(a ORDER BY id) FROM agent_log a) AS a,
    (SELECT json_agg(x ORDER BY id) FROM agent_actions x) AS x`)).rows[0]);

  assert.deepEqual(writes, []);
  assert.equal(after, before, 'audit must not change any row');
  assert.equal(JSON.stringify(report).includes(PII), false, 'no PII in report');
  assert.equal(JSON.stringify(report).includes('example.invalid'), false, 'no URLs in report');

  for (const id of ['target.read_only_transaction', 'inventory.tenant_scope', 'inventory.checkpoint',
    'city.no_city_invented_from_conflict', 'city.conflicts_recorded', 'soda.request_budget',
    'soda.incomplete_not_zero_match', 'soda.evidence_present', 'soda.no_fabrication_or_paid', 'research_only.classification',
    'gates.qualified_contacts', 'gates.setter_visibility', 'gates.recompute_exposure', 'outreach.none', 'dedup.no_duplicate_identities',
    'cycle.claimed_once', 'cycle.no_double_claim', 'cycle.single_diagnostic', 'cycle.no_outreach_or_paid_in_diagnostics']) {
    assert.equal(status(report, id), 'PASS', `${id}: ${JSON.stringify(report.checks.find(c => c.id === id))}`);
  }
  const soda = report.checks.find(c => c.id === 'soda.incomplete_not_zero_match').evidence;
  assert.equal(soda.completeness.incomplete, 1, 'the timed-out Chicago entity is incomplete');
  assert.equal(soda.authoritativeZeroMatch, 4, 'the four clean empty reads are authoritative zero-matches');
  // No unique index guards the claim in this schema (mirrors the migrations): reported, not hidden.
  assert.equal(status(report, 'cycle.claim_race_protection'), 'FAIL');
});

