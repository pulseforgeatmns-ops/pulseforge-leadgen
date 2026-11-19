'use strict';
// P0-SCOUT-LEVI-001: runs the real read-only production audit
// (scripts/leviScoutProductionAudit.js) against a disposable PostgreSQL whose
// touched tables use the production column names from the migrations
// (clients via utils/clientContext CLIENT_COLUMNS, cie_interview_sessions and
// cie_business_blueprints from migrations/2026-08-06-client-intelligence-engine.sql,
// agent_log.ran_at). Never production. Network other than the disposable
// database is denied.
let allowedPort = null;
const blocked = [];
const deny = name => function () { blocked.push(name); throw new Error(`Unexpected network: ${name}`); };
for (const protocol of ['http', 'https']) { const t = require(`node:${protocol}`); t.request = deny(`${protocol}.request`); t.get = deny(`${protocol}.get`); }
require('node:tls').connect = deny('tls.connect');
globalThis.fetch = deny('fetch');
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || (['127.0.0.1', 'localhost', '::1', undefined].includes(o.host) && Number(o.port) === allowedPort);
  if (!local) { blocked.push(`net.connect ${o.host}:${o.port}`); throw new Error('Unexpected network: net.connect'); }
  return originalConnect.apply(this, args);
};

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { importReviewOnlyContacts } = require('../services/reviewOnlyProspectImportService');
const { collect, platformOf, parseArgs } = require('../scripts/leviScoutProductionAudit');

const SECRET = 'AUDIT_PII_MARKER';
const contact = (i, extra = {}) => ({
  name: `${SECRET} Person${i}`, company: `${SECRET} Holdings ${i}`, email: `person${i}@example.invalid`,
  fit: `Executed purchase contract on the current flip; requesting $${10 + i},000 for rehab.`,
  risks: 'Lien position unverified.', reviewState: 'contact_now', market: 'Example County', category: 'borrower',
  sourceUrl: `https://example.invalid/source/${i}`, ...extra,
});

async function schema(db) {
  await db.query(`
    CREATE TABLE clients(id SERIAL PRIMARY KEY, name TEXT NOT NULL, slug TEXT NOT NULL, vertical TEXT,
      active BOOLEAN DEFAULT true, enabled_agents TEXT[], notes TEXT);
    CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, name TEXT, location TEXT);
    CREATE TABLE prospects(
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, company_id UUID REFERENCES companies(id),
      first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT, linkedin_url TEXT, source TEXT,
      icp_score INTEGER, status TEXT, notes TEXT, vertical TEXT, service_area_match TEXT,
      setter_visible BOOLEAN DEFAULT false, do_not_contact BOOLEAN DEFAULT false,
      ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false, discovery_method TEXT,
      acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE agent_log(id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, prospect_id UUID, payload JSONB,
      status TEXT, error_msg TEXT, duration_ms INTEGER, ran_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER);
    CREATE TABLE cie_interview_sessions(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'NEW', started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), completed_at TIMESTAMPTZ,
      current_stage TEXT NOT NULL DEFAULT 'Identity', summary TEXT, confidence_score NUMERIC(4,3),
      interview_state JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
    CREATE TABLE cie_business_blueprints(id UUID NOT NULL, client_id INTEGER NOT NULL, session_id UUID NOT NULL,
      version TEXT NOT NULL DEFAULT '1.0', status TEXT NOT NULL DEFAULT 'draft', generated_by TEXT NOT NULL DEFAULT 'CIE-v1',
      sections JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
    INSERT INTO clients(id,name,slug,vertical,enabled_agents) VALUES
      (21,'Tenant 21 (synthetic)','tenant-21','private_lending','{max}'), (22,'Other (synthetic)','other','cleaning','{scout}');`);
  const session = await db.query(`INSERT INTO cie_interview_sessions(client_id,status,interview_state)
    VALUES (21,'APPROVED','{"laEndeavorsWorkspaceCorrection":{"note":"${SECRET}"}}') RETURNING id`);
  await db.query(`INSERT INTO cie_business_blueprints(id,client_id,session_id,status,sections)
    VALUES (gen_random_uuid(),21,$1,'approved','{"services":"${SECRET}"}')`, [session.rows[0].id]);
  await db.query(`INSERT INTO agent_log(agent_name,action,status,payload,client_id) VALUES
    ('scout','run','success','{"reason":"synthetic"}',21), ('scout','run','success','{}',22)`);
}

const tableSnapshot = async db => {
  const out = {};
  for (const t of ['clients', 'companies', 'prospects', 'agent_log', 'cie_interview_sessions', 'cie_business_blueprints']) {
    out[t] = (await db.query(`SELECT to_jsonb(x) AS r FROM ${t} x ORDER BY 1::text`)).rows.map(r => JSON.stringify(r.r)).sort();
  }
  return JSON.stringify(out);
};

test('Levi production audit: every section reads, tenant-scoped, redacted, read-only', { timeout: 180000 }, async () => {
  const postgres = await startDisposablePostgres('levi-scout-audit-');
  allowedPort = Number(postgres.port);
  const db = new Pool({ connectionString: postgres.connectionString });
  try {
    await schema(db);
    const imported = await importReviewOnlyContacts([0, 1, 2, 3, 4, 5].map(i => contact(i)), { clientId: 21, db, actorId: null });
    assert.equal(imported.inserted.length, 6);
    await importReviewOnlyContacts([contact(6, { reviewState: 'qualify_first' })], { clientId: 21, db, actorId: null });
    // A duplicate identity entering by another path, an old record and a suppressed record.
    await db.query(`INSERT INTO prospects(client_id,email,source,status,created_at) VALUES
      (21,'PERSON0@example.invalid','scout','cold',NOW()+INTERVAL '1 minute'),
      (21,'old@example.invalid','scout','cold',NOW()-INTERVAL '90 days')`);
    await db.query(`UPDATE prospects SET do_not_contact=true WHERE client_id=21 AND email='person5@example.invalid'`);
    await importReviewOnlyContacts([contact(9)], { clientId: 22, db, actorId: null });

    const before = await tableSnapshot(db);
    const statements = [];
    const recording = { connect: async () => {
      const client = await db.connect();
      return { query: (sql, params) => { statements.push(String(sql)); return client.query(sql, params); }, release: () => client.release() };
    } };
    const report = await collect(recording, { now: new Date(Date.now() + 120000),
      expectDatabase: 'postgres', databaseUrl: postgres.connectionString });
    assert.equal(await tableSnapshot(db), before, 'audit changed data');
    assert.deepEqual(Object.keys(report.target).sort(), ['database', 'platform', 'serverVersionNum']);
    assert.equal(report.target.database, 'postgres');
    assert.equal(report.target.platform, 'local');
    assert.ok(report.target.serverVersionNum >= 160000);
    for (const detail of ['postgresql://', '127.0.0.1']) assert.ok(!JSON.stringify(report).includes(detail), 'report must not carry connection details');

    // Wrong target fails closed: no report, transaction rolled back, nothing written.
    await assert.rejects(collect(db, { expectDatabase: 'pulseforge_production_migration', databaseUrl: postgres.connectionString }),
      { code: 'audit_target_database_mismatch' });
    assert.equal(await tableSnapshot(db), before);

    for (const name of ['tenant', 'workspace', 'blueprints', 'runs', 'inventory']) {
      assert.ok(Array.isArray(report.sections[name]), `${name} unavailable: ${JSON.stringify(report.sections[name])}`);
    }
    assert.equal(report.sections.tenant[0].id, 21);
    assert.equal(report.sections.blueprints[0].status, 'approved');
    assert.equal(report.sections.workspace[0].has_workspace_correction, true);
    assert.equal(report.sections.runs.length, 1, 'foreign tenant run leaked');

    const p = report.production;
    assert.equal(p.allTime.records, 9, 'all tenant-21 rows, foreign excluded');
    assert.equal(p.last30Days.records, 8, '90-day-old record outside the weekly window');
    assert.equal(p.reviewOnlyContactNow.records, 6);
    assert.equal(p.allTime.duplicates, 1, 'only the later copy of a repeated identity is a duplicate');
    assert.equal(p.allTime.suppressed, 1);
    assert.deepEqual(p.allTime.bySource, { review_only_import: 7, scout: 2 });
    // No approved numeric range is supplied, so nothing may be called qualified.
    assert.equal(p.allTime.contractQualified, 0);
    assert.equal(p.allTime.blockers.financing_criteria_unverified, 9);
    assert.equal(p.observedProductionQualifiedNew, null);

    const text = JSON.stringify(report);
    for (const leak of [SECRET, 'example.invalid', 'person0@']) assert.ok(!text.includes(leak), `report leaks ${leak}`);
    assert.equal(report.sections.prospects, undefined);

    assert.equal(statements[0], 'BEGIN TRANSACTION READ ONLY');
    assert.equal(statements.at(-1), 'ROLLBACK');
    for (const sql of statements) {
      assert.match(sql.trim(), /^(SELECT|BEGIN TRANSACTION READ ONLY|SET LOCAL statement_timeout|SAVEPOINT audit_|RELEASE SAVEPOINT audit_|ROLLBACK)/);
    }
    assert.deepEqual(blocked, []);
  } finally {
    await db.end();
    await postgres.stop();
  }
});

test('platform classification and arguments never need Railway', () => {
  assert.equal(platformOf('postgresql://u:p@dpg-abc123-a/pulseforge_production_migration'), 'render');
  assert.equal(platformOf('postgresql://u:p@dpg-abc123-a.ohio-postgres.render.com/db'), 'render');
  assert.equal(platformOf('postgresql://u:p@postgres.railway.internal:5432/railway'), 'railway');
  assert.equal(platformOf('postgresql://u:p@127.0.0.1:5432/x'), 'local');
  assert.equal(platformOf(undefined), 'unknown');
  assert.deepEqual(parseArgs([]), {});
  assert.deepEqual(parseArgs(['--expect-database=pulseforge_production_migration']), { expectDatabase: 'pulseforge_production_migration' });
  assert.throws(() => parseArgs(['--write']), { code: 'audit_argument_invalid' });
});

test('review-only evidence fields: stored structurally, protections unchanged, audit reports freshness', { timeout: 180000 }, async () => {
  const postgres = await startDisposablePostgres('levi-import-evidence-');
  allowedPort = Number(postgres.port);
  const db = new Pool({ connectionString: postgres.connectionString });
  try {
    await schema(db);
    const first = await importReviewOnlyContacts([
      contact(1, { evidenceDate: '2026-11-10', sourceType: 'public_financing_request' }),
      contact(2, { sourceType: 'property_record_signal' }),
      contact(3),
    ], { clientId: 21, db, actorId: null });
    assert.equal(first.inserted.length, 3);
    const rows = (await db.query(`SELECT email, notes, acquisition_metadata AS m, setter_visible, do_not_contact,
      ao_outreach_review_required, source FROM prospects WHERE client_id=21 ORDER BY email`)).rows;
    const byEmail = Object.fromEntries(rows.map(r => [r.email, r]));
    assert.equal(byEmail['person1@example.invalid'].m.evidenceDate, '2026-11-10');
    assert.equal(byEmail['person1@example.invalid'].m.sourceType, 'public_financing_request');
    assert.ok(byEmail['person1@example.invalid'].m.importedAt, 'import time kept separately');
    assert.notEqual(byEmail['person1@example.invalid'].m.importedAt.slice(0, 10), '2026-11-10');
    assert.equal(byEmail['person3@example.invalid'].m.evidenceDate, null);
    for (const r of rows) {
      assert.equal(r.source, 'review_only_import');
      assert.equal(r.setter_visible, false);
      assert.equal(r.do_not_contact, false);
      assert.equal(r.ao_outreach_review_required, true);
      assert.equal(r.m.outreachAuthorized, false);
      assert.doesNotMatch(r.notes, /evidence date|source type/i, 'notes text unchanged');
    }

    // Dedup and tenant isolation still hold with the new fields present.
    const again = await importReviewOnlyContacts([contact(1, { evidenceDate: '2026-11-15', sourceType: 'referral' })],
      { clientId: 21, db, actorId: null });
    assert.equal(again.inserted.length, 0);
    assert.equal(again.skipped[0].reason, 'already_exists');
    assert.equal((await db.query(`SELECT acquisition_metadata->>'evidenceDate' AS d FROM prospects WHERE email='person1@example.invalid'`)).rows[0].d,
      '2026-11-10', 'a duplicate never overwrites provenance');
    const foreign = await importReviewOnlyContacts([contact(1, { evidenceDate: '2026-11-11', sourceType: 'referral' })],
      { clientId: 22, db, actorId: null });
    assert.equal(foreign.inserted.length, 1, 'another tenant is not deduplicated against tenant 21');

    // Invalid fields reject the whole import before any write.
    const before = await tableSnapshot(db);
    await assert.rejects(importReviewOnlyContacts([contact(7), contact(8, { evidenceDate: '2026-02-30' })], { clientId: 21, db, actorId: null }),
      { code: 'evidence_date_invalid' });
    await assert.rejects(importReviewOnlyContacts([contact(7, { sourceType: 'scraped' })], { clientId: 21, db, actorId: null }),
      { code: 'source_type_invalid' });
    assert.equal(await tableSnapshot(db), before);

    // Suppression is untouched by provenance and still counted.
    await db.query(`UPDATE prospects SET do_not_contact=true WHERE client_id=21 AND email='person1@example.invalid'`);
    const report = await collect(db, { databaseUrl: postgres.connectionString });
    const all = report.production.allTime;
    assert.equal(all.records, 3);
    assert.deepEqual(all.bySourceType, { public_financing_request: 1, property_record_signal: 1, unrecorded: 1 });
    assert.equal(all.evidenceDateUnpublished, 2);
    assert.equal(all.suppressed, 1);
    assert.equal(all.blockers.borrower_intent_unverified, 1);
    assert.equal(all.contractQualified, 0);
    assert.ok(!JSON.stringify(report).includes('example.invalid'));
    assert.deepEqual(blocked, []);
  } finally {
    await db.end();
    await postgres.stop();
  }
});
