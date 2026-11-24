'use strict';
// P0-SCOUT-LEVI-001 reliability: R1 (same-owner permits no longer fail the
// claimed weekly cycle) and R2 (concurrency-safe weekly claim + guarded unique
// index migration). Disposable PostgreSQL only; all other network is denied.
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
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { Pool, Client } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const {
  createProductionDeps, asContacts, mergeSameImportIdentity, CLAIM_LOCK_CLASS, CLAIM_SQL,
} = require('../services/leviScoutProductionDeps');
const { previewReviewOnlyContacts, importIdentityKey } = require('../services/reviewOnlyProspectImportService');
const { runResearchOnlyCycle } = require('../services/leviScoutResearchOnlyExecution');
const { authorityFor } = require('../services/leviScoutProductionRunner');
const { NYC_URL, CHICAGO_URL } = require('../services/leviScoutBorrowerDiscovery');

const ROOT = path.join(__dirname, '..');
const MIGRATION = path.join(ROOT, 'migrations', '2026-11-24-levi-scout-cycle-claim-unique.sql');
const PRECHECK = path.join(ROOT, 'migrations', '2026-11-24-levi-scout-cycle-claim-unique.precheck.sql');
const ROLLBACK = path.join(ROOT, 'migrations', '2026-11-24-levi-scout-cycle-claim-unique.rollback.sql');
const ENV = Object.freeze({ SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' });
const CYCLE = 'levi-week-2099-01-05';

const permitRow = (owner, permit, address, city = 'Chicago') => ({
  name: owner, company: owner, service_area_match: `${address}, ${city === 'Chicago' ? 'Chicago, IL' : 'New York, NY'}`,
  notes: 'Review state: qualify first.',
  acquisition_metadata: {
    sourceType: 'property_record_signal', reviewState: 'qualify_first', category: 'borrower', evidenceDate: '2026-11-20',
    city, provider: city === 'Chicago' ? 'chicago_open_data_permits' : 'nyc_open_data_permits',
    sourceUrls: [city === 'Chicago' ? `${CHICAGO_URL}?permit_=${permit}` : `${NYC_URL}?job_filing_number=${permit}`],
  },
});

// ---- R1 unit ----

test('R1: merge key is the import identity key, so preview can never reject a merged batch', () => {
  const batch = asContacts([
    permitRow('ACME HOLDINGS LLC', 'C1', '1 N A ST'),
    permitRow('ACME HOLDINGS LLC', 'C2', '2 N B ST'),
    permitRow('acme holdings llc', 'C3', '3 N C ST'),
    permitRow('OTHER PROPERTIES LLC', 'C4', '4 N D ST'),
  ]);
  assert.throws(() => previewReviewOnlyContacts(batch), { code: 'duplicate_contact' }, 'the defect: unmerged batch is rejected');
  const { contacts, merged } = mergeSameImportIdentity(batch);
  assert.equal(merged, 2);
  assert.equal(contacts.length, 2);
  assert.doesNotThrow(() => previewReviewOnlyContacts(contacts));
  assert.equal(new Set(contacts.map((c, i) => importIdentityKey(c, i))).size, contacts.length);
  const acme = contacts[0];
  assert.deepEqual(acme.sourceUrls, [`${CHICAGO_URL}?permit_=C1`, `${CHICAGO_URL}?permit_=C2`, `${CHICAGO_URL}?permit_=C3`]);
  assert.equal(acme.extraMetadata.mergedPermitCount, 2);
  assert.deepEqual(acme.extraMetadata.mergedPermits.map(p => p.market),
    ['2 N B ST, Chicago, IL', '3 N C ST, Chicago, IL'], 'each merged permit keeps its own location evidence');
  assert.equal(acme.extraMetadata.crossCityIdentityCollision, undefined);
  assert.equal(acme.market, '1 N A ST, Chicago, IL', 'the kept record is the first permit, unchanged');
});

test('R1: a same-name permit from another city is retained as evidence without creating a city conflict', () => {
  const { contacts, merged } = mergeSameImportIdentity(asContacts([
    permitRow('ACME HOLDINGS LLC', 'C1', '1 N A ST'),
    permitRow('ACME HOLDINGS LLC', 'N1', '10 MAIN ST', 'New York'),
  ]));
  assert.equal(merged, 1);
  assert.deepEqual(contacts[0].sourceUrls, [`${CHICAGO_URL}?permit_=C1`], 'no NYC host joins the Chicago record sources');
  assert.equal(contacts[0].extraMetadata.crossCityIdentityCollision, true);
  assert.equal(contacts[0].extraMetadata.mergedPermits[0].city, 'New York');
  assert.equal(contacts[0].extraMetadata.mergedPermits[0].sourceUrl, `${NYC_URL}?job_filing_number=N1`);
});

test('R2: a unique-violation on the claim is read as already claimed, never as a crash', async () => {
  const db = { query: async () => { throw Object.assign(new Error('dup'), { code: '23505' }); } };
  const deps = createProductionDeps({ db, env: ENV });
  assert.deepEqual(await deps.claimRun({ cycleId: CYCLE, authorityHash: 'h' }), { claimed: false, reason: 'unique_claim_exists' });
  const other = { query: async () => { throw Object.assign(new Error('boom'), { code: '57014' }); } };
  await assert.rejects(createProductionDeps({ db: other, env: ENV }).claimRun({ cycleId: CYCLE, authorityHash: 'h' }), { code: '57014' });
});

// ---- disposable PostgreSQL ----

const SCHEMA = `
  CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, name TEXT, location TEXT);
  CREATE TABLE prospects(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, company_id UUID REFERENCES companies(id),
    first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT, linkedin_url TEXT, source TEXT, icp_score INTEGER,
    status TEXT, notes TEXT, vertical TEXT, service_area_match TEXT, setter_visible BOOLEAN DEFAULT false,
    do_not_contact BOOLEAN DEFAULT false, ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false, discovery_method TEXT,
    acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE agent_log(id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, prospect_id UUID, payload JSONB, status TEXT,
    error_msg TEXT, ran_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER,
    CONSTRAINT agent_log_status_check CHECK (status IN ('success','failed','skipped','pending','completed','posted','in_progress')));`;

async function freshDatabase(t, prefix) {
  const pg = await startDisposablePostgres(prefix);
  allowedPort = Number(pg.port);
  const db = new Pool({ connectionString: pg.connectionString, max: 40 });
  db.on('error', () => {}); // idle clients see 57P01 when the disposable server stops
  t.after(async () => { await db.end().catch(() => {}); await pg.stop(); });
  await db.query(SCHEMA);
  return { pg, db };
}

const claims = async db => (await db.query(
  `SELECT count(*)::int AS n FROM agent_log WHERE agent_name = 'scout_levi' AND action = 'research_cycle_claim' AND payload->>'cycleId' = $1`,
  [CYCLE])).rows[0].n;

function psql(pg, file) {
  return execFileSync('psql', [pg.connectionString, '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', file], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
}

test('R1 regression: a batch with one owner on several permits completes after its claim and keeps all evidence', async t => {
  const { db } = await freshDatabase(t, 'levi-r1-');
  const now = new Date('2026-11-23T03:00:00Z');
  const day = '2026-11-22';
  const chi = [
    { permit_: 'C1', street_number: '100' }, { permit_: 'C2', street_number: '200' }, { permit_: 'C3', street_number: '300' },
  ].map(x => ({ contact_1_type: 'OWNER', contact_1_name: 'SAME OWNER HOLDINGS LLC', permit_type: 'PERMIT - RENOVATION/ALTERATION',
    issue_date: day, street_direction: 'N', street_name: 'STATE', suffix: 'ST', ...x }));
  chi.push({ contact_1_type: 'OWNER', contact_1_name: 'SOLO PROPERTIES LLC', permit_type: 'PERMIT - RENOVATION/ALTERATION',
    issue_date: day, permit_: 'C4', street_number: '400', street_direction: 'N', street_name: 'STATE', suffix: 'ST' });
  const nyc = [{ owner_s_business_name: 'SAME OWNER HOLDINGS LLC', job_type: 'ALTERATION', first_permit_date: day,
    job_description: 'Interior rehab of dwelling', job_filing_number: 'N1', house_no: '1', street_name: 'MAIN ST', borough: 'BROOKLYN' }];
  let sodaContactCalls = 0;
  const fetchImpl = async url => {
    const u = String(url);
    if (u.startsWith(NYC_URL)) return { ok: true, json: async () => nyc };
    if (u.startsWith(CHICAGO_URL)) return { ok: true, json: async () => chi };
    if (/ezma-pppn|r5kz-chrr/.test(u)) { sodaContactCalls += 1; return { ok: true, json: async () => [] }; }
    throw new Error(`unexpected url ${u}`);
  };
  const deps = createProductionDeps({ db, fetch: fetchImpl, env: ENV, now });
  const result = await runResearchOnlyCycle({ authority: authorityFor({ cycleId: CYCLE }), env: ENV, deps });

  assert.equal(result.status, 'completed', 'cycle completes instead of failing after claim');
  assert.equal(result.discovered, 5);
  assert.equal(result.persisted, 2, 'one record per import identity');
  assert.equal(result.mergedSameIdentity, 3);
  assert.equal(await claims(db), 1);
  const diag = (await db.query(`SELECT status, payload FROM agent_log WHERE action = 'research_cycle_diagnostic'`)).rows;
  assert.equal(diag.length, 1);
  assert.equal(diag[0].status, 'completed');
  assert.equal(diag[0].payload.mergedSameIdentity, 3);
  assert.ok(sodaContactCalls <= 4 * 4, 'SODA contact lookups stay within the per-entity budget');

  const rows = (await db.query(`SELECT first_name, last_name, notes, setter_visible, ao_outreach_review_required,
    service_area_match, acquisition_metadata FROM prospects WHERE client_id = 21 ORDER BY first_name`)).rows;
  assert.equal(rows.length, 2);
  const same = rows.find(r => `${r.first_name} ${r.last_name}`.startsWith('SAME'));
  const m = same.acquisition_metadata;
  // NYC (interleaved first) is the kept record; the Chicago permits are retained as other-city evidence.
  assert.equal(m.city, 'New York');
  assert.equal(m.mergedPermitCount, 3);
  assert.equal(m.crossCityIdentityCollision, true);
  assert.deepEqual(m.mergedPermits.map(p => p.city), ['Chicago', 'Chicago', 'Chicago']);
  assert.deepEqual(m.mergedPermits.map(p => p.sourceUrl).sort(),
    ['C1', 'C2', 'C3'].map(id => `${CHICAGO_URL}?permit_=${id}`));
  assert.equal(m.sourceUrls.every(u => u.startsWith(NYC_URL)), true, 'kept record sources stay single-city');
  assert.notEqual(m.cityInference && m.cityInference.conflict, true, 'no city conflict manufactured');
  for (const row of rows) {
    assert.equal(row.setter_visible, false);
    assert.equal(row.ao_outreach_review_required, true);
    assert.equal(row.acquisition_metadata.outreachAuthorized, false);
  }

  const replay = await runResearchOnlyCycle({ authority: authorityFor({ cycleId: CYCLE }), env: ENV,
    deps: { ...deps, discover: async () => { throw new Error('replay must not discover'); } } });
  assert.equal(replay.status, 'replay_blocked');
  assert.equal(await claims(db), 1);
});

test('R2 baseline: the legacy INSERT ... WHERE NOT EXISTS double-claims under interleaving', async t => {
  const { pg, db } = await freshDatabase(t, 'levi-r2-legacy-');
  const a = new Client({ connectionString: pg.connectionString });
  const b = new Client({ connectionString: pg.connectionString });
  a.on('error', () => {}); b.on('error', () => {});
  await a.connect(); await b.connect();
  t.after(async () => { await a.end().catch(() => {}); await b.end().catch(() => {}); });
  const params = id => ['scout_levi', JSON.stringify({ cycleId: CYCLE, by: id }), 21, CYCLE];
  await a.query('BEGIN'); await b.query('BEGIN');
  assert.equal((await a.query(CLAIM_SQL, params('a'))).rowCount, 1);
  assert.equal((await b.query(CLAIM_SQL, params('b'))).rowCount, 1, 'B cannot see A\'s uncommitted claim');
  await a.query('COMMIT'); await b.query('COMMIT');
  assert.equal(await claims(db), 2, 'R2 reproduced: two claims for one week');
});

test('R2: concurrent claimRun calls produce exactly one claim (advisory lock, no index)', async t => {
  const { db } = await freshDatabase(t, 'levi-r2-lock-');
  const results = await Promise.all(Array.from({ length: 30 }, () =>
    createProductionDeps({ db, env: ENV }).claimRun({ cycleId: CYCLE, authorityHash: 'h' })));
  assert.equal(results.filter(r => r.claimed).length, 1);
  assert.equal(await claims(db), 1);
  // A different week is still claimable.
  assert.equal((await createProductionDeps({ db, env: ENV }).claimRun({ cycleId: 'levi-week-2099-01-12', authorityHash: 'h' })).claimed, true);
});

test('R2: a claim waits for an in-flight claim and then sees it', async t => {
  const { pg, db } = await freshDatabase(t, 'levi-r2-wait-');
  const holder = new Client({ connectionString: pg.connectionString });
  holder.on('error', () => {});
  await holder.connect();
  t.after(() => holder.end().catch(() => {}));
  await holder.query('BEGIN');
  await holder.query('SELECT pg_advisory_xact_lock($1::int, $2::int)', [CLAIM_LOCK_CLASS, 21]);
  await holder.query(CLAIM_SQL, ['scout_levi', JSON.stringify({ cycleId: CYCLE }), 21, CYCLE]);
  let settled = false;
  const pending = createProductionDeps({ db, env: ENV }).claimRun({ cycleId: CYCLE, authorityHash: 'h' })
    .then(r => { settled = true; return r; });
  await new Promise(r => setTimeout(r, 300));
  assert.equal(settled, false, 'second claimant is blocked by the lock');
  await holder.query('COMMIT');
  assert.deepEqual(await pending, { claimed: false });
  assert.equal(await claims(db), 1);
});

test('R2 migration: precheck is read-only, duplicates abort the build without deleting, clean build enforces uniqueness', async t => {
  const { pg, db } = await freshDatabase(t, 'levi-r2-mig-');
  // Existing duplicate (as legacy code could have produced).
  await db.query(`INSERT INTO agent_log(agent_name, action, status, payload, client_id) VALUES
    ('scout_levi','research_cycle_claim','success','{"cycleId":"${CYCLE}"}',21),
    ('scout_levi','research_cycle_claim','success','{"cycleId":"${CYCLE}"}',21),
    ('scout_levi','research_cycle_diagnostic','completed','{"cycleId":"${CYCLE}"}',21),
    ('scout','research_cycle_claim','success','{"cycleId":"${CYCLE}"}',21)`);
  const pre = psql(pg, PRECHECK);
  assert.match(pre, /duplicate_claim_groups\s*\n-+\s*\n\s*1\b/);
  const before = (await db.query('SELECT count(*)::int AS n FROM agent_log')).rows[0].n;
  assert.throws(() => psql(pg, MIGRATION), err => /levi_claim_duplicates_present: 1/.test(String(err.stderr)));
  assert.equal((await db.query('SELECT count(*)::int AS n FROM agent_log')).rows[0].n, before, 'nothing deleted');
  assert.equal((await db.query(`SELECT count(*)::int AS n FROM pg_indexes WHERE indexname = 'agent_log_scout_levi_cycle_claim_uniq'`)).rows[0].n, 0);

  // Operator resolution in this disposable DB only; then a clean, idempotent build.
  await db.query(`DELETE FROM agent_log WHERE id = (SELECT max(id) FROM agent_log WHERE agent_name='scout_levi' AND action='research_cycle_claim')`);
  psql(pg, MIGRATION);
  psql(pg, MIGRATION);
  const idx = (await db.query(`SELECT i.indisvalid, i.indisunique FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
    WHERE c.relname = 'agent_log_scout_levi_cycle_claim_uniq'`)).rows;
  assert.deepEqual(idx, [{ indisvalid: true, indisunique: true }]);

  // Other agents and actions are unaffected by the partial index.
  await db.query(`INSERT INTO agent_log(agent_name, action, status, payload, client_id) VALUES
    ('scout','research_cycle_claim','success','{"cycleId":"${CYCLE}"}',21),
    ('scout_levi','research_cycle_diagnostic','completed','{"cycleId":"${CYCLE}"}',21)`);

  // With the index, even the legacy interleaving cannot double-claim: B waits for A, then 23505.
  const a = new Client({ connectionString: pg.connectionString });
  const b = new Client({ connectionString: pg.connectionString });
  a.on('error', () => {}); b.on('error', () => {});
  await a.connect(); await b.connect();
  t.after(async () => { await a.end().catch(() => {}); await b.end().catch(() => {}); });
  const next = 'levi-week-2099-01-12';
  const params = ['scout_levi', JSON.stringify({ cycleId: next }), 21, next];
  await a.query('BEGIN'); await b.query('BEGIN');
  await a.query(CLAIM_SQL, params);
  const bInsert = b.query(CLAIM_SQL, params).then(() => 'inserted', e => e.code);
  await new Promise(r => setTimeout(r, 200));
  await a.query('COMMIT');
  assert.equal(await bInsert, '23505');
  await b.query('ROLLBACK');

  // And concurrent claimRun with the index still yields exactly one claim.
  const third = 'levi-week-2099-01-19';
  const results = await Promise.all(Array.from({ length: 20 }, () =>
    createProductionDeps({ db, env: ENV }).claimRun({ cycleId: third, authorityHash: 'h' })));
  assert.equal(results.filter(r => r.claimed).length, 1);

  psql(pg, ROLLBACK);
  assert.equal((await db.query(`SELECT count(*)::int AS n FROM pg_indexes WHERE indexname = 'agent_log_scout_levi_cycle_claim_uniq'`)).rows[0].n, 0);
});

test('R8 (open, not fixed here): setter handoff recompute must not expose tenant-21 review-only contact_now rows',
  { todo: 'shared setter gate exempts only tenant 10 from review-only exclusion; needs an approved gate change' }, async t => {
    const { db } = await freshDatabase(t, 'levi-r8-');
    await db.query(`ALTER TABLE prospects ADD COLUMN setter_updated_at TIMESTAMPTZ`);
    const { importReviewOnlyContacts } = require('../services/reviewOnlyProspectImportService');
    await importReviewOnlyContacts([{ name: 'Review Only Person', company: 'Review Co', reviewState: 'contact_now',
      market: 'Example County', sourceUrl: 'https://example.invalid/1' }], { clientId: 21, db, actorId: 'test' });
    const { recomputeSetterVisibility } = require('../utils/setterVisibility');
    await recomputeSetterVisibility(db, { clientId: 21, reason: 'handoff', preserveOverrides: true });
    const row = (await db.query(`SELECT icp_score, setter_visible, ao_outreach_review_required FROM prospects WHERE client_id = 21`)).rows[0];
    assert.equal(row.ao_outreach_review_required, true);
    assert.equal(row.setter_visible, false, `review-only row became setter-visible (icp_score ${row.icp_score})`);
  });
