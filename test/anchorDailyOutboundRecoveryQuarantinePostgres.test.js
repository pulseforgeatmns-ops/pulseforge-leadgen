'use strict';

// Only disposable loopback PostgreSQL. No application pool, provider transport,
// live identity, credentials, or production connection variables are consumed.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const quarantine = require('../services/anchorEmailQuarantine');
const preflight = require('../scripts/anchorEmailRecoveryPreflight');
const { GovernedOutboundStore } = require('../services/governedOutboundStore');
const { withTenantOutreachExecutorLock, EXECUTOR_LOCK_NAMESPACE } = require('../services/tenantOutreachExecutorLock');
const root = path.resolve(__dirname, '..');
const proposalPath = path.join(root, 'docs/sql/anchor-email-recovery-schema-proposal.sql');
const proposal = fs.readFileSync(proposalPath, 'utf8');
const decision = Object.freeze({ tenantId: '10', itemId: 'test-old-item', envelopeId: 'test-old-envelope',
  programId: 'test-program', missionId: 'test-old-mission', canonicalId: 'test-old-canonical',
  candidateId: 'test-candidate', prospectId: 'test-crm-contact', companyId: 'test-company', expectedRevision: 'test-revision',
  attemptToken: '2026-10-12 12:34:56.123456+00', recipientEmail: 'held@held-business.test',
  companyNameKey: 'heldbusiness', companyDomain: 'held-business.test',
  expectedStatus: 'uncertain', expectedReason: 'test_missing_column', approvalRef: 'local-synthetic-quarantine-approval' });
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function tracedPool(pool, hook) {
  return { query: pool.query.bind(pool), async connect() {
    const db = await pool.connect();
    return { async query(sql, values) { return hook(sql, values, () => db.query(sql, values)); }, release: db.release.bind(db) };
  } };
}

test('SPEC-261 quarantine, schema guards and preflight on native PostgreSQL', { timeout: 120000 }, async t => {
  const instance = await startDisposablePostgres('anchor-quarantine-');
  const pool = new Pool({ connectionString: instance.connectionString, max: 12 });
  t.after(async () => { await pool.end(); await instance.stop(); });
  assert.match((await pool.query('SHOW server_version')).rows[0].server_version, /^17\./);
  async function install() {
    const db = await pool.connect();
    try {
      await db.query("SELECT set_config('anchor_email_recovery.maintenance_armed','true',false),set_config('anchor_email_recovery.provider_gate_verified','closed',false),set_config('anchor_email_recovery.verified_source_revision',$1,false)", ['a'.repeat(40)]);
      await db.query(proposal);
    } finally { await db.query('ROLLBACK'); db.release(); }
  }
  async function reset({ installSchema = true, review = true } = {}) {
    // Destruction is restricted to this freshly-created disposable fixture.
    await pool.query(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;
      CREATE TABLE companies(id text PRIMARY KEY,client_id integer NOT NULL,name text,domain text,website text);
      CREATE TABLE prospects(id text PRIMARY KEY,client_id integer NOT NULL,company_id text,email text${review ? ',ao_outreach_review_required boolean' : ''});
      CREATE TABLE acquisition_outbound_envelopes(id text PRIMARY KEY,tenant_id text,program_id text,mission_id text,revision text);
      CREATE TABLE acquisition_outbound_items(id text PRIMARY KEY,tenant_id text,envelope_id text,candidate_id text,prospect_id text,company_id text,email text,status text,reason text,attempted_at timestamptz,provider_message_id text);
      CREATE TABLE acquisition_mission_outbound_executions(id text PRIMARY KEY,tenant_id text,mission_id text,prospect_id text,status text,prepared_artifact_revision text,provider_message_id text,sent_at timestamptz,attempted_at timestamptz,payload jsonb);
      INSERT INTO companies VALUES('test-company',10,'Held Business LLC','held-business.test','https://www.held-business.test'),('fresh-company',10,'Fresh Business Inc','fresh-business.test','https://fresh-business.test'),('other-tenant-company',13,'Other Tenant Ltd','other.test',NULL);
      INSERT INTO prospects(id,client_id,company_id,email) VALUES('test-crm-contact',10,'test-company','held@held-business.test'),('fresh-contact',10,'fresh-company','fresh@fresh-business.test'),('other-tenant-contact',13,'other-tenant-company','other@other.test');
      INSERT INTO acquisition_outbound_envelopes VALUES('test-old-envelope','10','test-program','test-old-mission','test-revision');
      INSERT INTO acquisition_outbound_items VALUES('test-old-item','10','test-old-envelope','test-candidate','test-crm-contact','test-company','held@held-business.test','uncertain','test_missing_column','2026-10-12 12:34:56.123456+00',NULL);
      INSERT INTO acquisition_mission_outbound_executions VALUES('test-old-canonical','10','test-old-mission','test-candidate','attempted','test-revision',NULL,NULL,'2026-10-12 12:34:56.123+00','{"email":"held@held-business.test"}');`);
    if (installSchema) await install();
  }
  const oldRows = async () => (await pool.query(`SELECT 'item' AS kind,to_jsonb(i) AS row FROM acquisition_outbound_items i WHERE id='test-old-item'
    UNION ALL SELECT 'canonical',to_jsonb(c) FROM acquisition_mission_outbound_executions c WHERE id='test-old-canonical' ORDER BY kind`)).rows;
  const count = async () => Number((await pool.query('SELECT count(*) AS n FROM anchor_email_quarantines')).rows[0].n);
  const fresh = { tenantId: '10', itemId: 'fresh-item', prospectId: 'fresh-contact', companyId: 'fresh-company', email: 'fresh@fresh-business.test' };

  await t.test('schema proposal aborts before DDL for absent/open/unknown maintenance evidence', async () => {
    await reset({ installSchema: false });
    const db = await pool.connect();
    try {
      for (const [armed, gate, revision] of [['','', ''],['false','closed','a'.repeat(40)],['true','open','a'.repeat(40)],['true','unknown','a'.repeat(40)],['true','closed','unknown']]) {
        await db.query("SELECT set_config('anchor_email_recovery.maintenance_armed',$1,false),set_config('anchor_email_recovery.provider_gate_verified',$2,false),set_config('anchor_email_recovery.verified_source_revision',$3,false)", [armed, gate, revision]);
        await assert.rejects(db.query(proposal), /anchor_recovery_verified_closed_gate_required/);
        await db.query('ROLLBACK');
        assert.equal((await db.query("SELECT to_regclass('public.anchor_email_recovery_control') AS table")).rows[0].table, null);
      }
    } finally { db.release(); }
  });

  await t.test('proposal creates only separate recovery tables and no shared prospect defaults/backfill', async () => {
    await reset({ installSchema: false, review: false });
    const before = (await pool.query('SELECT * FROM prospects ORDER BY id')).rows;
    await install();
    assert.deepEqual((await pool.query('SELECT * FROM prospects ORDER BY id')).rows, before);
    assert.equal((await pool.query("SELECT count(*)::int n FROM information_schema.columns WHERE table_name='prospects' AND column_name='ao_outreach_review_required'")).rows[0].n, 0);
    assert.equal((await pool.query('SELECT maintenance_armed FROM anchor_email_recovery_control')).rows[0].maintenance_armed, true);
  });

  await t.test('exact microsecond quarantine preserves both original ledgers and canonical candidate identity', async () => {
    await reset(); const before = await oldRows();
    const applied = await quarantine.applyQuarantine(pool, decision);
    assert.equal(applied.applied, true); assert.equal(applied.outcome, 'UNKNOWN'); assert.equal(applied.retryAllowed, false);
    assert.deepEqual(await oldRows(), before);
    const rows = await quarantine.listUnresolved(pool, '10');
    assert.equal(rows.length, 1); assert.equal(rows[0].canonical_id, decision.canonicalId);
    assert.equal(rows[0].attempt_token, decision.attemptToken);
    assert.equal(rows[0].candidate_id, decision.candidateId);
    const saved = await quarantine.requireQuarantine(pool, quarantine.decisionHash(decision));
    assert.equal(quarantine.classifyBlockingRows(rows, [saved]).blockingCount, 0);
    assert.equal(quarantine.classifyBlockingRows(rows, [saved]).rawCount, 1);
  });

  await t.test('identical decision is idempotent; missing approval or changed decision cannot replace it', async () => {
    await reset(); const first = await quarantine.applyQuarantine(pool, decision);
    const repeat = await quarantine.applyQuarantine(pool, decision);
    assert.equal(repeat.idempotent, true); assert.equal(repeat.applied, false); assert.equal(await count(), 1);
    assert.equal(repeat.quarantine.created_at.getTime(), first.quarantine.created_at.getTime());
    await assert.rejects(quarantine.applyQuarantine(pool, { ...decision, approvalRef: '' }), /invalid_decision/);
    await assert.rejects(quarantine.applyQuarantine(pool, { ...decision, approvalRef: 'changed-synthetic-approval' }), /decision_conflict/);
    await assert.rejects(quarantine.requireQuarantine(pool, '0'.repeat(64)), /quarantine_required/);
    await assert.rejects(quarantine.requireQuarantine(pool), /hash_required/);
  });

  await t.test('quarantines are immutable and survive late receipts and terminal status changes', async () => {
    await reset(); await quarantine.applyQuarantine(pool, decision);
    for (const sql of ["UPDATE anchor_email_quarantines SET approval_ref='rewrite'", 'DELETE FROM anchor_email_quarantines', 'TRUNCATE anchor_email_quarantines']) await assert.rejects(pool.query(sql), /permanent_record/);
    await pool.query("UPDATE acquisition_outbound_items SET status='sent',provider_message_id='late-local-receipt'; UPDATE acquisition_mission_outbound_executions SET status='sent',provider_message_id='late-local-receipt',sent_at=now()");
    assert.equal((await quarantine.applyQuarantine(pool, decision)).idempotent, true);
    await assert.rejects(quarantine.assertNotQuarantined(pool, { ...fresh, itemId: decision.itemId }), /quarantined/);
    assert.equal(await count(), 1);
  });

  for (const mutation of ["attempted_at=attempted_at+interval '1 microsecond'", "attempted_at=NULL", "status='sent'", "reason='changed'", "provider_message_id='receipt'", "candidate_id='other-candidate'", "prospect_id='fresh-contact'", "company_id='fresh-company'"]) {
    await t.test(`CAS rejects stale original item: ${mutation}`, async () => {
      await reset(); await pool.query(`UPDATE acquisition_outbound_items SET ${mutation}`);
      const before = await oldRows(); await assert.rejects(quarantine.applyQuarantine(pool, decision), /attempt_changed/);
      assert.equal(await count(), 0); assert.deepEqual(await oldRows(), before);
    });
  }

  await t.test('canonical revision/contact substitution fails without conflating candidate and contact', async () => {
    for (const mutation of ["prospect_id='test-crm-contact'", "prepared_artifact_revision='other-revision'", "payload='{}'::jsonb || '{\"email\":\"other@example.test\"}'::jsonb", "provider_message_id='late-receipt'"]) {
      await reset(); await pool.query(`UPDATE acquisition_mission_outbound_executions SET ${mutation}`);
      await assert.rejects(quarantine.applyQuarantine(pool, decision), /attempt_changed/); assert.equal(await count(), 0);
    }
  });

  await t.test('second UNKNOWN and NULL attempt token remain raw blockers; session TZ cannot round tokens', async () => {
    await reset(); const saved = (await quarantine.applyQuarantine(pool, decision)).quarantine;
    await pool.query(`INSERT INTO acquisition_outbound_items SELECT 'second-item','10',envelope_id,'second-candidate','fresh-contact','fresh-company','fresh@fresh-business.test','attempted','unknown',NULL,NULL FROM acquisition_outbound_items LIMIT 1`);
    const db = await pool.connect();
    try {
      await db.query("SET TIME ZONE 'Pacific/Auckland'");
      const rows = await quarantine.listUnresolved(db, '10');
      assert.equal(rows.find(r => r.item_id === decision.itemId).attempt_token, decision.attemptToken);
      const result = quarantine.classifyBlockingRows(rows, [saved]);
      assert.equal(result.rawCount, 2); assert.equal(result.quarantinedCount, 1); assert.equal(result.blockingCount, 1);
      assert.equal(result.blockingRows[0].attempt_token, null);
    } finally { await db.query("SET TIME ZONE 'UTC'"); db.release(); }
  });

  await t.test('recipient/contact/company/item aliases block independently; other tenant remains distinct', async () => {
    await reset(); await quarantine.applyQuarantine(pool, decision);
    for (const change of [{ email: decision.recipientEmail.toUpperCase() }, { prospectId: decision.prospectId }, { companyId: decision.companyId }, { itemId: decision.itemId }]) {
      await assert.rejects(quarantine.assertNotQuarantined(pool, { ...fresh, ...change }), /quarantined/);
    }
    assert.equal(await quarantine.assertNotQuarantined(pool, fresh), true);
    assert.equal(await quarantine.assertNotQuarantined(pool, { ...fresh, tenantId: '13' }), true);
  });

  await t.test('recreated companies with matching name/domain and shared-profile ambiguity stay blocked', async () => {
    await reset(); await quarantine.applyQuarantine(pool, decision);
    for (const [name, domain, website] of [['Unique Fresh Name','held-business.test',null],['Held Business Incorporated','new-domain.test',null],['Fresh Business','gmail.com',null],['Fresh Business','fresh-business.test','https://facebook.com/fresh']]) {
      await pool.query('UPDATE companies SET name=$1,domain=$2,website=$3 WHERE id=$4', [name, domain, website, fresh.companyId]);
      await assert.rejects(quarantine.assertNotQuarantined(pool, fresh), /quarantined_company_alias|company_identity_unknown/);
    }
    await pool.query("UPDATE companies SET name='Fresh Business',domain='fresh-business.test',website=NULL WHERE id='fresh-company'; UPDATE companies SET name='Renamed Held Business' WHERE id='test-company'");
    await assert.rejects(quarantine.assertNotQuarantined(pool, fresh), /company_identity_changed/);
    await pool.query("DELETE FROM companies WHERE id='test-company'");
    await assert.rejects(quarantine.assertNotQuarantined(pool, fresh), /company_identity_unknown/);
  });

  await t.test('dry-run inspection/preflight use only reads, no mutation locks, and report missing shared schema by tenant', async () => {
    await reset({ review: false }); const before = await oldRows();
    const db = await pool.connect();
    try {
      await db.query('BEGIN READ ONLY');
      const reads = { query(sql, values) { assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE|ALTER|CREATE|TRUNCATE|FOR SHARE|FOR UPDATE|pg_advisory)\b/i); return db.query(sql, values); } };
      const result = await quarantine.inspectQuarantine(reads, decision);
      assert.equal(result.outcome, 'UNKNOWN'); assert.equal(result.retryAllowed, false); assert.equal(result.applied, false); assert.equal(result.exactAttemptMatches, true);
      const report = await preflight.inspectPreflight(reads);
      assert.equal(report.status, 'blocked'); assert.equal(report.activationAllowed, false);
      assert.ok(report.blockers.includes('missing_column:prospects.ao_outreach_review_required'));
      assert.deepEqual(report.sharedProspectsCountsByTenant.map(r => [r.tenant_id, r.total, r.review_unknown]), [['10',2,2],['13',1,1]]);
      assert.equal((await db.query('SELECT txid_current_if_assigned() AS id')).rows[0].id, null);
    } finally { await db.query('ROLLBACK'); db.release(); }
    assert.deepEqual(await oldRows(), before); assert.equal(await count(), 0);
  });

  await t.test('existing tenant and scheduler locks exclude quarantine apply without writes', async () => {
    await reset(); const db = await pool.connect();
    try {
      for (const [keys, halted] of [[[261018,10],'overlap'],[[EXECUTOR_LOCK_NAMESPACE,0],'scheduler_overlap']]) {
        await db.query('SELECT pg_advisory_lock($1,$2)', keys);
        try { const result = await quarantine.applyQuarantine(pool, decision); assert.equal(result.halted, halted); assert.equal(result.retryAllowed, false); assert.equal(result.applied, false); }
        finally { await db.query('SELECT pg_advisory_unlock($1,$2)', keys); }
      }
    } finally { db.release(); }
    assert.equal(await count(), 0);
  });

  await t.test('apply excludes dispatch while holding recovery/identity/item locks; lock order is shared with callers', async () => {
    await reset(); const entered = deferred(), resume = deferred(), trace = [];
    const wrapped = tracedPool(pool, async (sql, values, query) => {
      trace.push({ sql, values }); const result = await query();
      if (/SELECT i\.\*/.test(sql)) { entered.resolve(); await resume.promise; }
      return result;
    });
    const applying = quarantine.applyQuarantine(wrapped, decision); await entered.promise;
    try {
      assert.equal((await new GovernedOutboundStore(pool, '10').lock(() => assert.fail('dispatch must be excluded'))).halted, 'overlap');
      assert.equal((await withTenantOutreachExecutorLock(pool, () => assert.fail('scheduler must be excluded'))).halted, 'scheduler_overlap');
      const db = await pool.connect();
      try {
        await db.query("BEGIN; SET LOCAL lock_timeout='100ms'");
        await assert.rejects(db.query("UPDATE acquisition_outbound_items SET reason='raced' WHERE id='test-old-item'"), error => error.code === '55P03');
        await db.query('ROLLBACK');
      } finally { db.release(); }
    } finally { resume.resolve(); }
    assert.equal((await applying).applied, true);
    const keys = trace.filter(q => /pg_advisory_xact_lock\(/.test(q.sql)).map(q => q.values[0]);
    assert.deepEqual(keys, [quarantine.RECOVERY_LOCK_KEY, ...quarantine.identityLockKeys(decision)]);
    assert.ok(trace.findIndex(q => /recovery_control.*FOR UPDATE/.test(q.sql)) < trace.findIndex(q => /SELECT i\.\*/.test(q.sql)));
  });

  await t.test('concurrent item change before row acquisition aborts CAS atomically and releases all locks', async () => {
    await reset(); const entered = deferred(), resume = deferred();
    const wrapped = tracedPool(pool, async (sql, values, query) => { if (/SELECT i\.\*/.test(sql)) { entered.resolve(); await resume.promise; } return query(); });
    const applying = quarantine.applyQuarantine(wrapped, decision); const rejected = assert.rejects(applying, /attempt_changed/);
    await entered.promise;
    try { await pool.query("UPDATE acquisition_outbound_items SET attempted_at=attempted_at+interval '1 microsecond'"); }
    finally { resume.resolve(); }
    await rejected; assert.equal(await count(), 0);
    assert.equal(await new GovernedOutboundStore(pool, '10').lock(async () => 'released'), 'released');
    assert.equal(await withTenantOutreachExecutorLock(pool, async () => 'released'), 'released');
  });

  await t.test('shared recovery handoff lock and company FOR SHARE serialize quarantine and identity edits', async () => {
    await reset(); const db = await pool.connect(); const reached = deferred();
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))', [quarantine.RECOVERY_LOCK_KEY]);
    const wrapped = tracedPool(pool, async (sql, values, query) => { if (/pg_advisory_xact_lock\(/.test(sql)) reached.resolve(); return query(); });
    let settled = false; const applying = quarantine.applyQuarantine(wrapped, decision).finally(() => { settled = true; });
    await reached.promise; await new Promise(resolve => setTimeout(resolve, 30)); assert.equal(settled, false);
    await db.query('ROLLBACK'); db.release(); await applying;
    const guard = await pool.connect(), writer = await pool.connect();
    try {
      await guard.query('BEGIN'); await quarantine.assertNotQuarantined(guard, { ...fresh, lockCompanyRows: true });
      await writer.query("BEGIN; SET LOCAL lock_timeout='100ms'");
      await assert.rejects(writer.query("UPDATE companies SET domain='held-business.test',website=NULL WHERE id='fresh-company'"), error => error.code === '55P03');
    } finally { await writer.query('ROLLBACK'); writer.release(); await guard.query('ROLLBACK'); guard.release(); }
  });

  await t.test('rollback retains maintenance/no-resend and once-only durable reservation marker', async () => {
    await reset(); await quarantine.applyQuarantine(pool, decision);
    for (const sql of ["UPDATE anchor_email_recovery_control SET maintenance_armed=false", 'DELETE FROM anchor_email_recovery_control', 'TRUNCATE anchor_email_recovery_control']) await assert.rejects(pool.query(sql));
    await pool.query("UPDATE anchor_email_recovery_control SET manifest='{}'; UPDATE anchor_email_recovery_control SET manifest=NULL");
    assert.equal((await pool.query('SELECT maintenance_armed FROM anchor_email_recovery_control')).rows[0].maintenance_armed, true);
    await pool.query(`INSERT INTO anchor_email_recovery_reservations(manifest_id,item_id,attempt_token,prospect_id,company_id,recipient_email)
      VALUES('test-manifest','fresh-item','2026-10-12 13:00:00.123456+00','fresh-contact','fresh-company','fresh@fresh-business.test')`);
    const guard = await pool.connect();
    await guard.query('BEGIN');
    await pool.query("UPDATE anchor_email_recovery_reservations SET dispatch_started_at=now() WHERE item_id='fresh-item' AND dispatch_started_at IS NULL");
    await guard.query('ROLLBACK'); guard.release();
    assert.ok((await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations')).rows[0].dispatch_started_at);
    for (const sql of ["UPDATE anchor_email_recovery_reservations SET dispatch_started_at=NULL", "UPDATE anchor_email_recovery_reservations SET manifest_id='retry'", 'DELETE FROM anchor_email_recovery_reservations', 'TRUNCATE anchor_email_recovery_reservations']) await assert.rejects(pool.query(sql), /permanent/);
    assert.equal(await count(), 1);
  });
});

test('preflight CLI defaults to disconnected and rejects remote/apply inputs', async () => {
  assert.equal(JSON.parse((await preflight.main([], {})).output).connected, false);
  assert.equal((await preflight.main(['--apply'], {})).exitCode, 2);
  for (const target of ['postgres://host.invalid/db', 'postgres://localhost:5432/db', 'postgres://127.0.0.1:5432/db?host=remote.test']) assert.throws(() => preflight.localConfig(target));
});

test('proposal is excluded from actual startup schema runners and contains no review policy DDL', () => {
  assert.equal(path.dirname(proposalPath), path.join(root, 'docs/sql'));
  for (const file of ['server.js','utils/clientsStartup.js','utils/aoProspectRoutingSchema.js','utils/aoCrmSchema.js','package.json','railway.json','Dockerfile']) {
    const source = fs.readFileSync(path.join(root,file),'utf8');
    assert.ok(!source.includes('anchor-email-recovery-schema-proposal'));
  }
  assert.doesNotMatch(proposal, /ALTER\s+TABLE\s+(?:public\.)?prospects|UPDATE\s+prospects|DROP\s+TABLE/i);
  assert.match(fs.readFileSync(path.join(root,'utils/aoCrmSchema.js'),'utf8'), /fs\.readFileSync/);
});
