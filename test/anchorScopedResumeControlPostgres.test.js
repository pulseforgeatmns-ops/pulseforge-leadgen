'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { applyCrmPreparationSchema, schema: crmSchema } = require('./helpers/anchorCrmPreparationSchema');
const { schema, applyScopedSchema, seedScopedResume } = require('./helpers/anchorScopedResumePostgresFixture');
const control = require('../services/anchorScopedResumeControl');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { QUERIES, TABLES, classifySchema } = require('../scripts/rolloutPreflight');
const options = f => ({ now: f.now, env: f.env });
const install = (pool, f, extra = {}) => control.installScopedResume(pool, { manifest: f.manifest, approvalReference: f.manifest.approvalReference, ...extra }, f.actor, options(f));
const revoke = (pool, f) => control.revokeScopedResume(pool, { manifestId: f.manifest.id, approvalReference: 'local-synthetic-explicit-revocation' }, f.actor, options(f));
const consume = (pool, f, operationId, extra = {}) => control.consumeScopedOperation(pool, { manifest: f.manifest, capability: 'preparation', operation: 'freeze', operationId, ...extra }, options(f));
const initialization = f => ({ action: 'initialize_null_review_false', approvalReference: 'local-synthetic-reviewed-null-initialization',
  identities: f.manifest.admissions.map(({ prospectId, companyId, recipientEmail, identityFingerprint }) => ({ prospectId, companyId, recipientEmail, identityFingerprint })) });
async function rows(pool, sql, values = []) { return (await pool.query(sql, values)).rows; }
async function count(pool, table) { return Number((await rows(pool, `SELECT count(*) AS n FROM ${table}`))[0].n); }
async function recoverySchema(pool) {
  return rows(pool, `SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,
    pg_get_expr(d.adbin,d.adrelid) AS default_value FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
    LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE c.relname=ANY($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum`,
  [['anchor_email_recovery_control','anchor_email_quarantines','anchor_email_recovery_reservations']]);
}
test('native SPEC262 scoped control and immutable consumption', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-scoped-control-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 8 });
  t.after(async () => { await pool.end(); await pg.stop(); });
  await t.test('native PostgreSQL server is supported and no schema auto-install occurs', async () => {
    assert.match((await rows(pool, 'SELECT version() AS v'))[0].v, /PostgreSQL /);
    assert.ok(Number((await rows(pool, 'SHOW server_version_num'))[0].server_version_num) >= 140000);
    const f = await seedScopedResume(pool, { schemaInstalled: false });
    await assert.rejects(install(pool, f), /anchor_scoped_resume_grants/);
    assert.equal((await rows(pool, "SELECT to_regclass('anchor_scoped_resume_grants') AS t"))[0].t, null);
  });
  await t.test('guarded manual schema adds only nullable/no-default review; old spreadsheet contract stays blocked', async () => {
    await seedScopedResume(pool, { schemaInstalled: false });
    const before = await recoverySchema(pool);
    await pool.query('ALTER TABLE prospects DROP COLUMN ao_outreach_review_required');
    const db = await pool.connect();
    try { await assert.rejects(db.query(schema), /schema_approval_required/); await db.query('ROLLBACK'); }
    finally { db.release(); }
    await assert.rejects(applyScopedSchema(pool), /review_column_approval_required/);
    await applyScopedSchema(pool, { addReviewColumn: true });
    assert.deepEqual(await recoverySchema(pool), before);
    const col = (await rows(pool, QUERIES.columns, [TABLES])).find(c => c.table_name === 'prospects' && c.column_name === 'ao_outreach_review_required');
    assert.equal(col.type_name, 'bool'); assert.equal(col.not_null, false); assert.equal(col.default_expr, null);
    assert.equal((await rows(pool, 'SELECT count(*)::int AS n FROM prospects WHERE ao_outreach_review_required IS NOT NULL'))[0].n, 0);
    const report = classifySchema({ columns: [col], tables: [], constraints: [], indexes: [], triggers: [] });
    for (const id of ['column.prospects.ao_outreach_review_required', 'default.prospects.ao_outreach_review_required']) assert.equal(report.checks.find(c => c.id === id).status, 'blocked');
    assert.equal((await rows(pool, "SELECT count(*)::int AS n FROM information_schema.columns WHERE table_name='prospects' AND column_name='ao_call_suppressed'"))[0].n, 0);
  });
  await t.test('manual schema preserves existing explicit flags/default/not-null and rejects incompatible type atomically', async () => {
    await seedScopedResume(pool, { schemaInstalled: false });
    await pool.query('ALTER TABLE prospects ALTER COLUMN ao_outreach_review_required SET DEFAULT true,ALTER COLUMN ao_outreach_review_required SET NOT NULL');
    const before = await rows(pool, 'SELECT id,ao_outreach_review_required FROM prospects ORDER BY id');
    await applyScopedSchema(pool);
    const col = (await rows(pool, QUERIES.columns, [TABLES])).find(c => c.table_name === 'prospects' && c.column_name === 'ao_outreach_review_required');
    assert.equal(col.not_null, true); assert.equal(col.default_expr, 'true');
    assert.deepEqual(await rows(pool, 'SELECT id,ao_outreach_review_required FROM prospects ORDER BY id'), before);
    await seedScopedResume(pool, { schemaInstalled: false });
    await pool.query('ALTER TABLE prospects ALTER COLUMN ao_outreach_review_required TYPE text USING ao_outreach_review_required::text');
    await assert.rejects(applyScopedSchema(pool), /review_column_incompatible/);
    assert.equal((await rows(pool, "SELECT to_regclass('anchor_scoped_resume_grants') AS t"))[0].t, null);
  });
  await t.test('installation is explicit, current-actor-bound, exact and idempotent; no unrelated writes', async () => {
    const f = await seedScopedResume(pool);
    const frozen = await rows(pool, 'SELECT row_to_json(q) AS row FROM anchor_email_quarantines q');
    await assert.rejects(control.installScopedResume(pool, { manifest: f.manifest }, f.actor, options(f)), /explicit_approval_required/);
    await assert.rejects(control.installScopedResume(pool, { manifest: f.manifest, approvalReference: f.manifest.approvalReference }, f.actor, { ...options(f), env: {} }), /activation_required/);
    await pool.query("UPDATE users SET role='viewer'"); await assert.rejects(install(pool, f), /operator_invalid/);
    await pool.query("UPDATE users SET role='admin'");
    assert.equal((await install(pool, f)).installed, true);
    assert.equal((await install(pool, f)).idempotent, true);
    assert.equal(await count(pool, 'anchor_scoped_resume_grants'), 1);
    assert.equal((await rows(pool, 'SELECT maintenance_armed FROM anchor_email_recovery_control'))[0].maintenance_armed, true);
    assert.deepEqual(await rows(pool, 'SELECT row_to_json(q) AS row FROM anchor_email_quarantines q'), frozen);
    await control.assertInstalledScopedResume(pool, f.manifest, options(f));
    await assert.rejects(control.assertInstalledScopedResume(pool, { ...f.manifest, approvalReference: 'different' }, options(f)), /not_installed/);
  });
  await t.test('current actor removal/role change and program reauthorization revoke runtime authority', async () => {
    const f = await seedScopedResume(pool); await install(pool, f);
    await pool.query('UPDATE users SET active=false');
    await assert.rejects(control.assertInstalledScopedResume(pool, f.manifest, options(f)), /operator_invalid/);
    await pool.query("UPDATE users SET active=true,role='manager'");
    await assert.rejects(control.assertInstalledScopedResume(pool, f.manifest, options(f)), /operator_invalid/);
    await pool.query("UPDATE users SET role='admin'");
    await pool.query("UPDATE acquisition_outbound_programs SET authorized_by='different-operator'");
    await assert.rejects(control.assertInstalledScopedResume(pool, f.manifest, options(f)), /operator_invalid/);
  });
  await t.test('CRM operation schema is manual, closed-only, exact and preserves prior consumption on repeat', async () => {
    const f = await seedScopedResume(pool, { preparation: true });
    const db = await pool.connect();
    try { await assert.rejects(db.query(crmSchema), /schema_approval_required/); await db.query('ROLLBACK'); } finally { db.release(); }
    await install(pool, f);
    await consume(pool, f, 'local-synthetic-preexisting-consumption');
    const before = await rows(pool, 'SELECT row_to_json(c) AS row FROM anchor_scoped_resume_consumption c');
    await assert.rejects(applyCrmPreparationSchema(pool), /tenant_10_closed_required/);
    await revoke(pool, f);
    await applyCrmPreparationSchema(pool);
    await applyCrmPreparationSchema(pool);
    assert.deepEqual(await rows(pool, 'SELECT row_to_json(c) AS row FROM anchor_scoped_resume_consumption c'), before);
    await pool.query('ALTER TABLE anchor_scoped_resume_consumption DROP CONSTRAINT anchor_scoped_resume_consumption_check1');
    await assert.rejects(applyCrmPreparationSchema(pool), /schema_drift/);
  });
  await t.test('CRM draft identities and later frozen items each consume the explicit preparation item budget', async () => {
    const f = await seedScopedResume(pool, { preparation: true });
    await applyCrmPreparationSchema(pool);
    f.manifest.scopes.preparation.operations.push('prepare_existing_crm');
    f.manifest.scopes.preparation.dailyItemCap = 1;
    f.manifest.scopes.preparation.totalItemCap = 1;
    await install(pool, f);
    const input = { manifest: f.manifest, capability: 'preparation', operation: 'prepare_existing_crm',
      operationId: 'local-synthetic-crm-draft-budget', identities: f.manifest.admissions.slice(0, 1) };
    await assert.rejects(control.consumeScopedOperation(pool, { ...input, itemCount: 0 }, options(f)), /operation_item_count_invalid/);
    const result = await control.consumeScopedOperation(pool, input, options(f));
    assert.equal(result.itemCount, 1);
    await assert.rejects(consume(pool, f, 'local-synthetic-freeze-after-draft', { identities: input.identities }), /operation_cap_reached/);
    assert.equal((await rows(pool, 'SELECT item_count FROM anchor_scoped_resume_consumption'))[0].item_count, 1);
  });
  await t.test('NULL initialization requires independent exact fingerprint approval; true cannot be released', async () => {
    const f = await seedScopedResume(pool);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id=$1', [f.prospect.id]);
    await assert.rejects(install(pool, f), /outreach_review_required/);
    assert.equal(await count(pool, 'anchor_scoped_resume_grants'), 0);
    const bad = initialization(f); bad.identities[0].identityFingerprint = 'a'.repeat(64);
    await assert.rejects(install(pool, f, { reviewInitialization: bad }), /initialization_not_admitted/);
    assert.equal((await install(pool, f, { reviewInitialization: initialization(f) })).initialized, 1);
    assert.equal((await rows(pool, 'SELECT ao_outreach_review_required AS flag FROM prospects WHERE id=$1', [f.prospect.id]))[0].flag, false);
    const audit = (await rows(pool, 'SELECT review_initializations AS a FROM anchor_scoped_resume_grants'))[0].a;
    assert.equal(audit[0].beforeReview, null); assert.equal(audit[0].afterReview, false); assert.equal(audit[0].identityFingerprint, f.manifest.admissions[0].identityFingerprint);
    assert.equal((await install(pool, f, { reviewInitialization: initialization(f) })).idempotent, true);
    await seedScopedResume(pool);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1', [f.prospect.id]);
    await assert.rejects(install(pool, f, { reviewInitialization: initialization(f) }), /review_cas_failed/);
    assert.equal((await rows(pool, 'SELECT ao_outreach_review_required AS flag FROM prospects WHERE id=$1', [f.prospect.id]))[0].flag, true);
  });
  await t.test('a later held identity rolls back every earlier NULL initialization in the batch', async () => {
    const f = await seedScopedResume(pool);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id=$1', [f.prospect.id]);
    const approvedInitialization = initialization(f);
    const q = f.quarantine;
    const bound = await require('../services/anchorScopedResume').readScopedIdentity(pool, {
      tenantId: '10', prospectId: q.prospect_id, companyId: q.company_id, recipientEmail: q.recipient_email,
    }, { lock: false });
    f.manifest.admissions.push({ prospectId: q.prospect_id, companyId: q.company_id, recipientEmail: q.recipient_email,
      companyNameKey: bound.companyNameKey, companyDomain: bound.companyDomain, identityFingerprint: bound.identityFingerprint,
      approvalReference: 'local-explicit-but-still-held-identity' });
    // The fresh prospect sorts after the original quarantine; change the fixture
    // identity so the permitted CAS is attempted first, proving rollback.
    const fresh = f.manifest.admissions[0];
    const earlierId = '00000000-0000-4000-8000-000000000002';
    await pool.query('UPDATE prospects SET id=$1 WHERE id=$2', [earlierId, fresh.prospectId]);
    fresh.prospectId = earlierId;
    const changed = await require('../services/anchorScopedResume').readScopedIdentity(pool, { tenantId: '10', ...fresh }, { lock: false });
    fresh.identityFingerprint = changed.identityFingerprint;
    approvedInitialization.identities = [{ prospectId: fresh.prospectId, companyId: fresh.companyId, recipientEmail: fresh.recipientEmail, identityFingerprint: fresh.identityFingerprint }];
    await assert.rejects(install(pool, f, { reviewInitialization: approvedInitialization }), /outreach_review_required/);
    assert.equal((await rows(pool, 'SELECT ao_outreach_review_required AS flag FROM prospects WHERE id=$1', [earlierId]))[0].flag, null);
    assert.equal(await count(pool, 'anchor_scoped_resume_grants'), 0);
  });
  await t.test('installation diagnostics run inside native read-only transactions without row locks', async () => {
    const f = await seedScopedResume(pool); await install(pool, f);
    const db = await pool.connect();
    try { await db.query('BEGIN READ ONLY'); await control.assertInstalledScopedResume(db, f.manifest, options(f)); await db.query('COMMIT'); }
    finally { await db.query('ROLLBACK'); db.release(); }
  });
  await t.test('changed provenance/source/actor or unresolved uncertainty never installs', async () => {
    for (const sql of ["UPDATE prospects SET source='different' WHERE ao_outreach_review_required=false",
      "UPDATE acquisition_missions SET payload=jsonb_set(payload,'{objective}','\"changed\"')",
      'UPDATE users SET active=false',
      "UPDATE acquisition_outbound_items SET status='uncertain',attempted_at=now(),reason='unknown' WHERE status='pending'"]) {
      const f = await seedScopedResume(pool); await pool.query(sql);
      await assert.rejects(install(pool, f)); assert.equal(await count(pool, 'anchor_scoped_resume_grants'), 0);
      assert.equal((await rows(pool, 'SELECT manifest FROM anchor_email_recovery_control'))[0].manifest, null);
    }
  });
  await t.test('durable concurrent operation caps are atomic and replays cannot repeat effects', async () => {
    const f = await seedScopedResume(pool, { preparation: true }); await install(pool, f);
    const result = await Promise.allSettled([consume(pool, f, 'op-1'), consume(pool, f, 'op-2'), consume(pool, f, 'op-3')]);
    assert.equal(result.filter(r => r.status === 'fulfilled').length, 2);
    assert.equal(result.filter(r => r.status === 'rejected')[0].reason.code, 'anchor_scoped_resume_operation_cap_reached');
    const saved = await rows(pool, 'SELECT operation_id FROM anchor_scoped_resume_consumption');
    await assert.rejects(consume(pool, f, saved[0].operation_id), /operation_consumed/);
    await assert.rejects(consume(pool, f, 'op-under-count', { itemCount: 0 }), /item_count_invalid/);
    for (const sql of ['DELETE FROM anchor_scoped_resume_consumption', 'TRUNCATE anchor_scoped_resume_consumption', 'UPDATE anchor_scoped_resume_consumption SET item_count=0']) await assert.rejects(pool.query(sql), /permanent_record/);
    assert.equal(await count(pool, 'anchor_scoped_resume_consumption'), 2);
  });
  await t.test('canonical approval consumes an operation but does not double-count frozen identities', async () => {
    const f = await seedScopedResume(pool, { preparation: true });
    f.manifest.scopes.preparation.dailyItemCap = 1;
    f.manifest.scopes.preparation.totalItemCap = 1;
    f.manifest.scopes.preparation.operations.push('approve_canonical_envelope');
    f.manifest.scopes.email.approvalMode = 'delegated_existing_program';
    await install(pool, f);
    assert.equal((await consume(pool, f, 'freeze-one')).itemCount, 1);
    assert.equal((await consume(pool, f, 'approve-one', { operation: 'approve_canonical_envelope', itemCount: 0 })).itemCount, 0);
    const spent = (await rows(pool, 'SELECT count(*)::int AS operations,sum(item_count)::int AS items FROM anchor_scoped_resume_consumption'))[0];
    assert.deepEqual(spent, { operations: 2, items: 1 });
    await assert.rejects(consume(pool, f, 'read-only-reuse', { operation: 'reuse_existing_artifacts' }), /capability_closed/);
  });
  await t.test('budget survives failed downstream work, revocation, replacement, and local-day rollover', async () => {
    const f = await seedScopedResume(pool, { preparation: true });
    f.program.policy.expiresAt = '2026-10-23T00:00:00.000Z'; f.program.policy_hash = hash(f.program.policy);
    f.manifest.policyHash = f.program.policy_hash; f.manifest.expiresAt = '2026-10-22T20:30:00.000Z';
    await pool.query('UPDATE acquisition_outbound_programs SET policy=$1,policy_hash=$2 WHERE id=$3', [f.program.policy, f.program.policy_hash, f.program.id]);
    await install(pool, f); await consume(pool, f, 'failed-effect');
    await assert.rejects(Promise.reject(new Error('synthetic downstream failure')), /downstream/);
    await consume(pool, f, 'day-one-second'); await assert.rejects(consume(pool, f, 'day-one-third'), /cap_reached/);
    await revoke(pool, f); const oldManifest = f.manifest;
    await assert.rejects(consume(pool, f, 'revoked-effect'), /closed/);
    await assert.rejects(install(pool, f), /install_conflict/);
    f.manifest = { ...f.manifest, id: 'local-replacement-grant' }; f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    await install(pool, f); await assert.rejects(consume(pool, f, 'replacement-day-one'), /cap_reached/);
    f.now = new Date('2026-10-20T16:00:00.000Z'); await consume(pool, f, 'day-two-first');
    await assert.rejects(consume(pool, f, 'day-two-second'), /cap_reached/);
    assert.equal(await count(pool, 'anchor_scoped_resume_consumption'), 3);
    await assert.rejects(control.assertInstalledScopedResume(pool, oldManifest, options(f)), /not_installed/);
  });
  await t.test('grant/review audit is immutable and revoke is durable one-way without clearing latch', async () => {
    const f = await seedScopedResume(pool); await install(pool, f);
    for (const sql of ["UPDATE anchor_scoped_resume_grants SET approval_ref='forged'", 'DELETE FROM anchor_scoped_resume_grants', 'TRUNCATE anchor_scoped_resume_grants CASCADE']) await assert.rejects(pool.query(sql), /permanent/);
    assert.equal((await revoke(pool, f)).revoked, true); assert.equal((await revoke(pool, f)).idempotent, true);
    await assert.rejects(pool.query('UPDATE anchor_scoped_resume_grants SET revoked_at=NULL,revoke_approval_ref=NULL,revoked_by=NULL,revoked_role=NULL'), /permanent_grant/);
    const state = (await rows(pool, 'SELECT maintenance_armed,manifest FROM anchor_email_recovery_control'))[0];
    assert.deepEqual(state, { maintenance_armed: true, manifest: null });
  });
  await t.test('revoke waits for a guarded in-flight operation then closes all fresh work', async () => {
    const f = await seedScopedResume(pool, { preparation: true }); await install(pool, f); await consume(pool, f, 'reserved-before-guard');
    const guard = await pool.connect(); await guard.query('BEGIN');
    await guard.query("SELECT pg_advisory_xact_lock_shared(hashtextextended('anchor-email-recovery:10',0))");
    await control.assertInstalledScopedResume(guard, f.manifest, options(f));
    let finished = false; const revoking = revoke(pool, f).then(r => { finished = true; return r; });
    await new Promise(resolve => setTimeout(resolve, 80)); assert.equal(finished, false);
    await guard.query('COMMIT'); guard.release(); await revoking;
    await assert.rejects(consume(pool, f, 'after-revoke'), /closed/);
    assert.equal(await count(pool, 'anchor_scoped_resume_consumption'), 1);
  });
  await t.test('research unsupported contract cannot gain authority from a caller digest', async () => {
    const f = await seedScopedResume(pool);
    await assert.rejects(control.assertScopedResumeProviderBudget(pool, f.manifest, { providerBudgetHash: 'a'.repeat(64) }), /provider_budget_unverified/);
    await assert.rejects(consume(pool, f, 'disabled-preparation'), /capability_closed/);
  });
});
