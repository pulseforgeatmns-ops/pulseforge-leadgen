'use strict';
// Synthetic fixtures and native PostgreSQL only. External transports fail closed.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { createHash } = require('node:crypto');
const { Pool } = require('pg');
const deny = require('./helpers/denyExternalProviders.cjs');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const control = require('../services/anchorScopedResumeControl');
const authority = require('../services/anchorCrmPreparationAuthority');
const { prepareDrafts, actorFromRequest } = require('../services/anchorCrmPreparationAction');
const persistence = require('../services/acquisitionMissionPersistence');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const revision = 'a'.repeat(40), saturday = '2026-10-24T16:00:00.000Z';
function harness() {
  const filename = path.join(__dirname, 'anchorScopedResumePreparationAuthorityPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,protectedState,snapshot,effects,noSchemaWrites,afterConsumption:h.afterConsumption};', {
    module, exports: module.exports, __dirname, process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}
const h = harness();
const options = f => ({ env: f.env, now: () => f.now, sessionId: f.sessionId });
const input = f => ({ revision, manifestHash: hash(f.manifest), sourceMissionHash: hash(f.source) });
const actor = f => ({ id: '3', role: 'admin', sessionId: f.sessionId });
const install = (pool, f, a = actor(f)) => control.installScopedResume(h.noSchemaWrites(pool),
  { manifest: f.manifest, approvalReference: f.manifest.approvalReference }, a, options(f));
const revoke = (pool, f, a = actor(f)) => control.revokeScopedResume(h.noSchemaWrites(pool),
  { manifestId: f.manifest.id, approvalReference: 'synthetic-explicit-preparation-revocation' }, a, options(f));
const run = (pool, f, a = actor(f)) => prepareDrafts(h.noSchemaWrites(pool), input(f), a, options(f));
async function saveSession(pool, f) {
  await pool.query('INSERT INTO session(sid,sess,expire) VALUES($1,$2,$3) ON CONFLICT(sid) DO UPDATE SET sess=excluded.sess,expire=excluded.expire',
    [f.sessionId, f.session, new Date('2099-01-01T00:00:00Z')]);
}
async function setup(pool, t, accountClientId = null) {
  const f = await h.setup(pool, t);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS password_change_required boolean DEFAULT false;
    ALTER TABLE clients ADD COLUMN IF NOT EXISTS operator_switchable boolean DEFAULT true;
    CREATE TABLE IF NOT EXISTS session(sid text PRIMARY KEY,sess json NOT NULL,expire timestamp NOT NULL);
    DELETE FROM session;
    INSERT INTO users(id,client_id,role,active,email_verified,password_change_required)
      VALUES('19',10,'ao',true,true,false);
    UPDATE acquisition_outbound_programs SET authorized_by='19';`);
  await pool.query(`INSERT INTO users(id,client_id,role,active,email_verified,password_change_required)
    VALUES('3',$1,'admin',true,true,false)`, [accountClientId]);
  await pool.query('DELETE FROM users WHERE id=$1', [f.operator.id]);
  f.program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [f.program.id])).rows[0];
  f.source = JSON.parse(JSON.stringify(f.source).replaceAll('local-operator', '19'));
  await pool.query('UPDATE acquisition_missions SET payload=$1 WHERE id=$2', [f.source, f.source.id]);
  f.manifest.scopeHash = hash(missionScope(f.source));
  await pool.query('UPDATE acquisition_outbound_programs SET scope_hash=$1 WHERE id=$2', [f.manifest.scopeHash, f.program.id]);
  f.program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [f.program.id])).rows[0];
  f.sessionId = 'synthetic-preparation-admin-session-not-a-live-bearer';
  f.session = { authenticated: true, active_client_id: 10, user: { id: '3', role: 'admin', client_id: accountClientId,
    active: true, email_verified: true, password_change_required: false } };
  await saveSession(pool, f);
  f.manifest.operatorId = '3'; f.manifest.operatorRole = 'admin';
  Object.assign(f.manifest.preparationAuthority, { originalAuthorizerId: '19', approverAccountClientId: accountClientId,
    approverSessionHash: createHash('sha256').update(f.sessionId).digest('hex'),
    programBindingHash: authority.programBindingHash(f.program), sourceMissionHash: hash(f.source) });
  return f;
}
async function assertNoWrites(pool, action, expected) {
  const before = await h.snapshot(pool);
  await assert.rejects(action, expected);
  assert.deepEqual(await h.snapshot(pool), before, 'rejected authority must not leave durable writes');
  assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  assert.equal(deny.attempts(), 0);
}
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
function barrier() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
async function waitForDatabaseLock(pool, queryPattern) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const result = await pool.query(`SELECT pid FROM pg_stat_activity WHERE datname=current_database()
      AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE $1`, [queryPattern]);
    if (result.rows.length) return;
    await pause(20);
  }
  assert.fail(`Expected a real PostgreSQL row-lock waiter for ${queryPattern}`);
}
const consume = (pool, f, operationId) => control.consumeScopedOperation(h.noSchemaWrites(pool), {
  manifest: f.manifest, capability: 'preparation', operation: 'prepare_existing_crm',
  operationId, identities: f.manifest.admissions,
}, options(f));

async function mutate(pool, f, kind) {
  if (kind === 'current_ao') await pool.query("UPDATE users SET role='ao' WHERE id='3'");
  if (kind === 'stale_actor') await pool.query("DELETE FROM users WHERE id='3'");
  if (kind === 'inactive_actor') await pool.query("UPDATE users SET active=false WHERE id='3'");
  if (kind === 'role_changed') await pool.query("UPDATE users SET role='manager' WHERE id='3'");
  if (kind === 'wrong_account_tenant') await pool.query("UPDATE users SET client_id=11 WHERE id='3'");
  if (kind === 'account_binding_changed') await pool.query("UPDATE users SET client_id=10 WHERE id='3'");
  if (kind === 'password_reset') await pool.query("UPDATE users SET password_change_required=true WHERE id='3'");
  if (kind === 'unverified_actor') await pool.query("UPDATE users SET email_verified=false WHERE id='3'");
  if (kind === 'disabled_tenant') await pool.query('UPDATE clients SET active=false WHERE id=10');
  if (kind === 'nonswitchable_tenant') await pool.query('UPDATE clients SET operator_switchable=false WHERE id=10');
  if (kind === 'deleted_session') await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
  if (kind === 'expired_session') await pool.query('UPDATE session SET expire=$1 WHERE sid=$2', [f.now, f.sessionId]);
  if (kind === 'revoked_session') f.session.authenticated = false;
  if (kind === 'wrong_selected_tenant') f.session.active_client_id = 11;
  if (kind === 'missing_selected_tenant') delete f.session.active_client_id;
  if (kind === 'session_actor_changed') f.session.user.id = '19';
  if (kind === 'session_role_changed') f.session.user.role = 'ao';
  if (kind === 'session_account_changed') f.session.user.client_id = 10;
  if (kind === 'impersonation') f.session.impersonation = { active: true };
  if (kind === 'session_password_reset') f.session.user.password_change_required = true;
  if (['revoked_session', 'wrong_selected_tenant', 'missing_selected_tenant', 'session_actor_changed', 'session_role_changed',
    'session_account_changed', 'impersonation', 'session_password_reset'].includes(kind)) await saveSession(pool, f);
}

test('current preparation admin is distinct from immutable historical outbound authorizer', { timeout: 240000 }, async t => {
  const pg = await startDisposablePostgres('anchor-admin-identity-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 14 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop(); });
  const keys = ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID', 'ANCHOR_EMAIL_RECOVERY_MAINTENANCE',
    'ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'ANCHOR_RESEARCH_PROVIDERS_PAUSED', 'RAILWAY_GIT_COMMIT_SHA'];
  const old = Object.fromEntries(keys.map(k => [k, process.env[k]]));
  t.after(() => keys.forEach(k => old[k] === undefined ? delete process.env[k] : process.env[k] = old[k]));
  t.mock.timers.enable({ apis: ['Date'], now: +new Date(saturday) });

  for (const accountClientId of [null, 10]) await t.test(`${accountClientId === null ? 'platform' : 'tenant-bound'} admin prepares READY with separately audited approval and unchanged history`, async () => {
    const f = await setup(pool, t, accountClientId), before = await h.protectedState(pool, f);
    assert.equal(f.program.authorized_by, '19');
    assert.equal((await pool.query("SELECT role FROM users WHERE id='19'")).rows[0].role, 'ao');
    assert.equal((await pool.query("SELECT client_id FROM users WHERE id='3'")).rows[0].client_id, accountClientId);
    const originalHash = authority.programBindingHash(f.program);
    const installed = await install(pool, f);
    assert.equal(installed.installed, true); assert.equal(installed.providerAuthorized, false);
    const result = await run(pool, f);
    assert.equal(result.status, 'ready_for_review'); assert.equal(result.sent, 0); assert.equal(result.providerCalls, 0);
    assert.equal(result.executionApproved, false); assert.equal(result.envelopeAuthorized, false); assert.equal(result.retryAllowed, false);
    const saved = await persistence.loadMissionSnapshot(result.missionId, '10', pool, { skipEnsure: true });
    assert.equal(saved.mission.stage, 'ready'); assert.equal(saved.mission.pendingOperatorDecision.kind, 'execution_approval');
    assert.equal(saved.mission.orchestrationMissionId, f.source.id);
    assert.equal(saved.mission.crmPreparation.providerReadiness, 'not_checked');
    const grant = (await pool.query('SELECT * FROM anchor_scoped_resume_grants')).rows[0];
    assert.equal(grant.operator_id, '3'); assert.equal(grant.operator_role, 'admin');
    assert.equal(grant.manifest.preparationAuthority.originalAuthorizerId, '19');
    assert.equal(grant.manifest.preparationAuthority.approverAccountClientId, accountClientId);
    assert.equal(grant.manifest.preparationAuthority.approverSessionHash, f.manifest.preparationAuthority.approverSessionHash);
    assert.ok(!JSON.stringify(grant).includes(f.sessionId), 'raw session bearer must not enter grant audit');
    assert.equal(authority.programBindingHash((await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [f.program.id])).rows[0]), originalHash);
    assert.deepEqual(await h.protectedState(pool, f), before);
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
    const after = await h.snapshot(pool);
    await assert.rejects(run(pool, f), e => e.status === 'already_consumed' && e.retryAllowed === false);
    assert.deepEqual(await h.snapshot(pool), after);
    assert.equal(deny.attempts(), 0);
  });

  const invalid = ['current_ao', 'stale_actor', 'inactive_actor', 'role_changed', 'wrong_account_tenant', 'account_binding_changed',
    'password_reset', 'unverified_actor', 'disabled_tenant', 'nonswitchable_tenant', 'deleted_session', 'expired_session',
    'revoked_session', 'wrong_selected_tenant', 'missing_selected_tenant', 'session_actor_changed', 'session_role_changed',
    'session_account_changed', 'impersonation', 'session_password_reset'];
  for (const phase of ['install', 'action', 'consume', 'revoke']) await t.test(`${phase} rejects stale actor, account, tenant and session state before any durable claim`, async () => {
    for (const kind of invalid) {
      const f = await setup(pool, t);
      if (phase !== 'install') await install(pool, f);
      await mutate(pool, f, kind);
      await assertNoWrites(pool, () => phase === 'install' ? install(pool, f) : phase === 'revoke' ? revoke(pool, f)
        : phase === 'consume' ? control.consumeScopedOperation(h.noSchemaWrites(pool), { manifest: f.manifest,
          capability: 'preparation', operation: 'prepare_existing_crm', operationId: 'synthetic-stale-identity-consume',
          identities: f.manifest.admissions }, options(f)) : run(pool, f),
        phase !== 'action' ? { code: 'anchor_crm_preparation_approver_invalid' }
          : e => e.code === 'anchor_crm_action_rejected' && e.status === 'rejected' && !e.writesMayHaveOccurred && !e.retryAllowed);
    }
  });

  await t.test('request actor rejects impersonation and lacks no implicit tenant selection', async () => {
    const f = await setup(pool, t);
    for (const mutation of [s => { delete s.authenticated; }, s => { delete s.active_client_id; },
      s => { s.active_client_id = 11; }, s => { s.user.role = 'ao'; }, s => { s.impersonation = { active: true }; },
      s => { s.user.password_change_required = true; }]) {
      const session = structuredClone(f.session); mutation(session);
      assert.throws(() => actorFromRequest({ session, sessionID: f.sessionId }));
    }
    for (const a of [{ id: '3', role: 'admin' }, { ...actor(f), sessionId: 'unbound-session' }, { ...actor(f), id: '19', role: 'ao' }])
      await assertNoWrites(pool, () => install(pool, f, a));
  });

  await t.test('manifest session digest cannot replace native session context at internal authority boundaries', async () => {
    const f = await setup(pool, t); await install(pool, f);
    for (const sessionId of [undefined, 'different-native-session']) {
      const opts = { ...options(f), sessionId };
      await assertNoWrites(pool, () => authority.readPreparationState(pool, { ...opts, now: f.now, lockControl: false }),
        { code: 'anchor_crm_preparation_approver_invalid' });
      await assertNoWrites(pool, () => control.consumeScopedOperation(h.noSchemaWrites(pool), {
        manifest: f.manifest, capability: 'preparation', operation: 'prepare_existing_crm',
        operationId: 'synthetic-unbound-native-session', identities: f.manifest.admissions,
      }, opts), { code: 'anchor_crm_preparation_approver_invalid' });
    }
  });

  await t.test('identity is rechecked between read-only preflight and durable consumption', async () => {
    const f = await setup(pool, t); await install(pool, f);
    let revoked = false;
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); let preflight = false;
      return { release: db.release.bind(db), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text;
        if (sql === 'BEGIN READ ONLY') preflight = true;
        const result = await db.query(q, values);
        if (preflight && sql === 'COMMIT' && !revoked) {
          revoked = true;
          await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
        }
        return result;
      } };
    } };
    await assert.rejects(run(wrapped, f), e => e.status === 'blocked_review_required' && !e.retryAllowed);
    assert.equal(revoked, true);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
    assert.equal(deny.attempts(), 0);
  });

  await t.test('concurrent current-admin requests create exactly one claim and child', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const results = await Promise.allSettled([run(pool, f), run(pool, f)]);
    assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
    assert.equal(results.find(r => r.status === 'rejected').reason.status, 'already_consumed');
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
  });

  await t.test('a real session row-lock wait cannot extend authentication beyond persisted database-clock expiry', async () => {
    const f = await setup(pool, t); await install(pool, f);
    await pool.query("UPDATE session SET expire=clock_timestamp()+interval '500 milliseconds' WHERE sid=$1", [f.sessionId]);
    const before = await h.snapshot(pool), locker = await pool.connect();
    let pending;
    try {
      await locker.query('BEGIN');
      await locker.query('SELECT sid FROM session WHERE sid=$1 FOR UPDATE', [f.sessionId]);
      assert.equal((await locker.query('SELECT expire>clock_timestamp() AS unexpired FROM session WHERE sid=$1', [f.sessionId])).rows[0].unexpired, true,
        'authentication must be live when the contending consumption starts');
      pending = consume(pool, f, 'synthetic-expired-after-real-lock-wait');
      pending.catch(() => {});
      await waitForDatabaseLock(pool, 'SELECT sid,sess FROM "session"%');
      await locker.query(`SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM expire-clock_timestamp()))+0.05)
        FROM session WHERE sid=$1`, [f.sessionId]);
      assert.equal((await locker.query('SELECT expire<=clock_timestamp() AS expired FROM session WHERE sid=$1', [f.sessionId])).rows[0].expired, true);
      await locker.query('COMMIT');
      await assert.rejects(pending, { code: 'anchor_crm_preparation_approver_invalid' });
      assert.deepEqual(await h.snapshot(pool), before);
      assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
    } finally { await locker.query('ROLLBACK'); locker.release(); if (pending) await pending.catch(() => {}); }
    assert.equal(deny.attempts(), 0);
  });

  await t.test('a concurrent session DELETE that owns the row lock wins before consumption without a durable claim', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const deleting = await pool.connect(); let pending;
    try {
      await deleting.query('BEGIN');
      await deleting.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
      pending = consume(pool, f, 'synthetic-delete-wins-consumption-race');
      pending.catch(() => {});
      await waitForDatabaseLock(pool, 'SELECT sid,sess FROM "session"%');
      await deleting.query('COMMIT');
      await assert.rejects(pending, { code: 'anchor_crm_preparation_approver_invalid' });
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM session WHERE sid=$1', [f.sessionId])).rows[0].n, 0);
      assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
    } finally { await deleting.query('ROLLBACK'); deleting.release(); if (pending) await pending.catch(() => {}); }
    assert.equal(deny.attempts(), 0);
  });

  await t.test('consumption holding the session lock serializes a concurrent DELETE and retains its claim after revocation', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const inserted = barrier(), releaseInsert = barrier();
    let deletion, paused = false;
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); let consumed = false;
      return { release: db.release.bind(db), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text;
        const result = await db.query(q, values);
        if (/INSERT INTO anchor_scoped_resume_consumption/.test(sql) && !paused) {
          consumed = true; paused = true; inserted.resolve();
          await releaseInsert.promise;
        }
        // Ensure the already-waiting revocation commits before subsequent
        // canonical bindings, rather than relying on scheduler timing.
        if (sql === 'COMMIT' && consumed && deletion) await deletion;
        return result;
      } };
    } };
    const pending = run(wrapped, f); pending.catch(() => {});
    try {
      await Promise.race([inserted.promise, pending.then(() => assert.fail('Preparation bypassed the insert barrier'))]);
      deletion = pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
      deletion.catch(() => {});
      await waitForDatabaseLock(pool, 'DELETE FROM session WHERE sid=$1%');
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_scoped_resume_consumption')).rows[0].n, 0,
        'uncommitted consumption must remain invisible while deletion is blocked');
      releaseInsert.resolve();
      await assert.rejects(pending, e => e.status === 'blocked_review_required' && e.writesMayHaveOccurred && !e.retryAllowed);
      assert.equal((await deletion).rowCount, 1);
      assert.deepEqual(await h.effects(pool), { consumed: 1, children: 0, reservations: 0, execution_approvals: 0 });
      await saveSession(pool, f);
      const before = await h.snapshot(pool);
      await assert.rejects(run(pool, f), e => e.status === 'already_consumed' && !e.retryAllowed);
      assert.deepEqual(await h.snapshot(pool), before);
    } finally { releaseInsert.resolve(); await pending.catch(() => {}); if (deletion) await deletion.catch(() => {}); }
    assert.equal(deny.attempts(), 0);
  });

  await t.test('platform admin closes its grant without rewriting history and only a newly valid grant can install', async () => {
    const f = await setup(pool, t), before = await h.protectedState(pool, f);
    await install(pool, f);
    const result = await revoke(pool, f);
    assert.equal(result.revoked, true); assert.equal(result.idempotent, false);
    const grant = (await pool.query('SELECT * FROM anchor_scoped_resume_grants WHERE manifest_id=$1', [f.manifest.id])).rows[0];
    assert.equal(grant.revoked_by, '3'); assert.equal(grant.revoked_role, 'admin');
    assert.deepEqual(grant.manifest, JSON.parse(JSON.stringify(f.manifest))); assert.equal(grant.manifest_hash, hash(f.manifest));
    assert.ok(grant.revoked_at); assert.equal(grant.revoke_approval_ref, 'synthetic-explicit-preparation-revocation');
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest, null);
    assert.deepEqual(await h.protectedState(pool, f), before);
    const closed = await h.snapshot(pool);
    assert.equal((await revoke(pool, f)).idempotent, true);
    assert.deepEqual(await h.snapshot(pool), closed);
    await assertNoWrites(pool, () => install(pool, f));
    await assertNoWrites(pool, () => run(pool, f));
    f.manifest.id = 'synthetic-new-grant-after-explicit-close';
    f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    assert.equal((await install(pool, f)).installed, true);
    assert.deepEqual(await h.protectedState(pool, f), before);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });

  await t.test('a fresh authenticated platform-admin session can close a grant whose original session is gone', async () => {
    const f = await setup(pool, t); await install(pool, f);
    await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
    const before = await h.snapshot(pool);
    await assert.rejects(revoke(pool, f));
    assert.deepEqual(await h.snapshot(pool), before);
    f.sessionId = 'synthetic-fresh-admin-session-for-close';
    await saveSession(pool, f);
    // A different, otherwise-valid admin cannot close someone else's grant.
    await pool.query("INSERT INTO users(id,client_id,role,active,email_verified,password_change_required) VALUES('4',NULL,'admin',true,true,false)");
    const differentSession = { ...f.session, user: { ...f.session.user, id: '4' } };
    await pool.query('INSERT INTO session(sid,sess,expire) VALUES($1,$2,$3)',
      ['synthetic-distinct-admin-close', differentSession, '2099-01-01']);
    await assertNoWrites(pool, () => revoke(pool, f, { id: '4', role: 'admin', sessionId: 'synthetic-distinct-admin-close' }));
    // Fresh authentication authorizes only closure, never use of the old grant.
    await assertNoWrites(pool, () => run(pool, f));
    f.now = new Date(+new Date(f.manifest.expiresAt) + 1);
    assert.equal((await revoke(pool, f)).revoked, true, 'expired grants remain safely closable');
    const closedGrant = (await pool.query('SELECT manifest,manifest_hash FROM anchor_scoped_resume_grants WHERE manifest_id=$1', [f.manifest.id])).rows[0];
    assert.deepEqual(closedGrant.manifest, JSON.parse(JSON.stringify(f.manifest)));
    assert.equal(closedGrant.manifest_hash, hash(f.manifest));
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest, null);
    f.now = new Date(saturday);
    f.manifest.id = 'synthetic-new-session-new-grant';
    f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    await assertNoWrites(pool, () => install(pool, f));
    f.manifest.preparationAuthority.approverSessionHash = createHash('sha256').update(f.sessionId).digest('hex');
    assert.equal((await install(pool, f)).installed, true);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });

  await t.test('closing a consumed grant with fresh authentication never refunds or restores spent identities', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const wrapped = h.afterConsumption(pool, () => pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]));
    await assert.rejects(run(wrapped, f), e => e.status === 'blocked_review_required' && !e.retryAllowed);
    const consumed = (await pool.query('SELECT row_to_json(c) AS row FROM anchor_scoped_resume_consumption c')).rows;
    f.sessionId = 'synthetic-fresh-admin-session-close-consumed';
    await saveSession(pool, f);
    assert.equal((await revoke(pool, f)).revoked, true);
    f.manifest.id = 'synthetic-no-regrant-of-consumed-identities';
    f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    Object.assign(f.manifest.preparationAuthority, { approverSessionHash: createHash('sha256').update(f.sessionId).digest('hex'),
      consumedOperations: 1, consumedItems: 1 });
    f.manifest.scopes.preparation.totalCap = 2;
    f.manifest.scopes.preparation.totalItemCap = 2;
    const before = await h.snapshot(pool);
    await assert.rejects(install(pool, f), { code: 'anchor_crm_preparation_identity_consumed' });
    assert.deepEqual(await h.snapshot(pool), before);
    assert.deepEqual((await pool.query('SELECT row_to_json(c) AS row FROM anchor_scoped_resume_consumption c')).rows, consumed);
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 0, reservations: 0, execution_approvals: 0 });
    assert.equal(deny.attempts(), 0);
  });

  await t.test('session revocation after committed consumption blocks canonical work without refund or replay', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const wrapped = h.afterConsumption(pool, () => pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]));
    await assert.rejects(run(wrapped, f), e => e.status === 'blocked_review_required' && e.writesMayHaveOccurred && !e.retryAllowed);
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 0, reservations: 0, execution_approvals: 0 });
    await saveSession(pool, f);
    const before = await h.snapshot(pool);
    await assert.rejects(run(pool, f), e => e.status === 'already_consumed' && !e.retryAllowed);
    assert.deepEqual(await h.snapshot(pool), before);
    assert.equal(deny.attempts(), 0);
  });
});
