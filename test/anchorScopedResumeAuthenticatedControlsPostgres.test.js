'use strict';
// Real Express, signed synthetic cookies, persisted sessions and disposable PostgreSQL.
// No production credentials and no outbound transports.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const http = require('node:http'), express = require('express'), session = require('express-session');
const PgStore = require('connect-pg-simple')(session), signature = require('cookie-signature');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const deny = require('./helpers/denyExternalProviders.cjs');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const revision = 'a'.repeat(40), origin = 'https://preparation-controls.example.invalid';
const base = '/api/v1/tenant-outreach/anchor-program/preparation-approval/';
const cookieSecret = 'synthetic-local-only-control-session-cookie-signing-secret';
function harness() {
  const filename = path.join(__dirname, 'anchorScopedResumeAdminIdentityPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,saveSession,mutate,h,actor,options};', {
    module, exports: module.exports, __dirname, process, console, structuredClone, setTimeout,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}
const fixture = harness(), h = fixture.h;
function proposal(f) {
  if (f.previewProposal) return JSON.parse(JSON.stringify(f.previewProposal));
  const p = JSON.parse(JSON.stringify(f.manifest));
  delete p.operatorId; delete p.operatorRole;
  delete p.preparationAuthority.approverAccountClientId;
  delete p.preparationAuthority.approverSessionHash;
  return p;
}
function intent(f) {
  return { id: f.manifest.id, programId: f.manifest.programId, sourceMissionId: f.manifest.sourceMissionId,
    validFrom: f.manifest.validFrom, expiresAt: f.manifest.expiresAt, approvalReference: f.manifest.approvalReference,
    preparationApprovalReference: f.manifest.scopes.preparation.approvalReference,
    dailyOperationCap: f.manifest.scopes.preparation.dailyCap, dailyItemCap: f.manifest.scopes.preparation.dailyItemCap,
    admissions: f.manifest.admissions.map(({ prospectId, approvalReference }) => ({ prospectId, approvalReference })) };
}
function request(server, f, action, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port: server.address().port,
      path: action === 'prepare-drafts' ? '/api/v1/tenant-outreach/anchor-program/prepare-drafts' : base + action,
      method: 'POST', headers: { origin, 'content-type': 'application/json',
        cookie: 'connect.sid=' + encodeURIComponent('s:' + signature.sign(f.sessionId, cookieSecret)), ...headers } }, res => {
      let text = ''; res.setEncoding('utf8'); res.on('data', chunk => { text += chunk; });
      res.on('end', () => { try { resolve({ status: res.statusCode, headers: res.headers, text, body: JSON.parse(text) }); }
        catch (error) { reject(Object.assign(error, { responseText: text })); } });
    });
    req.on('error', reject); req.end(JSON.stringify(body));
  });
}
function stub(modulePath, exports) {
  const id = require.resolve(modulePath), before = require.cache[id];
  require.cache[id] = { id, filename: id, loaded: true, exports };
  return () => { delete require.cache[id]; if (before) require.cache[id] = before; };
}
async function setup(pool, t, clientId = null) {
  const f = await fixture.setup(pool, t, clientId);
  f.session.cookie = { originalMaxAge: null, expires: null, httpOnly: true, path: '/' };
  await fixture.saveSession(pool, f);
  return f;
}
function noSecrets(response, f) {
  assert.equal(response.headers['cache-control'], 'no-store');
  assert.ok(!response.text.includes(f.sessionId), 'native bearer must not enter response');
  assert.ok(!response.text.includes(f.manifest.preparationAuthority.approverSessionHash), 'session digest must not enter response');
  assert.equal(deny.attempts(), 0);
}
async function grantCount(pool) { return (await pool.query('SELECT count(*)::int AS n FROM anchor_scoped_resume_grants')).rows[0].n; }

test('authenticated preview/install/revoke lifecycle uses existing persisted sessions without implicit activation', { timeout: 240000 }, async t => {
  const pg = await startDisposablePostgres('anchor-auth-controls-');
  const pool = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 14 });
  const keys = ['APP_URL', 'RAILWAY_GIT_COMMIT_SHA', 'ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID',
    'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'ANCHOR_RESEARCH_PROVIDERS_PAUSED'];
  const envBefore = Object.fromEntries(keys.map(k => [k, process.env[k]]));
  const restores = [stub('../db', h.noSchemaWrites(pool)), stub('../services/tenantOutreachScheduler', {}),
    stub('../services/governedMissionExecutionStatus', {})];
  const paths = ['../routes/tenantOutreach', '../middleware/auth', '../packages/max/workspace/TenantContextResolver'];
  const cached = paths.map(p => [require.resolve(p), require.cache[require.resolve(p)]]);
  cached.forEach(([id]) => { delete require.cache[id]; });
  const store = new PgStore({ pool, createTableIfMissing: false, pruneSessionInterval: false });
  const app = express(); app.use(express.json({ strict: true }));
  app.use(session({ store, secret: cookieSecret, resave: false, saveUninitialized: false,
    cookie: { httpOnly: true, sameSite: 'strict', secure: false } }));
  app.use(require('../routes/tenantOutreach'));
  app.use((error, req, res, next) => res.status(500).json({ error: 'test_internal_error' }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve, reject) => { server.once('listening', resolve); server.once('error', reject); });
  t.after(async () => {
    await new Promise(resolve => server.close(resolve)); await store.close();
    cached.forEach(([id, old]) => { delete require.cache[id]; if (old) require.cache[id] = old; });
    restores.reverse().forEach(restore => restore());
    keys.forEach(k => envBefore[k] === undefined ? delete process.env[k] : process.env[k] = envBefore[k]);
    resetAcquisitionMissionRuntime(); await closePostgresFixture(pool, pg);
  });
  t.mock.timers.enable({ apis: ['Date'], now: +new Date('2026-10-24T16:00:00Z') });
  process.env.APP_URL = origin;
  async function preview(f) {
    const response = await request(server, f, 'preview', { revision, intent: intent(f) });
    assert.equal(response.status, 200, response.text); noSecrets(response, f);
    assert.ok(response.body.proposal, 'preview must provide non-secret exact install proposal');
    const account = f.manifest.preparationAuthority.approverAccountClientId;
    const digest = f.manifest.preparationAuthority.approverSessionHash;
    f.previewProposal = response.body.proposal;
    f.manifest = { ...response.body.proposal, operatorId: '3', operatorRole: 'admin',
      preparationAuthority: { ...response.body.proposal.preparationAuthority,
        approverAccountClientId: account, approverSessionHash: digest } };
    assert.equal(response.body.manifestHash, hash(f.manifest));
    return response.body;
  }
  async function install(f, previewed) {
    const response = await request(server, f, 'install', { revision, proposal: proposal(f), manifestHash: previewed.manifestHash });
    noSecrets(response, f); return response;
  }
  async function readback(f, manifestHash = hash(f.manifest)) {
    const response = await request(server, f, 'preview', { revision, receipt: { manifestId: f.manifest.id, manifestHash } });
    noSecrets(response, f);
    if (response.status === 200) { assert.equal(response.body.effectsScope, 'this_request_only'); assert.equal(response.body.historicalPreparationOutcome, 'not_checked'); assert.equal(response.body.preparationInvoked, false); }
    return response;
  }
  async function revoke(f, manifestHash = hash(f.manifest)) {
    const response = await request(server, f, 'revoke', { revision, manifestId: f.manifest.id, manifestHash,
      approvalReference: 'synthetic-explicit-authenticated-close' });
    noSecrets(response, f); return response;
  }

  for (const clientId of [null, 10]) await t.test(`${clientId === null ? 'platform' : 'tenant-bound'} admin previews, installs once, prepares once and revokes without changing historical authority`, async () => {
    const f = await setup(pool, t, clientId), before = await h.snapshot(pool), protectedBefore = await h.protectedState(pool, f);
    const p = await preview(f);
    assert.deepEqual(await h.snapshot(pool), before, 'preview must be database read-only, including session state');
    const first = await install(f, p); assert.equal(first.status, 200, first.text);
    assert.equal(await grantCount(pool), 1);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
    const installed = await h.snapshot(pool);
    const repeated = await install(f, p); assert.equal(repeated.status, 200, repeated.text);
    assert.deepEqual(await h.snapshot(pool), installed, 'lost success response/repeated click cannot duplicate installation');
    const grant = (await pool.query('SELECT * FROM anchor_scoped_resume_grants')).rows[0];
    assert.equal(grant.operator_id, '3'); assert.equal(grant.manifest.preparationAuthority.originalAuthorizerId, '19');
    assert.deepEqual(grant.review_initializations, []);
    assert.equal(JSON.stringify(grant).includes(f.sessionId), false);
    const prepared = await request(server, f, 'prepare-drafts', { revision, manifestHash: p.manifestHash, sourceMissionHash: hash(f.source) });
    assert.equal(prepared.status, 200, prepared.text); assert.equal(prepared.body.status, 'ready_for_review');
    assert.equal(prepared.body.sent, 0); assert.equal(prepared.body.providerCalls, 0);
    assert.equal(prepared.body.executionApproved, false); assert.equal(prepared.body.envelopeAuthorized, false); noSecrets(prepared, f);
    const alreadySpent = await h.snapshot(pool), repeatedAfterPreparation = await install(f, p);
    assert.equal(repeatedAfterPreparation.status, 200, repeatedAfterPreparation.text);
    assert.equal(repeatedAfterPreparation.body.status, 'already_installed');
    assert.equal(repeatedAfterPreparation.body.freshApproval, false);
    assert.deepEqual(await h.snapshot(pool), alreadySpent);
    const consumedReceipt = await readback(f, p.manifestHash);
    assert.equal(consumedReceipt.status, 200, consumedReceipt.text); assert.equal(consumedReceipt.body.status, 'consumed');
    assert.equal(consumedReceipt.body.consumedOperations, 1); assert.equal(consumedReceipt.body.consumedItems, 1);
    assert.equal((await revoke(f)).status, 200);
    const closed = await h.snapshot(pool);
    assert.equal((await revoke(f)).status, 200);
    assert.deepEqual(await h.snapshot(pool), closed);
    assert.notEqual((await install(f, p)).status, 200);
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
    assert.deepEqual(await h.protectedState(pool, f), protectedBefore);
  });

  await t.test('foreign-origin, absent-session and wrong-tenant requests cannot preview or mutate', async () => {
    const f = await setup(pool, t), before = await h.snapshot(pool);
    for (const action of ['preview', 'install', 'revoke']) {
      const body = action === 'revoke' ? { revision, manifestId: f.manifest.id, manifestHash: hash(f.manifest), approvalReference: 'synthetic-close' }
        : action === 'preview' ? { revision, intent: intent(f) } : { revision, proposal: proposal(f), manifestHash: hash(f.manifest) };
      for (const headers of [{ origin: 'https://foreign.example.invalid' }, { cookie: '' }, { 'sec-fetch-site': 'cross-site' }]) {
        const response = await request(server, f, action, body, headers);
        assert.ok([401, 403].includes(response.status), response.text); noSecrets(response, f);
        assert.deepEqual(await h.snapshot(pool), before);
      }
    }
    f.session.active_client_id = 11; await fixture.saveSession(pool, f);
    const wrongTenant = await h.snapshot(pool);
    const denied = await request(server, f, 'preview', { revision, intent: intent(f) });
    assert.equal(denied.status, 403, denied.text); assert.deepEqual(await h.snapshot(pool), wrongTenant);
  });

  await t.test('unreviewed candidates stay unreviewed and reviewInitialization cannot be smuggled into install', async () => {
    const f = await setup(pool, t);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id::text=$1', [f.manifest.admissions[0].prospectId]);
    const before = await h.snapshot(pool);
    for (const action of ['preview', 'install']) {
      const response = await request(server, f, action, action === 'preview' ? { revision, intent: intent(f) } : { revision, proposal: proposal(f), manifestHash: hash(f.manifest) });
      if (action === 'preview') { assert.equal(response.status, 200, response.text); assert.equal(response.body.installable, false); assert.ok(response.body.blockers.includes('outreach_review_required')); }
      else assert.notEqual(response.status, 200, response.text);
      noSecrets(response, f); assert.deepEqual(await h.snapshot(pool), before);
    }
    const injected = await request(server, f, 'install', { revision, proposal: proposal(f), manifestHash: hash(f.manifest),
      reviewInitialization: { approvalReference: 'not-authorized', identities: f.manifest.admissions } });
    assert.notEqual(injected.status, 200, injected.text); assert.deepEqual(await h.snapshot(pool), before);
    assert.equal(await grantCount(pool), 0);
  });

  await t.test('stale revision, manifest hash, source, program and session changes reject before installation', async () => {
    for (const kind of ['revision', 'manifestHash', 'source', 'program', 'identity', 'accounting', 'role', 'session']) {
      const f = await setup(pool, t), p = await preview(f);
      const body = { revision, proposal: proposal(f), manifestHash: p.manifestHash };
      if (kind === 'revision') body.revision = 'b'.repeat(40);
      if (kind === 'manifestHash') body.manifestHash = 'b'.repeat(64);
      if (kind === 'source') await pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{status}', '\"changed\"') WHERE id=$1", [f.source.id]);
      if (kind === 'program') await pool.query("UPDATE acquisition_outbound_programs SET last_error='changed' WHERE id=$1", [f.program.id]);
      if (kind === 'identity') await pool.query("UPDATE prospects SET email='changed@different.example.invalid' WHERE id::text=$1", [f.manifest.admissions[0].prospectId]);
      if (kind === 'accounting') {
        const c = require('../services/anchorScopedResumeControl');
        const historical = { ...f.manifest, id: 'synthetic-other-history' };
        const opts = { env: { ...f.env, ANCHOR_EMAIL_SCOPED_RESUME_ID: historical.id }, now: () => f.now, sessionId: f.sessionId };
        await c.installScopedResume(h.noSchemaWrites(pool), { manifest: historical, approvalReference: historical.approvalReference }, fixture.actor(f), opts);
        await c.consumeScopedOperation(h.noSchemaWrites(pool), { manifest: historical, capability: 'preparation', operation: 'prepare_existing_crm',
          operationId: 'synthetic-existing-history', identities: historical.admissions }, opts);
        await c.revokeScopedResume(h.noSchemaWrites(pool), { manifestId: historical.id, approvalReference: 'synthetic-close-prior-history' }, fixture.actor(f), opts);
      }
      if (kind === 'role') await pool.query("UPDATE users SET role='ao' WHERE id='3'");
      if (kind === 'session') await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
      const before = await h.snapshot(pool);
      const response = await request(server, f, 'install', body);
      assert.notEqual(response.status, 200, `${kind}: ${response.text}`); noSecrets(response, f);
      assert.deepEqual(await h.snapshot(pool), before); assert.equal(await grantCount(pool), kind === 'accounting' ? 1 : 0);
    }
  });

  await t.test('failure after grant INSERT rolls back atomically; explicit replay then installs once', async () => {
    const f = await setup(pool, t), p = await preview(f), before = await h.snapshot(pool);
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect();
      return { release: db.release.bind(db), async query(q, values) {
        const result = await db.query(q, values);
        if (/INSERT INTO anchor_scoped_resume_grants/.test(typeof q === 'string' ? q : q.text)) throw Error('synthetic-local-disconnect-before-commit');
        return result;
      } };
    } };
    await assert.rejects(require('../services/anchorCrmPreparationApproval').installPreparationApproval(h.noSchemaWrites(wrapped),
      { revision, proposal: proposal(f), manifestHash: p.manifestHash }, fixture.actor(f), { env: f.env, now: () => f.now, sessionId: f.sessionId }), error => { assert.match(error.message, /synthetic-local-disconnect-before-commit/); return true; });
    assert.deepEqual(await h.snapshot(pool), before);
    assert.equal((await install(f, p)).status, 200);
    const once = await h.snapshot(pool);
    assert.equal((await install(f, p)).status, 200);
    assert.deepEqual(await h.snapshot(pool), once); assert.equal(await grantCount(pool), 1);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });

  await t.test('fresh session of the same audited admin can close but cannot consume old authority', async () => {
    const f = await setup(pool, t), p = await preview(f);
    assert.equal((await install(f, p)).status, 200);
    await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
    f.sessionId = 'synthetic-fresh-cookie-for-explicit-close'; await fixture.saveSession(pool, f);
    const before = await h.snapshot(pool);
    const prepare = await request(server, f, 'prepare-drafts', { revision, manifestHash: p.manifestHash, sourceMissionHash: hash(f.source) });
    assert.notEqual(prepare.status, 200); assert.deepEqual(await h.snapshot(pool), before);
    const wrongPin = await revoke(f, 'b'.repeat(64)); assert.notEqual(wrongPin.status, 200);
    assert.deepEqual(await h.snapshot(pool), before);
    assert.equal((await revoke(f)).status, 200);
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest, null);
    const grant = (await pool.query('SELECT * FROM anchor_scoped_resume_grants')).rows[0];
    assert.deepEqual(grant.manifest, JSON.parse(JSON.stringify(f.manifest))); assert.equal(grant.manifest_hash, p.manifestHash);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });
  await t.test('simultaneous installation requests serialize to one fresh approval and one truthful replay', async () => {
    const f = await setup(pool, t), p = await preview(f);
    const results = await Promise.all([install(f, p), install(f, p)]);
    for (const response of results) assert.equal(response.status, 200, response.text);
    assert.deepEqual(results.map(r => r.body.status).sort(), ['already_installed', 'installed']);
    assert.equal(results.filter(r => r.body.freshApproval === true).length, 1);
    assert.equal(results.filter(r => r.body.idempotent === true && r.body.freshApproval === false).length, 1);
    assert.equal(await grantCount(pool), 1);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });

  await t.test('preview reports inactive settings without changing them or installing authority', async () => {
    const f = await setup(pool, t);
    delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
    const before = await h.snapshot(pool), env = { ...process.env };
    const p = await preview(f);
    assert.equal(p.installable, false); assert.ok(p.blockers.includes('preparation_settings_not_activated'));
    assert.deepEqual(await h.snapshot(pool), before); assert.deepEqual({ ...process.env }, env);
    const denied = await install(f, p); assert.notEqual(denied.status, 200, denied.text);
    assert.deepEqual(await h.snapshot(pool), before); assert.deepEqual({ ...process.env }, env);
    assert.equal(await grantCount(pool), 0);
  });

  await t.test('closing a consumed batch does not permit a fresh preview to refund its identity', async () => {
    const f = await setup(pool, t), p = await preview(f);
    assert.equal((await install(f, p)).status, 200);
    const prepared = await request(server, f, 'prepare-drafts', { revision, manifestHash: p.manifestHash, sourceMissionHash: hash(f.source) });
    assert.equal(prepared.status, 200, prepared.text);
    assert.equal((await revoke(f)).status, 200);
    const ledger = (await pool.query('SELECT row_to_json(c) AS row FROM anchor_scoped_resume_consumption c')).rows;
    f.manifest.id = 'synthetic-new-intent-no-refund';
    process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    const before = await h.snapshot(pool);
    const newPreview = await preview(f);
    assert.equal(newPreview.consumedOperations, 1); assert.equal(newPreview.consumedItems, 1);
    assert.equal(newPreview.installable, false);
    assert.ok(newPreview.blockers.includes('anchor_crm_preparation_identity_consumed'));
    assert.notEqual((await install(f, newPreview)).status, 200);
    assert.deepEqual(await h.snapshot(pool), before);
    assert.deepEqual((await pool.query('SELECT row_to_json(c) AS row FROM anchor_scoped_resume_consumption c')).rows, ledger);
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
  });

  await t.test('persisted revoked role, binding, tenant and session state deny all new controls', async () => {
    for (const kind of ['current_ao', 'inactive_actor', 'account_binding_changed', 'password_reset',
      'disabled_tenant', 'revoked_session', 'session_role_changed', 'impersonation']) {
      const f = await setup(pool, t), p = await preview(f);
      assert.equal((await install(f, p)).status, 200);
      await fixture.mutate(pool, f, kind);
      const before = await h.snapshot(pool);
      const bodies = {
        preview: { revision, intent: intent(f) },
        install: { revision, proposal: proposal(f), manifestHash: p.manifestHash },
        revoke: { revision, manifestId: f.manifest.id, manifestHash: p.manifestHash, approvalReference: 'synthetic-denied-close' },
      };
      for (const [action, body] of Object.entries(bodies)) {
        const denied = await request(server, f, action, body);
        assert.notEqual(denied.status, 200, `${kind} ${action}: ${denied.text}`); noSecrets(denied, f);
        assert.deepEqual(await h.snapshot(pool), before);
      }
    }
  });
  await t.test('lost install COMMIT acknowledgement is reconciled before a truthful idempotent replay', async () => {
    const f = await setup(pool, t), p = await preview(f);
    let committed = false;
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); let inserted = false;
      return { release: db.release.bind(db), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text;
        const result = await db.query(q, values);
        if (/INSERT INTO anchor_scoped_resume_grants/.test(sql)) inserted = true;
        if (sql === 'COMMIT' && inserted) { committed = true; throw Error('synthetic-lost-commit-acknowledgement'); }
        return result;
      } };
    } };
    await assert.rejects(require('../services/anchorCrmPreparationApproval').installPreparationApproval(h.noSchemaWrites(wrapped),
      { revision, proposal: proposal(f), manifestHash: p.manifestHash }, fixture.actor(f), { env: f.env, now: () => f.now, sessionId: f.sessionId }), error => { assert.match(error.message, /synthetic-lost-commit-acknowledgement/); return true; });
    assert.equal(committed, true); assert.equal(await grantCount(pool), 1);
    const saved = await h.snapshot(pool);
    const receipt = await readback(f, p.manifestHash);
    assert.equal(receipt.status, 200, receipt.text);
    assert.equal(receipt.body.status, 'installed'); assert.equal(receipt.body.recorded, true);
    assert.equal(receipt.body.installed, true);
    assert.deepEqual(await h.snapshot(pool), saved, 'reconciliation is read-only');
    const replay = await install(f, p);
    assert.equal(replay.status, 200, replay.text); assert.equal(replay.body.status, 'already_installed');
    assert.equal(replay.body.freshApproval, false); assert.equal(replay.body.idempotent, true);
    assert.deepEqual(await h.snapshot(pool), saved);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });
  await t.test('lost revoke acknowledgement remains closed and fresh-session expired readback never refreshes authority', async () => {
    const f = await setup(pool, t), p = await preview(f);
    const absent = await readback(f, p.manifestHash);
    assert.equal(absent.status, 200, absent.text); assert.equal(absent.body.status, 'not_installed');
    assert.equal(absent.body.recorded, false); assert.equal(absent.body.installed, false);
    assert.equal((await install(f, p)).status, 200);
    let committed = false;
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); let revoked = false;
      return { release: db.release.bind(db), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text;
        const result = await db.query(q, values);
        if (/UPDATE anchor_scoped_resume_grants SET revoked_at/.test(sql)) revoked = true;
        if (sql === 'COMMIT' && revoked) { committed = true; throw Error('synthetic-lost-revoke-acknowledgement'); }
        return result;
      } };
    } };
    await assert.rejects(require('../services/anchorCrmPreparationApproval').revokePreparationApproval(h.noSchemaWrites(wrapped),
      { revision, manifestId: f.manifest.id, manifestHash: p.manifestHash, approvalReference: 'synthetic-explicit-close-ack-loss' },
      fixture.actor(f), { env: f.env, now: () => f.now, sessionId: f.sessionId }), error => { assert.match(error.message, /synthetic-lost-revoke-acknowledgement/); return true; });
    assert.equal(committed, true);
    await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
    f.sessionId = 'synthetic-readback-rotated-session'; await fixture.saveSession(pool, f);
    t.mock.timers.setTime(+new Date(f.manifest.expiresAt) + 1000);
    const before = await h.snapshot(pool);
    const receipt = await readback(f, p.manifestHash);
    assert.equal(receipt.status, 200, receipt.text); assert.equal(receipt.body.status, 'revoked');
    assert.equal(receipt.body.recorded, true); assert.equal(receipt.body.installed, false);
    assert.equal(receipt.body.revoked, true); assert.equal(receipt.body.expired, true);
    assert.equal(receipt.body.currentControlMatches, false); assert.equal(receipt.body.maintenanceArmed, true);
    assert.equal(receipt.body.proposal, undefined);
    assert.deepEqual(await h.snapshot(pool), before);
    const replay = await revoke(f, p.manifestHash);
    assert.equal(replay.status, 200, replay.text); assert.equal(replay.body.status, 'already_revoked');
    assert.deepEqual(await h.snapshot(pool), before);
    const wrongPin = await readback(f, 'b'.repeat(64)); assert.notEqual(wrongPin.status, 200);
    assert.deepEqual(await h.snapshot(pool), before);
    assert.deepEqual(await h.effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
  });
  assert.equal(deny.attempts(), 0);
});
