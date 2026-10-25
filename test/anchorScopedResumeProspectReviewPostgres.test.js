'use strict';
// Exact prospect review through real same-origin HTTP and persisted synthetic
// sessions. Only disposable PostgreSQL is used; external transports are denied.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const express = require('express'), session = require('express-session');
const PgStore = require('connect-pg-simple')(session), { Pool } = require('pg');
const deny = require('./helpers/denyExternalProviders.cjs');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');

function harness(name, exported) {
  const filename = path.join(__dirname, name), local = createRequire(filename), module = { exports: {} };
  const load = Object.assign(id => id === 'node:test' ? () => {} : local(id), { resolve: local.resolve, cache: require.cache });
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + `\nmodule.exports={${exported}};`, {
    module, exports: module.exports, __dirname, process, console, structuredClone, setTimeout,
    require: load,
  }, { filename });
  return module.exports;
}
const { fixture, h, setup, intent, request: rawRequest, noSecrets, stub, revision, origin, cookieSecret } = harness(
  'anchorScopedResumeAuthenticatedControlsPostgres.test.js', 'fixture,h,setup,intent,request,noSecrets,stub,revision,origin,cookieSecret');
const { extraAdmission } = harness('anchorScopedResumeCrmPreparationPostgres.test.js', 'extraAdmission');
const json = value => JSON.parse(JSON.stringify(value));
const request = async (...args) => json(await rawRequest(...args));
const options = f => ({ env: f.env, now: () => new Date(f.now), sessionId: f.sessionId });
const noEffects = { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 };
const review = (f, ids = f.manifest.admissions.map(a => a.prospectId)) => ({
  action: 'initialize_null_review_false', approvalReference: 'synthetic-separate-exact-prospect-review', prospectIds: ids,
});
const installBody = p => ({ revision, proposal: p.proposal, manifestHash: p.manifestHash,
  reviewInitialization: p.reviewInitialization, reviewConfirmationHash: p.reviewConfirmationHash });
const confirmHash = body => hash({ kind: 'anchor_preparation_review_confirmation_v1', revision: body.revision,
  manifestHash: body.manifestHash, reviewInitializations: body.reviewInitialization.identities
    .map(a => ({ ...a, approvalReference: body.reviewInitialization.approvalReference, beforeReview: null, afterReview: false }))
    .sort((a, b) => a.prospectId.localeCompare(b.prospectId)) });

test('exact authenticated prospect review is explicit, atomic and never outbound authority', { timeout: 240000 }, async t => {
  const pg = await startDisposablePostgres('anchor-prospect-review-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 14 });
  const keys = ['APP_URL', 'RAILWAY_GIT_COMMIT_SHA', 'ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID',
    'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'ANCHOR_RESEARCH_PROVIDERS_PAUSED'];
  const old = Object.fromEntries(keys.map(k => [k, process.env[k]]));
  const restores = [stub('../db', h.noSchemaWrites(pool)), stub('../services/tenantOutreachScheduler', {}),
    stub('../services/governedMissionExecutionStatus', {})];
  const cached = ['../routes/tenantOutreach', '../middleware/auth', '../packages/max/workspace/TenantContextResolver']
    .map(p => [require.resolve(p), require.cache[require.resolve(p)]]);
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
    cached.forEach(([id, previous]) => { delete require.cache[id]; if (previous) require.cache[id] = previous; });
    restores.reverse().forEach(restore => restore());
    keys.forEach(k => old[k] === undefined ? delete process.env[k] : process.env[k] = old[k]);
    resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop();
  });
  t.mock.timers.enable({ apis: ['Date'], now: +new Date('2026-10-24T16:00:00Z') });
  process.env.APP_URL = origin;
  async function fresh(clientId = null) {
    const f = await setup(pool, t, clientId);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id::text=$1', [f.manifest.admissions[0].prospectId]);
    return f;
  }
  async function preview(f, selected = review(f)) {
    const response = await request(server, f, 'preview', { revision, intent: intent(f), review: selected });
    noSecrets(response, f); assert.equal(response.status, 200, response.text);
    assert.ok(response.body.proposal, response.text);
    return response.body;
  }
  async function install(f, p, body = installBody(p)) {
    const response = await request(server, f, 'install', body); noSecrets(response, f); return response;
  }
  async function readback(f, p) {
    const response = await request(server, f, 'preview', { revision, receipt: { manifestId: p.proposal.id, manifestHash: p.manifestHash } });
    noSecrets(response, f); assert.equal(response.status, 200, response.text); return response.body;
  }
  async function close(f, p) {
    const response = await request(server, f, 'revoke', { revision, manifestId: p.proposal.id, manifestHash: p.manifestHash,
      approvalReference: 'synthetic-explicit-close-reviewed-grant' });
    noSecrets(response, f); return response;
  }
  async function state(f) {
    return (await pool.query('SELECT ao_outreach_review_required AS reviewed FROM prospects WHERE id::text=$1',
      [f.manifest.admissions[0].prospectId])).rows[0].reviewed;
  }
  async function deniedUnchanged(f, p, body = installBody(p)) {
    const before = await h.snapshot(pool), result = await install(f, p, body);
    assert.notEqual(result.status, 200, result.text); assert.deepEqual(await h.snapshot(pool), before);
    assert.deepEqual(await h.effects(pool), noEffects); return result;
  }
  async function durableGrant() { return (await pool.query('SELECT * FROM anchor_scoped_resume_grants')).rows[0]; }

  for (const clientId of [null, 10]) await t.test(`${clientId === null ? 'platform' : 'tenant-bound'} admin previews exact NULL review without writes and confirms one audited CAS`, async () => {
    const f = await fresh(clientId), before = await h.snapshot(pool), protectedBefore = await h.protectedState(pool, f);
    const p = await preview(f);
    assert.equal(p.installable, true); assert.deepEqual(p.blockers, []);
    assert.equal(p.preparationInvoked, false); assert.equal(p.sent, 0); assert.equal(p.providerCalls, 0);
    assert.equal(p.reviewInitialization.action, 'initialize_null_review_false');
    assert.equal(p.reviewInitialization.approvalReference, review(f).approvalReference);
    assert.deepEqual(p.reviewInitialization.identities, json(f.manifest.admissions.map(a => ({
      prospectId: a.prospectId, companyId: a.companyId, recipientEmail: a.recipientEmail, identityFingerprint: a.identityFingerprint,
    }))));
    assert.equal(p.reviewConfirmationHash, confirmHash(installBody(p)));
    assert.equal(p.reviewEffects.buyingIntent, 'not_asserted');
    assert.deepEqual(p.reviewEffects.identities, [{ prospectId: f.manifest.admissions[0].prospectId, beforeReview: null, afterReview: false }]);
    assert.deepEqual(await h.snapshot(pool), before); assert.equal(await state(f), null);
    assert.equal((await install(f, p)).status, 200);
    const grant = await durableGrant();
    assert.equal(await state(f), false); assert.equal(grant.operator_id, '3');
    assert.deepEqual(grant.review_initializations, p.reviewInitialization.identities.map(a => ({ ...a,
      approvalReference: review(f).approvalReference, beforeReview: null, afterReview: false })));
    const after = await h.protectedState(pool, f);
    delete protectedBefore.prospects; delete after.prospects;
    assert.deepEqual(after, protectedBefore, 'review installation must preserve program/source, quarantine, sender and other protections');
    assert.deepEqual(await h.effects(pool), noEffects);
    const saved = await h.snapshot(pool), receipt = await readback(f, p);
    assert.equal(receipt.status, 'installed'); assert.equal(receipt.reviewInitializationCount, 1);
    assert.deepEqual(receipt.reviewInitializations, grant.review_initializations);
    assert.equal(receipt.currentReviewState, 'not_checked'); assert.equal(receipt.reviewAuditScope, 'historical_committed_transition');
    assert.equal(receipt.reviewInitializationHash, hash({ kind: 'anchor_preparation_review_audit_v1', manifestHash: p.manifestHash,
      reviewInitializations: grant.review_initializations }));
    assert.equal(receipt.historicalPreparationOutcome, 'not_checked');
    assert.deepEqual(await h.snapshot(pool), saved, 'receipt is strictly read-only');
    const replay = await install(f, p); assert.equal(replay.status, 200, replay.text);
    assert.equal(replay.body.status, 'already_installed'); assert.equal(replay.body.freshApproval, false);
    assert.deepEqual(await h.snapshot(pool), saved);
    assert.equal((await close(f, p)).status, 200); assert.equal(await state(f), false);
    assert.deepEqual((await durableGrant()).review_initializations, grant.review_initializations);
    assert.equal((await readback(f, p)).status, 'revoked');
    await deniedUnchanged(f, p);
  });

  await t.test('preview cancellation and ordinary install leave unreviewed candidates unchanged', async () => {
    const f = await fresh(), before = await h.snapshot(pool), p = await preview(f);
    assert.deepEqual(await h.snapshot(pool), before);
    const ordinary = await request(server, f, 'preview', { revision, intent: intent(f) });
    assert.equal(ordinary.status, 200); assert.equal(ordinary.body.installable, false);
    assert.ok(ordinary.body.blockers.includes('outreach_review_required'));
    await deniedUnchanged(f, p, { revision, proposal: p.proposal, manifestHash: p.manifestHash });
    assert.equal(await state(f), null);
    assert.equal((await readback(f, p)).status, 'not_installed');
  });

  await t.test('only explicitly selected admitted identities are reviewed; omitted NULL identity blocks the entire transaction', async () => {
    const f = await fresh(), other = json(await extraAdmission(pool, f, 31));
    f.manifest.admissions.push(other); f.manifest.scopes.preparation.dailyItemCap = 2;
    await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id::text=$1', [other.prospectId]);
    const selected = review(f, [f.manifest.admissions[0].prospectId]), before = await h.snapshot(pool);
    const blocked = await preview(f, selected);
    assert.equal(blocked.installable, false); assert.ok(blocked.blockers.includes('outreach_review_required'));
    assert.deepEqual(await h.snapshot(pool), before);
    assert.deepEqual(blocked.reviewInitialization.identities.map(a => a.prospectId), selected.prospectIds);
    await deniedUnchanged(f, blocked);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=false WHERE id::text=$1', [other.prospectId]);
    const p = await preview(f, selected); assert.equal(p.installable, true);
    assert.equal((await install(f, p)).status, 200);
    assert.equal((await durableGrant()).review_initializations.length, 1);
  });

  for (const [label, mutation] of [
    ['missing confirmation hash', b => { delete b.reviewConfirmationHash; }],
    ['missing exact plan', b => { delete b.reviewInitialization; }],
    ['wrong confirmation hash', b => { b.reviewConfirmationHash = 'b'.repeat(64); }],
    ['changed review reference', b => { b.reviewInitialization.approvalReference += '-changed'; }],
    ['changed exact recipient', b => { b.reviewInitialization.identities[0].recipientEmail = 'other@example.invalid'; }],
    ['changed fingerprint', b => { b.reviewInitialization.identities[0].identityFingerprint = 'b'.repeat(64); }],
    ['removed review identity', b => { b.reviewInitialization.identities = []; }],
    ['changed revision', b => { b.revision = 'b'.repeat(40); }],
    ['actor injection', b => { b.operatorId = '19'; }],
  ]) await t.test(`${label} cannot change prospect state`, async () => {
    const f = await fresh(), p = await preview(f), body = json(installBody(p)); mutation(body);
    await deniedUnchanged(f, p, body); assert.equal(await state(f), null);
  });

  for (const [label, mutation] of [
    ['non-admitted identity', b => { b.reviewInitialization.identities[0].prospectId = 'not-admitted'; }],
    ['duplicate exact identity', b => { b.reviewInitialization.identities.push(json(b.reviewInitialization.identities[0])); }],
    ['arbitrary review action', b => { b.reviewInitialization.action = 'release_true_hold'; }],
    ['caller-supplied before state', b => { b.reviewInitialization.identities[0].beforeReview = true; }],
  ]) await t.test(`recomputed hash does not authorize ${label}`, async () => {
    const f = await fresh(), p = await preview(f), body = json(installBody(p)); mutation(body); body.reviewConfirmationHash = confirmHash(body);
    await deniedUnchanged(f, p, body); assert.equal(await state(f), null);
  });

  for (const [kind, sql] of [
    ['true review hold', 'UPDATE prospects SET ao_outreach_review_required=true WHERE id::text=$1'],
    ['already reviewed elsewhere', 'UPDATE prospects SET ao_outreach_review_required=false WHERE id::text=$1'],
    ['true DNC', 'UPDATE prospects SET do_not_contact=true WHERE id::text=$1'],
    ['unknown DNC', 'UPDATE prospects SET do_not_contact=NULL WHERE id::text=$1'],
    ['synthetic', 'UPDATE prospects SET is_synthetic=true WHERE id::text=$1'],
    ['unknown synthetic state', 'UPDATE prospects SET is_synthetic=NULL WHERE id::text=$1'],
    ['identity drift', "UPDATE prospects SET email='drift@example.invalid' WHERE id::text=$1"],
  ]) await t.test(`stale preview cannot release ${kind}`, async () => {
    const f = await fresh(), p = await preview(f); await pool.query(sql, [f.manifest.admissions[0].prospectId]);
    await deniedUnchanged(f, p);
  });

  await t.test('fresh review preview cannot override an existing true hold or company-alias quarantine', async () => {
    for (const kind of ['hold', 'quarantine']) {
      const f = await fresh();
      if (kind === 'hold') await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id::text=$1', [f.manifest.admissions[0].prospectId]);
      else await pool.query('UPDATE companies SET name=$1 WHERE id::text=$2', [f.quarantine.company_name_key, f.manifest.admissions[0].companyId]);
      const before = await h.snapshot(pool), p = await preview(f);
      assert.equal(p.installable, false, kind); assert.deepEqual(await h.snapshot(pool), before);
      await deniedUnchanged(f, p);
    }
  });

  for (const kind of ['current_ao', 'inactive_actor', 'account_binding_changed', 'password_reset', 'unverified_actor',
    'disabled_tenant', 'deleted_session', 'revoked_session', 'wrong_selected_tenant', 'session_role_changed', 'impersonation']) {
    await t.test(`${kind} invalidates prospect review confirmation`, async () => {
      const f = await fresh(), p = await preview(f); await fixture.mutate(pool, f, kind);
      await deniedUnchanged(f, p); assert.equal(await state(f), null);
    });
  }

  await t.test('cross-origin, absent-cookie and cross-site review confirmation have zero effects', async () => {
    const f = await fresh(), p = await preview(f), before = await h.snapshot(pool);
    for (const headers of [{ origin: 'https://foreign.example.invalid' }, { cookie: '' }, { 'sec-fetch-site': 'cross-site' }]) {
      for (const [action, body] of [['preview', { revision, intent: intent(f), review: review(f) }], ['install', installBody(p)]]) {
        const result = await request(server, f, action, body, headers);
        assert.ok([401, 403].includes(result.status)); noSecrets(result, f);
        assert.deepEqual(await h.snapshot(pool), before);
      }
    }
  });

  await t.test('simultaneous exact confirmations produce one audited update and a truthful replay', async () => {
    const f = await fresh(), p = await preview(f), results = await Promise.all([install(f, p), install(f, p)]);
    assert.ok(results.every(r => r.status === 200), results.map(r => r.text).join('\n'));
    assert.deepEqual(results.map(r => r.body.status).sort(), ['already_installed', 'installed']);
    assert.equal(results.filter(r => r.body.freshApproval).length, 1);
    assert.equal((await durableGrant()).review_initializations.length, 1);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_scoped_resume_grants')).rows[0].n, 1);
    assert.equal(await state(f), false); assert.deepEqual(await h.effects(pool), noEffects);
  });

  await t.test('failure immediately after review UPDATE rolls back review and grant atomically', async () => {
    const f = await fresh(), p = await preview(f), before = await h.snapshot(pool); let updated = false;
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); return { release: db.release.bind(db), async query(q, values) {
        const result = await db.query(q, values), sql = typeof q === 'string' ? q : q.text;
        if (/UPDATE prospects SET ao_outreach_review_required=false/.test(sql)) { updated = true; throw Error('synthetic-after-review-update'); }
        return result;
      } };
    } };
    await assert.rejects(require('../services/anchorCrmPreparationApproval').installPreparationApproval(h.noSchemaWrites(wrapped),
      installBody(p), fixture.actor(f), options(f)), /synthetic-after-review-update/);
    assert.equal(updated, true); assert.deepEqual(await h.snapshot(pool), before); assert.equal(await state(f), null);
    const receipt = await readback(f, p); assert.equal(receipt.status, 'not_installed');
    assert.equal((await install(f, p)).status, 200); assert.equal(await state(f), false);
  });

  await t.test('lost COMMIT acknowledgement is resolved by persisted exact audit, not a second review', async () => {
    const f = await fresh(), p = await preview(f); let committed = false;
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); let inserted = false;
      return { release: db.release.bind(db), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text, result = await db.query(q, values);
        if (/INSERT INTO anchor_scoped_resume_grants/.test(sql)) inserted = true;
        if (sql === 'COMMIT' && inserted) { committed = true; throw Error('synthetic-review-commit-ack-lost'); }
        return result;
      } };
    } };
    await assert.rejects(require('../services/anchorCrmPreparationApproval').installPreparationApproval(h.noSchemaWrites(wrapped),
      installBody(p), fixture.actor(f), options(f)), /synthetic-review-commit-ack-lost/);
    assert.equal(committed, true); assert.equal(await state(f), false);
    const saved = await h.snapshot(pool), receipt = await readback(f, p);
    assert.equal(receipt.status, 'installed'); assert.equal(receipt.reviewInitializationCount, 1);
    assert.deepEqual(receipt.reviewInitializations, (await durableGrant()).review_initializations);
    const retry = await install(f, p); assert.equal(retry.status, 200, retry.text); assert.equal(retry.body.freshApproval, false);
    assert.deepEqual(await h.snapshot(pool), saved);
    const changed = json(installBody(p)); changed.reviewInitialization.approvalReference += '-new'; changed.reviewConfirmationHash = confirmHash(changed);
    await deniedUnchanged(f, p, changed);
  });

  await t.test('fresh same-admin session reads and closes historical review without taking over preparation', async () => {
    const f = await fresh(), p = await preview(f); assert.equal((await install(f, p)).status, 200);
    const audit = (await durableGrant()).review_initializations;
    await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
    f.sessionId = 'synthetic-replacement-prospect-review-session'; await fixture.saveSession(pool, f);
    const before = await h.snapshot(pool), receipt = await readback(f, p);
    assert.deepEqual(receipt.reviewInitializations, audit); assert.deepEqual(await h.snapshot(pool), before);
    await deniedUnchanged(f, p);
    const preparation = await request(server, f, 'prepare-drafts', { revision, manifestHash: p.manifestHash, sourceMissionHash: hash(f.source) });
    assert.notEqual(preparation.status, 200); assert.deepEqual(await h.snapshot(pool), before);
    assert.equal((await close(f, p)).status, 200); assert.equal(await state(f), false);
    assert.deepEqual((await durableGrant()).review_initializations, audit);
    assert.deepEqual(await h.effects(pool), noEffects);
  });

  await t.test('competing separate review references serialize without silently adopting the losing approval', async () => {
    const f = await fresh(), first = await preview(f), other = review(f); other.approvalReference += '-distinct';
    const second = await preview(f, other);
    assert.equal(first.manifestHash, second.manifestHash);
    assert.notEqual(first.reviewConfirmationHash, second.reviewConfirmationHash);
    const results = await Promise.all([install(f, first), install(f, second)]);
    assert.equal(results.filter(r => r.status === 200).length, 1);
    assert.equal(results.filter(r => r.status === 409).length, 1);
    const winner = results[0].status === 200 ? first : second;
    const audit = (await durableGrant()).review_initializations;
    assert.equal(audit.length, 1); assert.equal(audit[0].approvalReference, winner.reviewInitialization.approvalReference);
    assert.equal(await state(f), false); assert.deepEqual(await h.effects(pool), noEffects);
  });

  await t.test('missing activation blocks exact review installation without activating settings', async () => {
    const f = await fresh(); delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
    const env = { ...process.env }, before = await h.snapshot(pool), p = await preview(f);
    assert.equal(p.installable, false); assert.ok(p.blockers.includes('preparation_settings_not_activated'));
    assert.deepEqual(await h.snapshot(pool), before); assert.deepEqual({ ...process.env }, env);
    await deniedUnchanged(f, p); assert.equal(await state(f), null); assert.deepEqual({ ...process.env }, env);
  });

  await t.test('acknowledged review replay after preparation never recreates capacity or refunds consumption', async () => {
    const f = await fresh(), p = await preview(f); assert.equal((await install(f, p)).status, 200);
    const prepared = await request(server, f, 'prepare-drafts', { revision, manifestHash: p.manifestHash, sourceMissionHash: p.sourceMissionHash });
    assert.equal(prepared.status, 200, prepared.text); assert.equal(prepared.body.status, 'ready_for_review');
    assert.equal(prepared.body.executionApproved, false); assert.equal(prepared.body.envelopeAuthorized, false);
    assert.equal(prepared.body.sent, 0); assert.equal(prepared.body.providerCalls, 0);
    const saved = await h.snapshot(pool), replay = await install(f, p);
    assert.equal(replay.status, 200, replay.text); assert.equal(replay.body.freshApproval, false); assert.equal(replay.body.initialized, 0);
    assert.deepEqual(await h.snapshot(pool), saved);
    const receipt = await readback(f, p); assert.equal(receipt.status, 'consumed');
    assert.equal(receipt.reviewInitializationCount, 1); assert.equal(receipt.consumedOperations, 1); assert.equal(receipt.consumedItems, 1);
    assert.equal((await close(f, p)).status, 200); assert.equal(await state(f), false);
    assert.deepEqual(await h.effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
    assert.deepEqual((await h.snapshot(pool)).anchor_scoped_resume_consumption, saved.anchor_scoped_resume_consumption);
  });

  await t.test('a later true review hold survives receipt and revocation; historical audit is not current approval', async () => {
    const f = await fresh(), p = await preview(f); assert.equal((await install(f, p)).status, 200);
    const audit = (await durableGrant()).review_initializations;
    await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id::text=$1', [f.manifest.admissions[0].prospectId]);
    const before = await h.snapshot(pool), receipt = await readback(f, p);
    assert.equal(receipt.currentReviewState, 'not_checked'); assert.deepEqual(receipt.reviewInitializations, audit);
    assert.deepEqual(await h.snapshot(pool), before); await deniedUnchanged(f, p);
    assert.equal((await close(f, p)).status, 200); assert.equal(await state(f), true);
    assert.deepEqual((await durableGrant()).review_initializations, audit);
    assert.deepEqual(await h.effects(pool), noEffects);
  });

  await t.test('session revocation during a real control row-lock wait wins before any review UPDATE', async () => {
    const f = await fresh(), p = await preview(f), locker = await pool.connect(); let pending;
    try {
      await locker.query('BEGIN');
      await locker.query('SELECT * FROM anchor_email_recovery_control WHERE singleton=true FOR UPDATE');
      pending = install(f, p); pending.catch(() => {});
      let waiting = false;
      for (let n = 0; n < 100; n++) {
        const { rows } = await pool.query(`SELECT pid FROM pg_stat_activity WHERE datname=current_database()
          AND wait_event_type='Lock' AND query LIKE 'SELECT * FROM anchor_email_recovery_control%FOR UPDATE'`);
        if (rows.length) { waiting = true; break; }
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      assert.equal(waiting, true, 'the install must actually contend for the PostgreSQL control lock');
      await pool.query('DELETE FROM session WHERE sid=$1', [f.sessionId]);
      const before = await h.snapshot(pool);
      await locker.query('COMMIT');
      const result = await pending; assert.equal(result.status, 409, result.text);
      assert.deepEqual(await h.snapshot(pool), before); assert.equal(await state(f), null);
      assert.deepEqual(await h.effects(pool), noEffects);
    } finally { await locker.query('ROLLBACK'); locker.release(); if (pending) await pending.catch(() => {}); }
  });

  await t.test('native session expiry after the review UPDATE rolls back review and grant before commit', async () => {
    const f = await fresh(), p = await preview(f); let updated = false;
    await pool.query("UPDATE session SET expire=clock_timestamp()+interval '1 second' WHERE sid=$1", [f.sessionId]);
    const before = await h.snapshot(pool);
    const wrapped = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); return { release: db.release.bind(db), async query(q, values) {
        const result = await db.query(q, values), sql = typeof q === 'string' ? q : q.text;
        if (/UPDATE prospects SET ao_outreach_review_required=false/.test(sql)) {
          updated = true;
          await pool.query(`SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM expire-clock_timestamp()))+0.05)
            FROM session WHERE sid=$1`, [f.sessionId]);
          assert.equal((await pool.query('SELECT expire<=clock_timestamp() AS expired FROM session WHERE sid=$1', [f.sessionId])).rows[0].expired, true);
        }
        return result;
      } };
    } };
    await assert.rejects(require('../services/anchorCrmPreparationApproval').installPreparationApproval(h.noSchemaWrites(wrapped),
      installBody(p), fixture.actor(f), options(f)), { code: 'anchor_crm_preparation_approver_invalid' });
    assert.equal(updated, true); assert.deepEqual(await h.snapshot(pool), before); assert.equal(await state(f), null);
    assert.deepEqual(await h.effects(pool), noEffects);
  });
  assert.equal(deny.attempts(), 0);
});
