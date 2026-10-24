'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { prepareDrafts } = require('../services/anchorCrmPreparationAction');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
function harness() {
  const filename = path.join(__dirname, 'anchorScopedResumeCrmPreparationPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,install,afterConsumption};', {
    module, exports: module.exports, __dirname, process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}
const h = harness();
const revision = 'a'.repeat(40);
const input = f => ({ revision, manifestHash: hash(f.manifest), sourceMissionHash: f.input.sourceMissionHash });
const actor = f => ({ id: f.operator.id, role: 'admin' });
const opts = f => ({ env: { ...f.env, RAILWAY_GIT_COMMIT_SHA: revision }, now: () => f.now });
async function effects(pool) {
  return (await pool.query(`SELECT
    (SELECT count(*)::int FROM anchor_scoped_resume_consumption) AS consumed,
    (SELECT count(*)::int FROM acquisition_missions WHERE id LIKE 'mission_crm_%') AS children,
    (SELECT count(*)::int FROM anchor_email_recovery_reservations) AS reservations`)).rows[0];
}
test('admin action uses native canonical PostgreSQL preparation without providers', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('crm-action-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 12 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop(); });
  t.mock.timers.enable({ apis: ['Date'], now: +new Date('2026-10-19T16:00:00Z') });
  const old = process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
  process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'local-scoped-resume';
  t.after(() => old === undefined ? delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID : process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = old);
  await t.test('admin gets review-only READY; replay is not fresh success', async () => {
    const f = await h.setup(pool); await h.install(pool, f);
    const result = await prepareDrafts(pool, input(f), actor(f), opts(f));
    assert.equal(result.status, 'ready_for_review'); assert.equal(result.sent, 0);
    assert.equal(result.providerCalls, 0); assert.equal(result.executionApproved, false);
    assert.equal(result.envelopeAuthorized, false); assert.equal(result.retryAllowed, false);
    const before = await effects(pool);
    await assert.rejects(prepareDrafts(pool, input(f), actor(f), opts(f)), e =>
      e.status === 'already_consumed' && e.retryAllowed === false && e.writesMayHaveOccurred === true);
    assert.deepEqual(await effects(pool), before);
    assert.deepEqual(before, { consumed: 1, children: 1, reservations: 0 });
  });
  await t.test('bad actors, grant pins, revision, source and unsupported options fail before writes', async () => {
    for (const mode of ['manager', 'unknown', 'inactive', 'wrong_tenant', 'manifest', 'source', 'revision', 'extra']) {
      const f = await h.setup(pool); await h.install(pool, f);
      const a = actor(f), i = input(f);
      if (mode === 'manager') a.role = 'manager';
      if (mode === 'unknown') a.id = 'other-admin';
      if (mode === 'inactive') await pool.query('UPDATE users SET active=false');
      if (mode === 'wrong_tenant') await pool.query('UPDATE users SET client_id=11');
      if (mode === 'manifest') i.manifestHash = 'b'.repeat(64);
      if (mode === 'source') i.sourceMissionHash = 'b'.repeat(64);
      if (mode === 'revision') i.revision = 'b'.repeat(40);
      if (mode === 'extra') i.send = true;
      await assert.rejects(prepareDrafts(pool, i, a, opts(f)), e => e.status === 'rejected' && !e.writesMayHaveOccurred);
      assert.deepEqual(await effects(pool), { consumed: 0, children: 0, reservations: 0 }, mode);
    }
  });
  await t.test('concurrent duplicate requests consume once and do not send', async () => {
    const f = await h.setup(pool); await h.install(pool, f);
    const results = await Promise.allSettled([1, 2].map(() => prepareDrafts(pool, input(f), actor(f), opts(f))));
    assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
    assert.equal(results.find(r => r.status === 'rejected').reason.status, 'already_consumed');
    assert.deepEqual(await effects(pool), { consumed: 1, children: 1, reservations: 0 });
  });
  await t.test('post-consumption failure preserves claim and blocks recovery replay', async () => {
    const f = await h.setup(pool); await h.install(pool, f);
    const wrapped = h.afterConsumption(pool, async () => { await pool.query('UPDATE users SET active=false'); });
    await assert.rejects(prepareDrafts(wrapped, input(f), actor(f), opts(f)), e => e.status === 'blocked_review_required' && e.writesMayHaveOccurred);
    await pool.query('UPDATE users SET active=true');
    await assert.rejects(prepareDrafts(pool, input(f), actor(f), opts(f)), e => e.status === 'already_consumed');
    assert.deepEqual(await effects(pool), { consumed: 1, children: 0, reservations: 0 });
  });
  await t.test('revision or manifest changes after consumption cannot produce drafts', async () => {
    for (const mode of ['revision', 'manifest']) {
      const f = await h.setup(pool); await h.install(pool, f);
      const o = opts(f);
      const wrapped = h.afterConsumption(pool, async () => {
        if (mode === 'revision') o.env.RAILWAY_GIT_COMMIT_SHA = 'b'.repeat(40);
        else o.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'replacement';
      });
      await assert.rejects(prepareDrafts(wrapped, input(f), actor(f), o), e =>
        e.status === 'blocked_review_required' && e.writesMayHaveOccurred && !e.retryAllowed);
      assert.deepEqual(await effects(pool), { consumed: 1, children: 0, reservations: 0 });
    }
  });
  await t.test('expired program is not extended or reset', async () => {
    const f = await h.setup(pool); await h.install(pool, f);
    const before = (await pool.query('SELECT row_to_json(p) AS value FROM acquisition_outbound_programs p')).rows;
    const o = opts(f); o.now = () => new Date('2026-10-24T12:00:00Z');
    await assert.rejects(prepareDrafts(pool, input(f), actor(f), o));
    assert.deepEqual((await pool.query('SELECT row_to_json(p) AS value FROM acquisition_outbound_programs p')).rows, before);
    assert.deepEqual(await effects(pool), { consumed: 0, children: 0, reservations: 0 });
  });
});
