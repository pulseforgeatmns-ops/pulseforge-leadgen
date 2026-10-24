'use strict';
// Independent bounded review of the LOCAL CRM-only preparation bridge.
// All contacts, domains and approvals are synthetic; PostgreSQL is disposable.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const control = require('../services/anchorScopedResumeControl');
const { prepareExistingCrm } = require('../services/anchorScopedResumeCrmPreparation');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const persistence = require('../services/acquisitionMissionPersistence');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');

function fixtureHarness() {
  const filename = path.join(__dirname, 'anchorScopedResumeCrmPreparationPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,install,extraAdmission,afterConsumption};', {
    module, exports: module.exports, __dirname: path.dirname(filename), process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}
const h = fixtureHarness();
const run = (pool, f) => prepareExistingCrm(pool, f.input, { env: f.env, now: () => f.now });
async function effects(pool) {
  return (await pool.query(`SELECT
    (SELECT count(*)::int FROM anchor_scoped_resume_consumption) AS consumed,
    (SELECT count(*)::int FROM acquisition_mission_contributions) AS contributions,
    (SELECT count(*)::int FROM acquisition_missions WHERE id LIKE 'mission_crm_%') AS children,
    (SELECT count(*)::int FROM anchor_email_recovery_reservations) AS reservations`)).rows[0];
}
async function heldRow(pool, f) {
  return (await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1', [f.quarantine.item_id])).rows[0].row;
}

test('independent CRM bridge admission, replay and uncertainty regressions', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-crm-adversarial-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 12 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop(); });
  t.mock.timers.enable({ apis: ['Date'], now: +new Date('2026-10-19T16:00:00Z') });
  const keys = ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'];
  const previous = Object.fromEntries(keys.map(k => [k, process.env[k]]));
  keys.forEach(k => delete process.env[k]);
  process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'local-scoped-resume';
  t.after(() => keys.forEach(k => previous[k] === undefined ? delete process.env[k] : process.env[k] = previous[k]));
  let networkCalls = 0;
  const forbidden = () => { networkCalls++; throw new Error('external network forbidden in CRM preparation'); };
  for (const method of ['get', 'post', 'request']) t.mock.method(require('axios'), method, forbidden);
  t.mock.method(require('node:https'), 'request', forbidden);
  t.mock.method(require('node:http'), 'request', forbidden);
  t.mock.method(globalThis, 'fetch', forbidden);
  // Scout is frozen by the canonical package. Its provider entrypoint cannot be
  // monkey-patched; the private CRM discoverImpl plus forbidden network leaves
  // no provider fallback available in this positive real execution.

  await t.test('attempt history cannot be erased by another CRM contact, same email, or company alias', async () => {
    for (const identityMode of ['company', 'contact', 'duplicate_email', 'company_name_alias', 'company_domain_alias']) {
      const f = await h.setup(pool); await h.install(pool, f);
      const beforeHeld = await heldRow(pool, f);
      let priorId = f.manifest.admissions[0].companyId, priorEmail = f.manifest.admissions[0].recipientEmail;
      if (identityMode === 'contact') priorId = f.manifest.admissions[0].prospectId;
      if (identityMode.startsWith('company_') || identityMode === 'duplicate_email') {
        const extra = await h.extraAdmission(pool, f, 2);
        priorId = extra.companyId;
        priorEmail = extra.recipientEmail;
        if (identityMode === 'duplicate_email') {
          priorEmail = f.manifest.admissions[0].recipientEmail;
          await pool.query('UPDATE prospects SET email=$1 WHERE id=$2', [priorEmail, extra.prospectId]);
        } else if (identityMode === 'company_name_alias') {
          await pool.query('UPDATE companies SET name=$1 WHERE id=$2', ['Fresh Synthetic Enterprise', extra.companyId]);
        } else {
          await pool.query('UPDATE companies SET domain=$1,website=$2 WHERE id=$3',
            [f.manifest.admissions[0].companyDomain, `https://${f.manifest.admissions[0].companyDomain}`, extra.companyId]);
        }
      }
      await pool.query(`INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,mission_id,prospect_id,status,payload)
        VALUES('synthetic-prior','10','synthetic-prior-mission',$1,'attempted',$2)`, [priorId, { email: priorEmail }]);
      await assert.rejects(run(pool, f), undefined, identityMode);
      assert.deepEqual(await effects(pool), { consumed: 0, contributions: 0, children: 0, reservations: 0 }, identityMode);
      assert.deepEqual(await heldRow(pool, f), beforeHeld);
    }
  });

  await t.test('post-reservation expiry and activation removal burn the operation and create no artifacts', async () => {
    for (const mode of ['expiry', 'activation_removed']) {
      const f = await h.setup(pool); await h.install(pool, f);
      const originalNow = new Date(f.now);
      const hooked = h.afterConsumption(pool, async () => {
        if (mode === 'expiry') f.now = new Date(f.manifest.expiresAt);
        else delete f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
      });
      await assert.rejects(run(hooked, f));
      assert.deepEqual(await effects(pool), { consumed: 1, contributions: 0, children: 0, reservations: 0 });
      f.now = originalNow; f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
      await assert.rejects(run(pool, f), /operation_consumed/);
    }
  });

  await t.test('new grant ID and incidental source changes do not reset an already prepared batch', async () => {
    const f = await h.setup(pool); await h.install(pool, f);
    const result = await run(pool, f), before = await effects(pool), beforeHeld = await heldRow(pool, f);
    await control.revokeScopedResume(pool, { manifestId: f.manifest.id, approvalReference: 'synthetic-reviewed-replacement' },
      { id: f.operator.id, role: f.operator.role }, { env: f.env, now: f.now });
    f.manifest.id = 'synthetic-replacement-grant';
    f.manifest.approvalReference = 'synthetic-explicit-replacement-approval';
    f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    await h.install(pool, f);
    await assert.rejects(run(pool, f), /operation_consumed/);
    f.source.title = 'An incidental source title edit';
    await pool.query('UPDATE acquisition_missions SET payload=$1 WHERE id=$2', [f.source, f.source.id]);
    f.input.sourceMissionHash = hash(f.source);
    await assert.rejects(run(pool, f), /operation_consumed/);
    assert.deepEqual(await effects(pool), before);
    assert.deepEqual(await heldRow(pool, f), beforeHeld);
    const saved = await persistence.loadMissionSnapshot(result.missionId, '10', pool, { skipEnsure: true });
    assert.equal(saved.mission.stage, 'ready');
    assert.equal(saved.mission.pendingOperatorDecision.kind, 'execution_approval');
    assert.doesNotMatch(JSON.stringify(saved), /No blacklist concerns|Healthy\. Safe\. Send\./);
    assert.equal(saved.mission.crmPreparation.providerReadiness, 'not_checked');
  });

  await t.test('provider/email authority and caller callbacks are rejected rather than entering this preparation slice', async () => {
    const f = await h.setup(pool); await h.install(pool, f);
    for (const option of ['runScout', 'runPaige', 'runEmmett', 'discoverImpl', 'provider']) {
      await assert.rejects(prepareExistingCrm(pool, f.input, { env: f.env, now: f.now, [option]: forbidden }), /input_invalid/);
    }
    assert.deepEqual(await effects(pool), { consumed: 0, contributions: 0, children: 0, reservations: 0 });
    const g = await h.setup(pool);
    g.manifest.scopes.email = require('./helpers/anchorScopedResumeFixture').scopedResumeFixture().manifest.scopes.email;
    await h.install(pool, g);
    await assert.rejects(run(pool, g), /preparation_only_required/);
    assert.deepEqual(await effects(pool), { consumed: 0, contributions: 0, children: 0, reservations: 0 });
    assert.equal(networkCalls, 0);
  });
});
