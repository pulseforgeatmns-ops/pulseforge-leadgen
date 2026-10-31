'use strict';
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs'), path = require('node:path');
const { startResearchOnlyHarness, placesFetch, ENV } = require('./helpers/scoutResearchOnlyFixture');
const { confirmCycleRelease, releaseNextResearchCycle } = require('./helpers/releaseResearchCycle');
const quiet = { log() {}, warn() {}, error() {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

test('durable per-cycle execution allowance (native PostgreSQL, synthetic providers)', { timeout: 600000 }, async t => {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const { pool } = h;
  const grants = require('../services/scoutResearchOnlyGrant'), missions = require('../services/scoutResearchMission');
  const control = require('../services/maxOutboundControlLoop');
  const { hash } = require('../services/scoutBoundedAcquisitionBudget');
  const google = placesFetch(), savedFetch = globalThis.fetch;
  globalThis.fetch = google.fetchImpl; t.after(() => { globalThis.fetch = savedFetch; });
  const mp = await missions.previewResearchMission(pool, { approvalRef: 'synthetic-cycle-release-mission' }, h.actor);
  const mission = await missions.installResearchMission(pool, { approvalRef: 'synthetic-cycle-release-mission', mission: mp.mission, missionHash: mp.missionHash }, h.actor);
  await pool.query("INSERT INTO api_spend_policies(provider,daily_usd_limit,paused,updated_by) VALUES('google_places',50,false,'operator')");
  const program = (await pool.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY authorized_at DESC LIMIT 1")).rows[0];
  const source = await control._test.loadSource(pool, program, '10');
  let sequence = 0;
  const table = async name => (await pool.query(`SELECT to_jsonb(x) row FROM ${name} x ORDER BY to_jsonb(x)::text`)).rows;
  const protectedNames = ['anchor_email_recovery_control','anchor_email_quarantines','anchor_scoped_resume_grants','api_spend_policies',
    'acquisition_outbound_items','acquisition_outbound_envelopes','acquisition_missions','companies','prospects','scout_unenriched'];
  const protectedBefore = Object.fromEntries(await Promise.all(protectedNames.map(async n => [n, await table(n)])));
  const count = async (name, grantId) => Number((await pool.query(`SELECT count(*)::int n FROM ${name} WHERE grant_id=$1`, [grantId])).rows[0].n);
  const install = async (ms = 86400000) => {
    const p = await grants.previewResearchOnlyGrant(pool, { approvalRef: `synthetic-cycle-grant-${++sequence}`, researchMissionId: mission.missionId,
      expiresAt: new Date(Date.now() + ms).toISOString(), statement: 'Synthetic approval: ceiling eight, only first cycle released.' }, h.actor);
    return grants.installResearchOnlyGrant(pool, { manifest: p.manifest, manifestHash: p.manifestHash }, h.actor);
  };
  const close = grantId => grants.revokeResearchOnlyGrant(pool, { grantId, approvalReference: `synthetic-close-${++sequence}` }, { kind: 'operator_cli' });
  t.afterEach(async () => {
    for (const row of (await pool.query('SELECT grant_id FROM scout_research_only_grants WHERE revoked_at IS NULL')).rows)
      await close(row.grant_id);
  });
  const claim = (id, extra = {}) => grants.claimResearchCycle(pool, { program, source, cycleId: id, ...extra });
  const tick = id => control.runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: id, logger: quiet,
    runPreparationRefill: async () => assert.fail('no downstream preparation') });
  const preview = grantId => grants.previewResearchCycleRelease(pool, { grantId, approvalReference: `synthetic-release-${++sequence}`,
    statement: 'Synthetic explicit review authorizes only the next cycle.' }, h.actor);
  const input = p => ({ release: p.release, releaseHash: p.releaseHash });
  const childClaim = cycleId => new Promise((resolve, reject) => {
    assert.match(h.postgres.connectionString, /localhost|127\.0\.0\.1/);
    const script = `(async()=>{const {Pool}=require('pg');const p=new Pool({connectionString:process.env.DATABASE_URL,max:2});
      try{const g=require('./services/scoutResearchOnlyGrant'),c=require('./services/maxOutboundControlLoop');
      const program=(await p.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY authorized_at DESC LIMIT 1")).rows[0];
      try{await g.claimResearchCycle(p,{program,source:await c._test.loadSource(p,program,'10'),cycleId:${JSON.stringify(cycleId)}});
        console.log(JSON.stringify({claimed:true}));}catch(e){console.log(JSON.stringify({claimed:false,code:e.code}));}}
      finally{await p.end();}})().catch(e=>{console.error(e);process.exitCode=1});`;
    const child = spawn(process.execPath, ['--require','./test/helpers/denyExternalProviders.cjs','-e',script],
      { cwd: path.join(__dirname,'..'), env: { PATH: process.env.PATH, ...ENV, DATABASE_URL: h.postgres.connectionString } });
    let stdout = '', stderr = '';
    child.stdout.on('data', x => { stdout += x; }); child.stderr.on('data', x => { stderr += x; });
    child.on('error', reject); child.on('exit', code => {
      if (code) return reject(Error(stderr || stdout));
      try { resolve(JSON.parse(stdout.trim().split('\n').at(-1))); } catch (e) { reject(e); }
    });
  });

  await t.test('one released cycle, repeated recurring invocations, restart and explicit cycle-two approval', async () => {
    const g = await install();
    assert.equal(g.maxCycles, 8); assert.equal(g.releasedCycles, 1);
    const grantBefore = await table('scout_research_only_grants');
    const result = await tick('synthetic-release-first');
    assert.equal(result.researchOnly.status, 'completed', JSON.stringify(result.researchOnly));
    assert.equal(google.calls.filter(x => x.path.endsWith('/textsearch/json')).length, 1);
    const paid = google.calls.length, spendBefore = await table('scout_research_google_spend');
    for (let n = 0; n < 8; n++) {
      const held = await tick(`synthetic-held-tick-${n}`);
      assert.equal(held.researchOnly.status, 'awaiting_cycle_release');
      assert.equal(held.halted, 'research_only_awaiting_cycle_release');
    }
    assert.equal(await count('scout_research_only_cycles', g.grantId), 1);
    assert.equal(google.calls.length, paid); assert.deepEqual(await table('scout_research_google_spend'), spendBefore);
    assert.deepEqual(await childClaim('synthetic-restarted-worker'), { claimed: false, code: 'research_only_awaiting_cycle_release' });
    assert.deepEqual(await childClaim('synthetic-release-first'), { claimed: false, code: 'research_only_cycle_already_claimed' });
    let receipt = await grants.inspectResearchOnlyGrant(pool, g.grantId);
    assert.equal(receipt.status, 'awaiting_cycle_release'); assert.equal(receipt.cyclesRemaining, 7);
    assert.equal(receipt.releasedCyclesRemaining, 0); assert.equal(receipt.releases.length, 1);
    assert.equal(receipt.releases[0].previous_allowance, 0); assert.equal(receipt.releases[0].released_cycles, 1);
    const p = await preview(g.grantId), stale = await preview(g.grantId);
    const noPassword = grants.releaseResearchCycle(pool, input(p), h.actor);
    await assert.rejects(noPassword, { code: 'fresh_confirmation_required' });
    await confirmCycleRelease(pool, h.actor, 'scout_research_approval');
    await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor), { code: 'fresh_confirmation_required' });
    await confirmCycleRelease(pool, h.actor);
    const approvals = await Promise.all([grants.releaseResearchCycle(pool, input(p), h.actor), grants.releaseResearchCycle(pool, input(p), h.actor)]);
    assert.equal(approvals.filter(x => !x.idempotent).length, 1);
    assert.equal(approvals[0].releasedCycles, 2);
    await assert.rejects(grants.releaseResearchCycle(pool, input(stale), h.actor), { code: 'research_only_release_allowance_changed' });
    const c2 = await claim('synthetic-reviewed-cycle-two'); assert.equal(c2.cycleNumber, 2);
    await grants.finishResearchCycle(pool, c2, { completed: false, result: { reason: 'synthetic_stop' } });
    await assert.rejects(claim('synthetic-no-three'), { code: 'research_only_awaiting_cycle_release' });
    receipt = await grants.inspectResearchOnlyGrant(pool, g.grantId);
    assert.equal(receipt.releases.length, 2); assert.equal(receipt.releases[1].administrator_id, h.actor.id);
    assert.equal(receipt.releases[1].approval_reference, p.release.approvalReference);
    assert.ok(receipt.releases[1].created_at); assert.equal(receipt.releases[1].previous_allowance, 1);
    assert.equal(receipt.releases[1].released_cycles, 2);
    assert.deepEqual(await table('scout_research_only_grants'), grantBefore, 'release changes no grant terms');
    await assert.rejects(pool.query('UPDATE scout_research_cycle_releases SET released_cycles=8'), /permanent/);
    await assert.rejects(pool.query('DELETE FROM scout_research_cycle_releases'), /permanent/);
    await assert.rejects(pool.query('TRUNCATE scout_research_cycle_releases'), /permanent/);
    await assert.rejects(pool.query(`INSERT INTO scout_research_only_cycles(grant_id,cycle_number,tenant_id,cycle_id,execution_token,lease_expires_at)
      VALUES($1,3,'10','synthetic-sql-bypass',$2,clock_timestamp()+interval '1 minute')`, [g.grantId, require('node:crypto').randomUUID()]), /awaiting_cycle_release/);
    await close(g.grantId);
  });

  await t.test('concurrent claims, failure and expired-lease restart never refund the released cycle', async () => {
    const g = await install();
    const races = await Promise.allSettled(Array.from({ length: 8 }, (_, i) => claim(`synthetic-concurrent-${i}`)));
    const winners = races.filter(r => r.status === 'fulfilled'); assert.equal(winners.length, 1);
    await assert.rejects(preview(g.grantId), { code: 'research_only_release_not_due' });
    await grants.finishResearchCycle(pool, winners[0].value, { completed: false, result: { reason: 'synthetic_failure' } });
    await assert.rejects(claim('synthetic-failure-retry'), { code: 'research_only_awaiting_cycle_release' });
    await releaseNextResearchCycle(pool, h.actor, g.grantId, 'synthetic-expired-lease-release');
    const orphan = await claim('synthetic-orphan-worker', { leaseMs: 100 });
    await sleep(200);
    assert.deepEqual(await childClaim('synthetic-after-worker-death'), { claimed: false, code: 'research_only_awaiting_cycle_release' });
    assert.equal(await count('scout_research_only_cycles', g.grantId), 2);
    await assert.rejects(grants.runInResearchScope(orphan, () => grants.googleMeter().reserve({ endpoint: 'text_search', tenantId: '10' }), pool), { code: grants.SPEND_CODE });
    await releaseNextResearchCycle(pool, h.actor, g.grantId, 'synthetic-reviewed-next-after-death');
    const c3 = await claim('synthetic-next-new-cycle'); assert.equal(c3.cycleNumber, 3);
    await grants.finishResearchCycle(pool, c3, { completed: false, result: {} });
    const rows = (await pool.query('SELECT state FROM scout_research_only_cycles WHERE grant_id=$1 ORDER BY cycle_number', [g.grantId])).rows;
    assert.deepEqual(rows.map(r => r.state), ['stopped','abandoned','stopped']);
    await close(g.grantId);
  });

  await t.test('invalid, foreign, tampered, expired-session and unverified release approvals fail closed', async () => {
    const g = await install(), c = await claim('synthetic-auth-consumed');
    await grants.finishResearchCycle(pool, c, { completed: false, result: {} });
    const p = await preview(g.grantId);
    for (const mutate of [r => { r.newAllowance = 8; }, r => { r.tenantId = '13'; }, r => { r.expiresAt = '2099-01-01'; }, r => { r.operator.role = 'ao'; }]) {
      const r = structuredClone(p.release); mutate(r);
      await assert.rejects(grants.releaseResearchCycle(pool, { release: r, releaseHash: hash(r) }, h.actor), { code: 'research_only_release_invalid' });
    }
    await assert.rejects(grants.releaseResearchCycle(pool, { ...input(p), releaseHash: '0'.repeat(64) }, h.actor), { code: 'research_only_release_exact_confirmation_required' });
    for (const actor of [{ ...h.actor, role: 'ao' }, { ...h.actor, id: '72' }, { ...h.actor, sessionId: 'synthetic-other-session' }])
      await assert.rejects(grants.releaseResearchCycle(pool, input(p), actor), { code: 'research_only_operator_invalid' });
    for (const value of [false, null]) {
      await pool.query('UPDATE users SET email_verified=$2 WHERE id=$1', [h.actor.id, value]);
      try { await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor)); }
      finally { await pool.query('UPDATE users SET email_verified=true WHERE id=$1', [h.actor.id]); }
    }
    await pool.query(`UPDATE "session" SET expire='2000-01-01' WHERE sid=$1`, [h.actor.sessionId]);
    try { await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor)); }
    finally { await pool.query(`UPDATE "session" SET expire='2099-01-01' WHERE sid=$1`, [h.actor.sessionId]); }
    await confirmCycleRelease(pool, h.actor);
    await pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{freshConfirmation,expiresAt}','1')::json WHERE sid=$1`, [h.actor.sessionId]);
    await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor), { code: 'fresh_confirmation_required' });
    const old = process.env.RAILWAY_GIT_COMMIT_SHA; process.env.RAILWAY_GIT_COMMIT_SHA = 'c'.repeat(40);
    try {
      await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor), { code: 'research_only_revision_mismatch' });
      await assert.rejects(claim('synthetic-wrong-revision'), { code: 'research_only_revision_mismatch' });
    } finally { process.env.RAILWAY_GIT_COMMIT_SHA = old; }
    assert.equal(await count('scout_research_cycle_releases', g.grantId), 1);
    await close(g.grantId);
    await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor), { code: 'research_only_grant_revoked' });
    assert.equal(await claim('synthetic-revoked'), null);
  });

  await t.test('expiry and missing enforcement prevent claims and releases', async () => {
    const g = await install(3000), c = await claim('synthetic-expiring');
    await grants.finishResearchCycle(pool, c, { completed: false, result: {} });
    const p = await preview(g.grantId);
    await sleep(3100);
    await assert.rejects(claim('synthetic-expired'), { code: 'research_only_grant_window_inactive' });
    await assert.rejects(grants.releaseResearchCycle(pool, input(p), h.actor), { code: 'research_only_grant_window_inactive' });
    await close(g.grantId);
    const next = await install();
    await pool.query('ALTER TABLE scout_research_only_cycles DISABLE TRIGGER scout_research_cycles_release_guard');
    try { await assert.rejects(claim('synthetic-disabled-guard'), { code: 'research_only_schema_guards_missing' }); }
    finally { await pool.query('ALTER TABLE scout_research_only_cycles ENABLE TRIGGER scout_research_cycles_release_guard'); }
    await close(next.grantId);
  });

  assert.deepEqual(Object.fromEntries(await Promise.all(protectedNames.map(async n => [n, await table(n)]))), protectedBefore,
    'maintenance, suppression/quarantine, shared budgets, CRM and outbound are unchanged');
  assert.equal(deny.attempts(), 0);
});

test('migration preserves legacy grants and claims without inventing release authority', { timeout: 600000 }, async t => {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const { pool } = h, grants = require('../services/scoutResearchOnlyGrant'), missions = require('../services/scoutResearchMission');
  const mp = await missions.previewResearchMission(pool, { approvalRef: 'synthetic-legacy-mission' }, h.actor);
  const m = await missions.installResearchMission(pool, { approvalRef: 'synthetic-legacy-mission', mission: mp.mission, missionHash: mp.missionHash }, h.actor);
  const p = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'synthetic-legacy-grant', researchMissionId: m.missionId,
    expiresAt: new Date(Date.now() + 86400000).toISOString(), statement: 'Synthetic historical ceiling-only grant.' }, h.actor);
  const migration = name => fs.readFileSync(path.join(__dirname,'../migrations',name),'utf8');
  await pool.query(migration('2026-10-30-scout-research-cycle-releases.rollback.sql'));
  const v = p.manifest;
  // A disposable pre-migration database, never a production repair or approval.
  await pool.query(`INSERT INTO scout_research_only_grants(grant_id,approval_ref,tenant_id,program_id,mission_id,research_mission_id,
    search_fingerprint,manifest,operator_id,operator_role,approver_session_hash,max_cycles,daily_google_usd_micros,not_before,expires_at)
    VALUES($1,$2,'10',$3,$4,$5,$6,$7,$8,'admin',$9,8,5000000,$10,$11)`,
  [p.grantId,v.approvalRef,v.programId,v.missionId,v.researchMissionId,v.fingerprint,v,v.operator.id,v.operator.sessionHash,v.notBefore,v.expiresAt]);
  await pool.query(`INSERT INTO scout_research_only_cycles(grant_id,cycle_number,tenant_id,cycle_id,execution_token,lease_expires_at,state,finished_at)
    VALUES($1,1,'10','synthetic-legacy-consumed',$2,now()+interval '1 minute','completed',now())`, [p.grantId,require('node:crypto').randomUUID()]);
  const rows = async table => (await pool.query(`SELECT to_jsonb(x) row FROM ${table} x ORDER BY to_jsonb(x)::text`)).rows;
  const before = [await rows('scout_research_only_grants'),await rows('scout_research_only_cycles')];
  await pool.query(migration('2026-10-30-scout-research-cycle-releases.sql'));
  assert.deepEqual([await rows('scout_research_only_grants'),await rows('scout_research_only_cycles')], before);
  const receipt = await grants.inspectResearchOnlyGrant(pool, p.grantId);
  assert.equal(receipt.status, 'awaiting_cycle_release'); assert.equal(receipt.releasedCycles, 0);
  assert.deepEqual(receipt.releases, []);
  const again = await grants.installResearchOnlyGrant(pool, { manifest: p.manifest, manifestHash: p.manifestHash }, h.actor);
  assert.equal(again.idempotent, true); assert.equal(again.releasedCycles, 0);
  await assert.rejects(grants.previewResearchCycleRelease(pool, { grantId: p.grantId, approvalReference: 'synthetic-no-backfill', statement: 'No retroactive permission.' }, h.actor),
    { code: 'research_only_release_initial_missing' });
  const rollbackDb = await pool.connect();
  try { await assert.rejects(rollbackDb.query(migration('2026-10-30-scout-research-cycle-releases.rollback.sql')), /schema_in_use/); }
  finally { await rollbackDb.query('ROLLBACK'); rollbackDb.release(); }
  assert.deepEqual([await rows('scout_research_only_grants'),await rows('scout_research_only_cycles')], before);
  assert.equal(deny.attempts(), 0);
});
