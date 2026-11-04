'use strict';
// SPEC-SCOUT-P0-005: research continuity across revision changes, on disposable
// PostgreSQL with synthetic providers. No production grant, release or search.
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');
const { startResearchOnlyHarness, placesFetch, ENV } = require('./helpers/scoutResearchOnlyFixture');
const { releaseNextResearchCycle } = require('./helpers/releaseResearchCycle');
const quiet = { log() {}, warn() {}, error() {} };
const OTHER = 'c'.repeat(40), THIRD = 'd'.repeat(40);

test('revision mismatch holds, concurrent workers and successor-grant continuity', { timeout: 600000 }, async t => {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const { pool } = h;
  const grants = require('../services/scoutResearchOnlyGrant'), missions = require('../services/scoutResearchMission');
  const control = require('../services/maxOutboundControlLoop');
  const google = placesFetch(), savedFetch = globalThis.fetch;
  globalThis.fetch = google.fetchImpl; t.after(() => { globalThis.fetch = savedFetch; process.env.RAILWAY_GIT_COMMIT_SHA = ENV.RAILWAY_GIT_COMMIT_SHA; });
  const mp = await missions.previewResearchMission(pool, { approvalRef: 'synthetic-continuity-mission' }, h.actor);
  const mission = await missions.installResearchMission(pool, { approvalRef: 'synthetic-continuity-mission', mission: mp.mission, missionHash: mp.missionHash }, h.actor);
  await pool.query("INSERT INTO api_spend_policies(provider,daily_usd_limit,paused,updated_by) VALUES('google_places',50,false,'operator')");
  const program = (await pool.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY authorized_at DESC LIMIT 1")).rows[0];
  const source = await control._test.loadSource(pool, program, '10');
  const table = async name => (await pool.query(`SELECT to_jsonb(x) row FROM ${name} x ORDER BY to_jsonb(x)::text`)).rows;
  let seq = 0;
  const install = async () => {
    const p = await grants.previewResearchOnlyGrant(pool, { approvalRef: `synthetic-continuity-grant-${++seq}`, researchMissionId: mission.missionId,
      expiresAt: new Date(Date.now() + 86400000).toISOString(), statement: 'Synthetic approval for continuity testing.' }, h.actor);
    return grants.installResearchOnlyGrant(pool, { manifest: p.manifest, manifestHash: p.manifestHash }, h.actor);
  };
  const deploy = sha => { process.env.RAILWAY_GIT_COMMIT_SHA = sha; };
  const tick = id => control.runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: id, logger: quiet,
    runPreparationRefill: async () => assert.fail('research-only operation never prepares') });
  const holds = async () => (await pool.query(`SELECT payload FROM agent_actions WHERE action_type='scout_replenishment_blocked'
    AND payload->>'reason'='research_only_revision_mismatch' ORDER BY id`)).rows.map(r => r.payload);
  const childClaim = cycleId => new Promise((resolve, reject) => {
    const script = `(async()=>{const {Pool}=require('pg');const p=new Pool({connectionString:process.env.DATABASE_URL,max:2});
      try{const g=require('./services/scoutResearchOnlyGrant'),c=require('./services/maxOutboundControlLoop');
      const program=(await p.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY authorized_at DESC LIMIT 1")).rows[0];
      const source=await c._test.loadSource(p,program,'10');
      await new Promise(r=>setTimeout(r,Math.max(0,Number(process.env.START_AT)-Date.now())));
      try{await g.claimResearchCycle(p,{program,source,cycleId:${JSON.stringify(cycleId)}});console.log(JSON.stringify({claimed:true}));}
      catch(e){console.log(JSON.stringify({claimed:false,code:e.code}));}}finally{await p.end();}})().catch(e=>{console.error(e);process.exitCode=1});`;
    const child = spawn(process.execPath, ['--require', './test/helpers/denyExternalProviders.cjs', '-e', script],
      { cwd: path.join(__dirname, '..'), env: { PATH: process.env.PATH, ...ENV, DATABASE_URL: h.postgres.connectionString, START_AT: String(startAt) } });
    let out = '', err = '';
    child.stdout.on('data', x => { out += x; }); child.stderr.on('data', x => { err += x; });
    child.on('error', reject); child.on('exit', code => code ? reject(Error(err || out)) : resolve(JSON.parse(out.trim().split('\n').at(-1))));
  });
  let startAt = 0;

  // 1. Valid research-only execution under a released grant (acceptance 1, 10).
  const g = await install();
  const first = await tick('synthetic-continuity-first');
  assert.equal(first.researchOnly.status, 'completed', JSON.stringify(first.researchOnly));
  assert.equal(first.sent, 0); assert.equal(first.preparedAdded, 0); assert.equal(first.prepareRequested, false);
  assert.equal((await tick('synthetic-continuity-held')).halted, 'research_only_awaiting_cycle_release', 'acceptance 3');

  // 2. Exact one-cycle claim under two independent worker processes plus this one (acceptance 2).
  await releaseNextResearchCycle(pool, h.actor, g.grantId, 'synthetic-continuity-release-2');
  startAt = Date.now() + 1500;
  const racers = [childClaim('synthetic-worker-a'), childClaim('synthetic-worker-b'),
    new Promise(r => setTimeout(r, startAt - Date.now())).then(() => grants.claimResearchCycle(pool, { program, source, cycleId: 'synthetic-worker-main' }))
      .then(c => ({ claimed: true, claim: c }), e => ({ claimed: false, code: e.code }))];
  const outcomes = await Promise.all(racers);
  assert.equal(outcomes.filter(o => o.claimed).length, 1, JSON.stringify(outcomes));
  for (const o of outcomes.filter(o => !o.claimed))
    assert.ok(['research_only_cycle_in_progress', 'research_only_awaiting_cycle_release'].includes(o.code), o.code);
  const won = outcomes.find(o => o.claim);
  if (won) await grants.finishResearchCycle(pool, won.claim, { completed: false, result: { reason: 'synthetic_race_stop' } });
  else await pool.query(`UPDATE scout_research_only_cycles SET state='stopped',finished_at=clock_timestamp(),result='{}'::jsonb
    WHERE grant_id=$1 AND state='claimed'`, [g.grantId]);
  assert.equal(Number((await pool.query('SELECT count(*) n FROM scout_research_only_cycles WHERE grant_id=$1', [g.grantId])).rows[0].n), 2);

  // 3. A deploy changes the running revision: fail closed, actionable, nothing repinned (acceptance 4, 5, 7).
  await releaseNextResearchCycle(pool, h.actor, g.grantId, 'synthetic-continuity-release-3');
  const frozen = { grants: await table('scout_research_only_grants'), cycles: await table('scout_research_only_cycles'),
    spend: await table('scout_research_google_spend'), releases: await table('scout_research_cycle_releases') };
  const paid = google.calls.length;
  deploy(OTHER);
  for (let i = 0; i < 6; i++) {
    const held = await tick(`synthetic-mismatch-${i}`);
    assert.equal(held.halted, 'research_only_revision_mismatch');
    assert.equal(held.researchOnly.status, 'revision_mismatch');
    assert.deepEqual(held.researchOnly.diagnostic, { grantId: g.grantId, pinnedRevision: ENV.RAILWAY_GIT_COMMIT_SHA, deployedRevision: OTHER });
    assert.equal(held.sent, 0);
  }
  assert.equal(google.calls.length, paid, 'no provider call while held');
  for (const [name, rows] of Object.entries({ scout_research_only_grants: frozen.grants, scout_research_only_cycles: frozen.cycles,
    scout_research_google_spend: frozen.spend, scout_research_cycle_releases: frozen.releases }))
    assert.deepEqual(await table(name), rows, `${name} unchanged by mismatch holds`);
  assert.equal((await holds()).length, 1, 'one durable hold for an unchanged grant/revision');
  assert.match((await holds())[0].detail, new RegExp(`grant=${g.grantId.slice(0, 12)}:pinned=${'b'.repeat(12)}:deployed=${'c'.repeat(12)}`));
  let receipt = await grants.inspectResearchOnlyGrant(pool, g.grantId);
  assert.equal(receipt.executionStatus, 'revision_mismatch');
  assert.equal(receipt.status, 'installed', 'historical status alias kept');
  assert.deepEqual(receipt.revision, { pinned: ENV.RAILWAY_GIT_COMMIT_SHA, deployed: OTHER, matches: false });
  assert.equal(receipt.hold.releasable, false); assert.match(receipt.hold.action, /successor/);
  assert.equal(receipt.expectedRevision, ENV.RAILWAY_GIT_COMMIT_SHA, 'pin never changes');
  await assert.rejects(grants.claimResearchCycle(pool, { program, source, cycleId: 'synthetic-direct-claim' }), { code: 'research_only_revision_mismatch' });
  await assert.rejects(grants.previewResearchCycleRelease(pool, { grantId: g.grantId, approvalReference: 'synthetic-blocked-release',
    statement: 'Must not release under a mismatched revision.' }, h.actor), { code: 'research_only_revision_mismatch' });

  // 4. Another deploy is a new durable diagnostic (acceptance 9).
  deploy(THIRD);
  assert.equal((await tick('synthetic-mismatch-third')).researchOnly.diagnostic.deployedRevision, THIRD);
  assert.equal((await holds()).length, 2);

  // 5. Redeploying the pinned revision clears the hold without new authority.
  deploy(ENV.RAILWAY_GIT_COMMIT_SHA);
  receipt = await grants.inspectResearchOnlyGrant(pool, g.grantId);
  assert.equal(receipt.executionStatus, 'installed'); assert.equal(receipt.hold, null);
  assert.equal(receipt.releasedCycles, 3, 'no allowance gained or lost across the hold');

  // 6. Successor grant: only by explicit revoke + approval, history preserved (acceptance 5, 6).
  deploy(OTHER);
  const candidatesBefore = await table('scout_research_only_cycles');
  await grants.revokeResearchOnlyGrant(pool, { grantId: g.grantId, approvalReference: 'synthetic-continuity-revoke' }, { kind: 'operator_cli' });
  const successor = await install();
  assert.notEqual(successor.grantId, g.grantId);
  assert.equal(successor.expectedRevision, OTHER, 'successor pins the running revision');
  assert.equal(successor.releasedCycles, 1, 'successor starts with exactly one released cycle');
  assert.deepEqual((await table('scout_research_only_cycles')).filter(r => r.row.grant_id === g.grantId),
    candidatesBefore.filter(r => r.row.grant_id === g.grantId), 'old cycles preserved');
  const old = await grants.inspectResearchOnlyGrant(pool, g.grantId);
  assert.equal(old.status, 'revoked'); assert.equal(old.hold, null); assert.equal(old.releases.length, 3);
  assert.equal((await tick('synthetic-successor-first')).researchOnly.status, 'completed');
  await grants.revokeResearchOnlyGrant(pool, { grantId: successor.grantId, approvalReference: 'synthetic-continuity-close' }, { kind: 'operator_cli' });
});
