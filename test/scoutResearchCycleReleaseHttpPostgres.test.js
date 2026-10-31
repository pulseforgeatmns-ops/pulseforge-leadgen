'use strict';
// Capture only the synthetic fixture's loopback client before denying providers.
const { startApprovalHarness, ADMIN, AO, OTHER_ADMIN } = require('./helpers/scoutResearchApprovalHarness');
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const API = '/api/v1/tenant-outreach/anchor-program/';

test('cycle releases require a direct Anchor admin, exact preview and a fresh purpose-bound password', { timeout: 600000 }, async t => {
  const h = await startApprovalHarness(); t.after(() => h.close());
  const { pool, http, adminCookie } = h;
  const post = (route, body, cookie = adminCookie, headers) => http(route, { method: 'POST', body, cookie, headers });
  const confirm = (purpose = 'scout_research_approval', cookie = adminCookie) => post('/api/v1/auth/fresh-confirmation',
    { purpose, password: h.fixturePassword }, cookie);
  const mp = (await post(API + 'research-mission/preview', { approvalRef: 'synthetic-http-release-mission' })).json;
  assert.equal((await confirm()).status, 200);
  const m = await post(API + 'research-mission/install', { approvalRef: 'synthetic-http-release-mission', mission: mp.mission, missionHash: mp.missionHash });
  assert.equal(m.status, 200, m.text);
  const gp = (await post(API + 'research-only-grant/preview', { approvalRef: 'synthetic-http-release-grant', researchMissionId: m.json.missionId,
    expiresAt: new Date(Date.now() + 86400000).toISOString(), statement: 'Synthetic controlled first cycle only.' })).json;
  assert.equal(gp.initiallyReleasedCycles, 1); assert.equal(gp.maxCycles, 8);
  assert.equal((await confirm()).status, 200);
  const installed = await post(API + 'research-only-grant/install', { manifest: gp.manifest, manifestHash: gp.manifestHash });
  assert.equal(installed.status, 200, installed.text);
  const grantId = installed.json.grantId;
  const grants = require('../services/scoutResearchOnlyGrant'), control = require('../services/maxOutboundControlLoop');
  const program = (await pool.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10'")).rows[0];
  const source = await control._test.loadSource(pool, program, '10');
  const claim = id => grants.claimResearchCycle(pool, { program, source, cycleId: id });
  const first = await claim('synthetic-http-cycle-one');
  await grants.finishResearchCycle(pool, first, { completed: false, result: { reason: 'synthetic_review_fixture' } });
  const immutable = (await pool.query('SELECT to_jsonb(g) row FROM scout_research_only_grants g WHERE grant_id=$1', [grantId])).rows;
  const request = { grantId, approvalReference: 'synthetic-http-release-two', statement: 'Synthetic explicit review approves cycle two only.' };
  const preview = await post(API + 'research-cycle-release/preview', request);
  assert.equal(preview.status, 200, preview.text); assert.equal(preview.json.previewOnly, true);
  const body = { release: preview.json.release, releaseHash: preview.json.releaseHash };
  const release = (cookie = adminCookie, headers, data = body) => post(API + 'research-cycle-release/release', data, cookie, headers);

  await t.test('anonymous, wrong tenant, AO, impersonation and cross-origin requests cannot release', async () => {
    assert.equal((await release(null)).status, 401);
    for (const user of [AO, OTHER_ADMIN]) {
      const signedIn = await h.loginAs(user);
      assert.equal((await release(signedIn.cookie)).status, 403);
      assert.equal((await post(API + 'research-cycle-release/preview', request, signedIn.cookie)).status, 403);
    }
    const session = (await h.sessionRow(adminCookie)).sess;
    const impersonated = await h.seedSession('synthetic-release-impersonated', { ...session, impersonation: { active: true } });
    assert.equal((await release(impersonated)).status, 403);
    for (const headers of [{ origin: 'https://untrusted.example' }, { 'sec-fetch-site': 'cross-site' }])
      assert.equal((await release(adminCookie, headers)).status, 403);
    assert.equal((await release(adminCookie, undefined, { ...body, tenantId: '13' })).status, 409);
    assert.equal((await release(adminCookie, undefined, { ...body, releaseHash: '0'.repeat(64) })).status, 409);
  });

  await t.test('the install password purpose, another session and an expired marker cannot authorize a release', async () => {
    assert.equal((await release()).json.code, 'fresh_confirmation_required');
    assert.equal((await confirm()).status, 200);
    assert.equal((await release()).status, 403, 'install purpose is not a cycle-release purpose');
    const otherSession = await h.loginAs(ADMIN);
    assert.equal((await confirm('scout_research_cycle_release', otherSession.cookie)).status, 200);
    assert.equal((await release()).status, 403);
    assert.equal((await release(otherSession.cookie)).status, 409, 'the exact preview is bound to its original session');
    assert.equal((await confirm('scout_research_cycle_release')).status, 200);
    await pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{freshConfirmation,expiresAt}','1')::json WHERE sid=$1`, [h.actor.sessionId]);
    assert.equal((await release()).status, 403);
    assert.equal((await grants.inspectResearchOnlyGrant(pool, grantId)).releasedCycles, 1);
  });

  await t.test('one password confirmation and concurrent retries create one release, without changing grant terms', async () => {
    assert.equal((await confirm('scout_research_cycle_release')).status, 200);
    const results = await Promise.all([release(), release()]);
    for (const r of results) assert.equal(r.status, 200, r.text);
    assert.equal(results.filter(r => r.json.idempotent === false).length, 1);
    assert.equal((await h.sessionRow(adminCookie)).sess.freshConfirmation, undefined);
    let receipt = (await post(API + 'research-only-grant/inspect', { grantId })).json;
    assert.equal(receipt.releasedCycles, 2); assert.equal(receipt.releases.length, 2);
    assert.equal(receipt.releases[1].administrator_id, ADMIN.id);
    assert.equal(receipt.releases[1].approval_reference, request.approvalReference);
    assert.deepEqual((await pool.query('SELECT to_jsonb(g) row FROM scout_research_only_grants g WHERE grant_id=$1', [grantId])).rows, immutable);
    const second = await claim('synthetic-http-cycle-two');
    await grants.finishResearchCycle(pool, second, { completed: false, result: {} });
    assert.equal((await release()).json.idempotent, true, 'a retry never releases cycle three');
    await assert.rejects(claim('synthetic-http-no-three'), { code: 'research_only_awaiting_cycle_release' });
    receipt = await grants.inspectResearchOnlyGrant(pool, grantId);
    assert.equal(receipt.status, 'awaiting_cycle_release'); assert.equal(receipt.cyclesConsumed, 2);
    const pending = (await post(API + 'research-cycle-release/preview', { ...request, approvalReference: 'synthetic-http-release-three' })).json;
    assert.equal((await post(API + 'research-only-grant/revoke', { grantId, approvalReference: 'synthetic-http-close' })).status, 200);
    assert.equal((await confirm('scout_research_cycle_release')).status, 200);
    const revoked = await release(adminCookie, undefined, { release: pending.release, releaseHash: pending.releaseHash });
    assert.equal(revoked.status, 409); assert.equal(revoked.json.code, 'research_only_grant_revoked');
    assert.equal((await grants.inspectResearchOnlyGrant(pool, grantId)).releases.length, 2);
  });
  assert.equal((await pool.query('SELECT count(*)::int n FROM scout_research_google_spend')).rows[0].n, 0);
  assert.equal((await pool.query('SELECT count(*)::int n FROM scout_unenriched')).rows[0].n, 0);
  assert.equal(process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE, 'true');
  assert.equal(process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED, 'true');
  assert.equal(process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED, 'false');
  assert.equal(deny.attempts(), 0);
});
