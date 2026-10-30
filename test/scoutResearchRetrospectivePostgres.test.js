'use strict';
// Capture only the fixture's loopback HTTP client before denying all global
// provider transports, matching the existing approval PostgreSQL harness.
const { startApprovalHarness, AO, OTHER_ADMIN } = require('./helpers/scoutResearchApprovalHarness');
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');

test('native admin export reads all 79 synthetic tickets without changing research or downstream records', { timeout: 600000 }, async t => {
  const h = await startApprovalHarness(); t.after(() => h.close());
  const { pool, http, adminCookie } = h;
  const API = '/api/v1/tenant-outreach/anchor-program/', path = API + 'research-only-grant/export';
  const post = (route, body, cookie = adminCookie, headers) => http(route, { method: 'POST', cookie, body, headers });
  const confirm = () => post('/api/v1/auth/fresh-confirmation', { purpose: 'scout_research_approval', password: h.fixturePassword });
  const missionPreview = (await post(API + 'research-mission/preview', { approvalRef: 'synthetic-export-mission' })).json;
  assert.equal((await confirm()).status, 200);
  const mission = await post(API + 'research-mission/install', { approvalRef: 'synthetic-export-mission',
    mission: missionPreview.mission, missionHash: missionPreview.missionHash });
  assert.equal(mission.status, 200, mission.text);
  const preview = await post(API + 'research-only-grant/preview', { approvalRef: 'synthetic-export-grant',
    researchMissionId: mission.json.missionId, expiresAt: new Date(Date.now() + 86400000).toISOString(), statement: 'Synthetic fixture authority only.' });
  assert.equal(preview.json.installable, true, preview.text);
  assert.equal((await confirm()).status, 200);
  const installed = await post(API + 'research-only-grant/install', { manifest: preview.json.manifest, manifestHash: preview.json.manifestHash });
  assert.equal(installed.status, 200, installed.text);
  const grantId = installed.json.grantId, fingerprint = preview.json.fingerprint;
  const { PLAN_VERSION } = require('../services/scoutQueryCheckpoints');
  const tickets = require('../services/scoutCandidateTickets');
  const raw = Array.from({ length: 79 }, (_, i) => ({ placeId: `synthetic-export-${i}`, name: `Synthetic Facility ${i}`,
    website: `https://fixture-${i}.example`, address: 'Manchester, NH 03101, USA', placeTypes: ['primary_school'] }));
  await pool.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates)
    VALUES('10',$1,'synthetic-export-query',$2,'fetched',$3::jsonb)`, [fingerprint, PLAN_VERSION, JSON.stringify(raw)]);
  await tickets.createCandidateTicketStore({ pool, tenantId: '10', fingerprint }).transferQuery('synthetic-export-query');
  const outcomes = [[19, 'handled', 'queued'], [34, 'deferred', 'evidence_budget_deferred'], [17, 'blocked', 'ownership_policy_observed'],
    [5, 'blocked', 'geography_unresolved'], [3, 'deferred', 'identity_incomplete'], [1, 'handled', 'policy_excluded']];
  let index = 0;
  for (const [n, state, reason] of outcomes) for (let j = 0; j < n; j++, index++)
    await pool.query("UPDATE scout_candidate_work SET state=$1,reason=$2 WHERE tenant_id='10' AND search_fingerprint=$3 AND candidate_key=$4",
      [state, reason, fingerprint, tickets.candidateKey(raw[index])]);
  const tables = ['scout_candidate_work', 'scout_candidate_memberships', 'scout_query_checkpoints', 'scout_research_only_grants',
    'scout_research_only_cycles', 'scout_research_google_spend', 'scout_research_candidates', 'anchor_email_recovery_control',
    'anchor_email_quarantines', 'acquisition_outbound_items', 'scout_unenriched', 'companies', 'prospects'];
  const snapshot = async () => Object.fromEntries(await Promise.all(tables.map(async table => [table,
    (await pool.query(`SELECT to_jsonb(x) AS row FROM ${table} x ORDER BY to_jsonb(x)::text`)).rows])));
  const before = await snapshot();
  const response = await post(path, { grantId });
  assert.equal(response.status, 200, response.text); assert.equal(response.json.records.length, 79);
  assert.equal(response.json.authoritativeRawExport, true);
  assert.equal(response.json.exportScope.observations, 79);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
  assert.match(response.headers.get('content-disposition'), /^attachment; filename="scout-research-cohort-[a-f0-9]{12}\.json"$/);
  for (const [n, , reason] of outcomes) assert.equal(response.json.records.filter(r => r.previous.reason === reason).length, n);
  assert.deepEqual(await snapshot(), before, 'export has no research, recovery, suppression or downstream writes');
  assert.equal(deny.attempts(), 0, 'no external provider transport');

  await t.test('same-origin, direct verified Anchor admin and explicit exact grant remain mandatory', async () => {
    assert.equal((await post(path, { grantId }, null)).status, 401);
    const ao = await h.loginAs(AO), other = await h.loginAs(OTHER_ADMIN);
    for (const cookie of [ao.cookie, other.cookie]) assert.equal((await post(path, { grantId }, cookie)).status, 403);
    const session = (await h.sessionRow(adminCookie)).sess;
    const impersonated = await h.seedSession('synthetic-export-impersonated', { ...session, impersonation: { active: true } });
    assert.equal((await post(path, { grantId }, impersonated)).status, 403);
    for (const headers of [{ origin: 'https://untrusted.example' }, { 'sec-fetch-site': 'cross-site' }])
      assert.equal((await post(path, { grantId }, adminCookie, headers)).status, 403);
    for (const body of [{}, { grantId, tenantId: '13' }, { grantId: 'bad' }, { grantId: '0'.repeat(64) }]) {
      const invalid = await post(path, body); assert.equal(invalid.status, 409, invalid.text);
      assert.equal(invalid.json.writesMayHaveOccurred, false); assert.equal(invalid.json.status, 'rejected');
    }
    assert.deepEqual(await snapshot(), before);
  });
  await t.test('revoked authority still permits a read-only historical export', async () => {
    const revoked = await post(API + 'research-only-grant/revoke', { grantId, approvalReference: 'synthetic-export-revoke' });
    assert.equal(revoked.status, 200, revoked.text);
    const afterRevocation = await snapshot();
    assert.equal((await post(path, { grantId })).json.records.length, 79);
    assert.deepEqual(await snapshot(), afterRevocation);
  });
});
