'use strict';
// SPEC-SCOUT-P0-008 Part B (#1072): explicit renewal of a revision-held
// research-only grant. Exact-revision pinning is unchanged; renewal is an
// administrator-approved successor with identical scope, only the remaining
// cycles, the same expiry and the same (tenant-wide) daily ceiling.
// Disposable PostgreSQL, synthetic Places only.
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { startResearchOnlyHarness, placesFetch, REVISION } = require('./helpers/scoutResearchOnlyFixture');

const quiet = { log() {}, warn() {}, error() {} };
const iso = ms => new Date(Math.floor((Date.now() + ms) / 1000) * 1000).toISOString();
const NEXT = 'd'.repeat(40);

test('SPEC-SCOUT-P0-008: revision-held research grants renew explicitly without widening authority', { timeout: 600000 }, async t => {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const { pool } = h;
  const grants = require('../services/scoutResearchOnlyGrant');
  const missions = require('../services/scoutResearchMission');
  const { hash } = require('../services/scoutBoundedAcquisitionBudget');
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  require('../utils/placesQueryCache').clearPlacesQueryCache();
  globalThis.fetch = placesFetch().fetchImpl;
  await pool.query(`INSERT INTO api_spend_policies(provider,daily_request_limit,monthly_request_limit,daily_usd_limit,monthly_usd_limit,paused,updated_by)
    VALUES('google_places',NULL,NULL,50,500,false,'operator')`);
  const cycle = id => runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: id, logger: quiet,
    runPreparationRefill: async () => assert.fail('preparation must never run') });
  const deploy = revision => { process.env.RAILWAY_GIT_COMMIT_SHA = revision; };
  t.after(() => deploy(REVISION));
  const crm = async () => (await pool.query(`SELECT (SELECT count(*) FROM scout_unenriched)::int q,
    (SELECT count(*) FROM prospects)::int p, (SELECT count(*) FROM companies)::int c`)).rows[0];
  const crmBefore = await crm();

  const mp = await missions.previewResearchMission(pool, { approvalRef: 'renew-mission' }, h.actor);
  const research = await missions.installResearchMission(pool, { approvalRef: 'renew-mission', mission: mp.mission, missionHash: mp.missionHash }, h.actor);
  const gp = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'renew-grant-1', expiresAt: iso(2 * 86400000),
    statement: 'Synthetic operator approval for research-only Scout discovery.', researchMissionId: research.missionId }, h.actor);
  const original = await grants.installResearchOnlyGrant(pool, { manifest: gp.manifest, manifestHash: gp.manifestHash }, h.actor);
  assert.equal((await cycle('renew-c1')).researchOnly.status, 'completed');
  const input = (overrides = {}) => ({ grantId: original.grantId, approvalRef: 'renew-grant-2',
    statement: 'Renew for the running revision with unchanged scope.', ...overrides });

  await t.test('no renewal while the grant matches the running revision', async () => {
    const preview = await grants.previewResearchOnlyGrantRenewal(pool, input(), h.actor);
    assert.ok(preview.blockers.includes('research_only_renewal_not_required'));
    assert.equal(preview.installable, false);
  });

  await t.test('legacy semantics: a deployment holds the commit-pinned grant exactly as before', async () => {
    deploy(NEXT);
    const held = await cycle('renew-held');
    assert.equal(held.halted, 'research_only_revision_mismatch');
    const inspected = await grants.inspectResearchOnlyGrant(pool, original.grantId);
    assert.equal(inspected.executionStatus, 'revision_mismatch');
    assert.equal(inspected.hold.renewable, true);
    assert.equal(inspected.expectedRevision, REVISION, 'nothing is repinned automatically');
  });

  let preview;
  await t.test('preview: same scope, remaining cycles, same expiry and budget, new pin only', async () => {
    preview = await grants.previewResearchOnlyGrantRenewal(pool, input(), h.actor);
    assert.deepEqual(preview.blockers, [], JSON.stringify(preview.renewal));
    assert.equal(preview.installable, true);
    assert.deepEqual(preview.renewal.changedKeys, []);
    assert.equal(preview.renewal.cyclesConsumed, 1);
    assert.equal(preview.renewal.cyclesCarried, 7);
    assert.equal(preview.manifest.maxCycles, 7, 'never a fresh eight-cycle ceiling');
    assert.equal(preview.manifest.expiresAt, gp.manifest.expiresAt, 'expiry is never extended');
    assert.equal(preview.manifest.dailyGoogleUsdMicros, gp.manifest.dailyGoogleUsdMicros);
    assert.deepEqual(preview.manifest.permissions, gp.manifest.permissions);
    assert.deepEqual(preview.manifest.providers, gp.manifest.providers);
    assert.equal(preview.manifest.expectedRevision, NEXT);
    assert.equal(preview.manifest.version, 2, 'the successor is an ordinary commit-pinned grant');
    // Read-only: the predecessor is still open.
    assert.equal((await pool.query('SELECT revoked_at FROM scout_research_only_grants WHERE grant_id=$1', [original.grantId])).rows[0].revoked_at, null);
  });

  await t.test('tampered successors fail closed: budget, cycles, expiry, permissions, tenant, hash', async () => {
    const tamper = mutate => { const m = structuredClone(preview.manifest); mutate(m); return { grantId: original.grantId, manifest: m, manifestHash: hash(m) }; };
    await assert.rejects(grants.renewResearchOnlyGrant(pool, { grantId: original.grantId, manifest: preview.manifest, manifestHash: '0'.repeat(64) }, h.actor),
      { code: 'research_only_exact_confirmation_required' });
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.dailyGoogleUsdMicros += 1; }), h.actor),
      { code: 'research_only_manifest_invalid' }, 'spending above the ceiling is never renewable');
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.dailyGoogleUsdMicros -= 1; }), h.actor),
      { code: 'research_only_renewal_scope_changed' }, 'any budget change needs a new approval, not a renewal');
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.maxCycles = 8; }), h.actor), { code: 'research_only_renewal_scope_changed' });
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.expiresAt = iso(5 * 86400000); }), h.actor),
      { code: 'research_only_renewal_scope_changed' });
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.permissions.enrichment = true; }), h.actor),
      { code: 'research_only_manifest_invalid' }, 'research-only can never enable enrichment');
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.permissions.outbound = true; }), h.actor),
      { code: 'research_only_manifest_invalid' });
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.tenantId = '13'; }), h.actor),
      { code: 'research_only_manifest_invalid' }, 'tenant mismatch fails closed');
    await assert.rejects(grants.renewResearchOnlyGrant(pool, tamper(m => { m.maxCycles = 9; }), h.actor), { code: 'research_only_manifest_invalid' });
    await assert.rejects(grants.renewResearchOnlyGrant(pool, { grantId: original.grantId, manifest: preview.manifest, manifestHash: preview.manifestHash },
      { ...h.actor, sessionId: 'another-session' }), { code: 'research_only_operator_invalid' });
    assert.equal((await pool.query("SELECT count(*)::int n FROM scout_research_only_grants WHERE revoked_at IS NULL")).rows[0].n, 1,
      'every refusal left the predecessor open and installed nothing');
  });

  await t.test('email-recovery maintenance and quarantine remain preconditions', async () => {
    process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false';
    try {
      await assert.rejects(grants.renewResearchOnlyGrant(pool, { grantId: original.grantId, manifest: preview.manifest,
        manifestHash: preview.manifestHash }, h.actor), { code: 'research_only_closed_environment_required' });
    } finally { process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'true'; }
    const blocked = await grants.previewResearchOnlyGrantRenewal(pool, input(), h.actor);
    assert.deepEqual(blocked.blockers, []);
  });

  let successor;
  await t.test('renew: one transaction revokes the predecessor and installs the successor; research resumes', async () => {
    successor = await grants.renewResearchOnlyGrant(pool, { grantId: original.grantId, manifest: preview.manifest,
      manifestHash: preview.manifestHash }, h.actor);
    assert.equal(successor.renewed, true);
    const rows = (await pool.query(`SELECT grant_id,revoked_at IS NOT NULL AS revoked,revoke_approval_ref,revoked_by,max_cycles
      FROM scout_research_only_grants ORDER BY installed_at`)).rows;
    assert.deepEqual(rows.map(r => [r.revoked, r.max_cycles]), [[true, 8], [false, 7]]);
    assert.equal(rows[0].revoke_approval_ref, 'renew-grant-2', 'the audit trail links predecessor to successor');
    assert.match(rows[0].revoked_by, /^admin:71:renewal$/);
    assert.equal((await grants.renewResearchOnlyGrant(pool, { grantId: original.grantId, manifest: preview.manifest,
      manifestHash: preview.manifestHash }, h.actor)).idempotent, true);
    const resumed = await cycle('renew-c2');
    assert.equal(resumed.researchOnly.status, 'completed', JSON.stringify(resumed.researchOnly));
    assert.equal(resumed.researchOnly.grantId, successor.grantId);
  });

  await t.test('revoked and expired grants are never renewable; expiry stays expiry', async () => {
    const again = await grants.previewResearchOnlyGrantRenewal(pool, input({ approvalRef: 'renew-grant-3' }), h.actor);
    assert.ok(again.blockers.includes('research_only_grant_revoked'));
    deploy('e'.repeat(40));
    const chained = await grants.previewResearchOnlyGrantRenewal(pool, input({ grantId: successor.grantId, approvalRef: 'renew-grant-4' }),
      h.actor, { env: { ...process.env } });
    assert.deepEqual(chained.blockers, [], 'a still-valid successor can itself be renewed after another deployment');
    // A real short-lived grant: renewal can never revive an expired one.
    deploy(REVISION);
    await grants.revokeResearchOnlyGrant(pool, { grantId: successor.grantId, approvalReference: 'renew-close-successor' }, h.actor);
    const sp = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'renew-short', expiresAt: iso(4000),
      statement: 'Synthetic short-lived approval.', researchMissionId: research.missionId }, h.actor);
    const short = await grants.installResearchOnlyGrant(pool, { manifest: sp.manifest, manifestHash: sp.manifestHash }, h.actor);
    await new Promise(resolve => setTimeout(resolve, 5000));
    deploy(NEXT);
    const expired = await grants.previewResearchOnlyGrantRenewal(pool, input({ grantId: short.grantId, approvalRef: 'renew-grant-6' }), h.actor);
    assert.ok(expired.blockers.includes('research_only_grant_expired'));
    assert.equal(expired.installable, false);
    assert.equal(expired.manifest, undefined, 'no successor is derived for an expired grant');
  });

  await t.test('a live scope change (new outbound program) blocks renewal; a new approval is required', async () => {
    deploy(REVISION);
    const open = (await pool.query("SELECT grant_id FROM scout_research_only_grants WHERE revoked_at IS NULL")).rows;
    for (const row of open) await grants.revokeResearchOnlyGrant(pool, { grantId: row.grant_id, approvalReference: `renew-close-${row.grant_id.slice(0, 8)}` }, h.actor);
    const fp = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'renew-scope-1', expiresAt: iso(86400000),
      statement: 'Synthetic approval before a scope change.', researchMissionId: research.missionId }, h.actor);
    const scoped = await grants.installResearchOnlyGrant(pool, { manifest: fp.manifest, manifestHash: fp.manifestHash }, h.actor);
    await h.approveProgram();
    deploy(NEXT);
    const blocked = await grants.previewResearchOnlyGrantRenewal(pool, input({ grantId: scoped.grantId, approvalRef: 'renew-scope-2' }), h.actor);
    assert.ok(blocked.blockers.includes('research_only_renewal_scope_changed'), JSON.stringify(blocked.blockers));
    assert.ok(blocked.renewal.changedKeys.includes('programId'));
    assert.equal(blocked.installable, false);
    await assert.rejects(grants.renewResearchOnlyGrant(pool, { grantId: scoped.grantId, manifest: blocked.manifest,
      manifestHash: blocked.manifestHash }, h.actor), { code: 'research_only_renewal_scope_changed' });
    assert.equal((await pool.query('SELECT revoked_at FROM scout_research_only_grants WHERE grant_id=$1', [scoped.grantId])).rows[0].revoked_at, null);
    deploy(REVISION);
  });

  await t.test('boundaries: no enrichment, CRM or queue writes, no real transport', async () => {
    assert.deepEqual(await crm(), crmBefore);
    assert.equal(deny.attempts(), 0);
  });
});
