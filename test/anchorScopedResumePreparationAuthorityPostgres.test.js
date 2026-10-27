'use strict';
// Native canonical pipeline + disposable PostgreSQL. All records are synthetic;
// the provider-deny preload must be active for this regression suite.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const deny = require('./helpers/denyExternalProviders.cjs');
const amo = require('../packages/acquisition-mission');
const { hash, clock, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const scope = require('../services/anchorScopedResume');
const control = require('../services/anchorScopedResumeControl');
const recovery = require('../services/anchorEmailRecovery');
const integration = require('../services/anchorEmailRecoveryIntegration');
const persistence = require('../services/acquisitionMissionPersistence');
const { prepareDrafts, actorFromRequest } = require('../services/anchorCrmPreparationAction');
const { prepareExistingCrm } = require('../services/anchorScopedResumeCrmPreparation');
const { GovernedOutboundStore } = require('../services/governedOutboundStore');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');

function harness() {
  const filename = path.join(__dirname, 'anchorScopedResumeCrmPreparationPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,install,extraAdmission,afterConsumption};', {
    module, exports: module.exports, __dirname, process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}
const h = harness();
const revision = 'a'.repeat(40), saturday = '2026-10-24T16:00:00.000Z';
const authority = () => require('../services/anchorCrmPreparationAuthority');
const actor = f => ({ id: f.operator.id, role: 'admin', sessionId: f.sessionId });
const options = f => ({ env: f.env, now: () => f.now, sessionId: f.sessionId });
const input = f => ({ revision, manifestHash: hash(f.manifest), sourceMissionHash: hash(f.source) });
function noSchemaWrites(pool) {
  const query = (db, q, values) => {
    const sql = typeof q === 'string' ? q : q.text;
    assert.doesNotMatch(sql, /\b(?:CREATE|ALTER|DROP|TRUNCATE)\b/i, 'authorized draft action must not bootstrap schema');
    return db.query(q, values);
  };
  return { query: (q, values) => query(pool, q, values), async connect() {
    const db = await pool.connect();
    return { query: (q, values) => query(db, q, values), release: db.release.bind(db) };
  } };
}
const run = (pool, f) => prepareDrafts(noSchemaWrites(pool), input(f), actor(f), options(f));
const install = (pool, f) => control.installScopedResume(pool,
  { manifest: f.manifest, approvalReference: f.manifest.approvalReference }, actor(f), options(f));
const revoke = (pool, f) => control.revokeScopedResume(pool,
  { manifestId: f.manifest.id, approvalReference: 'synthetic-explicit-revocation' }, actor(f), options(f));
const consume = (pool, f, operationId, overrides = {}) => control.consumeScopedOperation(pool, {
  manifest: f.manifest, capability: 'preparation', operation: 'prepare_existing_crm', operationId,
  identities: f.manifest.admissions, ...overrides,
}, options(f));
async function rows(pool, table) {
  return (await pool.query(`SELECT row_to_json(t) AS row FROM "${table}" t`)).rows.map(r => JSON.stringify(r.row)).sort();
}
async function snapshot(pool) {
  const names = (await pool.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).rows;
  return Object.fromEntries(await Promise.all(names.map(async ({ tablename }) => [tablename, await rows(pool, tablename)])));
}
async function protectedState(pool, f) {
  const tables = ['acquisition_outbound_programs', 'acquisition_outbound_envelopes', 'acquisition_outbound_items',
    'acquisition_outbound_lifecycle', 'acquisition_mission_outbound_executions', 'anchor_email_recovery_reservations',
    'anchor_email_quarantines', 'prospects', 'companies', 'users', 'clients', 'tenant_mailbox_integrations'];
  const result = Object.fromEntries(await Promise.all(tables.map(async table => [table, await rows(pool, table)])));
  result.source = (await pool.query('SELECT row_to_json(m) AS row FROM acquisition_missions m WHERE id=$1', [f.source.id])).rows[0].row;
  return result;
}
async function effects(pool) {
  return (await pool.query(`SELECT
    (SELECT count(*)::int FROM anchor_scoped_resume_consumption) AS consumed,
    (SELECT count(*)::int FROM acquisition_missions WHERE id LIKE 'mission_crm_%') AS children,
    (SELECT count(*)::int FROM anchor_email_recovery_reservations) AS reservations,
    (SELECT count(*)::int FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval'
      AND payload->'payload'->>'decisionKind'='execution_approval') AS execution_approvals`)).rows[0];
}
function pinManifest(f, { consumedOperations = 0, consumedItems = 0 } = {}) {
  f.manifest.version = 2;
  f.manifest.policyHash = f.program.policy_hash;
  f.manifest.validFrom = new Date(+f.now - 3 * 3600000).toISOString();
  f.manifest.expiresAt = new Date(+f.now + 4 * 3600000).toISOString();
  f.manifest.scopes.preparation = { enabled: true, approvalReference: 'synthetic-explicit-preparation-only-continuation',
    dailyCap: 1, totalCap: consumedOperations + 1, dailyItemCap: f.manifest.admissions.length,
    totalItemCap: consumedItems + f.manifest.admissions.length, operations: ['prepare_existing_crm'] };
  f.manifest.preparationAuthority = { kind: 'existing_crm_continuation',
    originalAuthorizerId: String(f.program.authorized_by), approverAccountClientId: 10,
    approverSessionHash: require('../services/anchorCrmPreparationIdentity').sessionBindingHash(f.sessionId),
    programBindingHash: authority().programBindingHash(f.program), sourceMissionHash: hash(f.source),
    consumedOperations, consumedItems };
  f.env = { ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true',
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true', RAILWAY_GIT_COMMIT_SHA: revision };
  Object.assign(process.env, f.env);
  delete process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;
}
async function setup(pool, t) {
  t.mock.timers.setTime(+new Date(saturday));
  const f = await h.setup(pool);
  f.now = new Date(saturday);
  f.sessionId = 'synthetic-preparation-session';
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS password_change_required boolean DEFAULT false;
    ALTER TABLE clients ADD COLUMN IF NOT EXISTS operator_switchable boolean DEFAULT true;
    CREATE TABLE IF NOT EXISTS "session" (sid text PRIMARY KEY, sess json NOT NULL, expire timestamp NOT NULL);`);
  await pool.query('UPDATE users SET id=$1 WHERE id=$2', ['71', f.operator.id]);
  f.operator.id = '71'; f.manifest.operatorId = '71';
  await pool.query('INSERT INTO "session" (sid,sess,expire) VALUES ($1,$2,$3)', [f.sessionId,
    { authenticated: true, active_client_id: 10, user: { id: '71', role: 'admin', client_id: 10,
      active: true, email_verified: true, password_change_required: false } }, '2099-01-01']);
  await pool.query('ALTER TABLE acquisition_outbound_programs ADD COLUMN last_tick_at timestamptz, ADD COLUMN last_error text');
  const policy = { ...f.program.policy, startsAt: '2026-10-23T13:00:00.000Z', expiresAt: '2026-10-23T21:00:00.000Z' };
  await pool.query('UPDATE acquisition_outbound_programs SET policy=$1,policy_hash=$2 WHERE id=$3', [policy, hash(policy), f.program.id]);
  f.program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [f.program.id])).rows[0];
  await pool.query('UPDATE acquisition_outbound_inbox_health SET last_success_at=$1', [f.now]);
  pinManifest(f);
  return f;
}
async function assertNoWrites(pool, action, expected) {
  const before = await snapshot(pool);
  await assert.rejects(action, expected);
  assert.deepEqual(await snapshot(pool), before);
}

test('separate finite preparation authority cannot renew expired outbound authority', { timeout: 240000 }, async t => {
  const pg = await startDisposablePostgres('anchor-prep-authority-');
  const pool = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 14 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await closePostgresFixture(pool, pg); });
  const keys = ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID', 'ANCHOR_EMAIL_RECOVERY_MAINTENANCE',
    'ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'ANCHOR_RESEARCH_PROVIDERS_PAUSED', 'RAILWAY_GIT_COMMIT_SHA'];
  const old = Object.fromEntries(keys.map(k => [k, process.env[k]]));
  t.after(() => keys.forEach(k => old[k] === undefined ? delete process.env[k] : process.env[k] = old[k]));
  t.mock.timers.enable({ apis: ['Date'], now: +new Date(saturday) });

  await t.test('Saturday continuation creates canonical READY without changing Friday authority or sending', async () => {
    const f = await setup(pool, t), before = await protectedState(pool, f);
    assert.equal(new Date(f.now).getUTCDay(), 6);
    assert.ok(windowReason(f.program.policy, f.now, true));
    assert.throws(() => recovery.validateLivePolicy(f.program, f.manifest, f.now));
    const installed = await install(pool, f);
    assert.equal(installed.installed, true); assert.equal(installed.providerAuthorized, false);
    assert.equal((await recovery.loadRecoveryState(pool, { env: f.env, now: f.now, sessionId: f.sessionId })).mode, 'maintenance');
    const result = await run(pool, f);
    assert.equal(result.status, 'ready_for_review'); assert.equal(result.sent, 0); assert.equal(result.providerCalls, 0);
    assert.equal(result.executionApproved, false); assert.equal(result.envelopeAuthorized, false); assert.equal(result.retryAllowed, false);
    const saved = await persistence.loadMissionSnapshot(result.missionId, '10', pool, { skipEnsure: true });
    assert.equal(saved.mission.stage, 'ready'); assert.equal(saved.mission.orchestrationMissionId, f.source.id);
    assert.equal(saved.mission.pendingOperatorDecision.kind, 'execution_approval');
    assert.equal(saved.mission.crmPreparation.preparationOnly, true);
    assert.equal(saved.mission.crmPreparation.providerReadiness, 'not_checked');
    for (const [specialist, kind] of [['scout', 'discovery'], ['max', 'prioritization'], ['max', 'acquisition_approach'], ['paige', 'variants'], ['emmett', 'capacity']])
      assert.ok(saved.contributions.some(c => c.specialist === specialist && c.kind === kind), `${specialist}/${kind}`);
    assert.equal(require('../packages/acquisition-mission/ExecutionApproval').findExecutionApprovals(saved.contributions).length, 0);
    const capacity = require('../packages/acquisition-mission/ContributionSupersession').unwrapSpecialistPayload(amo.findEmmettCapacity(saved.contributions, saved.mission));
    assert.equal(capacity.preparationEvidence.authentication.spf.state, 'UNKNOWN');
    assert.equal(capacity.preparationEvidence.blacklist.listed, null);
    assert.doesNotMatch(JSON.stringify(saved), /No blacklist concerns|Healthy\. Safe\. Send\./);
    assert.deepEqual(await protectedState(pool, f), before);
    assert.deepEqual(await effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
    const after = await snapshot(pool);
    await assert.rejects(run(pool, f), e => e.status === 'already_consumed' && !e.retryAllowed);
    assert.deepEqual(await snapshot(pool), after);
    assert.equal(deny.attempts(), 0);
    assert.throws(() => require('../utils/anchorResearchProviderPause').assertResearchProviderAllowed('synthetic-provider', '10'), { code: 'anchor_research_provider_paused' });
  });

  await t.test('unauthenticated, non-admin and tenant-mismatched actors have no writes', async () => {
    for (const kind of ['absent', 'manager', 'unknown', 'inactive', 'wrong_tenant', 'session_tenant', 'impersonation']) {
      const f = await setup(pool, t); await install(pool, f);
      let a = actor(f);
      if (kind === 'absent') a = null;
      if (kind === 'manager') a.role = 'manager';
      if (kind === 'unknown') a.id = 'unrelated-admin';
      if (kind === 'inactive') await pool.query('UPDATE users SET active=false WHERE id=$1', [f.operator.id]);
      if (kind === 'wrong_tenant') await pool.query('UPDATE users SET client_id=11 WHERE id=$1', [f.operator.id]);
      if (['session_tenant', 'impersonation'].includes(kind)) {
        const before = await snapshot(pool);
        const session = { user: { id: f.operator.id, role: 'admin', client_id: 10, active: true }, active_client_id: kind === 'session_tenant' ? 11 : 10,
          ...(kind === 'impersonation' ? { impersonation: { active: true } } : {}) };
        assert.throws(() => actorFromRequest({ session }));
        assert.deepEqual(await snapshot(pool), before);
      } else await assertNoWrites(pool, () => prepareDrafts(pool, input(f), a, options(f)));
      assert.deepEqual(await effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 }, kind);
    }
  });

  await t.test('no flags, wider scopes, non-admin installation and altered baselines cannot activate', async () => {
    for (const kind of ['maintenance', 'outbound', 'research', 'activation', 'legacy_activation', 'manager', 'freeze', 'refill', 'approve', 'email', 'baseline', 'items', 'too_long', 'before_expiry']) {
      const f = await setup(pool, t);
      if (kind === 'maintenance') f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false';
      if (kind === 'outbound') f.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
      if (kind === 'research') f.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'false';
      if (kind === 'activation') delete f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
      if (kind === 'legacy_activation') f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = 'legacy-grant';
      if (kind === 'manager') { f.manifest.operatorRole = 'manager'; await pool.query("UPDATE users SET role='manager' WHERE id=$1", [f.operator.id]); }
      if (['freeze', 'refill', 'approve'].includes(kind)) f.manifest.scopes.preparation.operations.push(kind === 'approve' ? 'approve_canonical_envelope' : kind);
      if (kind === 'email') f.manifest.scopes.email = require('./helpers/anchorScopedResumeFixture').scopedResumeFixture().manifest.scopes.email;
      if (kind === 'baseline') { f.manifest.preparationAuthority.consumedOperations = 1; f.manifest.scopes.preparation.totalCap = 2; }
      if (kind === 'items') { f.manifest.preparationAuthority.consumedItems = 1; f.manifest.scopes.preparation.totalItemCap = 2; }
      if (kind === 'too_long') f.manifest.expiresAt = '2026-10-26T16:00:00.000Z';
      if (kind === 'before_expiry') f.manifest.validFrom = '2026-10-23T20:00:00.000Z';
      await assertNoWrites(pool, () => kind === 'manager'
        ? control.installScopedResume(pool, { manifest: f.manifest, approvalReference: f.manifest.approvalReference }, { id: f.operator.id, role: 'manager' }, options(f))
        : install(pool, f));
    }
  });

  await t.test('changed full program or source pins, finite deadline and deactivated guards reject before consumption', async () => {
    for (const kind of ['authorized_at', 'last_tick_at', 'last_error', 'policy', 'scope', 'source', 'program_mode', 'program_tenant', 'deadline', 'not_started', 'revision', 'manifest', 'maintenance', 'outbound', 'research']) {
      const f = await setup(pool, t); await install(pool, f);
      if (kind === 'authorized_at') await pool.query("UPDATE acquisition_outbound_programs SET authorized_at=authorized_at+interval '1 second' WHERE id=$1", [f.program.id]);
      if (kind === 'last_tick_at') await pool.query('UPDATE acquisition_outbound_programs SET last_tick_at=$1 WHERE id=$2', [f.now, f.program.id]);
      if (kind === 'last_error') await pool.query("UPDATE acquisition_outbound_programs SET last_error='synthetic tick observation' WHERE id=$1", [f.program.id]);
      if (kind === 'policy') await pool.query("UPDATE acquisition_outbound_programs SET policy=jsonb_set(policy,'{dailyCap}','4') WHERE id=$1", [f.program.id]);
      if (kind === 'scope') await pool.query('UPDATE acquisition_outbound_programs SET scope_hash=$1 WHERE id=$2', ['f'.repeat(64), f.program.id]);
      if (kind === 'source') await pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{nonScopeAuditNote}','\"changed\"') WHERE id=$1", [f.source.id]);
      if (kind === 'program_mode') await pool.query("UPDATE acquisition_outbound_programs SET mode='shadow' WHERE id=$1", [f.program.id]);
      if (kind === 'program_tenant') await pool.query("UPDATE acquisition_outbound_programs SET tenant_id='11' WHERE id=$1", [f.program.id]);
      if (kind === 'deadline') f.now = new Date(f.manifest.expiresAt);
      if (kind === 'not_started') f.now = new Date(+new Date(f.manifest.validFrom) - 1);
      if (kind === 'revision') f.env.RAILWAY_GIT_COMMIT_SHA = 'b'.repeat(40);
      if (kind === 'manifest') f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'replacement';
      if (kind === 'maintenance') f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false';
      if (kind === 'outbound') f.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
      if (kind === 'research') f.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'false';
      await assertNoWrites(pool, () => run(pool, f));
      assert.equal((await effects(pool)).consumed, 0, kind);
    }
  });

  await t.test('true and NULL review/DNC/synthetic states, suppression and quarantined aliases remain closed', async () => {
    const mutations = [
      ['review_true', 'UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1'],
      ['review_null', 'UPDATE prospects SET ao_outreach_review_required=NULL WHERE id=$1'],
      ['dnc_true', 'UPDATE prospects SET do_not_contact=true WHERE id=$1'],
      ['dnc_null', 'UPDATE prospects SET do_not_contact=NULL WHERE id=$1'],
      ['synthetic_true', 'UPDATE prospects SET is_synthetic=true WHERE id=$1'],
      ['synthetic_null', 'UPDATE prospects SET is_synthetic=NULL WHERE id=$1'],
    ];
    for (const [kind, sql] of mutations) {
      const f = await setup(pool, t); await install(pool, f); await pool.query(sql, [f.prospect.id]);
      await assertNoWrites(pool, () => run(pool, f));
      assert.equal((await effects(pool)).consumed, 0, kind);
    }
    for (const kind of ['lifecycle', 'company_alias', 'quarantine_identity']) {
      const f = await setup(pool, t); await install(pool, f);
      if (kind === 'lifecycle') await pool.query('INSERT INTO acquisition_outbound_lifecycle(tenant_id,email,company_id,suppressed,state) VALUES($1,$2,$3,true,$4)', ['10', f.prospect.email, f.prospect.company_id, 'human_owned']);
      if (kind === 'company_alias') await pool.query('UPDATE companies SET name=$1 WHERE id=$2', [f.quarantine.company_name_key, f.prospect.company_id]);
      if (kind === 'quarantine_identity') await pool.query('UPDATE prospects SET company_id=$1,email=$2 WHERE id=$3', [f.quarantine.company_id, f.quarantine.recipient_email, f.prospect.id]);
      await assertNoWrites(pool, () => run(pool, f));
    }
  });

  await t.test('explicitly repinned quarantine identities and company aliases still cannot be installed', async () => {
    for (const kind of ['exact_company', 'name_alias', 'domain_alias']) {
      const f = await setup(pool, t);
      if (kind === 'exact_company') {
        await pool.query('UPDATE prospects SET company_id=$1,email=$2 WHERE id=$3', [f.quarantine.company_id, f.quarantine.recipient_email, f.prospect.id]);
        f.manifest.admissions[0].companyId = f.quarantine.company_id;
        f.manifest.admissions[0].recipientEmail = f.quarantine.recipient_email;
      }
      if (kind === 'name_alias') await pool.query('UPDATE companies SET name=$1 WHERE id=$2', [f.quarantine.company_name_key, f.prospect.company_id]);
      if (kind === 'domain_alias') await pool.query('UPDATE companies SET domain=$1,website=$2 WHERE id=$3', [f.quarantine.company_domain, `https://${f.quarantine.company_domain}`, f.prospect.company_id]);
      const bound = await scope.readScopedIdentity(pool, { tenantId: '10', ...f.manifest.admissions[0] }, { lock: false });
      Object.assign(f.manifest.admissions[0], { identityFingerprint: bound.identityFingerprint, companyNameKey: bound.companyNameKey, companyDomain: bound.companyDomain });
      await assertNoWrites(pool, () => install(pool, f));
      assert.deepEqual(await effects(pool), { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 });
    }
  });

  await t.test('concurrent requests permanently consume exactly one operation and canonical child', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const results = await Promise.allSettled([run(pool, f), run(pool, f)]);
    assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
    assert.equal(results.find(r => r.status === 'rejected').reason.status, 'already_consumed');
    assert.deepEqual(await effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
  });

  await t.test('changed authority or failure after consumption retains the claim without automatic replay', async () => {
    for (const kind of ['operator', 'program', 'source', 'review', 'revoke', 'schema', 'deadline', 'guards']) {
      const f = await setup(pool, t); await install(pool, f);
      const wrapped = h.afterConsumption(pool, async () => {
        if (kind === 'operator') await pool.query('UPDATE users SET active=false WHERE id=$1', [f.operator.id]);
        if (kind === 'program') await pool.query("UPDATE acquisition_outbound_programs SET authorized_at=authorized_at+interval '1 second' WHERE id=$1", [f.program.id]);
        if (kind === 'source') await pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{nonScopeAuditNote}','\"late change\"') WHERE id=$1", [f.source.id]);
        if (kind === 'review') await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1', [f.prospect.id]);
        if (kind === 'revoke') await revoke(pool, f);
        if (kind === 'schema') await pool.query('DROP TABLE acquisition_mission_observations');
        if (kind === 'deadline') f.now = new Date(f.manifest.expiresAt);
        if (kind === 'guards') f.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'false';
      });
      await assert.rejects(run(wrapped, f), e => e.status === 'blocked_review_required' && e.writesMayHaveOccurred && !e.retryAllowed);
      assert.deepEqual(await effects(pool), { consumed: 1, children: 0, reservations: 0, execution_approvals: 0 }, kind);
      if (kind === 'operator') await pool.query('UPDATE users SET active=true WHERE id=$1', [f.operator.id]);
      if (kind === 'review') await pool.query('UPDATE prospects SET ao_outreach_review_required=false WHERE id=$1', [f.prospect.id]);
      if (kind === 'program') await pool.query('UPDATE acquisition_outbound_programs SET authorized_at=$1 WHERE id=$2', [f.program.authorized_at, f.program.id]);
      if (kind === 'source') await pool.query('UPDATE acquisition_missions SET payload=$1 WHERE id=$2', [f.source, f.source.id]);
      if (kind === 'revoke') {
        f.manifest = { ...f.manifest, id: 'synthetic-replacement-after-consumed-revocation' };
        pinManifest(f, { consumedOperations: 1, consumedItems: 1 });
        await assertNoWrites(pool, () => install(pool, f), { code: 'anchor_crm_preparation_identity_consumed' });
        assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest, null);
        continue;
      }
      if (kind === 'guards') f.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
      if (kind === 'deadline') f.now = new Date(saturday);
      const before = await snapshot(pool);
      await assert.rejects(run(pool, f), e => e.status === 'already_consumed' && !e.retryAllowed);
      assert.deepEqual(await snapshot(pool), before);
    }
  });

  await t.test('two-identity grant refuses partial batches and charges every admitted item', async () => {
    const f = await setup(pool, t);
    f.manifest.admissions.push(await h.extraAdmission(pool, f, 8));
    pinManifest(f); await install(pool, f);
    await assertNoWrites(pool, () => consume(pool, f, 'synthetic-partial-batch', { identities: f.manifest.admissions.slice(0, 1) }));
    await assertNoWrites(pool, () => consume(pool, f, 'synthetic-forged-item-count', { itemCount: 1 }));
    await consume(pool, f, 'synthetic-whole-two-identity-batch');
    assert.deepEqual((await pool.query('SELECT item_count,jsonb_array_length(identities) AS identities FROM anchor_scoped_resume_consumption')).rows,
      [{ item_count: 2, identities: 2 }]);
    await assertNoWrites(pool, () => consume(pool, f, 'synthetic-second-whole-batch'));
    assert.deepEqual(await effects(pool), { consumed: 1, children: 0, reservations: 0, execution_approvals: 0 });
  });

  await t.test('failed consumed identities cannot replay through changed batches or newly reviewed aliases', async () => {
    for (const kind of ['expanded_batch', 'prospect', 'company', 'email', 'name', 'domain']) {
      const f = await setup(pool, t); await install(pool, f);
      const originalAdmission = { ...f.manifest.admissions[0] };
      const wrapped = h.afterConsumption(pool, () => pool.query('UPDATE users SET active=false WHERE id=$1', [f.operator.id]));
      await assert.rejects(run(wrapped, f), e => e.status === 'blocked_review_required' && !e.retryAllowed);
      await pool.query('UPDATE users SET active=true WHERE id=$1', [f.operator.id]);
      await revoke(pool, f);
      const consumed = await rows(pool, 'anchor_scoped_resume_consumption');
      let candidate = await h.extraAdmission(pool, f, 12);
      if (kind === 'prospect') {
        await pool.query('UPDATE prospects SET company_id=$1,email=$2 WHERE id=$3', [candidate.companyId, candidate.recipientEmail, originalAdmission.prospectId]);
        candidate.prospectId = originalAdmission.prospectId;
      }
      if (kind === 'company') {
        await pool.query('UPDATE prospects SET company_id=$1 WHERE id=$2', [originalAdmission.companyId, candidate.prospectId]);
        candidate.companyId = originalAdmission.companyId;
      }
      if (kind === 'email') {
        await pool.query('UPDATE prospects SET email=$1 WHERE id=$2', [originalAdmission.recipientEmail, candidate.prospectId]);
        candidate.recipientEmail = originalAdmission.recipientEmail;
      }
      if (kind === 'name') await pool.query('UPDATE companies SET name=$1 WHERE id=$2', [originalAdmission.companyNameKey, candidate.companyId]);
      if (kind === 'domain') await pool.query('UPDATE companies SET domain=$1,website=$2 WHERE id=$3', [originalAdmission.companyDomain, `https://${originalAdmission.companyDomain}`, candidate.companyId]);
      const bound = await scope.readScopedIdentity(pool, { tenantId: '10', ...candidate }, { lock: false });
      Object.assign(candidate, { companyNameKey: bound.companyNameKey, companyDomain: bound.companyDomain, identityFingerprint: bound.identityFingerprint });
      f.manifest = { ...f.manifest, id: `synthetic-regrant-overlap-${kind}`, admissions: kind === 'expanded_batch' ? [originalAdmission, candidate] : [candidate] };
      pinManifest(f, { consumedOperations: 1, consumedItems: 1 });
      await assertNoWrites(pool, () => install(pool, f), { code: 'anchor_crm_preparation_identity_consumed' });
      assert.deepEqual(await rows(pool, 'anchor_scoped_resume_consumption'), consumed);
      assert.deepEqual(await effects(pool), { consumed: 1, children: 0, reservations: 0, execution_approvals: 0 });
    }
  });

  await t.test('a subset of a consumed two-identity batch cannot be regranted', async () => {
    const f = await setup(pool, t); f.manifest.admissions.push(await h.extraAdmission(pool, f, 13));
    pinManifest(f); await install(pool, f); await consume(pool, f, 'synthetic-consumed-two-item-failure');
    await revoke(pool, f);
    f.manifest = { ...f.manifest, id: 'synthetic-subset-regrant', admissions: f.manifest.admissions.slice(0, 1) };
    pinManifest(f, { consumedOperations: 1, consumedItems: 2 });
    await assertNoWrites(pool, () => install(pool, f), { code: 'anchor_crm_preparation_identity_consumed' });
    assert.deepEqual((await pool.query('SELECT item_count FROM anchor_scoped_resume_consumption')).rows, [{ item_count: 2 }]);
  });

  await t.test('an actual original v1 preparation remains charged to the v2 continuation', async () => {
    t.mock.timers.setTime(+new Date('2026-10-19T16:00:00.000Z'));
    const f = await h.setup(pool); keys.forEach(k => delete process.env[k]);
    process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    await h.install(pool, f);
    await prepareExistingCrm(pool, f.input, { env: f.env, now: f.now, sessionId: f.sessionId });
    const historical = await rows(pool, 'anchor_scoped_resume_consumption');
    await revoke(pool, f);
    f.now = new Date(saturday);
  f.sessionId = 'synthetic-preparation-session';
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS password_change_required boolean DEFAULT false;
    ALTER TABLE clients ADD COLUMN IF NOT EXISTS operator_switchable boolean DEFAULT true;
    CREATE TABLE IF NOT EXISTS "session" (sid text PRIMARY KEY, sess json NOT NULL, expire timestamp NOT NULL);`);
  await pool.query('UPDATE users SET id=$1 WHERE id=$2', ['71', f.operator.id]);
  f.operator.id = '71'; f.manifest.operatorId = '71';
  await pool.query('INSERT INTO "session" (sid,sess,expire) VALUES ($1,$2,$3)', [f.sessionId,
    { authenticated: true, active_client_id: 10, user: { id: '71', role: 'admin', client_id: 10,
      active: true, email_verified: true, password_change_required: false } }, '2099-01-01']); t.mock.timers.setTime(+f.now);
    f.program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [f.program.id])).rows[0];
    const original = await protectedState(pool, f);
    await pool.query('UPDATE acquisition_outbound_inbox_health SET last_success_at=$1', [f.now]);
    const extra = await h.extraAdmission(pool, f, 9);
    f.manifest = { ...f.manifest, id: 'synthetic-v2-after-original-v1', admissions: [extra] };
    pinManifest(f); await assertNoWrites(pool, () => install(pool, f));
    pinManifest(f, { consumedOperations: 1, consumedItems: 1 });
    await install(pool, f); await run(pool, f);
    const ledger = await rows(pool, 'anchor_scoped_resume_consumption');
    assert.equal(ledger.length, 2); assert.ok(ledger.includes(historical[0]));
    assert.deepEqual((await pool.query('SELECT sum(item_count)::int AS items FROM anchor_scoped_resume_consumption')).rows, [{ items: 2 }]);
    assert.deepEqual(await rows(pool, 'acquisition_outbound_programs'), original.acquisition_outbound_programs);
    assert.deepEqual((await pool.query('SELECT row_to_json(m) AS row FROM acquisition_missions m WHERE id=$1', [f.source.id])).rows[0].row, original.source);
  });

  await t.test('original ledger survives new grant and day and bounds each explicitly approved increment', async () => {
    const f = await setup(pool, t); await install(pool, f); const original = await protectedState(pool, f);
    await run(pool, f); const firstLedger = await rows(pool, 'anchor_scoped_resume_consumption');
    await revoke(pool, f);
    const extra = await h.extraAdmission(pool, f, 7);
    f.now = new Date('2026-10-25T16:00:00.000Z'); t.mock.timers.setTime(+f.now);
    await pool.query('UPDATE acquisition_outbound_inbox_health SET last_success_at=$1', [f.now]);
    f.manifest = { ...f.manifest, id: 'synthetic-second-day-grant', admissions: [extra] };
    pinManifest(f, { consumedOperations: 0, consumedItems: 0 });
    await assertNoWrites(pool, () => install(pool, f));
    pinManifest(f, { consumedOperations: 1, consumedItems: 1 });
    await install(pool, f); await run(pool, f);
    const ledger = (await pool.query('SELECT manifest_id,local_day::text AS day,item_count FROM anchor_scoped_resume_consumption ORDER BY local_day')).rows;
    assert.deepEqual(ledger, [{ manifest_id: 'local-scoped-resume', day: '2026-10-24', item_count: 1 },
      { manifest_id: 'synthetic-second-day-grant', day: '2026-10-25', item_count: 1 }]);
    assert.ok((await rows(pool, 'anchor_scoped_resume_consumption')).includes(firstLedger[0]));
    await assertNoWrites(pool, () => consume(pool, f, 'another-operation-same-grant'));
    assert.deepEqual(await rows(pool, 'acquisition_outbound_programs'), original.acquisition_outbound_programs);
    assert.deepEqual((await pool.query('SELECT row_to_json(m) AS row FROM acquisition_missions m WHERE id=$1', [f.source.id])).rows[0].row, original.source);
    assert.deepEqual(await effects(pool), { consumed: 2, children: 2, reservations: 0, execution_approvals: 0 });
  });

  await t.test('v2 grants cannot authorize send, freeze, refill, canonical approval or research even if flags are opened', async () => {
    const f = await setup(pool, t); await install(pool, f);
    const result = await run(pool, f);
    const saved = await persistence.loadMissionSnapshot(result.missionId, '10', pool, { skipEnsure: true });
    const store = new GovernedOutboundStore(pool, '10');
    const a = f.manifest.admissions[0];
    const entry = { candidateId: a.companyId, prospectId: a.prospectId, companyId: a.companyId, email: a.recipientEmail,
      message: { subject: 'Synthetic review-only subject', body: 'Synthetic review-only body' },
      sender: { senderEmail: f.manifest.senderEmail, senderName: f.manifest.senderName, sendingDomain: f.manifest.sendingDomain }, revision: result.preparedArtifactRevision };
    const envelope = { id: 'never-frozen', program_id: f.program.id, tenant_id: '10', mission_id: result.missionId,
      local_day: clock(f.now).day, manifest: [], manifest_hash: hash([]), revision: result.preparedArtifactRevision, status: 'frozen', approval_id: null };
    for (const operation of ['freeze', 'refill', 'approve_canonical_envelope', 'generate_deterministic_existing_template'])
      await assertNoWrites(pool, () => consume(pool, f, `forbidden-${operation}`, { operation }));
    await assertNoWrites(pool, () => consume(pool, f, 'forbidden-research', { capability: 'research', operation: 'research_existing_source' }));
    await assertNoWrites(pool, () => store.freeze(f.program, clock(f.now).day, result.missionId, result.preparedArtifactRevision, [entry], { env: f.env, now: f.now, sessionId: f.sessionId }));
    await assertNoWrites(pool, () => store.appendToEnvelope(envelope, [entry], null, { env: f.env, now: f.now, sessionId: f.sessionId }));
    await assertNoWrites(pool, () => require('../services/governedOutboundAdapters').adapters(pool, { tenantId: '10' }).approve(saved, f.program, {}), { code: 'anchor_recovery_separate_preparation_approval_required' });
    await assertNoWrites(pool, () => require('../services/governedOutboundAdapters').adapters(pool, { tenantId: '10' }).approve(saved, f.program, {}, { preinitializedSchema: true, persistencePool: pool }), { code: 'anchor_recovery_separate_preparation_approval_required' });
    await assertNoWrites(pool, () => integration.assertLegacyPreparation(pool, { tenantId: '10', program: f.program, operation: 'approve_canonical_envelope' }, { env: f.env, now: f.now, sessionId: f.sessionId }));
    for (const flags of [f.env, { ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id }, { ...f.env, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'false', ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'true', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'false' }]) {
      assert.equal((await recovery.loadRecoveryState(pool, { env: flags, now: f.now })).mode, 'maintenance');
      await assertNoWrites(pool, () => scope.assertScopedResumeCapability(pool, { program: f.program, capability: 'email', operation: 'dispatch' }, { env: flags, now: f.now }));
      await assertNoWrites(pool, () => recovery.assertRecoveryLeaf(pool, { channel: 'email', identities: [{ clientId: 10, prospectId: a.prospectId }],
        context: { programId: f.program.id, envelopeId: envelope.id, itemId: 'never-claimed', provider: 'brevo', transport: 'governed_brevo' }, command: {} }, { env: flags, now: f.now }));
    }
    assert.deepEqual(await effects(pool), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
    assert.equal(deny.attempts(), 0);
  });

  await t.test('ordinary scoped authority still requires its original live outbound window', async () => {
    t.mock.timers.setTime(+new Date('2026-10-19T16:00:00.000Z'));
    const f = await h.setup(pool); keys.forEach(k => delete process.env[k]);
    process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = f.manifest.id;
    await h.install(pool, f);
    const ready = await prepareExistingCrm(pool, f.input, { env: f.env, now: f.now, sessionId: f.sessionId });
    assert.equal(ready.readyForReview, true);
    assert.equal((await recovery.loadRecoveryState(pool, { env: f.env, now: f.now, sessionId: f.sessionId })).mode, 'scoped_resume');
    const g = await h.setup(pool); await h.install(pool, g);
    await assertNoWrites(pool, () => prepareExistingCrm(pool, g.input, { env: g.env, now: new Date(saturday), sessionId: g.sessionId }));
    assert.equal((await effects(pool)).consumed, 0);
    assert.equal(deny.attempts(), 0);
  });
});
