'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { scopedResumeFixture, clone } = require('./helpers/anchorScopedResumeFixture');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const scope = require('../services/anchorScopedResume');
const authority = require('../services/anchorCrmPreparationAuthority');
const recovery = require('../services/anchorEmailRecovery');
function fixture() {
  const f = scopedResumeFixture();
  f.now = new Date('2026-10-24T16:00:00Z');
  f.sourceMission.stage = 'discover';
  f.program.scope_hash = hash(missionScope(f.sourceMission));
  f.manifest.scopeHash = f.program.scope_hash;
  f.program.policy.startsAt = '2026-10-23T13:00:00Z';
  f.program.policy.expiresAt = '2026-10-23T21:00:00Z';
  f.program.policy_hash = hash(f.program.policy);
  f.manifest.policyHash = f.program.policy_hash;
  f.manifest.version = 2; f.manifest.operatorId = '71';
  f.sessionId = 'synthetic-preparation-session';
  f.installation = { operator_id: '71', operator_role: 'admin' };
  f.manifest.validFrom = '2026-10-24T13:00:00Z';
  f.manifest.expiresAt = '2026-10-24T20:00:00Z';
  f.manifest.scopes.email = { enabled: false };
  f.manifest.scopes.preparation = { enabled: true, approvalReference: 'local-prepare-only', dailyCap: 1,
    totalCap: 1, dailyItemCap: 1, totalItemCap: 1, operations: ['prepare_existing_crm'] };
  f.manifest.preparationAuthority = { kind: 'existing_crm_continuation',
    originalAuthorizerId: String(f.program.authorized_by), approverAccountClientId: 10,
    approverSessionHash: require('../services/anchorCrmPreparationIdentity').sessionBindingHash(f.sessionId), programBindingHash: authority.programBindingHash(f.program),
    sourceMissionHash: hash(f.sourceMission), consumedOperations: 0, consumedItems: 0 };
  f.env = { ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true',
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true' };
  const query = f.db.query;
  f.db.query = async (sql, values) => {
    if (/SELECT sid,sess FROM "session"/.test(sql)) return { rows: [{ sid: f.sessionId,
      sess: { authenticated: true, active_client_id: 10, user: { id: '71', role: 'admin', client_id: 10 } } }] };
    if (/SELECT expire >/.test(sql)) return { rows: [{ unexpired: true }] };
    if (/SELECT id,role,active,client_id,email_verified,password_change_required/.test(sql)) return { rows: [{
      id: '71', role: 'admin', active: true, client_id: 10, email_verified: true, password_change_required: false }] };
    if (/SELECT id FROM clients WHERE id=10/.test(sql)) return { rows: [{ id: 10 }] };
    return query(sql, values);
  };
  return f;
}
test('typed authority retains complete immutable manifest and validates expired Saturday historical binding without send validation', () => {
  const f = fixture(), before = JSON.stringify(f.program);
  const manifest = scope.validateScopedResumeManifest(f.manifest, { now: f.now });
  assert.deepEqual(manifest, f.manifest);
  assert.ok(Object.isFrozen(manifest.preparationAuthority));
  authority.validatePreparationProgram(f.program, manifest, f.now);
  authority.assertPreparationSource(f.sourceMission, manifest);
  authority.assertPreparationEnvironment(manifest, { env: f.env });
  assert.throws(() => recovery.validateLivePolicy(f.program, manifest, f.now), /policy_invalid/);
  assert.equal(JSON.stringify(f.program), before);
});
test('strict version, type, exact capabilities, batch, window and cumulative cap contracts', () => {
  const changes = [m => { m.version = 1; }, m => { m.version = 3; }, m => { delete m.preparationAuthority; },
    m => { m.preparationAuthority.extra = true; }, m => { m.extra = true; },
    m => { m.preparationAuthority.kind = 'send'; }, m => { m.operatorRole = 'manager'; },
    m => { m.preparationAuthority.programBindingHash = ''; }, m => { m.preparationAuthority.sourceMissionHash = ''; },
    m => { m.preparationAuthority.consumedOperations = -1; }, m => { m.preparationAuthority.consumedItems = 0.5; },
    m => { m.scopes.preparation.totalCap = 2; }, m => { m.scopes.preparation.totalItemCap = 2; },
    m => { m.scopes.preparation.operations.push('freeze'); }, m => { m.scopes.preparation.operations = ['refill']; },
    m => { m.scopes.preparation.operations = ['approve_canonical_envelope']; },
    m => { m.scopes.research = { enabled: true }; }, m => { m.scopes.email = scopedResumeFixture().manifest.scopes.email; },
    m => { m.expiresAt = '2026-10-25T13:00:00.001Z'; }, m => { m.admissions = []; },
    m => { m.admissions.push(clone(m.admissions[0])); }];
  for (const change of changes) {
    const f = fixture(); change(f.manifest);
    assert.throws(() => scope.validateScopedResumeManifest(f.manifest, { now: f.now }), /resume_/);
  }
  for (const now of ['2026-10-24T12:59:59Z', '2026-10-24T20:00:00Z']) {
    assert.throws(() => scope.validateScopedResumeManifest(fixture().manifest, { now: new Date(now) }), /window_invalid/);
  }
});
test('full program row pin detects policy, authorization timestamp and bookkeeping changes', () => {
  const f = fixture();
  assert.equal(authority.programBindingHash({ ...f.program, authorized_at: new Date(f.program.authorized_at) }), authority.programBindingHash(f.program));
  for (const changed of [{ ...f.program, authorized_at: new Date('2026-10-20T16:00:00Z') },
    { ...f.program, last_tick_at: new Date('2026-10-24T16:00:00Z') }, { ...f.program, last_error: 'changed' },
    { ...f.program, mode: 'paused' }, { ...f.program, source_mission_id: 'replacement' },
    { ...f.program, policy: { ...f.program.policy, expiresAt: '2026-10-25T20:00:00Z' } }]) {
    assert.throws(() => authority.validatePreparationProgram(changed, f.manifest, f.now), /program_changed/);
  }
  for (const value of [undefined, NaN, Infinity, new Date(NaN), () => {}]) {
    assert.throws(() => authority.programBindingHash({ ...f.program, invalid: value }), /program_invalid/);
  }
});
test('even re-pinned active/unexpired or malformed historical policy is not continuation authority', () => {
  for (const update of [p => { p.expiresAt = '2026-10-25T20:00:00Z'; }, p => { p.preparationOnly = true; },
    p => { p.weekdays = undefined; }, p => { p.startsAt = '2026-02-30T00:00:00Z'; },
    p => { p.extra = true; }, p => { p.enrichmentLimit = 100; }]) {
    const f = fixture(); update(f.program.policy);
    f.program.policy_hash = hash(f.program.policy); f.manifest.policyHash = f.program.policy_hash;
    if (f.program.policy.weekdays === undefined) { delete f.program.policy.weekdays; f.program.policy_hash = hash(f.program.policy); f.manifest.policyHash = f.program.policy_hash; }
    f.manifest.preparationAuthority.programBindingHash = authority.programBindingHash(f.program);
    assert.throws(() => authority.validatePreparationProgram(f.program, f.manifest, f.now), /preparation_/);
  }
  const f = fixture(); f.manifest.validFrom = '2026-10-23T20:59:59Z';
  assert.throws(() => authority.validatePreparationProgram(f.program, f.manifest, f.now), /historical_window_invalid/);
});
test('full source payload pin includes non-scope payload and rejects cancellation', () => {
  const f = fixture();
  for (const source of [{ ...f.sourceMission, title: 'changed non-scope' }, { ...f.sourceMission, stage: 'ready' },
    { ...f.sourceMission, planCancelled: true }]) assert.throws(() => authority.assertPreparationSource(source, f.manifest), /source_changed/);
});
test('separate preparation reader is active only under all closed environment controls; shared readers always deny', async () => {
  const f = fixture();
  const state = await authority.readPreparationState(f.db, { env: f.env, now: f.now, sessionId: f.sessionId });
  assert.equal(state.mode, 'preparation_only'); assert.deepEqual(state.manifest, f.manifest);
  for (const key of ['ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_GOVERNED_OUTBOUND_ENABLED',
    'ANCHOR_RESEARCH_PROVIDERS_PAUSED', 'ANCHOR_EMAIL_SCOPED_RESUME_ID']) {
    const env = { ...f.env }; delete env[key];
    await assert.rejects(authority.readPreparationState(f.db, { env, now: f.now, sessionId: f.sessionId }), /maintenance_required/);
  }
  await assert.rejects(authority.readPreparationState(f.db, { env: { ...f.env, ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: 'other' }, now: f.now }), /maintenance_required/);
  for (const env of [f.env, { ...f.env, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'false',
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'true', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'false' }]) {
    const closed = await recovery.loadRecoveryState(f.db, { env, now: f.now, sessionId: f.sessionId });
    assert.equal(closed.mode, 'maintenance'); assert.equal(closed.reason, 'recovery_preparation_only');
    await assert.rejects(scope.assertScopedResumeCapability(f.db, { program: f.program, capability: 'preparation', operation: 'freeze' }, { env, now: f.now, sessionId: f.sessionId }), /preparation_only/);
  }
});
test('data-only canonical Scout adapter never invokes injected discovery, inventory or fallback and leaves research paused', async t => {
  const key = 'ANCHOR_RESEARCH_PROVIDERS_PAUSED', before = process.env[key];
  process.env[key] = 'true';
  t.after(() => before === undefined ? delete process.env[key] : process.env[key] = before);
  let callbacks = 0;
  const forbidden = () => { callbacks++; throw new Error('provider callback forbidden'); };
  const filename = require.resolve('../packages/max/workspace/ScoutDiscoveryExecutor');
  const local = require('node:module').createRequire(filename), module = { exports: {} };
  require('node:vm').runInNewContext(require('node:fs').readFileSync(filename, 'utf8'), {
    module, exports: module.exports, process, console,
    require: id => id === '../../scout' ? { Scout: { discover: forbidden } }
      : id === '../../../services/acquisitionMissionInventory'
        ? { discoverGovernedInventory: forbidden, discoverKnowledgeInventory: forbidden } : local(id),
  }, { filename });
  const scout = module.exports;
  const mission = { id: 'synthetic-crm-data-only', tenantId: '10', clientId: 10,
    objective: 'Find synthetic commercial offices.', stage: 'discover', status: 'Discovering',
    crmPreparation: { preparationOnly: true, providerCalls: 0 },
    structuredMission: { immutable: true, objective: 'Find synthetic commercial offices.',
      market: { segment: 'law_firm', industry: 'legal', buyer: 'office manager', label: 'Law firms' },
      geography: { region: 'Manchester NH', cities: ['Manchester'] }, constraints: [],
      evidence: { requireDecisionMaker: true }, successMetric: { qualifiedLeads: 1 } } };
  const candidate = { id: 'synthetic-contact', prospectId: 'synthetic-contact', companyId: 'synthetic-company',
    name: 'Synthetic Firm', website: 'https://synthetic.test', location: 'Manchester NH', industry: 'legal',
    signals: [], evidenceRefs: [{ id: 'crm_synthetic', source: 'crm:synthetic-contact',
      label: 'Existing CRM evidence only', observedAt: '2026-10-23T16:00:00Z' }], unknowns: ['Buyer intent unknown.'] };
  const runScout = scout.createExistingCrmScoutRunner({ payload: {
    source: 'explicit_approved_existing_crm', fitCandidates: [candidate], candidateUniverse: [candidate],
    qualifiedCount: 1, candidateCount: 1, discoveryStatus: 'complete' } });
  const result = await scout.runScoutForAmoMission(mission,
    { runScout, discoverImpl: forbidden, fixtureScoutDiscoveryResult: forbidden, allowFixtureFallback: true });
  assert.equal(result.status, 'completed');
  let getterReads = 0;
  const changingOptions = { get runScout() { return ++getterReads === 1 ? runScout : forbidden; } };
  assert.equal((await scout.runScoutForAmoMission(mission, changingOptions)).status, 'completed');
  assert.equal(getterReads, 1, 'capability membership and invocation use the same captured function');
  assert.equal(callbacks, 0); assert.equal(process.env[key], 'true');
  await assert.rejects(scout.runScoutForAmoMission(mission, { discoverImpl: forbidden }), /anchor_research_provider_paused/);
  await assert.rejects(scout.runScoutForAmoMission(mission, { runScout: Object.assign(forbidden, { preparationOnly: true }) }), /anchor_research_provider_paused/);
  await assert.rejects(scout.runScoutForAmoMission({ ...mission, tenantId: '13' }, { runScout }), /existing_crm_discovery_required/);
  assert.throws(() => scout.createExistingCrmScoutRunner({ payload: {} }), /existing_crm_discovery_required/);
  assert.equal(callbacks, 0);
});
