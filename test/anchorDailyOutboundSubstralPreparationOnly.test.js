'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { service } = require('../services/governedOutbound');
const { hash, missionScope, policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { governedOutboundPreparationEnabledForTenant, governedOutboundEnabledForTenant } = require('../services/governedOutboundTenant');
const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
const now = new Date('2026-10-01T17:00:00Z');

function switches(t, preparation = 'true') {
  for (const [name, value] of Object.entries({ SUBSTRAL_GOVERNED_OUTBOUND_ENABLED: 'false', SUBSTRAL_GOVERNED_PREPARATION_ENABLED: preparation,
    GOVERNED_OUTBOUND_TENANT_17_ENABLED: 'false' })) {
    const old = process.env[name];
    if (value == null) delete process.env[name]; else process.env[name] = value;
    t.after(() => { if (old === undefined) delete process.env[name]; else process.env[name] = old; });
  }
}
function fixture(extra = {}, options = {}) {
  const source = { id: 'source', tenantId: '17', objective: 'Reviewed Substral mission', stage: 'ready', structuredMission: { immutable: true, market: { segment: 'small_business_owner' }, geography: { scope: 'nationwide', region: 'United States' } } };
  const p = policy({ tenantId: '17', sourceMissionId: 'source', senderEmail: 'sender@substral.example', inboxIntegrationId: 'inbox',
    preparationOnly: true, dailyCap: 2, totalCap: 4, spacingMinutes: 60, startHour: 9, endHour: 17, sendingIdentityId: 'substral-identity', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'], startsAt: '2026-10-01T04:00:00Z', expiresAt: '2026-10-08T04:00:00Z' }, now);
  const program = { id: 'program', tenant_id: '17', mode: 'shadow', authorized_by: 'operator', policy: p, policy_hash: hash(p), scope_hash: hash(missionScope(source)), source_mission_id: 'source' };
  const crm = { id: 'p1', prospect_id: 'p1', company_id: 'co1', company_name: 'Harbor Property Management', company_domain: 'harbor.example',
    domain: 'harbor.example', client_id: 17, email: 'office@harbor.example', email_verified: true, email_status: 'valid',
    service_area_match: true, vertical: 'small_business_owner', acquisition_metadata: { contactResolution: { finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: 'office@harbor.example' } }, enrichment_provenance: { email: { source: 'website_email', source_url: 'https://harbor.example/contact' } }, ...extra };
  const pool = { query: async sql => {
    if (/acquisition_knowledge_objects/.test(sql)) return { rows: [] };
    if (/SELECT p\.\*, c\.name/.test(sql)) return { rows: [crm] };
    throw new Error(`Unexpected database operation: ${sql}`);
  } };
  const events = []; const items = []; let providerCalls = 0; let approvalCalls = 0;
  let envelope = options.noEnvelope
    ? null
    : { id: 'envelope', program_id: program.id, mission_id: 'daily', status: 'frozen', revision: 'r1', manifest: [] };
  const candidate = {
    candidateId: crm.id,
    item: {
      candidateId: crm.id, prospectId: crm.id, companyId: crm.company_id,
      company: crm.company_name, domain: crm.domain, email: crm.email,
      sendable: true, dnc: false, paige: { candidateId: crm.id },
    },
    message: {
      candidateId: crm.id, companyId: crm.company_id, companyName: crm.company_name,
      subject: `A Studio Substral question for ${crm.company_name}`, body: 'Would a conversation about your website be useful?',
    },
  };
  const svc = service({ pool, tenantId: '17', now: () => now, adapters: {
    loadMission: async id => ({ mission: { ...source, id, stage: 'ready' } }), validateTenant: async () => {},
    prepare: async () => ({ mission: { ...source, id: 'daily', stage: 'ready' } }),
    prepared: async () => ({ candidates: options.preparedCandidate ? [candidate, ...(options.extraCandidates || [])] : [], revision: 'r1', capacity: 5,
      sender: { senderName: 'Jacob', senderEmail: 'sender@substral.example' } }),
    contact: options.contact || (async () => crm),
    infrastructure: options.infrastructure || (async () => ({ operating: { planningDailyCapacity: 5, dispatchCapacityNow: 5, minSpacingMinutes: 60 }, assessed: { governor: { outcome: 'proceed' } } })),
    send: async () => { providerCalls++; throw new Error('must not send'); },
    approve: async () => { approvalCalls++; throw new Error('must not authorize dispatch'); },
  } });
  Object.assign(svc.store, {
    lock: async fn => fn(), program: async () => program, expire: async () => {},
    counts: async () => ({ today: 0, total: 0, uncertain: 0 }), preparationCounts: async () => ({ today: items.length, total: items.length }), envelope: async () => envelope, items: async () => items,
    health: async () => {}, candidateOwnership: async () => null, suppression: async () => null,
    event: async (type, key, payload) => events.push({ type, key, payload }),
    appendToEnvelope: async (_envelope, selected) => {
      for (const entry of selected) items.push({ id: entry.candidateId, status: 'pending', snapshot: entry });
      envelope.manifest.push(...selected); return envelope;
    },
    freeze: async (_program, _day, missionId, revision, selected) => {
      envelope = { id: 'fresh-envelope', program_id: program.id, mission_id: missionId, status: 'frozen',
        revision, manifest: selected, manifest_hash: hash(selected), approval_id: null };
      for (const entry of selected) items.push({ id: entry.candidateId, status: 'pending', snapshot: entry });
      return envelope;
    },
  });
  return { svc, items, events, program, source, crm, candidate, envelope: () => envelope,
    providerCalls: () => providerCalls, approvalCalls: () => approvalCalls };
}


const { substralPreparationGrantReason } = require('../services/substralPreparationGrant');

test('Substral shadow preparation creates bound reviewed copy, is repeat-safe, and never sends', async t => {
  switches(t);
  const f = fixture({}, { noEnvelope: true, preparedCandidate: true });
  const result = await f.svc.runPreparationRefill();
  assert.equal(result.preparedAdded, 1);
  assert.equal(result.sendingEnabled, false);
  assert.equal(f.items[0].snapshot.message.companyId, f.crm.company_id);
  assert.equal(f.items[0].snapshot.email, f.crm.email);
  assert.equal((await f.svc.runPreparationRefill()).preparedAdded, 0);
  assert.equal(f.items.length, 1);
  assert.equal((await f.svc.tick()).halted, 'preparation_only_grant');
  await assert.rejects(f.svc.setMode(f.program.id, 'active', f.program.policy_hash, { id: 'operator', role: 'admin' }), /preparation_only_grant/);
  assert.equal(f.providerCalls(), 0);
  assert.equal(f.approvalCalls(), 0);
});

test('Substral cannot prepare generic inventory copy without reviewed Paige candidates', async t => {
  switches(t);
  const f = fixture();
  const result = await f.svc.runPreparationRefill();
  assert.equal(result.preparedAdded, 0);
  assert.equal(f.items.length, 0);
  assert.equal(f.providerCalls(), 0);
});

test('Substral grant rejects scope, expiry, mode, source and bounded-authority changes', async t => {
  switches(t);
  for (const [change, reason] of [
    [f => { delete f.program.policy.preparationOnly; }, 'preparation_shadow_grant_required'],
    [f => { f.program.mode = 'active'; }, 'preparation_shadow_grant_required'],
    [f => { f.program.policy.dailyCap++; }, 'policy_changed'],
    [f => { f.source.objective = 'Changed'; }, 'source_scope_changed'],
    [f => { f.source.structuredMission.immutable = false; }, 'source_scope_changed'],
    [f => { f.source.stage = 'discover'; }, 'source_scope_changed'],
    [f => { f.program.policy.expiresAt = '2026-10-01T16:00:00Z'; f.program.policy_hash = hash(f.program.policy); }, 'authorization_expired'],
  ]) {
    const f = fixture(); change(f);
    assert.equal(substralPreparationGrantReason(f.program, f.source, now), reason);
  }
});

test('preparation flag never substitutes for either explicit send-off flag', async t => {
  switches(t);
  for (const flag of ['SUBSTRAL_GOVERNED_OUTBOUND_ENABLED', 'GOVERNED_OUTBOUND_TENANT_17_ENABLED']) {
    for (const value of [undefined, 'true']) {
      if (value === undefined) delete process.env[flag]; else process.env[flag] = value;
      const f = fixture();
      assert.equal(substralPreparationGrantReason(f.program, f.source, now), 'preparation_requires_disabled_sending');
    }
    process.env[flag] = 'false';
  }
  delete process.env.SUBSTRAL_GOVERNED_PREPARATION_ENABLED;
  const f = fixture();
  assert.equal(substralPreparationGrantReason(f.program, f.source, now), 'preparation_requires_disabled_sending');
});

test('preparation bounds are explicit, tenant-17-only, and daily/lifetime counts include unsent items', async t => {
  switches(t);
  const f = fixture({}, { noEnvelope: true, preparedCandidate: true });
  for (const name of ['dailyCap', 'totalCap', 'startsAt', 'expiresAt', 'startHour', 'endHour', 'spacingMinutes', 'allowedContactClassifications']) {
    const input = { ...f.program.policy }; delete input[name];
    assert.throws(() => policy(input, now));
  }
  for (const tenantId of ['1', '10', '13']) assert.throws(() => policy({ ...f.program.policy, tenantId }, now), /invalid_preparation_only_scope/);
  f.svc.store.preparationCounts = async () => ({ today: 0, total: 4 });
  assert.equal((await f.svc.runPreparationRefill()).prepareSkippedReason, 'preparation_budget_exhausted');
  assert.equal(f.items.length, 0);
  assert.equal(f.providerCalls(), 0);
});

test('Max blocks discovery before research when the shadow grant is missing or changed', async t => {
  switches(t);
  for (const change of [f => { delete f.program.policy.preparationOnly; }, f => { f.source.objective = 'Changed'; }, f => { f.program.policy.dailyCap++; }]) {
    const f = fixture(); change(f); let research = 0;
    const result = await runMaxOutboundControlLoop({ tenantId: '17', pool: {}, store: f.svc.store,
      now, program: f.program, source: f.source,
      governedAdapters: { loadMission: async () => ({ mission: f.source }) },
      scoutRamp: async () => { research++; },
    });
    assert.ok(result.halted);
    assert.equal(research, 0);
  }
});

test('Max prepares existing reviewed stock while research stays explicitly unauthorized', async t => {
  switches(t);
  const f = fixture({}, { noEnvelope: true, preparedCandidate: true });
  let research = 0; let preparation = 0;
  f.svc.store.rampMetrics = async () => null;
  const clean = { clean: [{ prospectId: 'p1', candidateId: 'p1', companyId: 'co1', email: f.crm.email }], excluded: [], scope: {}, exclusionCounts: {} };
  const result = await runMaxOutboundControlLoop({ tenantId: '17', pool: {}, store: f.svc.store,
    now, program: f.program, source: { ...f.source, objective: 'Stale caller data must not be used' },
    governedAdapters: { loadMission: async () => ({ mission: f.source }) },
    infrastructure: { cap: 2, snapshot: { sentToday: 0 }, assessed: { governor: { outcome: 'proceed' } },
      operating: { dispatchCapacityNow: 0, planningDailyCapacity: 2, recommendedSafeDailyCapacity: 2,
        authorizationLimitedCapacity: 2, governor: 'proceed' } },
    timestamps: {}, funnel: {}, inventory: { ...clean, clean: [] }, inventoryAfter: clean,
    scoutRamp: async ({ source }) => { research++; assert.equal(source.mission, f.source); return null; },
    runPreparationRefill: async () => { preparation++; return f.svc.runPreparationRefill(); },
    logger: { log() {}, warn() {} },
  });
  assert.equal(research, 0);
  assert.equal(result.discoveryBlockedReason, 'preparation_research_not_authorized');
  assert.equal(preparation, 1);
  assert.equal(result.preparedAdded, 1);
  assert.equal(f.providerCalls(), 0, 'outbound send transport remains unused; research is separately mocked');
});

test('a preparation grant never enables tenant 1 or 13 preparation switches', async t => {
  switches(t);
  for (const tenant of ['1', '13']) {
    const key = tenant === '1' ? 'PULSEFORGE' : 'BABRUN';
    const names = [`${key}_GOVERNED_PREPARATION_ENABLED`, `${key}_GOVERNED_OUTBOUND_ENABLED`, `GOVERNED_OUTBOUND_TENANT_${tenant}_ENABLED`];
    for (const name of names) { const old = process.env[name]; process.env[name] = name.includes('PREPARATION') ? 'true' : 'false';
      t.after(() => { if (old === undefined) delete process.env[name]; else process.env[name] = old; }); }
    assert.equal(governedOutboundPreparationEnabledForTenant(tenant), false);
    assert.equal(governedOutboundEnabledForTenant(tenant), false);
  }
});

test('preparation budget counts all rows and commit boundary rejects an exhausted or changed grant', async () => {
  const { GovernedOutboundStore } = require('../services/governedOutboundStore');
  const f = fixture();
  const store = new GovernedOutboundStore({}, '17');
  const db = { query: async sql => { assert.match(sql, /count\(\*\)/); assert.doesNotMatch(sql, /status\s*=/); return { rows: [{ today: 1, total: 3 }] }; } };
  await store.assertPreparationBudget(db, f.program, '2026-10-01', 1);
  await assert.rejects(store.assertPreparationBudget(db, f.program, '2026-10-01', 2), /preparation_budget_exhausted/);
  const statements = [];
  const connection = { query: async sql => { statements.push(sql); return { rows: sql.startsWith('SELECT mode') ? [{ mode: 'active', policy_hash: f.program.policy_hash, scope_hash: f.program.scope_hash }] : [] }; }, release() {} };
  store.pool = { connect: async () => connection };
  await assert.rejects(store.freeze(f.program, '2026-10-01', 'daily', 'r1', [], { requireShadow: true }), /replenishment_grant_changed/);
  assert.equal(statements.some(sql => sql.includes('INSERT')), false);
  assert.ok(statements.includes('ROLLBACK'));
});


test('capacity migration cannot strip preparation-only authority through false, null or undefined', async t => {
  switches(t);
  const f = fixture();
  for (const preparationOnly of [false, null, undefined, '', 0]) {
    await assert.rejects(f.svc.migrateOperatorDelegatedCapacity({ preparationOnly }, { id: 'operator', role: 'admin' }), /preparation_only_grant/);
  }
});


test('live envelope authorization, cancellation or revision change blocks append before inserts', async () => {
  const { GovernedOutboundStore } = require('../services/governedOutboundStore');
  const f = fixture();
  for (const liveEnvelope of [
    { status: 'authorized', approval_id: 'approval' },
    { status: 'expired', approval_id: null },
    { status: 'frozen', approval_id: null, manifest_hash: 'changed' },
  ]) {
    const statements = [];
    const connection = { query: async sql => {
      statements.push(sql);
      if (sql.startsWith('SELECT mode')) return { rows: [{ mode: 'shadow', policy_hash: f.program.policy_hash, scope_hash: f.program.scope_hash }] };
      if (sql.startsWith('SELECT local_day')) { assert.match(sql, /FOR UPDATE/); return { rows: [{ local_day: '2026-10-01', revision: 'r1', manifest_hash: hash([]), ...liveEnvelope }] }; }
      return { rows: [] };
    }, release() {} };
    const store = new GovernedOutboundStore({ connect: async () => connection }, '17');
    await assert.rejects(store.appendToEnvelope(f.envelope(), [{}], 'r1', { shadowProgram: f.program }), /replenishment_grant_changed/);
    assert.equal(statements.some(sql => sql.includes('INSERT')), false);
    assert.ok(statements.includes('ROLLBACK'));
  }
});


test('delegated capacity cannot bypass the preparation daily bound', () => {
  const f = fixture();
  for (const dailyCap of ['unbounded', '2', 1.5, 999, 0, -1, NaN, Infinity, 5]) {
    assert.throws(() => policy({ ...f.program.policy, dailyCap, operatorDelegatedMaximumDailyCapacity: 2 }, now), /invalid_preparation_bounds/);
  }
});


test('production adapter refuses non-Ready preparation and recovery before runtime or providers', async () => {
  const { adapters } = require('../services/governedOutboundAdapters');
  const f = fixture();
  const adapter = adapters({ query: () => assert.fail('No database or provider work') }, {
    tenantId: '17', runtime: () => assert.fail('No paid mission runtime'),
  });
  for (const [stage, recovery] of [['discover', null], ['ready', { nextMissionId: 'recovery' }]]) {
    await assert.rejects(adapter.prepare(f.program, { mission: { ...f.source, stage } },
      '2026-10-01', {}, recovery), /preparation_research_not_authorized/);
  }
});


test('first envelope checks live governor and planning capacity before committing any budget', async t => {
  switches(t);
  for (const [infrastructure, reason] of [
    [async () => { throw Object.assign(new Error('emmett_governor_halted'), { code: 'emmett_governor_halted' }); }, 'emmett_governor_halted'],
    [async () => ({ assessed: { governor: { outcome: 'halt' } }, operating: { planningDailyCapacity: 5 } }), 'emmett_governor_halted'],
    [async () => ({ assessed: { governor: { outcome: 'proceed', halt: true } }, operating: { planningDailyCapacity: 5 } }), 'emmett_governor_halted'],
    [async () => ({ assessed: { governor: { outcome: 'proceed' } }, operating: { planningDailyCapacity: 0 } }), 'emmett_capacity_exhausted'],
    [async () => ({ assessed: { governor: { outcome: 'proceed' } }, operating: {} }), 'emmett_capacity_unavailable'],
    [async () => ({ operating: { planningDailyCapacity: 5 } }), 'emmett_governor_halted'],
  ]) {
    const f = fixture({}, { noEnvelope: true, preparedCandidate: true, infrastructure });
    const result = await f.svc.runPreparationRefill();
    assert.equal(result.prepareSkippedReason, reason);
    assert.equal(result.preparedAdded, 0);
    assert.equal(f.envelope(), null);
    assert.equal(f.items.length, 0);
    assert.deepEqual(await f.svc.store.preparationCounts(), { today: 0, total: 0 });
    assert.equal(f.providerCalls(), 0);
  }
});


test('first envelope limits reviewed artifacts to current planning capacity', async t => {
  switches(t);
  const extraCandidates = [];
  const f = fixture({}, { noEnvelope: true, preparedCandidate: true, extraCandidates,
    contact: async id => id === 'p1' ? f.crm : { ...f.crm, id: 'p2', prospect_id: 'p2', company_id: 'co2',
      company_name: 'Second Property Management', email: 'owner@second.example',
      acquisition_metadata: { contactResolution: { finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: 'owner@second.example' } } },
    infrastructure: async () => ({ assessed: { governor: { outcome: 'slow' } },
      operating: { planningDailyCapacity: 1, dispatchCapacityNow: 0, minSpacingMinutes: 60 } }),
  });
  extraCandidates.push({ candidateId: 'p2',
    item: { ...f.candidate.item, candidateId: 'p2', prospectId: 'p2', companyId: 'co2',
      company: 'Second Property Management', email: 'owner@second.example', paige: { candidateId: 'p2' } },
    message: { ...f.candidate.message, candidateId: 'p2', companyId: 'co2', companyName: 'Second Property Management' },
  });
  const result = await f.svc.runPreparationRefill();
  assert.equal(result.preparedAdded, 1);
  assert.equal(f.envelope().manifest.length, 1);
  assert.equal(f.items.length, 1);
  assert.equal(f.program.policy.dailyCap, 2);
  assert.equal(f.providerCalls(), 0);
});


test('a later refill governor halt reports the initial committed batch and consumed budget', async t => {
  switches(t);
  let infrastructureCalls = 0;
  const f = fixture({}, { noEnvelope: true, preparedCandidate: true,
    infrastructure: async () => {
      infrastructureCalls++;
      if (infrastructureCalls > 1) throw Object.assign(new Error('emmett_governor_halted'), { code: 'emmett_governor_halted' });
      return { assessed: { governor: { outcome: 'proceed' } },
        operating: { planningDailyCapacity: 2, dispatchCapacityNow: 2, minSpacingMinutes: 60 } };
    },
  });
  const result = await f.svc.runPreparationRefill();
  assert.equal(infrastructureCalls, 2);
  assert.equal(result.prepareSkippedReason, 'emmett_governor_halted');
  assert.equal(result.preparedAdded, 1);
  assert.equal(result.pendingPrepared, 1);
  assert.equal(f.envelope().status, 'frozen');
  assert.equal(f.envelope().manifest.length, 1);
  assert.equal(f.items.length, 1);
  assert.deepEqual(await f.svc.store.preparationCounts(), { today: 1, total: 1 });
  const audit = f.events.find(event => event.type === 'preparation_refill_evaluated');
  assert.equal(audit.payload.preparedAdded, 1);
  assert.equal(audit.payload.pendingPrepared, 1);
  assert.equal(audit.payload.prepareSkippedReason, 'emmett_governor_halted');
  assert.equal(f.providerCalls(), 0);
  const retry = await f.svc.runPreparationRefill();
  assert.equal(retry.preparedAdded, 0);
  assert.equal(retry.pendingPrepared, 1);
  assert.equal(f.items.length, 1);
});
