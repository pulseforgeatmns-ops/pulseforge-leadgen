'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { runMaxOutboundControlLoop, defaultScoutRamp, applyPreparationRefill } = require('../services/maxOutboundControlLoop');
const { adapters } = require('../services/governedOutboundAdapters');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { scopedResumeFixture } = require('./helpers/anchorScopedResumeFixture');
const quiet = { log() {}, warn() {} };
const noDatabase = { query: async sql => { throw new Error(`Unexpected database dependency: ${sql}`); },
  connect: async () => { throw new Error('Unexpected recovery connection'); } };

function setEnv(t, values) {
  for (const [key, value] of Object.entries(values)) {
    const old = process.env[key];
    if (value == null) delete process.env[key]; else process.env[key] = value;
    t.after(() => { if (old == null) delete process.env[key]; else process.env[key] = old; });
  }
}
function scopedFixture(t) {
  const f = scopedResumeFixture();
  // Bind adapter entrypoints to the fixture's valid weekday/business-hour clock.
  // Keep the real policy window and expiry validation active.
  t.mock.timers.enable({ apis: ['Date'], now: +f.now });
  f.program.policy.startsAt = new Date(+f.now - 2 * 3600000).toISOString();
  f.program.policy.expiresAt = new Date(+f.now + 2 * 3600000).toISOString();
  f.program.policy_hash = hash(f.program.policy);
  f.manifest.policyHash = f.program.policy_hash;
  f.manifest.validFrom = new Date(+f.now - 3600000).toISOString();
  f.manifest.expiresAt = new Date(+f.now + 3600000).toISOString();
  setEnv(t, { ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id,
    ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: null, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: null,
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', ANCHOR_GOVERNED_PREPARATION_ENABLED: 'true' });
  return f;
}
function preparation(f, operations = ['reuse_existing_artifacts', 'refill']) {
  f.manifest.scopes.preparation = { enabled: true, approvalReference: 'local-preparation-only',
    dailyCap: 3, totalCap: 6, dailyItemCap: 3, totalItemCap: 6, operations };
}

test('preparation gate uses the same supplied clock and activation environment at both checks', async () => {
  const f = scopedResumeFixture(); preparation(f, ['reuse_existing_artifacts']);
  const { assertLegacyPreparation } = require('../services/anchorEmailRecoveryIntegration');
  const input = { tenantId: '10', program: f.program, operation: 'reuse_existing_artifacts' };
  const grant = await assertLegacyPreparation(f.db, input, { now: f.now, env: f.env });
  assert.equal(grant.resumeGrant.id, f.manifest.id);
  await assert.rejects(assertLegacyPreparation(f.db, input, { now: new Date(Date.parse(f.manifest.expiresAt) + 1), env: f.env }),
    { code: 'anchor_recovery_separate_preparation_approval_required' });
});
function baseMax(tenantId = '13') {
  const inventory = { clean: [], excluded: [], scope: {}, exclusionCounts: {} };
  return { pool: noDatabase, tenantId, logger: quiet, now: new Date('2026-10-19T16:00:00Z'),
    program: { id: `local-${tenantId}`, tenant_id: tenantId, mode: 'active',
      source_mission_id: `source-${tenantId}`, policy: { dailyCap: 1 } },
    source: { id: `source-${tenantId}`, payload: {} },
    store: { tenantId, clientId: Number(tenantId), event: async () => {}, envelope: async () => null },
    inventory, inventoryAfter: inventory, timestamps: {}, funnel: {},
    infrastructure: { cap: 1, snapshot: { sentToday: 0 },
      operating: { planningDailyCapacity: 1, authorizationLimitedCapacity: 1,
        dispatchCapacityNow: 1, scheduleLimitedCapacity: 1, governor: 'proceed' } } };
}

for (const tenantId of ['1', '13', '17']) {
  test(`tenant ${tenantId} preparation has no recovery query, lock or table dependency`, async () => {
    let calls = 0;
    const result = await applyPreparationRefill({ pool: noDatabase, tenantId,
      program: { id: `program-${tenantId}`, tenant_id: tenantId }, preparation: { prepareRequested: 1 },
      runPreparationRefill: async () => { calls++; return { preparedAdded: 1, pendingPrepared: 1 }; } });
    assert.equal(result.preparedAdded, 1);
    assert.equal(calls, 1);
    const sentinel = Object.assign(new Error('legacy-runtime-reached'), { code: 'legacy-runtime-reached' });
    const adapter = adapters(noDatabase, { tenantId, runtime: { engine() { throw sentinel; } } });
    await assert.rejects(adapter.prepare({ id: `program-${tenantId}`, tenant_id: tenantId, policy: {} },
      { mission: { stage: 'discover' } }, '2026-10-19', {}), { code: sentinel.code });
  });
}

test('non-Anchor Max continues its existing callback and audit without any recovery schema', async () => {
  for (const tenantId of ['13', '17']) {
    const options = baseMax(tenantId);
    let callbacks = 0, events = 0;
    options.scoutRamp = async () => { callbacks++; return { promoted: 1 }; };
    options.store.event = async () => { events++; };
    const result = await runMaxOutboundControlLoop(options);
    assert.equal(result.programId, options.program.id);
    assert.equal(callbacks, 1);
    assert.equal(events, 1);
  }
});

test('non-Anchor research retains original enrichment limits without recovery reads', async () => {
  const counts = [];
  const result = await defaultScoutRamp({ pool: noDatabase, logger: quiet,
    store: { tenantId: '13', clientId: 13 }, program: { id: 'local-babrun', tenant_id: '13', policy: {} },
    source: { payload: {} }, plan: { deficit: 100 }, skipVerificationRetry: true,
    recoverExistingInventory: async () => ({ payload: { qualifiedCount: 1 } }),
    enrichment: { run: async input => { counts.push(input.limit); return { considered: input.limit, promoted: input.limit }; } } });
  assert.deepEqual(counts, [5, 5, 5]);
  assert.equal(result.discoveryAttempted, false);
});

test('installed maintenance blocks Anchor before recovery, enrichment, discovery and preparation callbacks', async () => {
  let effects = 0;
  const blocked = async () => { effects++; throw new Error('Unapproved callback'); };
  const pool = { query: async sql => ({ rows: /to_regclass/.test(sql)
    ? [{ control_table: 'anchor_email_recovery_control' }]
    : [{ singleton: true, maintenance_armed: true, manifest: null }] }) };
  await assert.rejects(defaultScoutRamp({ pool, program: { id: 'p10', tenant_id: '10' },
    runDiscovery: blocked, recoverExistingInventory: blocked, enrichment: { run: blocked } }),
  { code: 'anchor_recovery_separate_preparation_approval_required' });
  await assert.rejects(applyPreparationRefill({ pool, tenantId: '10', preparation: { prepareRequested: 1 },
    runPreparationRefill: blocked }), { code: 'anchor_recovery_separate_preparation_approval_required' });
  const result = await runMaxOutboundControlLoop({ ...baseMax('10'), pool, scoutRamp: blocked, refillAoBooks: blocked });
  assert.equal(result.halted, 'anchor_recovery_separate_preparation_approval_required');
  assert.equal(effects, 0);
});

test('scoped research-disabled grant causes zero provider or recovery pipeline effects', async t => {
  const f = scopedFixture(t); let effects = 0;
  const blocked = async () => { effects++; throw new Error('Unapproved provider pipeline'); };
  await assert.rejects(defaultScoutRamp({ pool: f.db, program: f.program, now: f.now,
    runDiscovery: blocked, recoverExistingInventory: blocked, enrichment: { run: blocked } }),
  { code: 'anchor_scoped_resume_capability_closed' });
  assert.equal(effects, 0);
  assert.ok(f.calls.every(({ sql }) => !/INSERT|UPDATE |DELETE|CREATE |ALTER /.test(sql)));
});

test('scoped Max reports blocked research and cannot assign AO accounts or invoke injected providers', async t => {
  const f = scopedFixture(t); let effects = 0;
  const blocked = async () => { effects++; throw new Error('Unapproved effect'); };
  const result = await runMaxOutboundControlLoop({ ...baseMax('10'), pool: f.db, now: f.now,
    program: f.program, source: { mission: f.sourceMission }, scoutRamp: blocked,
    refillAoBooks: blocked, runPreparationRefill: blocked });
  assert.equal(result.discoveryBlockedReason, 'anchor_scoped_resume_capability_closed');
  assert.equal(result.aoRefill, null);
  assert.equal(effects, 0);
});

test('execute=false keeps scoped Max diagnostic read-only without reservations, locks or callbacks', async t => {
  const f = scopedFixture(t); let effects = 0;
  const blocked = async () => { effects++; throw new Error('Read-only observation caused a write'); };
  const options = baseMax('10'); options.store.event = blocked;
  await runMaxOutboundControlLoop({ ...options, pool: f.db, now: f.now, execute: false,
    program: f.program, source: { mission: f.sourceMission }, scoutRamp: blocked,
    refillAoBooks: blocked, runPreparationRefill: blocked });
  assert.equal(effects, 0);
  assert.ok(f.calls.every(({ sql }) => !/FOR SHARE|FOR UPDATE|advisory|INSERT|UPDATE |DELETE|CREATE |ALTER /.test(sql)));
});

test('scoped preparation cannot reach paid copy generation even when deterministic template work is approved', async t => {
  const f = scopedFixture(t); preparation(f, ['generate_deterministic_existing_template']);
  let effects = 0;
  const adapter = adapters(f.db, { tenantId: '10', runtime: { engine() { effects++; throw new Error('Paid runtime'); } },
    runScout: async () => { effects++; }, enrich: async () => { effects++; } });
  await assert.rejects(adapter.prepare(f.program, { mission: { ...f.sourceMission, stage: 'discover' } },
    '2026-10-19', {}), { code: 'anchor_scoped_resume_paid_preparation_unverified' });
  assert.equal(effects, 0);
  assert.ok(f.calls.every(({ sql }) => !/INSERT|UPDATE |DELETE|CREATE |ALTER /.test(sql)));
});

test('scoped preparation reuses existing ready artifacts with no research, LLM or runtime effects', async t => {
  const f = scopedFixture(t); preparation(f);
  const artifactFixture = require('./helpers/anchorRecoveryPrepareExistingFixture').prepareExistingFixture();
  const source = { mission: { ...f.sourceMission, stage: 'ready' },
    contributions: artifactFixture.artifactRows.map(row => ({ ...row.payload, missionId: f.sourceMission.id })) };
  let effects = 0;
  const adapter = adapters(f.db, { tenantId: '10', runtime: { engine() { effects++; throw new Error('Paid runtime'); } },
    tenant: async () => ({ sender: { senderEmail: f.client.sender_email, senderName: f.client.sender_name,
      sendingDomain: f.client.sending_domain } }), runScout: async () => { effects++; }, enrich: async () => { effects++; } });
  assert.equal(await adapter.prepare(f.program, source, '2026-10-19', {}), source);
  assert.equal(effects, 0);
  assert.ok(f.calls.every(({ sql }) => !/INSERT|UPDATE |DELETE|CREATE |ALTER /.test(sql)));
});

test('scoped refill checks explicit operation and commits consumption before an injected callback', async t => {
  const f = scopedFixture(t); preparation(f);
  const control = require('../services/anchorScopedResumeControl');
  const order = [];
  t.mock.method(control, 'consumeScopedOperation', async (_db, input) => {
    assert.equal(input.operation, 'refill'); assert.equal(input.capability, 'preparation');
    assert.equal(input.manifest.id, f.manifest.id); assert.equal(input.itemCount, undefined);
    order.push('durably-consumed'); return { consumed: true };
  });
  await applyPreparationRefill({ pool: f.db, tenantId: '10', program: f.program, controlNow: f.now,
    preparation: { prepareRequested: 1 }, runPreparationRefill: async () => {
      order.push('callback'); return { preparedAdded: 1 }; } });
  assert.deepEqual(order, ['durably-consumed', 'callback']);
  f.manifest.scopes.preparation.operations = ['reuse_existing_artifacts'];
  await assert.rejects(applyPreparationRefill({ pool: f.db, tenantId: '10', program: f.program, controlNow: f.now,
    preparation: { prepareRequested: 1 }, runPreparationRefill: async () => { order.push('unapproved'); } }),
  { code: 'anchor_scoped_resume_capability_closed' });
  assert.deepEqual(order, ['durably-consumed', 'callback']);
});
