'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { service } = require('../services/governedOutbound');
const { hash, missionScope, policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { governedOutboundPreparationEnabledForTenant, governedOutboundEnabledForTenant } = require('../services/governedOutboundTenant');
const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
const now = new Date('2026-10-01T17:00:00Z');

function switches(t, preparation = 'true') {
  for (const [name, value] of Object.entries({ ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', ANCHOR_GOVERNED_PREPARATION_ENABLED: preparation,
    BABRUN_GOVERNED_OUTBOUND_ENABLED: 'false', GOVERNED_OUTBOUND_TENANT_13_ENABLED: 'false' })) {
    const old = process.env[name];
    if (value == null) delete process.env[name]; else process.env[name] = value;
    t.after(() => { if (old === undefined) delete process.env[name]; else process.env[name] = old; });
  }
}
function fixture(extra = {}) {
  const source = { id: 'source', tenantId: '10', objective: 'Cleaning for property managers', structuredMission: { immutable: true } };
  const p = policy({ tenantId: '10', sourceMissionId: 'source', senderEmail: 'sender@anchor.example', inboxIntegrationId: 'inbox',
    aoOwnerIds: [1], startsAt: '2026-10-01T04:00:00Z', expiresAt: '2026-10-08T04:00:00Z' }, now);
  const program = { id: 'program', tenant_id: '10', mode: 'active', policy: p, policy_hash: hash(p), scope_hash: hash(missionScope(source)), source_mission_id: 'source' };
  const crm = { id: 'p1', prospect_id: 'p1', company_id: 'co1', company_name: 'Harbor Property Management', company_domain: 'harbor.example',
    domain: 'harbor.example', client_id: 10, email: 'office@harbor.example', email_verified: true, email_status: 'valid',
    service_area_match: true, vertical: 'property_management', enrichment_provenance: { email: { source: 'website_email', source_url: 'https://harbor.example/contact' } }, ...extra };
  const pool = { query: async sql => {
    if (/acquisition_knowledge_objects/.test(sql)) return { rows: [] };
    if (/SELECT p\.\*, c\.name/.test(sql)) return { rows: [crm] };
    throw new Error(`Unexpected database operation: ${sql}`);
  } };
  const events = []; const items = []; let providerCalls = 0; let approvalCalls = 0;
  const envelope = { id: 'envelope', program_id: program.id, mission_id: 'daily', status: 'authorized', revision: 'r1', manifest: [] };
  const svc = service({ pool, tenantId: '10', now: () => now, adapters: {
    loadMission: async id => ({ mission: { ...source, id, stage: 'ready' } }), validateTenant: async () => {},
    prepared: async () => ({ candidates: [], revision: 'r1', sender: { senderName: 'Jacob', senderEmail: 'sender@anchor.example' } }),
    contact: async () => crm,
    infrastructure: async () => ({ operating: { dispatchCapacityNow: 5, minSpacingMinutes: 60 }, assessed: { governor: { outcome: 'proceed' } } }),
    send: async () => { providerCalls++; throw new Error('must not send'); },
    approve: async () => { approvalCalls++; throw new Error('must not authorize dispatch'); },
  } });
  Object.assign(svc.store, {
    lock: async fn => fn(), program: async () => program, expire: async () => {},
    counts: async () => ({ today: 0, total: 0, uncertain: 0 }), envelope: async () => envelope, items: async () => items,
    health: async () => {}, candidateOwnership: async () => null, suppression: async () => null,
    event: async (type, key, payload) => events.push({ type, key, payload }),
    appendToEnvelope: async (_envelope, selected) => {
      for (const entry of selected) items.push({ id: entry.candidateId, status: 'pending', snapshot: entry });
      envelope.manifest.push(...selected); return envelope;
    },
  });
  return { svc, items, events, program, source, crm, envelope, providerCalls: () => providerCalls, approvalCalls: () => approvalCalls };
}

test('explicit preparation-only grant prepares a bound item while normal tick and provider remain disabled', async t => {
  switches(t);
  const f = fixture();
  const result = await f.svc.runPreparationRefill();
  assert.equal(result.preparedAdded, 1);
  assert.equal(result.sendingEnabled, false);
  assert.equal(result.preparationEnabled, true);
  assert.equal(f.items.length, 1);
  const bound = f.items[0].snapshot;
  assert.equal(bound.prospectId, f.crm.id);
  assert.equal(bound.companyId, f.crm.company_id);
  assert.equal(bound.email, f.crm.email);
  assert.equal(bound.message.companyId, f.crm.company_id);
  assert.equal(bound.message.companyName, f.crm.company_name);
  assert.equal(bound.message.candidateId, f.crm.id);
  assert.equal((await f.svc.tick()).halted, 'environment_kill_switch');
  assert.equal(f.providerCalls(), 0);
  assert.equal(f.approvalCalls(), 0);
  const audit = f.events.find(x => x.type === 'preparation_refill_evaluated');
  assert.equal(audit.payload.programId, f.program.id);
  assert.equal(audit.payload.preparedAdded, 1);
  assert.equal(audit.payload.preparationDecisions[0].outcome, 'selected');
});

test('preparation defaults off and requires an explicit sending hold; it never enables another tenant', async t => {
  switches(t, null);
  const f = fixture();
  assert.equal((await f.svc.runPreparationRefill()).prepareSkippedReason, 'environment_kill_switch');
  process.env.ANCHOR_GOVERNED_PREPARATION_ENABLED = 'true';
  assert.equal(governedOutboundPreparationEnabledForTenant('10'), true);
  assert.equal(governedOutboundEnabledForTenant('10'), false);
  assert.equal(governedOutboundPreparationEnabledForTenant('13'), false);
  delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  assert.equal(governedOutboundPreparationEnabledForTenant('10'), false);
  assert.equal(f.items.length, 0);
});

test('held preparation records one valid terminal reason for missing evidence and mismatched company domains', async t => {
  switches(t);
  for (const [extra, reason] of [[{ enrichment_provenance: {} }, 'missing_email_provenance'],
    [{ email: 'support@unrelated.example' }, 'recipient_company_domain_mismatch']]) {
    const f = fixture(extra);
    const result = await f.svc.runPreparationRefill();
    assert.equal(result.prepareSkippedReason, 'no_clean_inventory');
    assert.equal(result.preparedAdded, 0);
    assert.deepEqual(result.preparationDecisions.map(x => x.reason), [reason]);
    assert.equal(f.events.find(x => x.type === 'preparation_refill_evaluated').payload.preparationDecisions.length, 1);
    assert.equal(f.items.length, 0);
    assert.equal(f.providerCalls(), 0);
  }
});

test('preparation-only configuration preserves program pause, expiration, uncertain-attempt and suppression gates', async t => {
  switches(t);
  for (const [change, reason] of [
    [f => { f.program.mode = 'paused'; }, 'program_disabled'],
    [f => { f.program.policy.expiresAt = '2026-10-01T16:00:00Z'; f.program.policy_hash = hash(f.program.policy); }, 'authorization_expired'],
    [f => { f.svc.store.counts = async () => ({ today: 0, total: 0, uncertain: 1 }); }, 'uncertain_send_requires_reconciliation'],
    [f => { f.svc.store.suppression = async () => 'ao_owned'; }, 'no_clean_inventory'],
  ]) {
    const f = fixture(); change(f);
    assert.equal((await f.svc.runPreparationRefill()).prepareSkippedReason, reason);
    assert.equal(f.items.length, 0); assert.equal(f.providerCalls(), 0);
  }
});

test('two same-hour critical control cycles retain separate fresh summaries and exact exclusion reasons', async t => {
  switches(t);
  const f = fixture(); const durableEvents = new Map();
  const store = { tenantId: '10', clientId: 10, envelope: async () => null,
    event: async (type, key, payload) => { if (!durableEvents.has(hash([type, key]))) durableEvents.set(hash([type, key]), payload); } };
  const options = {
    pool: {}, tenantId: '10', store, now, program: f.program, source: f.source, timestamps: {}, funnel: {},
    inventory: { clean: [], excluded: [{ prospectId: 'p1', reason: 'missing_email_provenance' }], scope: {}, exclusionCounts: { missing_email_provenance: 1 } },
    infrastructure: { cap: 5, snapshot: { sentToday: 0 }, assessed: { governor: { outcome: 'proceed' } },
      operating: { dispatchCapacityNow: 5, planningDailyCapacity: 5, recommendedSafeDailyCapacity: 5, governor: 'proceed' } },
    scoutRamp: async () => null,
  };
  options.inventoryAfter = options.inventory;
  const first = await runMaxOutboundControlLoop(options);
  const second = await runMaxOutboundControlLoop(options);
  assert.notEqual(first.cycleId, second.cycleId);
  assert.equal(durableEvents.size, 2);
  for (const event of durableEvents.values()) {
    assert.ok(event.cycleStartedAt); assert.ok(event.cycleCompletedAt);
    assert.equal(event.sendingEnabled, false); assert.equal(event.preparationEnabled, true);
    assert.equal(event.prepareSkippedReason, 'no_clean_inventory');
    assert.deepEqual(event.preparationDecisions.map(x => x.reason), ['missing_email_provenance']);
  }
  await runMaxOutboundControlLoop({ ...options, cycleId: first.cycleId });
  assert.equal(durableEvents.size, 2, 'replay of the same cycle remains idempotent');
});
