'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { policy, clock, windowReason, candidateReason, nextAction, hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const now = new Date('2026-09-18T14:00:00Z');
const input = { tenantId: '10', sourceMissionId: 'source', senderEmail: 'sender@anchor.example',
  inboxIntegrationId: 'inbox', aoOwnerIds: [1], startsAt: now.toISOString(), expiresAt: '2026-10-01T00:00:00Z' };

test('authorization is finite and cannot widen tenant, caps, hours, sequence or spacing', () => {
  const p = policy(input, now);
  assert.equal(p.dailyCap, 5); assert.equal(p.totalCap, 100); assert.equal(p.maxSequenceStep, 1);
  for (const change of [{ tenantId: '11' }, { dailyCap: 6 }, { totalCap: 101 }, { spacingMinutes: 0 },
    { dailyCap: 1.5 }, { expiresAt: '2027-01-01' }, { aoOwnerIds: [] }, { inboxIntegrationId: '' }]) {
    assert.throws(() => policy({ ...input, ...change }, now));
  }
  assert.deepEqual(policy({ ...input, weekdays: [0,6], startHour: 0 }, now).weekdays, [1,2,3,4,5]);
});
test('New York calendar closes weekends and 17:00, handles DST and expiration', () => {
  const p = policy(input, now);
  assert.equal(windowReason(p, now), null);
  assert.equal(windowReason(p, new Date('2026-09-18T21:00:00Z')), 'outside_business_hours');
  assert.equal(windowReason(p, new Date('2026-09-19T14:00:00Z')), 'weekend');
  assert.equal(windowReason(p, new Date('2026-10-01T00:00:00Z')), 'authorization_expired');
  assert.equal(clock(new Date('2026-11-02T14:00:00Z')).hour, 9);
  assert.equal(clock(new Date('2026-03-09T13:00:00Z')).hour, 9);
});
test('verified and projectable email, exact binding and safe Paige copy are all required', () => {
  const item = { email: 'ops@customer.example', sendable: true, paige: { candidateId: 'company' } };
  const crm = { email: item.email, email_verified: true, email_status: 'valid', do_not_contact: false,
    enrichment_provenance: { email: { source: 'website_email' } } };
  const message = { subject: 'Cleaning support', body: 'Would a written quote help?', candidateId: 'company' };
  assert.equal(candidateReason(item, crm, message), null);
  for (const change of [{ email_verified: false }, { email_status: 'catch_all' }, { do_not_contact: true },
    { email: 'other@customer.example' }, { email_provenance_source: 'pattern_first' }]) {
    assert.ok(candidateReason(item, { ...crm, ...change }, message));
  }
  assert.equal(candidateReason(item, crm, { ...message, body: 'Mission focus: internal' }), 'unsafe_paige_copy');
  assert.equal(candidateReason(item, crm, { ...message, candidateId: 'other' }), 'copy_binding_changed');
});
test('Max routes classified replies to human or paused lifecycle paths', () => {
  assert.deepEqual(nextAction('interested'), ['engaged', 'ao_handoff']);
  assert.deepEqual(nextAction('quote_request'), ['quote_requested', 'ao_handoff']);
  assert.deepEqual(nextAction('incumbent_vendor'), ['incumbent_vendor', 'ao_handoff']);
  assert.deepEqual(nextAction('unsubscribe'), ['dnc', 'stop']);
  for (const kind of ['not_now', 'wrong_person', 'out_of_office', 'unknown']) assert.notEqual(nextAction(kind)[1], 'send');
  assert.equal(hash({ a: 1, b: 2 }), hash({ b: 2, a: 1 }));
});

test('canonical approval binds the daily manifest and plain execution cannot use it', async () => {
  const amo = require('../packages/acquisition-mission');
  const steps = require('../packages/max/workspace/AmoOperatorApproval');
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.', targetSegment: 'Law Firms' });
  const opts = () => ({ engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.', allowFixtureFallback: true });
  for (const step of ['advancePlanAfterApproval','advanceDiscoveryAfterApproval','advancePrioritizationAfterApproval',
    'advanceMaxPrioritization','advanceAcquisitionApproach','advancePaigeVariants','advanceEmmettCapacity']) await steps[step](opts());
  // Supply verified recipients to the in-memory fixture before its approval.
  const fixtureCapacity = amo.findEmmettCapacity(engine.inspect(mission.id, { tenantId: '10' }).contributions);
  for (const item of fixtureCapacity.payload.queue.items) { item.email = 'ops@law.example'; item.sendable = true; }
  engine.store.updateContribution(fixtureCapacity.id, fixtureCapacity);
  const binding = { id: 'daily-1', candidateIds: ['co-harbor'], manifestHash: 'digest' };
  const approval = await steps.advanceExecutionAfterApproval({ ...opts(), governedApproval: binding });
  assert.deepEqual(approval.approval.payload.dailyEnvelope, binding);
  const snapshot = engine.inspect(mission.id, { tenantId: '10' });
  let calls = 0;
  const send = require('../packages/max/workspace/OutboundExecutionAdapter').executeOutboundBundle;
  const input = { mission: snapshot.mission, contributions: snapshot.contributions, approval: approval.approval,
    tenantId: '10', canonicalSender: { ...require('../packages/max/workspace/EmmettCapacityExecution').FIXTURE_CANONICAL_SENDER, tenantId: '10', clientId: 10 },
    senderReadiness: { ready: true }, resolveProspectAttributes: () => ({ email: 'ops@law.example' }),
    sendEmail: async () => { calls++; return { success: true, providerMessageId: 'mock' }; } };
  assert.equal((await send(input)).blockCode, 'governed_executor_required');
  assert.equal(calls, 0);
  input.sendEmail.beforeAttempt = async () => {};
  const result = await send({ ...input, governedEnvelopeId: 'daily-1' });
  assert.equal(result.summary.sent, 1, JSON.stringify(result.records)); assert.equal(calls, 1);
  // A partial tick must remain executable when delivery/open events arrive.
  const multiApproval = structuredClone(approval.approval);
  multiApproval.payload.dailyEnvelope.candidateIds.push('co-granite');
  engine.store.updateContribution(multiApproval.id, multiApproval);
  const run = require('../packages/max/workspace/EmmettOutboundExecution').runExecuteOutboundForAmoMission;
  const partial = await run({ ...input, engine, mission: snapshot.mission, governedEnvelopeId: 'daily-1', prospectId: 'co-harbor' });
  assert.equal(partial.summary.complete, false);
  const rest = await run({ ...input, engine, mission: snapshot.mission, governedEnvelopeId: 'daily-1', prospectId: 'co-granite' });
  assert.equal(rest.summary.complete, true);
});

test('production preparation runs Scout, enrichment, Max, Paige and Emmett through canonical mission stages', async () => {
  const amo = require('../packages/acquisition-mission');
  const steps = require('../packages/max/workspace/AmoOperatorApproval');
  const emmett = require('../packages/max/workspace/EmmettCapacityExecution');
  const { adapters } = require('../services/governedOutboundAdapters');
  const { missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
  const engine = amo.createAcquisitionMissionEngine();
  const source = engine.create({ tenantId: '10', objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.', targetSegment: 'Law Firms' });
  await steps.advancePlanAfterApproval({ engine, mission: source, tenantId: '10', question: 'Approved.' });
  const snapshot = engine.inspect(source.id, { tenantId: '10' });
  const sender = { ...emmett.FIXTURE_CANONICAL_SENDER, tenantId: '10', clientId: 10 };
  const program = { id: 'test-program', authorized_by: 'jake', scope_hash: hash(missionScope(snapshot.mission)),
    policy: { enrichmentLimit: 15, preparationAttemptsPerDay: 3, senderEmail: sender.senderEmail } };
  const contacts = Object.fromEntries(['co-harbor','co-granite'].map(id => [id, {
    prospect_id: id, company_id: id, email: `ops@${id}.example`, email_verified: true, email_status: 'valid',
    email_provenance_source: 'website_email', do_not_contact: false,
  }]));
  let enriched = 0;
  const events = [];
  const adapter = adapters({ query: async () => ({ rows: [] }) }, {
    persist: false,
    runtime: { engine: () => engine, create: async input => engine.create(input) },
    loadMission: async id => engine.inspect(id, { tenantId: '10' }),
    contact: async id => contacts[id],
    tenant: async () => ({ sender }),
    infrastructure: async () => ({ snapshot: emmett.fixtureInfrastructureSnapshot('10') }),
    runScout: async () => steps.fixtureScoutDiscoveryResult(),
    admission: { ensureMissionBoundCrmSchema: async () => {}, admitMissionBoundCandidate: async () => {} },
    enrich: async () => { enriched++; },
    runEmmett: (mission, opts) => emmett.runEmmettForAmoMission(mission, { ...opts, runEmmett: undefined, crmByProspectId: contacts }),
  });
  const store = { one: async () => ({ attempts: 0 }), suppression: async () => null,
    event: async (type, _key, data) => events.push({ type, ...data }) };
  const daily = await adapter.prepare(program, snapshot, '2026-09-18', store);
  assert.equal(daily.mission.stage, 'ready');
  assert.equal(enriched, 2);
  const ready = await adapter.prepared(daily, program);
  assert.equal(ready.candidates.length, 2);
  assert.ok(ready.candidates.every(row => candidateReason(row.item, contacts[row.candidateId], row.message) === null));
  assert.equal(events.find(row => row.type === 'inventory_replenished').eligible, 2);
});

test('cron rejects missing or wrong secrets, exposes only POST, and awaits the bounded worker', async () => {
  const router = require('../routes/cron');
  const worker = require('../anchorDailyOutboundCron');
  const route = router.stack.find(layer => layer.route?.path === '/cron/anchor-daily-outbound').route;
  assert.equal(route.methods.post, true); assert.equal(route.methods.get, undefined);
  const previous = process.env.CRON_SECRET;
  const original = worker.run;
  let calls = 0;
  worker.run = async () => { calls++; return { halted: 'no_program' }; };
  const response = () => ({ statusCode: 200, status(n) { this.statusCode=n; return this; },
    set() { return this; }, json(value) { this.body=value; return this; } });
  try {
    for (const [secret, supplied] of [['',''], ['test-secret','wrong'], ['test-secret','Bearer test-secret']]) {
      process.env.CRON_SECRET=secret;
      const res=response();
      await route.stack[0].handle({ get: () => supplied }, res);
      assert.equal(res.statusCode, supplied==='Bearer test-secret' ? 200 : 401);
    }
    assert.equal(calls, 1);
  } finally {
    worker.run=original;
    if (previous === undefined) delete process.env.CRON_SECRET; else process.env.CRON_SECRET=previous;
  }
});
