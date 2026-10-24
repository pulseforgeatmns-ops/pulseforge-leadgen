'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { policy, hash, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { service } = require('../services/governedOutbound');
const { validateGovernedSchedule } = require('../services/governedTenantSchedule');

const now = new Date('2026-10-13T15:00:00Z');
const input = {
  tenantId: '17', sourceMissionId: 'substral-source',
  senderEmail: 'sender@substral.example', inboxIntegrationId: 'substral-mailbox',
  sendingIdentityId: 'substral-identity',
  allowedContactClassifications: ['VERIFIED_ROLE_EMAIL'],
  dailyCap: 1, totalCap: 10, spacingMinutes: 240, startHour: 10, endHour: 16,
  startsAt: now.toISOString(), expiresAt: '2026-10-20T15:00:00Z',
};

test('Substral policy binds explicit contact authority and mailbox identity within existing limits', () => {
  const grant = policy({ ...input, tenantId: 17, senderEmail: ' SENDER@substral.example ',
    sendingIdentityId: ' substral-identity ', allowedContactClassifications: ['VERIFIED_ROLE_EMAIL', 'VERIFIED_ROLE_EMAIL'] }, now);
  assert.equal(grant.tenantId, '17');
  assert.equal(grant.senderEmail, input.senderEmail);
  assert.equal(grant.inboxIntegrationId, input.inboxIntegrationId);
  assert.equal(grant.sendingIdentityId, input.sendingIdentityId);
  assert.deepEqual(grant.allowedContactClassifications, ['VERIFIED_ROLE_EMAIL']);
  assert.deepEqual(grant.aoOwnerIds, []);
  assert.equal(grant.startHour, 10);
  assert.equal(grant.endHour, 16);
  assert.equal(grant.spacingMinutes, 240);
  assert.equal(grant.maxSequenceStep, 1);
  assert.equal(grant.dailyCap, 1);
  assert.equal(grant.totalCap, 10);
  const delegated = policy({ ...input, operatorDelegatedMaximumDailyCapacity: 5 }, now);
  assert.equal(delegated.dailyCap, 1);
  assert.equal(delegated.operatorDelegatedMaximumDailyCapacity, 5);
});

test('Substral never defaults or widens the operator-reviewed contact classification list', () => {
  for (const allowedContactClassifications of [undefined, null, [], 'VERIFIED_FOUNDER_EMAIL', {},
    ['UNRESOLVED'], ['VERIFIED_FOUNDER_EMAIL', 'REVIEW_REQUIRED']]) {
    assert.throws(() => policy({ ...input, allowedContactClassifications }, now), { code: 'invalid_contact_classifications' });
  }
  for (const allowedContactClassifications of [['VERIFIED_FOUNDER_EMAIL'], ['VERIFIED_ROLE_EMAIL'],
    ['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL']]) {
    assert.deepEqual(policy({ ...input, allowedContactClassifications }, now).allowedContactClassifications, allowedContactClassifications);
  }
});

test('Substral rejects missing identity, invalid scope, window, caps, spacing and expiration', () => {
  for (const sendingIdentityId of [undefined, null, '', '   ']) {
    assert.throws(() => policy({ ...input, sendingIdentityId }, now), { code: 'sending_identity_required' });
  }
  for (const change of [{ tenantId: '99' }, { sourceMissionId: '' }, { inboxIntegrationId: '' }, { senderEmail: 'invalid' }]) {
    assert.throws(() => policy({ ...input, ...change }, now), { code: 'invalid_governed_outbound_scope' });
  }
  for (const change of [{ startHour: 8 }, { endHour: 18 }, { startHour: 16 }, { endHour: 10 }, { startHour: 9.5 }]) {
    assert.throws(() => policy({ ...input, ...change }, now), { code: 'invalid_business_window' });
  }
  for (const change of [{ dailyCap: 0 }, { dailyCap: 51 }, { dailyCap: 1.5 }, { dailyCap: 11 },
    { totalCap: 0 }, { totalCap: 101 }, { totalCap: 1.5 }, { spacingMinutes: 59 }, { spacingMinutes: 241 },
    { spacingMinutes: 60.5 }, { operatorDelegatedMaximumDailyCapacity: 0 },
    { operatorDelegatedMaximumDailyCapacity: 51 }, { operatorDelegatedMaximumDailyCapacity: 11 },
    { operatorDelegatedMaximumDailyCapacity: 1.5 }]) {
    assert.throws(() => policy({ ...input, ...change }, now), { code: 'invalid_bounds' });
  }
  for (const change of [{ expiresAt: input.startsAt }, { expiresAt: '2026-12-01T00:00:00Z' },
    { startsAt: '2026-10-01T00:00:00Z', expiresAt: '2026-10-12T00:00:00Z' }]) {
    assert.throws(() => policy({ ...input, ...change }, now), { code: 'invalid_expiry' });
  }
  assert.throws(() => policy({ ...input, aoOwnerIds: [-1] }, now), { code: 'ao_owners_required' });
});

test('Substral keeps fixed weekday, timezone and first-touch constraints', () => {
  const grant = policy({ ...input, weekdays: [0, 6], timeZone: 'UTC', maxSequenceStep: 4 }, now);
  assert.deepEqual(grant.weekdays, [1, 2, 3, 4, 5]);
  assert.equal(grant.timeZone, 'America/New_York');
  assert.equal(grant.maxSequenceStep, 1);
  assert.equal(windowReason(grant, now), null);
  assert.equal(windowReason(grant, new Date('2026-10-14T13:00:00Z')), 'outside_business_hours');
  assert.equal(windowReason(grant, new Date('2026-10-14T20:00:00Z')), 'outside_business_hours');
  assert.equal(windowReason(grant, new Date('2026-10-17T15:00:00Z')), 'weekend');
  assert.equal(windowReason(grant, new Date(input.expiresAt)), 'authorization_expired');
});

test('Anchor defaults and Babrun founder-only mailbox policy are unchanged', () => {
  const base = { sourceMissionId: 'source', senderEmail: 'sender@business.example', inboxIntegrationId: 'inbox',
    startsAt: now.toISOString(), expiresAt: new Date(input.expiresAt).toISOString() };
  const common = { ...base, aoOwnerIds: [], dailyCap: 5, totalCap: 100, spacingMinutes: 60,
    timeZone: 'America/New_York', weekdays: [1, 2, 3, 4, 5], startHour: 9, endHour: 17,
    maxSequenceStep: 1, enrichmentLimit: 15, preparationAttemptsPerDay: 3 };
  assert.deepEqual(policy({ ...base, tenantId: '10', aoOwnerIds: [1], startHour: 0, endHour: 24 }, now),
    { ...common, tenantId: '10', aoOwnerIds: [1] });
  assert.throws(() => policy({ ...base, tenantId: '10' }, now), { code: 'ao_owners_required' });
  assert.deepEqual(policy({ ...base, tenantId: '13', sendingIdentityId: 'babrun-identity' }, now),
    { ...common, tenantId: '13', sendingIdentityId: 'babrun-identity', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] });
  assert.throws(() => policy({ ...base, tenantId: '13' }, now), { code: 'sending_identity_required' });
  assert.throws(() => policy({ ...base, tenantId: '13', sendingIdentityId: 'babrun-identity', endHour: 24 }, now),
    { code: 'invalid_business_window' });
});

test('Substral authorization still requires an operator, ready mission and exact reviewed policy hash', async () => {
  const source = { id: input.sourceMissionId, tenantId: '17', stage: 'ready',
    objective: 'Reviewed Studio Substral outreach', structuredMission: { immutable: true } };
  const created = [];
  const svc = service({ tenantId: '17', now: () => now,
    pool: { query: async () => assert.fail('No database calls allowed in this test') },
    adapters: { loadMission: async () => ({ mission: source }) } });
  svc.store.createProgram = async (...args) => { created.push(args); return { id: 'test-program' }; };
  const actor = { id: 'reviewer', role: 'manager' };
  await assert.rejects(svc.authorize(input, { id: 'viewer', role: 'viewer' }), { code: 'operator_required' });
  source.stage = 'draft';
  await assert.rejects(svc.authorize(input, actor), { code: 'ready_source_mission_required' });
  source.stage = 'ready';
  const review = await svc.authorize(input, actor);
  assert.equal(review.reviewRequired, true);
  assert.equal(created.length, 0);
  for (const change of [{ allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] },
    { sendingIdentityId: 'different-identity' }, { inboxIntegrationId: 'different-mailbox' },
    { senderEmail: 'different@substral.example' }, { endHour: 15 }, { dailyCap: 2 }]) {
    const changed = await svc.authorize({ ...input, ...change, reviewHash: review.reviewHash }, actor);
    assert.equal(changed.reviewRequired, true);
    assert.notEqual(changed.reviewHash, review.reviewHash);
    assert.equal(created.length, 0);
  }
  assert.deepEqual(await svc.authorize({ ...input, reviewHash: review.reviewHash }, actor), { id: 'test-program' });
  assert.deepEqual(created, [[review.policy, review.scopeHash, actor.id]]);
});

test('Substral durable schedule rejects a changed identity or mailbox before any provider boundary', async t => {
  for (const name of ['SUBSTRAL_GOVERNED_OUTBOUND_ENABLED', 'TENANT_EMMETT_CAPACITY_ENABLED']) {
    const prior = process.env[name];
    process.env[name] = 'true';
    t.after(() => { if (prior === undefined) delete process.env[name]; else process.env[name] = prior; });
  }
  const grant = policy(input, now);
  const program = { id: 'program', tenant_id: '17', mode: 'active', policy: grant, policy_hash: hash(grant) };
  for (const change of [{ sendingIdentityId: 'wrong-identity' }, { mailboxIntegrationId: 'wrong-mailbox' }]) {
    const schedule = { tenantId: '17', sendingIdentityId: change.sendingIdentityId || grant.sendingIdentityId,
      authorizationSnapshot: { governed: { programId: program.id, policyHash: program.policy_hash,
        mailboxIntegrationId: change.mailboxIntegrationId || grant.inboxIntegrationId } } };
    await assert.rejects(validateGovernedSchedule(schedule, { now, pool: {},
      governedStore: { program: async () => program, envelope: async () => assert.fail('Identity must be checked first') } }),
    { code: 'governed_identity_changed' });
  }
});
