'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  MESSAGE_STATUS,
  SEQUENCE_STATE,
  sendTenantEmail,
} = require('../services/tenantMailbox');

const {
  SCHEDULE_STATUS,
  PAST_DUE_POLICY,
  MemoryScheduleStore,
  authorizeScheduledOutreachSend,
  cancelScheduledOutreachSend,
  executeScheduledSend,
  executeDueScheduledSends,
  buildIdempotencyKey,
  isPastDueWindowExceeded,
} = require('../services/tenantOutreachScheduler');

const BABRUN_CANARY = Object.freeze({
  tenantId: '13',
  prospectId: '12264293-ba4d-494d-a1e7-f93ed1f62a2c',
  akObjectId: 'ak_babrun_prospect_p024',
  outreachAssetId: 'ak_babrun_outreach_final_05',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  recipientEmail: 'kaylee@kbpainting.com',
  missedScheduledFor: '2026-09-14T14:00:00.000Z',
});

function fakeSecretResolver(ref) {
  return { SMTP_PASSWORD_REF: 'dummy-smtp-password' }[ref];
}

function fakeTransport(result = {}) {
  const calls = [];
  return {
    calls,
    async sendMail(payload) {
      calls.push(payload);
      if (result.error) throw result.error;
      return { messageId: result.messageId || '<provider-msg@example.com>' };
    },
    async verify() {
      return true;
    },
  };
}

async function seedMailbox(overrides = {}) {
  const store = new MemoryTenantMailboxStore();
  const integration = await store.saveIntegration({
    id: overrides.integrationId || 'tmi_tenant_a',
    tenantId: overrides.tenantId || 'tenant-a',
    providerType: 'GENERIC_SMTP_IMAP',
    mailboxAddress: 'hello@example.com',
    smtpHost: 'smtp.example.com',
    smtpPort: 465,
    smtpTlsMode: 'SSL_TLS',
    smtpSecretRef: 'SMTP_PASSWORD_REF',
    status: MAILBOX_STATUS.ACTIVE,
  });
  const identity = await store.saveIdentity({
    id: overrides.identityId || 'tsi_tenant_a',
    tenantId: overrides.tenantId || 'tenant-a',
    mailboxIntegrationId: integration.id,
    senderEmail: 'hello@example.com',
    senderDisplayName: 'Sender',
    status: IDENTITY_STATUS.ACTIVE,
  });
  return { store, integration, identity };
}

async function seedScheduler(overrides = {}) {
  const mailbox = await seedMailbox(overrides);
  const scheduleStore = new MemoryScheduleStore({
    tenants: [{ tenantId: overrides.tenantId || 'tenant-a', active: true }],
    prospects: [{
      tenantId: overrides.tenantId || 'tenant-a',
      prospectId: overrides.prospectId || 'prospect-1',
      email: overrides.recipientEmail || 'buyer@example.com',
      doNotContact: false,
      booked: false,
      active: true,
    }],
    outreachAssets: [{
      tenantId: overrides.tenantId || 'tenant-a',
      id: overrides.outreachAssetId || 'asset-1',
      lifecycleState: 'STAKEHOLDER_VALIDATED',
    }],
  });
  return { ...mailbox, scheduleStore };
}

function dueSoon(baseNow = '2026-09-14T14:00:00.000Z', minutesAgo = 5) {
  const ms = new Date(baseNow).getTime() - minutesAgo * 60 * 1000;
  return new Date(ms).toISOString();
}

async function authorizeFixture(overrides = {}, opts = {}) {
  const executionNow = opts.now || overrides.executionNow || '2026-09-14T14:00:00.000Z';
  const seeded = overrides.seeded || await seedScheduler(overrides);
  const scheduledFor = overrides.scheduledFor || dueSoon(executionNow, 5);
  const result = await authorizeScheduledOutreachSend({
    tenantId: overrides.tenantId || 'tenant-a',
    prospectId: overrides.prospectId || 'prospect-1',
    outreachAssetId: overrides.outreachAssetId || 'asset-1',
    sendingIdentityId: seeded.identity.id,
    recipientEmail: overrides.recipientEmail || 'buyer@example.com',
    scheduledFor,
    timezone: 'America/New_York',
    subject: overrides.subject || 'Worth a look?',
    body: overrides.body || 'Hello from outreach.',
    authorizationSource: 'operator_test',
    authorizedBy: 'operator@test.com',
    sequenceStep: overrides.sequenceStep || 1,
    idempotencyKey: overrides.idempotencyKey,
    pastDuePolicy: overrides.pastDuePolicy,
    maxLatenessMinutes: overrides.maxLatenessMinutes ?? 120,
    threadId: overrides.threadId,
    outreachAssetVersion: overrides.outreachAssetVersion || 'v1',
  }, {
    scheduleStore: seeded.scheduleStore,
    mailboxStore: seeded.store,
    now: opts.authNow || '2026-09-14T12:00:00.000Z',
  });
  return { ...seeded, authorizeResult: result, scheduledFor, executionNow };
}

describe('SPEC-252 tenant outreach scheduling', () => {
  it('creates schedule from explicit authorization with frozen snapshot', async () => {
    const { authorizeResult } = await authorizeFixture();
    assert.equal(authorizeResult.created, true);
    assert.equal(authorizeResult.schedule.status, SCHEDULE_STATUS.SCHEDULED);
    assert.equal(authorizeResult.schedule.authorizationSnapshot.subject, 'Worth a look?');
    assert.equal(authorizeResult.schedule.authorizationSnapshot.outreachAssetId, 'asset-1');
    assert.equal(authorizeResult.schedule.authorizedBy, 'operator@test.com');
    assert.ok(authorizeResult.schedule.idempotencyKey);
  });

  it('enforces tenant isolation on authorization identity lookup', async () => {
    const seeded = await seedScheduler({ tenantId: 'tenant-a' });
    await assert.rejects(
      () => authorizeScheduledOutreachSend({
        tenantId: 'tenant-b',
        prospectId: 'prospect-1',
        outreachAssetId: 'asset-1',
        sendingIdentityId: seeded.identity.id,
        recipientEmail: 'buyer@example.com',
        scheduledFor: '2026-09-15T14:00:00.000Z',
        subject: 'Hi',
        body: 'Body',
        authorizationSource: 'test',
        authorizedBy: 'op',
      }, { scheduleStore: seeded.scheduleStore, mailboxStore: seeded.store }),
      (err) => err.code === 'sending_identity_tenant_mismatch'
    );
  });

  it('rejects duplicate idempotency key for same tenant', async () => {
    const first = await authorizeFixture({ idempotencyKey: 'dup-key-1' });
    const second = await authorizeScheduledOutreachSend({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-1',
      sendingIdentityId: first.identity.id,
      recipientEmail: 'buyer@example.com',
      scheduledFor: '2026-09-16T14:00:00.000Z',
      subject: 'Other',
      body: 'Other body',
      authorizationSource: 'operator_test',
      authorizedBy: 'operator@test.com',
      idempotencyKey: 'dup-key-1',
    }, { scheduleStore: first.scheduleStore, mailboxStore: first.store });
    assert.equal(second.duplicate, true);
    assert.equal(second.schedule.id, first.authorizeResult.schedule.id);
  });

  it('executes due send and links canonical outbound message', async () => {
    const seeded = await authorizeFixture({});
    const transport = fakeTransport();
    const outcome = await executeScheduledSend(seeded.authorizeResult.schedule, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: seeded.executionNow,
    });
    assert.equal(outcome.result, 'sent');
    assert.equal(outcome.schedule.status, SCHEDULE_STATUS.SENT);
    assert.ok(outcome.schedule.outboundMessageId);
    assert.equal(transport.calls.length, 1);
    assert.deepEqual(transport.calls[0].to, ['buyer@example.com']);
  });

  it('does not execute future send early', async () => {
    const seeded = await authorizeFixture({ scheduledFor: '2026-09-16T14:00:00.000Z' });
    const transport = fakeTransport();
    const claimed = await seeded.scheduleStore.claimDueSchedules({
      now: '2026-09-14T14:00:00.000Z',
      limit: 10,
      claimToken: 'claim-1',
    });
    assert.equal(claimed.length, 0);
    const outcome = await executeScheduledSend(seeded.authorizeResult.schedule, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: '2026-09-14T14:00:00.000Z',
    });
    assert.equal(outcome.result, 'defer');
    assert.equal(transport.calls.length, 0);
  });

  it('preserves exact outreach asset version in authorization snapshot', async () => {
    const { authorizeResult } = await authorizeFixture({ outreachAssetVersion: 'final_05_rev2' });
    assert.equal(authorizeResult.schedule.outreachAssetVersion, 'final_05_rev2');
    assert.equal(authorizeResult.schedule.authorizationSnapshot.outreachAssetVersion, 'final_05_rev2');
  });

  it('atomic claim prevents overlapping executor from double-processing same row', async () => {
    const seeded = await authorizeFixture({});
    const firstClaim = await seeded.scheduleStore.claimDueSchedules({
      now: seeded.executionNow,
      limit: 10,
      claimToken: 'claim-a',
    });
    const secondClaim = await seeded.scheduleStore.claimDueSchedules({
      now: seeded.executionNow,
      limit: 10,
      claimToken: 'claim-b',
    });
    assert.equal(firstClaim.length, 1);
    assert.equal(secondClaim.length, 0);
  });

  it('overlapping executor runs do not double-send', async () => {
    const seeded = await authorizeFixture({ idempotencyKey: 'send-once' });
    const transport = fakeTransport();
    const [runA, runB] = await Promise.all([
      executeDueScheduledSends({
        scheduleStore: seeded.scheduleStore,
        mailboxStore: seeded.store,
        transport,
        secretResolver: fakeSecretResolver,
        now: seeded.executionNow,
        lockClient: { query: async () => ({ rows: [{ locked: true }] }), release() {} },
        query: async () => ({ rows: [] }),
      }),
      executeDueScheduledSends({
        scheduleStore: seeded.scheduleStore,
        mailboxStore: seeded.store,
        transport,
        secretResolver: fakeSecretResolver,
        now: seeded.executionNow,
        lockClient: { query: async () => ({ rows: [{ locked: false }] }), release() {} },
        query: async () => ({ rows: [] }),
      }),
    ]);
    assert.equal(transport.calls.length, 1);
    assert.ok(runA.sent + runB.sent <= 1);
  });

  it('send failure marks FAILED without false SENT', async () => {
    const seeded = await authorizeFixture({});
    const claimed = await seeded.scheduleStore.claimDueSchedules({
      now: seeded.executionNow,
      limit: 1,
      claimToken: 'claim-fail',
    });
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport: fakeTransport({ error: Object.assign(new Error('smtp down'), { code: 'smtp_send_failed' }) }),
      secretResolver: fakeSecretResolver,
      now: seeded.executionNow,
    });
    assert.equal(outcome.result, 'failed');
    assert.equal(outcome.schedule.status, SCHEDULE_STATUS.FAILED);
    assert.notEqual(outcome.schedule.status, SCHEDULE_STATUS.SENT);
  });

  it('provider success + interruption recovers without double-send on retry', async () => {
    const executionNow = '2026-09-14T14:00:00.000Z';
    const seeded = await seedScheduler();
    const transport = fakeTransport();
    const key = 'crash-recovery-key';
    await sendTenantEmail({
      tenantId: 'tenant-a',
      sendingIdentityId: seeded.identity.id,
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-1',
      to: 'buyer@example.com',
      subject: 'Worth a look?',
      body: 'Hello from outreach.',
      metadata: { idempotencyKey: key },
    }, {
      store: seeded.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: '2026-09-14T13:00:00.000Z',
    });
    const schedule = await seeded.scheduleStore.saveSchedule({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-1',
      sendingIdentityId: seeded.identity.id,
      recipientEmail: 'buyer@example.com',
      scheduledFor: dueSoon(executionNow, 5),
      timezone: 'America/New_York',
      status: SCHEDULE_STATUS.EXECUTING,
      authorizationSource: 'test',
      authorizedBy: 'op',
      authorizedAt: '2026-09-14T12:00:00.000Z',
      authorizationSnapshot: { subject: 'Worth a look?', body: 'Hello from outreach.', recipientEmail: 'buyer@example.com' },
      idempotencyKey: key,
      sequenceStep: 1,
      maxLatenessMinutes: 120,
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    });
    const outcome = await executeScheduledSend(schedule, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: executionNow,
    });
    assert.equal(outcome.result, 'recovered_sent');
    assert.equal(outcome.schedule.status, SCHEDULE_STATUS.SENT);
    assert.equal(transport.calls.length, 1);
  });

  it('reply before scheduled follow-up prevents send', async () => {
    const seeded = await seedScheduler();
    const thread = await seeded.store.saveThread({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      sequenceState: SEQUENCE_STATE.PAUSED,
      replyState: 'reply_received',
      currentStatus: 'replied',
    });
    const fixture = await authorizeFixture({
      threadId: thread.id,
      sequenceStep: 2,
      seeded,
    });
    const claimed = await fixture.scheduleStore.claimDueSchedules({
      now: fixture.executionNow,
      limit: 1,
      claimToken: 'claim-reply',
    });
    const transport = fakeTransport();
    assert.ok(claimed[0], 'expected claimed schedule');
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: fixture.scheduleStore,
      mailboxStore: fixture.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: fixture.executionNow,
    });
    assert.equal(outcome.result, 'skipped');
    assert.ok(['thread_replied', 'thread_sequence_paused'].includes(outcome.schedule.skipReason));
    assert.equal(transport.calls.length, 0);
  });

  it('DNC prevents send', async () => {
    const seeded = await seedScheduler();
    seeded.scheduleStore.prospects.set('tenant-a:prospect-1', {
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      email: 'buyer@example.com',
      doNotContact: true,
    });
    const fixture = await authorizeFixture({ seeded });
    const claimed = await fixture.scheduleStore.claimDueSchedules({
      now: fixture.executionNow,
      limit: 1,
      claimToken: 'claim-dnc',
    });
    assert.ok(claimed[0]);
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: fixture.scheduleStore,
      mailboxStore: fixture.store,
      transport: fakeTransport(),
      secretResolver: fakeSecretResolver,
      now: fixture.executionNow,
    });
    assert.equal(outcome.result, 'skipped');
    assert.equal(outcome.schedule.skipReason, 'prospect_dnc');
  });

  it('hard bounce suppression prevents send', async () => {
    const seeded = await seedScheduler();
    await seeded.store.suppress({
      tenantId: 'tenant-a',
      email: 'buyer@example.com',
      reason: 'bounce',
      source: 'provider',
    });
    const fixture = await authorizeFixture({ seeded });
    const claimed = await fixture.scheduleStore.claimDueSchedules({
      now: fixture.executionNow,
      limit: 1,
      claimToken: 'claim-bounce',
    });
    assert.ok(claimed[0]);
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: fixture.scheduleStore,
      mailboxStore: fixture.store,
      transport: fakeTransport(),
      secretResolver: fakeSecretResolver,
      now: fixture.executionNow,
    });
    assert.equal(outcome.result, 'skipped');
    assert.equal(outcome.schedule.skipReason, 'hard_bounce');
  });

  it('operator cancellation prevents send', async () => {
    const seeded = await authorizeFixture({});
    await cancelScheduledOutreachSend({
      tenantId: 'tenant-a',
      scheduleId: seeded.authorizeResult.schedule.id,
    }, { scheduleStore: seeded.scheduleStore });
    const claimed = await seeded.scheduleStore.claimDueSchedules({
      now: seeded.executionNow,
      limit: 1,
      claimToken: 'claim-cancel',
    });
    assert.equal(claimed.length, 0);
    const schedule = await seeded.scheduleStore.getSchedule('tenant-a', seeded.authorizeResult.schedule.id);
    assert.equal(schedule.status, SCHEDULE_STATUS.CANCELLED);
  });

  it('disabled mailbox prevents send', async () => {
    const seeded = await seedScheduler();
    const integration = await seeded.store.getIntegration('tenant-a', seeded.integration.id);
    await seeded.store.saveIntegration({ ...integration, status: MAILBOX_STATUS.DISABLED });
    const fixture = await authorizeFixture({ seeded });
    const claimed = await fixture.scheduleStore.claimDueSchedules({
      now: fixture.executionNow,
      limit: 1,
      claimToken: 'claim-mailbox',
    });
    assert.ok(claimed[0]);
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: fixture.scheduleStore,
      mailboxStore: fixture.store,
      transport: fakeTransport(),
      secretResolver: fakeSecretResolver,
      now: fixture.executionNow,
    });
    assert.equal(outcome.result, 'skipped');
    assert.equal(outcome.schedule.skipReason, 'mailbox_inactive');
  });

  it('wrong tenant sender rejected at execution', async () => {
    const executionNow = '2026-09-14T14:00:00.000Z';
    const seeded = await seedScheduler({ tenantId: 'tenant-a' });
    const other = await seedMailbox({ tenantId: 'tenant-b', identityId: 'tsi_tenant_b' });
    const schedule = await seeded.scheduleStore.saveSchedule({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-1',
      sendingIdentityId: other.identity.id,
      recipientEmail: 'buyer@example.com',
      scheduledFor: dueSoon(executionNow, 5),
      maxLatenessMinutes: 120,
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
      timezone: 'America/New_York',
      status: SCHEDULE_STATUS.EXECUTING,
      authorizationSource: 'test',
      authorizedBy: 'op',
      authorizedAt: '2026-09-14T12:00:00.000Z',
      authorizationSnapshot: { subject: 'Hi', body: 'Body', recipientEmail: 'buyer@example.com' },
      idempotencyKey: 'wrong-tenant',
      sequenceStep: 1,
    });
    const outcome = await executeScheduledSend(schedule, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport: fakeTransport(),
      secretResolver: fakeSecretResolver,
      now: executionNow,
    });
    assert.equal(outcome.result, 'skipped');
    assert.equal(outcome.schedule.skipReason, 'sending_identity_inactive');
  });

  it('missed historical send does not auto-fire without policy', async () => {
    const missed = await authorizeFixture({
      scheduledFor: BABRUN_CANARY.missedScheduledFor,
      tenantId: BABRUN_CANARY.tenantId,
      prospectId: BABRUN_CANARY.prospectId,
      outreachAssetId: BABRUN_CANARY.outreachAssetId,
      recipientEmail: BABRUN_CANARY.recipientEmail,
      pastDuePolicy: PAST_DUE_POLICY.SKIP_PAST_DUE,
    });
    const transport = fakeTransport();
    const claimed = await missed.scheduleStore.claimDueSchedules({
      now: '2026-09-14T18:00:00.000Z',
      limit: 1,
      claimToken: 'claim-missed',
    });
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: missed.scheduleStore,
      mailboxStore: missed.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: '2026-09-14T18:00:00.000Z',
    });
    assert.equal(outcome.result, 'skipped');
    assert.equal(outcome.schedule.skipReason, 'missed_execution_window');
    assert.equal(transport.calls.length, 0);
  });

  it('sequence step ordering requires prior step SENT', async () => {
    const seeded = await authorizeFixture({ sequenceStep: 2 });
    const claimed = await seeded.scheduleStore.claimDueSchedules({
      now: seeded.executionNow,
      limit: 1,
      claimToken: 'claim-seq',
    });
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport: fakeTransport(),
      secretResolver: fakeSecretResolver,
      now: seeded.executionNow,
    });
    assert.equal(outcome.result, 'skipped');
    assert.equal(outcome.schedule.skipReason, 'prior_step_not_scheduled');

    await seeded.scheduleStore.saveSchedule({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-1',
      sendingIdentityId: seeded.identity.id,
      recipientEmail: 'buyer@example.com',
      scheduledFor: dueSoon(seeded.executionNow, 60),
      maxLatenessMinutes: 120,
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
      timezone: 'America/New_York',
      status: SCHEDULE_STATUS.SENT,
      authorizationSource: 'test',
      authorizedBy: 'op',
      authorizedAt: '2026-09-12T12:00:00.000Z',
      authorizationSnapshot: { subject: 'Step 1', body: 'Body', recipientEmail: 'buyer@example.com' },
      idempotencyKey: 'step-1-sent',
      sequenceStep: 1,
      outboundMessageId: 'tom_step1',
      executedAt: '2026-09-13T14:00:00.000Z',
    });

    const retry = await executeScheduledSend(claimed[0], {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport: fakeTransport(),
      secretResolver: fakeSecretResolver,
      now: seeded.executionNow,
    });
    assert.equal(retry.result, 'sent');
  });

  it('Babrun canary fixture authorization stores durable state without live send', async () => {
    const seeded = await seedScheduler({
      tenantId: BABRUN_CANARY.tenantId,
      identityId: BABRUN_CANARY.sendingIdentityId,
      prospectId: BABRUN_CANARY.prospectId,
      outreachAssetId: BABRUN_CANARY.outreachAssetId,
      recipientEmail: BABRUN_CANARY.recipientEmail,
    });
    const auth = await authorizeScheduledOutreachSend({
      tenantId: BABRUN_CANARY.tenantId,
      prospectId: BABRUN_CANARY.prospectId,
      acquisitionKnowledgeObjectId: BABRUN_CANARY.akObjectId,
      outreachAssetId: BABRUN_CANARY.outreachAssetId,
      sendingIdentityId: BABRUN_CANARY.sendingIdentityId,
      recipientEmail: BABRUN_CANARY.recipientEmail,
      scheduledFor: '2026-09-20T14:00:00.000Z',
      subject: 'Kaylee — quick question',
      body: 'Canary body — not sent live.',
      authorizationSource: 'operator_test',
      authorizedBy: 'jacob@gopulseforge.com',
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    }, { scheduleStore: seeded.scheduleStore, mailboxStore: seeded.store });
    assert.equal(auth.created, true);
    assert.equal(auth.schedule.status, SCHEDULE_STATUS.SCHEDULED);
    assert.equal(auth.schedule.recipientEmail, BABRUN_CANARY.recipientEmail);
    const transport = fakeTransport();
    const early = await executeScheduledSend(auth.schedule, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: '2026-09-14T18:00:00.000Z',
    });
    assert.equal(early.result, 'defer');
    assert.equal(transport.calls.length, 0);
  });

  it('buildIdempotencyKey is stable for identical authorization inputs', () => {
    const input = {
      tenantId: '13',
      prospectId: BABRUN_CANARY.prospectId,
      outreachAssetId: BABRUN_CANARY.outreachAssetId,
      sendingIdentityId: BABRUN_CANARY.sendingIdentityId,
      recipientEmail: BABRUN_CANARY.recipientEmail,
      sequenceStep: 1,
      scheduledFor: '2026-09-20T14:00:00.000Z',
    };
    assert.equal(buildIdempotencyKey(input), buildIdempotencyKey(input));
  });

  it('isPastDueWindowExceeded respects max lateness window', () => {
    const schedule = {
      scheduledFor: '2026-09-14T14:00:00.000Z',
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
      maxLatenessMinutes: 30,
    };
    assert.equal(isPastDueWindowExceeded(schedule, '2026-09-14T14:20:00.000Z'), false);
    assert.equal(isPastDueWindowExceeded(schedule, '2026-09-14T14:45:00.000Z'), true);
  });
});
