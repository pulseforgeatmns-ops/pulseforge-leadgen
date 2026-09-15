'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const http = require('node:http');

const {
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  EVENT_TYPES,
  SEQUENCE_STATE,
  MESSAGE_STATUS,
  sendTenantEmail,
  babrunMailboxConfig,
  isPollableIntegration,
} = require('../services/tenantMailbox');

const {
  executeTenantMailboxPolls,
  pollOneIntegration,
  integrationLockKey,
  POLL_EXECUTOR_LOCK_NAMESPACE,
} = require('../services/tenantMailboxPollExecutor');

const {
  SCHEDULE_STATUS,
  PAST_DUE_POLICY,
  MemoryScheduleStore,
  authorizeScheduledOutreachSend,
  executeScheduledSend,
} = require('../services/tenantOutreachScheduler');

function fakeSecretResolver(ref) {
  return {
    SMTP_PASSWORD_REF: 'dummy-smtp-password',
    IMAP_PASSWORD_REF: 'dummy-imap-password',
    BABRUN_MAILBOX_IMAP_PASSWORD: 'dummy-babrun-imap',
    BABRUN_MAILBOX_SMTP_PASSWORD: 'dummy-babrun-smtp',
  }[ref];
}

function fakeTransport(result = {}) {
  return {
    calls: [],
    async sendMail(payload) {
      this.calls.push(payload);
      if (result.error) throw result.error;
      return { messageId: result.messageId || '<provider-msg@example.com>' };
    },
    async verify() {
      return true;
    },
  };
}

function emptyImapAdapter() {
  return { async fetchNewMessages() { return []; } };
}

function replyImapAdapter(outboundRfcMessageId, overrides = {}) {
  return {
    async fetchNewMessages() {
      return [{
        uid: overrides.uid || 1,
        providerMessageId: overrides.providerMessageId || 'imap-reply-1',
        rfcMessageId: overrides.rfcMessageId || '<reply-1@example.com>',
        inReplyTo: outboundRfcMessageId,
        referencesHeader: outboundRfcMessageId,
        from: overrides.from || 'buyer@example.com',
        to: [overrides.to || 'hello@example.com'],
        subject: overrides.subject || 'Re: Worth a look?',
        body: overrides.body || 'Interested.',
      }];
    },
  };
}

async function seedActiveMailbox(overrides = {}) {
  const store = new MemoryTenantMailboxStore();
  const integration = await store.saveIntegration({
    id: overrides.integrationId || 'tmi_tenant_a',
    tenantId: overrides.tenantId || 'tenant-a',
    providerType: 'GENERIC_SMTP_IMAP',
    mailboxAddress: overrides.mailboxAddress || 'hello@example.com',
    displayName: 'Test Sender',
    smtpHost: 'smtp.example.com',
    smtpPort: 465,
    smtpTlsMode: 'SSL_TLS',
    imapHost: 'imap.example.com',
    imapPort: 993,
    imapTlsMode: 'SSL_TLS',
    smtpSecretRef: 'SMTP_PASSWORD_REF',
    imapSecretRef: 'IMAP_PASSWORD_REF',
    status: MAILBOX_STATUS.ACTIVE,
    ...overrides.integration,
  });
  const identity = await store.saveIdentity({
    id: overrides.identityId || 'tsi_tenant_a',
    tenantId: overrides.tenantId || 'tenant-a',
    mailboxIntegrationId: integration.id,
    senderEmail: overrides.mailboxAddress || 'hello@example.com',
    senderDisplayName: 'Test Sender',
    replyToAddress: 'reply@example.com',
    status: IDENTITY_STATUS.ACTIVE,
    ...overrides.identity,
  });
  return { store, integration, identity };
}

async function sendOutboundFixture(seeded, opts = {}) {
  const result = await sendTenantEmail({
    tenantId: seeded.integration.tenantId,
    sendingIdentityId: seeded.identity.id,
    missionId: 'mission-1',
    prospectId: 'prospect-1',
    outreachAssetId: 'asset-1',
    to: opts.to || 'buyer@example.com',
    subject: opts.subject || 'Worth a look?',
    body: 'Hello from tenant mailbox.',
    metadata: { idempotencyKey: opts.idempotencyKey || 'send-1' },
  }, {
    store: seeded.store,
    transport: fakeTransport({ messageId: opts.messageId || '<provider-msg@example.com>' }),
    secretResolver: fakeSecretResolver,
    now: opts.now || '2026-09-09T10:00:00.000Z',
  });
  return { ...seeded, result };
}

function createMemoryLockHarness(initialLocks = {}) {
  const held = new Map(Object.entries(initialLocks));
  return {
    held,
    tryAcquireLock(integrationId) {
      if (held.has(integrationId)) return false;
      held.set(integrationId, true);
      return true;
    },
    releaseLock(integrationId) {
      held.delete(integrationId);
    },
  };
}

async function listen(app) {
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, resolve));
  const { port } = server.address();
  return {
    base: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve()))),
  };
}

describe('SPEC-253 tenant mailbox poll executor', () => {
  it('discovers active IMAP-configured mailbox integrations', async () => {
    const active = await seedActiveMailbox();
    const inactive = await seedActiveMailbox({
      tenantId: 'tenant-b',
      integrationId: 'tmi_tenant_b_disabled',
      identityId: 'tsi_tenant_b',
      integration: { status: MAILBOX_STATUS.DISABLED },
    });
    const noImap = await seedActiveMailbox({
      tenantId: 'tenant-c',
      integrationId: 'tmi_tenant_c_no_imap',
      identityId: 'tsi_tenant_c',
      integration: { imapHost: null, imapPort: null, imapSecretRef: null },
    });

    const pollable = await active.store.listPollableIntegrations();
    assert.equal(pollable.length, 1);
    assert.equal(pollable[0].id, active.integration.id);
    assert.equal(isPollableIntegration(inactive.integration), false);
    assert.equal(isPollableIntegration(noImap.integration), false);
  });

  it('discovers Babrun tenant 13 integration without Babrun-specific branching', async () => {
    const cfg = babrunMailboxConfig(13);
    const store = new MemoryTenantMailboxStore();
    const integration = await store.saveIntegration({
      ...cfg.integration,
      status: MAILBOX_STATUS.ACTIVE,
    });
    await store.saveIdentity({
      ...cfg.identity,
      status: IDENTITY_STATUS.ACTIVE,
    });

    const pollable = await store.listPollableIntegrations();
    assert.equal(pollable.length, 1);
    assert.equal(pollable[0].id, 'tmi_13_babrun_hello');
    assert.equal(pollable[0].tenantId, '13');

    const result = await executeTenantMailboxPolls({
      mailboxStore: store,
      integrations: pollable,
      secretResolver: fakeSecretResolver,
      imapAdapter: emptyImapAdapter(),
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });
    assert.equal(result.success, true);
    assert.equal(result.integrations, 1);
    assert.equal(result.fetched, 0);
    assert.equal(result.empty, true);
  });

  it('polls multiple tenant mailboxes with tenant isolation', async () => {
    const tenantA = await seedActiveMailbox({ tenantId: 'tenant-a', integrationId: 'tmi_a' });
    const tenantB = await seedActiveMailbox({
      tenantId: 'tenant-b',
      integrationId: 'tmi_b',
      identityId: 'tsi_b',
      mailboxAddress: 'hello@tenant-b.example.com',
    });
    await tenantA.store.saveIntegration(tenantB.integration);
    await tenantA.store.saveIdentity(tenantB.identity);

    const allIntegrations = await tenantA.store.listPollableIntegrations();
    assert.equal(allIntegrations.length, 2);

    const fetchedByTenant = {};
    const result = await executeTenantMailboxPolls({
      mailboxStore: tenantA.store,
      integrations: allIntegrations,
      secretResolver: fakeSecretResolver,
      imapAdapter: {
        async fetchNewMessages({ integration }) {
          fetchedByTenant[integration.tenantId] = (fetchedByTenant[integration.tenantId] || 0) + 1;
          return [];
        },
      },
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });

    assert.equal(result.success, true);
    assert.equal(result.integrations, 2);
    assert.equal(result.polled, 2);
    assert.equal(Object.keys(fetchedByTenant).length, 2);
  });

  it('one mailbox failure does not halt other eligible mailboxes', async () => {
    const ok = await seedActiveMailbox({ tenantId: 'tenant-a', integrationId: 'tmi_ok' });
    const bad = await seedActiveMailbox({
      tenantId: 'tenant-b',
      integrationId: 'tmi_bad',
      identityId: 'tsi_bad',
      mailboxAddress: 'bad@example.com',
    });
    await ok.store.saveIntegration(bad.integration);
    await ok.store.saveIdentity(bad.identity);
    const integrations = [bad.integration, ok.integration];

    const result = await executeTenantMailboxPolls({
      mailboxStore: ok.store,
      integrations,
      secretResolver: fakeSecretResolver,
      imapAdapter: {
        async fetchNewMessages({ integration }) {
          if (integration.id === 'tmi_bad') {
            throw Object.assign(new Error('imap auth failed password=secret'), { code: 'imap_auth_failed' });
          }
          return [];
        },
      },
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });

    assert.equal(result.failed, 1);
    assert.equal(result.polled, 1);
    assert.equal(result.success, false);
    assert.equal(result.results.length, 2);
    const failed = result.results.find((row) => row.integrationId === 'tmi_bad');
    const succeeded = result.results.find((row) => row.integrationId === 'tmi_ok');
    assert.equal(failed.success, false);
    assert.equal(succeeded.success, true);
    assert.doesNotMatch(JSON.stringify(result), /dummy-imap-password|secret/);
  });

  it('prevents overlapping poll runs for the same integration', async () => {
    const seeded = await seedActiveMailbox();
    const lock = createMemoryLockHarness({ [seeded.integration.id]: true });

    const result = await pollOneIntegration(seeded.integration, {
      mailboxStore: seeded.store,
      secretResolver: fakeSecretResolver,
      imapAdapter: emptyImapAdapter(),
      tryAcquireLock: lock.tryAcquireLock.bind(lock),
      releaseLock: lock.releaseLock.bind(lock),
    });

    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'overlap');
    assert.equal(result.success, true);
    assert.equal(result.fetched, 0);
  });

  it('uses per-integration advisory lock namespace constants', () => {
    assert.equal(POLL_EXECUTOR_LOCK_NAMESPACE, 701253);
    assert.notEqual(integrationLockKey('tmi_a'), integrationLockKey('tmi_b'));
    assert.equal(integrationLockKey('tmi_a'), integrationLockKey('tmi_a'));
  });

  it('handles empty poll safely and distinguishes from failure', async () => {
    const seeded = await seedActiveMailbox();
    const result = await executeTenantMailboxPolls({
      mailboxStore: seeded.store,
      secretResolver: fakeSecretResolver,
      imapAdapter: emptyImapAdapter(),
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });

    assert.equal(result.success, true);
    assert.equal(result.fetched, 0);
    assert.equal(result.inserted, 0);
    assert.equal(result.failed, 0);
    assert.equal(result.empty, true);
  });

  it('ingests reply via recurring poll, pauses sequence, and suppresses follow-up', async () => {
    const seeded = await sendOutboundFixture(await seedActiveMailbox());
    const scheduleStore = new MemoryScheduleStore({
      tenants: [{ tenantId: 'tenant-a', active: true }],
      prospects: [{
        tenantId: 'tenant-a',
        prospectId: 'prospect-1',
        email: 'buyer@example.com',
        doNotContact: false,
        booked: false,
      }],
      outreachAssets: [
        { tenantId: 'tenant-a', id: 'asset-1', lifecycleState: 'STAKEHOLDER_VALIDATED' },
        { tenantId: 'tenant-a', id: 'asset-2', lifecycleState: 'STAKEHOLDER_VALIDATED' },
      ],
    });
    await scheduleStore.saveSchedule({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-1',
      sendingIdentityId: seeded.identity.id,
      recipientEmail: 'buyer@example.com',
      scheduledFor: '2026-09-13T14:00:00.000Z',
      timezone: 'America/New_York',
      status: SCHEDULE_STATUS.SENT,
      authorizationSource: 'test',
      authorizedBy: 'op',
      authorizedAt: '2026-09-09T10:00:00.000Z',
      authorizationSnapshot: { subject: 'Step 1', body: 'Hello', recipientEmail: 'buyer@example.com' },
      idempotencyKey: 'step-1-sent',
      sequenceStep: 1,
      executedAt: '2026-09-09T10:00:00.000Z',
    });
    await scheduleStore.saveSchedule({
      tenantId: 'tenant-a',
      prospectId: 'prospect-1',
      outreachAssetId: 'asset-2',
      sendingIdentityId: seeded.identity.id,
      recipientEmail: 'buyer@example.com',
      threadId: seeded.result.message.threadId,
      scheduledFor: '2026-09-14T14:00:00.000Z',
      timezone: 'America/New_York',
      status: SCHEDULE_STATUS.SCHEDULED,
      authorizationSource: 'test',
      authorizedBy: 'op',
      authorizedAt: '2026-09-09T10:00:00.000Z',
      authorizationSnapshot: { subject: 'Follow up', body: 'Checking in', recipientEmail: 'buyer@example.com' },
      idempotencyKey: 'follow-up-after-reply',
      sequenceStep: 2,
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
      maxLatenessMinutes: 120,
    });

    const pollResult = await executeTenantMailboxPolls({
      mailboxStore: seeded.store,
      integrations: [seeded.integration],
      secretResolver: fakeSecretResolver,
      imapAdapter: replyImapAdapter(seeded.result.message.rfcMessageId),
      tryAcquireLock: () => true,
      releaseLock: () => {},
      now: '2026-09-09T10:05:00.000Z',
    });

    assert.equal(pollResult.success, true);
    assert.equal(pollResult.replies, 1);
    assert.equal(pollResult.inserted, 1);

    const thread = await seeded.store.getThread('tenant-a', seeded.result.message.threadId);
    assert.equal(thread.replyState, 'reply_received');
    assert.equal(thread.sequenceState, SEQUENCE_STATE.PAUSED);

    const events = [...seeded.store.events.values()].filter(
      (event) => event.eventType === EVENT_TYPES.REPLY_RECEIVED
    );
    assert.equal(events.length, 1);

    const claimed = await scheduleStore.claimDueSchedules({
      now: '2026-09-14T14:00:00.000Z',
      limit: 1,
      claimToken: 'claim-follow-up',
    });
    const transport = fakeTransport();
    const outcome = await executeScheduledSend(claimed[0], {
      scheduleStore,
      mailboxStore: seeded.store,
      transport,
      secretResolver: fakeSecretResolver,
      now: '2026-09-14T14:00:00.000Z',
    });
    assert.equal(outcome.result, 'skipped');
    assert.ok(['thread_replied', 'thread_sequence_paused'].includes(outcome.schedule.skipReason));
    assert.equal(transport.calls.length, 0);
  });

  it('repeated poll is idempotent for the same reply', async () => {
    const seeded = await sendOutboundFixture(await seedActiveMailbox());
    const adapter = replyImapAdapter(seeded.result.message.rfcMessageId, { uid: 42 });
    const opts = {
      mailboxStore: seeded.store,
      integrations: [seeded.integration],
      secretResolver: fakeSecretResolver,
      imapAdapter: adapter,
      tryAcquireLock: () => true,
      releaseLock: () => {},
      now: '2026-09-09T10:05:00.000Z',
    };

    const first = await executeTenantMailboxPolls(opts);
    const second = await executeTenantMailboxPolls(opts);

    assert.equal(first.inserted, 1);
    assert.equal(second.inserted, 0);
    assert.equal(second.duplicates, 1);
    assert.equal(first.replies, 1);
    assert.equal(second.replies, 0);
  });

  it('skips integrations with unresolved credential references', async () => {
    const seeded = await seedActiveMailbox();
    const result = await executeTenantMailboxPolls({
      mailboxStore: seeded.store,
      secretResolver: () => null,
      imapAdapter: emptyImapAdapter(),
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });

    assert.equal(result.skipped, 1);
    assert.equal(result.polled, 0);
    assert.equal(result.results[0].reason, 'credential_unavailable');
  });

  it('cron route rejects invalid CRON_SECRET and accepts valid secret', async () => {
    const prevSecret = process.env.CRON_SECRET;
    process.env.CRON_SECRET = 'mailbox-poll-test-secret';

    const executorPath = require.resolve('../services/tenantMailboxPollExecutor');
    const cronPath = require.resolve('../routes/cron');
    const originalExecutor = require(executorPath);
    require.cache[executorPath] = {
      id: executorPath,
      filename: executorPath,
      loaded: true,
      exports: {
        ...originalExecutor,
        executeTenantMailboxPolls: async () => ({
          success: true,
          integrations: 0,
          polled: 0,
          skipped: 0,
          fetched: 0,
          inserted: 0,
          duplicates: 0,
          unmatched: 0,
          replies: 0,
          failed: 0,
          empty: true,
          results: [],
        }),
      },
    };
    delete require.cache[cronPath];
    const cronRouter = require('../routes/cron');
    const app = express();
    app.use(express.json());
    app.use('/', cronRouter);
    const { base, close } = await listen(app);

    try {
      const unauth = await fetch(`${base}/cron/tenant-mailbox-poll`);
      assert.equal(unauth.status, 401);
      const unauthBody = await unauth.json();
      assert.equal(unauthBody.error, 'Unauthorized');

      const authed = await fetch(
        `${base}/cron/tenant-mailbox-poll?secret=${encodeURIComponent('mailbox-poll-test-secret')}`
      );
      const body = await authed.json();
      assert.equal(authed.status, 200);
      assert.equal(body.success, true);
      assert.equal(body.integrations, 0);
      assert.equal(body.empty, true);
      assert.doesNotMatch(JSON.stringify(body), /dummy-imap-password|BABRUN_MAILBOX_IMAP_PASSWORD/);
    } finally {
      await close();
      require.cache[executorPath] = {
        id: executorPath,
        filename: executorPath,
        loaded: true,
        exports: originalExecutor,
      };
      delete require.cache[cronPath];
      if (prevSecret == null) delete process.env.CRON_SECRET;
      else process.env.CRON_SECRET = prevSecret;
    }
  });

  it('does not expose credentials in poll results', async () => {
    const seeded = await seedActiveMailbox();
    const result = await executeTenantMailboxPolls({
      mailboxStore: seeded.store,
      secretResolver: fakeSecretResolver,
      imapAdapter: emptyImapAdapter(),
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });

    const serialized = JSON.stringify(result);
    assert.doesNotMatch(serialized, /dummy-imap-password|dummy-smtp-password/);
    assert.doesNotMatch(serialized, /IMAP_PASSWORD_REF|SMTP_PASSWORD_REF/);
    if (result.results[0]?.integration) {
      assert.equal(result.results[0].integration.imapSecretRef, undefined);
      assert.equal(result.results[0].integration.smtpSecretRef, undefined);
    }
  });

  it('inactive mailbox is ignored by discovery', async () => {
    const store = new MemoryTenantMailboxStore();
    await store.saveIntegration({
      id: 'tmi_inactive',
      tenantId: 'tenant-x',
      providerType: 'GENERIC_SMTP_IMAP',
      mailboxAddress: 'inactive@example.com',
      imapHost: 'imap.example.com',
      imapPort: 993,
      imapSecretRef: 'IMAP_PASSWORD_REF',
      status: MAILBOX_STATUS.DISABLED,
    });

    const pollable = await store.listPollableIntegrations();
    assert.equal(pollable.length, 0);

    const result = await executeTenantMailboxPolls({
      mailboxStore: store,
      secretResolver: fakeSecretResolver,
      imapAdapter: emptyImapAdapter(),
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });
    assert.equal(result.integrations, 0);
    assert.equal(result.empty, true);
  });
});
