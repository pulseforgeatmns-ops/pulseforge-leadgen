'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  BABRUN_MAILBOX_CONFIG,
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  MESSAGE_STATUS,
  EVENT_TYPES,
  SEQUENCE_STATE,
  babrunMailboxConfig,
  publicIntegration,
  publicIdentity,
  sendTenantEmail,
  pollTenantMailbox,
  markTenantSuppression,
  verifyTenantMailbox,
} = require('../services/tenantMailbox');

async function seedStore(overrides = {}) {
  const store = new MemoryTenantMailboxStore();
  const integration = await store.saveIntegration({
    id: 'tmi_tenant_a',
    tenantId: 'tenant-a',
    providerType: 'GENERIC_SMTP_IMAP',
    mailboxAddress: 'hello@example.com',
    displayName: 'Fedir | Babrun',
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
    id: 'tsi_tenant_a',
    tenantId: 'tenant-a',
    mailboxIntegrationId: integration.id,
    senderEmail: 'hello@example.com',
    senderDisplayName: 'Fedir | Babrun',
    replyToAddress: 'reply@example.com',
    status: IDENTITY_STATUS.ACTIVE,
    ...overrides.identity,
  });
  return { store, integration, identity };
}

function fakeSecretResolver(ref) {
  return {
    SMTP_PASSWORD_REF: 'dummy-smtp-password',
    IMAP_PASSWORD_REF: 'dummy-imap-password',
  }[ref];
}

function fakeTransport(result = {}) {
  const calls = [];
  return {
    calls,
    async sendMail(payload) {
      calls.push(payload);
      if (result.error) throw result.error;
      return { messageId: result.messageId || '<provider-1@example.com>' };
    },
    async verify() {
      return true;
    },
  };
}

async function sendFixture(opts = {}) {
  const seeded = await seedStore(opts);
  const transport = opts.transport || fakeTransport({ messageId: '<provider-msg@example.com>' });
  const result = await sendTenantEmail({
    tenantId: 'tenant-a',
    sendingIdentityId: seeded.identity.id,
    missionId: 'mission-1',
    prospectId: 'prospect-1',
    outreachAssetId: 'asset-1',
    to: 'buyer@example.com',
    subject: 'Worth a look?',
    body: 'Hello from Babrun.',
    metadata: { idempotencyKey: opts.idempotencyKey || 'send-1' },
  }, {
    store: seeded.store,
    transport,
    secretResolver: fakeSecretResolver,
    now: '2026-09-09T10:00:00.000Z',
  });
  return { ...seeded, transport, result };
}

describe('SPEC-248 tenant mailbox safety', () => {
  it('TEST A: Tenant A cannot use Tenant B sending identity', async () => {
    const { store } = await seedStore();
    await assert.rejects(
      () => sendTenantEmail({
        tenantId: 'tenant-b',
        sendingIdentityId: 'tsi_tenant_a',
        to: 'buyer@example.com',
        subject: 'x',
        body: 'x',
      }, { store, transport: fakeTransport(), secretResolver: fakeSecretResolver }),
      (err) => err.code === 'mailbox_identity_tenant_mismatch'
    );
  });

  it('TEST B: SMTP credentials are resolved by secret reference and never returned', async () => {
    const { store, integration, identity } = await seedStore();
    const safeIntegration = publicIntegration(integration);
    const safeIdentity = publicIdentity(identity);
    assert.equal(safeIntegration.smtpSecretRef, undefined);
    assert.equal(safeIntegration.imapSecretRef, undefined);
    assert.equal(safeIntegration.hasSmtpSecretRef, true);
    assert.equal(safeIdentity.senderEmail, 'hello@example.com');

    const transport = fakeTransport();
    await sendTenantEmail({
      tenantId: 'tenant-a',
      sendingIdentityId: identity.id,
      to: 'buyer@example.com',
      subject: 'x',
      body: 'x',
      metadata: { idempotencyKey: 'secret-resolution' },
    }, { store, transport, secretResolver: fakeSecretResolver });
    assert.equal(transport.calls.length, 1);
    assert.doesNotMatch(JSON.stringify(transport.calls[0]), /dummy-smtp-password/);
  });

  it('TEST C and D: Outbound message persists with thread association and provider Message-ID', async () => {
    const { result, store } = await sendFixture();
    assert.equal(result.message.status, MESSAGE_STATUS.SENT);
    assert.equal(result.message.providerMessageId, '<provider-msg@example.com>');
    assert.ok(result.message.rfcMessageId.startsWith('<tom_'));
    const thread = await store.getThread('tenant-a', result.message.threadId);
    assert.equal(thread.latestOutboundMessageId, result.message.id);
    assert.equal(thread.sequenceState, SEQUENCE_STATE.SENT);
  });

  it('TEST E, G, and R: Inbound reply maps to the correct thread, pauses follow-up, and emits linkage event', async () => {
    const { store, integration, result } = await sendFixture();
    const poll = await pollTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
      once: true,
    }, {
      store,
      secretResolver: fakeSecretResolver,
      now: '2026-09-09T10:05:00.000Z',
      imapAdapter: {
        async fetchNewMessages() {
          return [{
            uid: 1,
            providerMessageId: 'imap-1',
            rfcMessageId: '<reply-1@example.com>',
            inReplyTo: result.message.rfcMessageId,
            referencesHeader: result.message.rfcMessageId,
            from: 'buyer@example.com',
            to: ['hello@example.com'],
            subject: 'Re: Worth a look?',
            body: 'Interested.',
          }];
        },
      },
    });
    assert.equal(poll.inserted, 1);
    const inbound = poll.results[0].message;
    assert.equal(inbound.threadId, result.message.threadId);
    const thread = await store.getThread('tenant-a', result.message.threadId);
    assert.equal(thread.replyState, 'reply_received');
    assert.equal(thread.sequenceState, SEQUENCE_STATE.PAUSED);
    assert.equal(poll.results[0].event.eventType, EVENT_TYPES.REPLY_RECEIVED);
    assert.equal(poll.results[0].event.payload.tenantId, 'tenant-a');
    assert.equal(poll.results[0].event.payload.missionId, 'mission-1');
    assert.equal(poll.results[0].event.payload.prospectId, 'prospect-1');
    assert.equal(poll.results[0].event.payload.threadId, result.message.threadId);
  });

  it('TEST F: Repeated IMAP polling does not duplicate replies', async () => {
    const { store, integration, result } = await sendFixture();
    const adapter = {
      async fetchNewMessages() {
        return [{
          uid: 7,
          providerMessageId: 'imap-dup',
          rfcMessageId: '<reply-dup@example.com>',
          inReplyTo: result.message.rfcMessageId,
          referencesHeader: result.message.rfcMessageId,
          from: 'buyer@example.com',
          to: ['hello@example.com'],
          subject: 'Re: Worth a look?',
          body: 'Yes.',
        }];
      },
    };
    const first = await pollTenantMailbox({ tenantId: 'tenant-a', integrationId: integration.id }, { store, secretResolver: fakeSecretResolver, imapAdapter: adapter });
    const second = await pollTenantMailbox({ tenantId: 'tenant-a', integrationId: integration.id }, { store, secretResolver: fakeSecretResolver, imapAdapter: adapter });
    assert.equal(first.inserted, 1);
    assert.equal(second.inserted, 0);
    assert.equal(second.duplicates, 1);
  });

  it('TEST H: Unsubscribe suppression prevents later send', async () => {
    const { store, identity } = await seedStore();
    await markTenantSuppression({
      tenantId: 'tenant-a',
      email: 'buyer@example.com',
      reason: 'unsubscribe',
      source: 'operator',
    }, { store });
    await assert.rejects(
      () => sendTenantEmail({
        tenantId: 'tenant-a',
        sendingIdentityId: identity.id,
        to: 'buyer@example.com',
        subject: 'x',
        body: 'x',
      }, { store, transport: fakeTransport(), secretResolver: fakeSecretResolver }),
      (err) => err.code === 'tenant_outreach_suppressed'
    );
  });

  it('TEST I: Wrong-tenant mailbox access is rejected', async () => {
    const { store, integration } = await seedStore();
    await assert.rejects(
      () => pollTenantMailbox({ tenantId: 'tenant-b', integrationId: integration.id }, {
        store,
        secretResolver: fakeSecretResolver,
        imapAdapter: { async fetchNewMessages() { return []; } },
      }),
      (err) => err.code === 'mailbox_integration_tenant_mismatch'
    );
  });

  it('TEST J: Disabled or revoked mailbox cannot send', async () => {
    const { store, identity } = await seedStore({ integration: { status: MAILBOX_STATUS.DISABLED } });
    await assert.rejects(
      () => sendTenantEmail({
        tenantId: 'tenant-a',
        sendingIdentityId: identity.id,
        to: 'buyer@example.com',
        subject: 'x',
        body: 'x',
      }, { store, transport: fakeTransport(), secretResolver: fakeSecretResolver }),
      (err) => err.code === 'mailbox_integration_disabled'
    );
  });

  it('TEST K: Missing secret produces explicit failure without leaking secret details', async () => {
    const { store, identity } = await seedStore();
    await assert.rejects(
      () => sendTenantEmail({
        tenantId: 'tenant-a',
        sendingIdentityId: identity.id,
        to: 'buyer@example.com',
        subject: 'x',
        body: 'x',
      }, { store, transport: fakeTransport(), secretResolver: () => null }),
      (err) => {
        assert.equal(err.code, 'mailbox_secret_missing');
        assert.doesNotMatch(err.message, /dummy|password-value|secret-value/i);
        return true;
      }
    );
  });

  it('TEST L: SMTP failure does not falsely mark message sent', async () => {
    const { store, identity } = await seedStore();
    await assert.rejects(
      () => sendTenantEmail({
        tenantId: 'tenant-a',
        sendingIdentityId: identity.id,
        to: 'buyer@example.com',
        subject: 'x',
        body: 'x',
        metadata: { idempotencyKey: 'smtp-failure' },
      }, {
        store,
        transport: fakeTransport({ error: Object.assign(new Error('smtp auth failed password=secret'), { code: 'smtp_auth_failed' }) }),
        secretResolver: fakeSecretResolver,
      }),
      (err) => {
        assert.equal(err.code, 'smtp_auth_failed');
        assert.doesNotMatch(err.message, /secret/);
        assert.equal(err.message.includes('smtp'), true);
        assert.equal(err.message.includes('[redacted]'), true);
        assert.equal(err.message.includes('password=secret'), false);
        return true;
      }
    );
    const failed = [...store.messages.values()].find((msg) => msg.metadata.idempotencyKey === 'smtp-failure');
    assert.equal(failed.status, MESSAGE_STATUS.FAILED);
    assert.equal(failed.sentAt, null);
  });

  it('TEST M: Retry behavior does not double-send after confirmed provider success', async () => {
    const { store, identity } = await seedStore();
    const transport = fakeTransport({ messageId: '<confirmed@example.com>' });
    const input = {
      tenantId: 'tenant-a',
      sendingIdentityId: identity.id,
      to: 'buyer@example.com',
      subject: 'x',
      body: 'x',
      metadata: { idempotencyKey: 'confirmed-send' },
    };
    const first = await sendTenantEmail(input, { store, transport, secretResolver: fakeSecretResolver });
    const second = await sendTenantEmail(input, { store, transport, secretResolver: fakeSecretResolver });
    assert.equal(first.sent, true);
    assert.equal(second.duplicate, true);
    assert.equal(transport.calls.length, 1);
  });

  it('TEST N: IMAP connection failure does not mutate reply state', async () => {
    const { store, integration, result } = await sendFixture();
    const before = await store.getThread('tenant-a', result.message.threadId);
    await assert.rejects(
      () => pollTenantMailbox({ tenantId: 'tenant-a', integrationId: integration.id }, {
        store,
        secretResolver: fakeSecretResolver,
        imapAdapter: { async fetchNewMessages() { throw Object.assign(new Error('imap unavailable'), { code: 'imap_unavailable' }); } },
      }),
      (err) => err.code === 'imap_unavailable'
    );
    const after = await store.getThread('tenant-a', result.message.threadId);
    assert.equal(after.replyState, before.replyState);
    assert.equal(after.sequenceState, before.sequenceState);
  });

  it('TEST O: Sending identity display name is independent from transport credentials', async () => {
    const { transport } = await sendFixture({
      identity: { senderDisplayName: 'Fedir | Babrun' },
    });
    assert.match(transport.calls[0].from, /Fedir \| Babrun/);
    assert.doesNotMatch(transport.calls[0].from, /dummy-smtp-password/);
  });

  it('TEST P: Babrun mailbox config references hello@babrun.com without storing a password', () => {
    const cfg = babrunMailboxConfig(13);
    assert.equal(cfg.integration.mailboxAddress, 'hello@babrun.com');
    assert.equal(cfg.identity.senderDisplayName, 'Fedir | Babrun');
    assert.equal(BABRUN_MAILBOX_CONFIG.smtpHost, 'mail.adm.tools');
    assert.equal(BABRUN_MAILBOX_CONFIG.smtpPort, 465);
    assert.equal(BABRUN_MAILBOX_CONFIG.imapPort, 993);
    assert.equal(cfg.integration.smtpPassword, undefined);
    assert.equal(cfg.integration.imapPassword, undefined);
    assert.equal(cfg.integration.password, undefined);
    assert.match(JSON.stringify(cfg), /BABRUN_MAILBOX_SMTP_PASSWORD/);
  });

  it('TEST Q: Mailbox verification exposes SMTP and IMAP verification state', async () => {
    const { store, integration } = await seedStore();
    const result = await verifyTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
      dkimSelector: 'selector1',
    }, {
      store,
      secretResolver: fakeSecretResolver,
      smtpVerifier: async () => true,
      imapVerifier: async () => true,
      async resolveTxt(name) {
        if (name === 'example.com') return [['v=spf1 include:example.net ~all']];
        if (name === '_dmarc.example.com') return [['v=DMARC1; p=none']];
        if (name === 'selector1._domainkey.example.com') return [['v=DKIM1; p=abc']];
        return [];
      },
    });
    assert.equal(result.verificationState.smtp.status, 'verified');
    assert.equal(result.verificationState.imap.status, 'verified');
    assert.equal(result.verificationState.spf.status, 'present');
    assert.equal(result.verificationState.dkim.status, 'present');
    assert.equal(result.verificationState.dmarc.status, 'present');
  });
});
