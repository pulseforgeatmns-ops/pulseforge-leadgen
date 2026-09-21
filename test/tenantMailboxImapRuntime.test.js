'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('module');

const {
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  EVENT_TYPES,
  SEQUENCE_STATE,
  pollTenantMailbox,
  sendTenantEmail,
} = require('../services/tenantMailbox');
const { pollOneIntegration } = require('../services/tenantMailboxPollExecutor');

function fakeSecretResolver(ref) {
  return {
    SMTP_PASSWORD_REF: 'dummy-smtp-password',
    IMAP_PASSWORD_REF: 'dummy-imap-password',
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
  };
}

async function seedStore() {
  const store = new MemoryTenantMailboxStore();
  const integration = await store.saveIntegration({
    id: 'tmi_tenant_a',
    tenantId: 'tenant-a',
    providerType: 'GENERIC_SMTP_IMAP',
    mailboxAddress: 'hello@example.com',
    smtpHost: 'smtp.example.com',
    smtpPort: 465,
    smtpTlsMode: 'SSL_TLS',
    imapHost: 'imap.example.com',
    imapPort: 993,
    imapTlsMode: 'SSL_TLS',
    smtpSecretRef: 'SMTP_PASSWORD_REF',
    imapSecretRef: 'IMAP_PASSWORD_REF',
    status: MAILBOX_STATUS.ACTIVE,
  });
  const identity = await store.saveIdentity({
    id: 'tsi_tenant_a',
    tenantId: 'tenant-a',
    mailboxIntegrationId: integration.id,
    senderEmail: 'hello@example.com',
    status: IDENTITY_STATUS.ACTIVE,
  });
  return { store, integration, identity };
}

describe('SPEC-253 IMAP runtime dependency', () => {
  it('production default poll path resolves imapflow at runtime', () => {
    const resolved = require.resolve('imapflow');
    assert.match(resolved, /imapflow/);
    const { ImapFlow } = require('imapflow');
    assert.equal(typeof ImapFlow, 'function');
  });

  it('empty mailbox poll succeeds with injected adapter', async () => {
    const { store, integration } = await seedStore();
    const poll = await pollTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
    }, {
      store,
      secretResolver: fakeSecretResolver,
      imapAdapter: { async fetchNewMessages() { return []; } },
    });
    assert.equal(poll.fetched, 0);
    assert.equal(poll.inserted, 0);
    assert.equal(poll.duplicates, 0);
    assert.equal(poll.unmatched, 0);
  });

  it('injected adapter remains the preferred IMAP implementation in tests', async () => {
    const { store, integration, identity } = await seedStore();
    const sent = await sendTenantEmail({
      tenantId: 'tenant-a',
      sendingIdentityId: identity.id,
      missionId: 'mission-1',
      prospectId: 'prospect-1',
      to: 'buyer@example.com',
      subject: 'Worth a look?',
      body: 'Hello.',
      metadata: { idempotencyKey: 'runtime-adapter-send' },
    }, {
      store,
      transport: fakeTransport({ messageId: '<outbound@example.com>' }),
      secretResolver: fakeSecretResolver,
    });

    let adapterUsed = false;
    const poll = await pollTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
    }, {
      store,
      secretResolver: fakeSecretResolver,
      imapAdapter: {
        async fetchNewMessages() {
          adapterUsed = true;
          return [{
            uid: 1,
            providerMessageId: 'imap-1',
            rfcMessageId: '<reply-1@example.com>',
            inReplyTo: sent.message.rfcMessageId,
            referencesHeader: sent.message.rfcMessageId,
            from: 'buyer@example.com',
            to: ['hello@example.com'],
            subject: 'Re: Worth a look?',
            body: 'Interested.',
          }];
        },
      },
    });

    assert.equal(adapterUsed, true);
    assert.equal(poll.inserted, 1);
    assert.equal(poll.results[0].event.eventType, EVENT_TYPES.REPLY_RECEIVED);

    const duplicatePoll = await pollTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
    }, {
      store,
      secretResolver: fakeSecretResolver,
      imapAdapter: {
        async fetchNewMessages() {
          return [{
            uid: 1,
            providerMessageId: 'imap-1',
            rfcMessageId: '<reply-1@example.com>',
            inReplyTo: sent.message.rfcMessageId,
            referencesHeader: sent.message.rfcMessageId,
            from: 'buyer@example.com',
            to: ['hello@example.com'],
            subject: 'Re: Worth a look?',
            body: 'Interested.',
          }];
        },
      },
    });
    assert.equal(duplicatePoll.inserted, 0);
    assert.equal(duplicatePoll.duplicates, 1);

    const thread = await store.getThread('tenant-a', sent.message.threadId);
    assert.equal(thread.sequenceState, SEQUENCE_STATE.PAUSED);
  });

  it('missing IMAP runtime fails closed without credential leakage', async () => {
    const tenantMailboxPath = require.resolve('../services/tenantMailbox');
    const originalLoad = Module._load;
    Module._load = function patchedLoad(request, parent, isMain) {
      if (request === 'imapflow') {
        const err = new Error("Cannot find module 'imapflow'");
        err.code = 'MODULE_NOT_FOUND';
        throw err;
      }
      return originalLoad.call(this, request, parent, isMain);
    };

    delete require.cache[tenantMailboxPath];
    const reloaded = require('../services/tenantMailbox');
    const { store, integration } = await seedStore();

    await assert.rejects(
      () => reloaded.pollTenantMailbox({
        tenantId: 'tenant-a',
        integrationId: integration.id,
      }, {
        store,
        secretResolver: fakeSecretResolver,
      }),
      (err) => {
        assert.equal(err.code, 'imap_adapter_unavailable');
        assert.match(err.message, /imapAdapter|imapflow/);
        assert.doesNotMatch(err.message, /dummy-imap-password/);
        assert.doesNotMatch(err.message, /IMAP_PASSWORD_REF/);
        return true;
      }
    );

    Module._load = originalLoad;
    delete require.cache[tenantMailboxPath];
    require('../services/tenantMailbox');
  });

  it('poll executor errors do not leak mailbox credentials', async () => {
    const { store, integration } = await seedStore();
    const result = await pollOneIntegration(integration, {
      mailboxStore: store,
      secretResolver: fakeSecretResolver,
      imapAdapter: {
        async fetchNewMessages({ secret }) {
          throw Object.assign(new Error(`auth failed password=${secret}`), { code: 'imap_auth_failed' });
        },
      },
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });
    assert.equal(result.success, false);
    assert.equal(result.code, 'imap_auth_failed');
    assert.doesNotMatch(result.error, /dummy-imap-password/);
    assert.match(result.error, /auth=\[redacted\]/);
  });
});
