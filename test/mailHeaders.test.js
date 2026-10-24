'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  getHeaderValue,
  parseRawMailHeaders,
  normalizeImapFlowFetchMessage,
} = require('../utils/mailHeaders');

const {
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  EVENT_TYPES,
  pollTenantMailbox,
  sendTenantEmail,
} = require('../services/tenantMailbox');

function imapflowHeaderBuffer(lines = []) {
  return Buffer.from([...lines, ''].join('\r\n'), 'utf8');
}

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

describe('SPEC-253 mail header normalization', () => {
  it('imapflow Buffer headers do not require headers.get', () => {
    const headers = imapflowHeaderBuffer([
      'Message-ID: <imapflow-msg@example.com>',
      'In-Reply-To: <parent@example.com>',
      'References: <a@example.com> <b@example.com>',
      'Subject: Re: Hello',
    ]);
    assert.equal(typeof headers.get, 'undefined');
    assert.doesNotThrow(() => getHeaderValue(headers, 'message-id'));
    assert.equal(getHeaderValue(headers, 'message-id'), '<imapflow-msg@example.com>');
  });

  it('extracts Message-ID case-insensitively', () => {
    const headers = imapflowHeaderBuffer(['Message-Id: <mid@example.com>']);
    assert.equal(getHeaderValue(headers, 'Message-ID'), '<mid@example.com>');
  });

  it('extracts In-Reply-To', () => {
    const headers = imapflowHeaderBuffer(['In-Reply-To: <reply-parent@example.com>']);
    assert.equal(getHeaderValue(headers, 'in-reply-to'), '<reply-parent@example.com>');
  });

  it('preserves multi-value References', () => {
    const headers = imapflowHeaderBuffer([
      'References: <first@example.com>',
      'References: <second@example.com>',
    ]);
    assert.equal(getHeaderValue(headers, 'references'), '<first@example.com> <second@example.com>');
  });

  it('extracts Subject/From/Date via imapflow fetch normalization', () => {
    const headers = imapflowHeaderBuffer([
      'Subject: Header Subject',
      'From: Header From <header-from@example.com>',
      'Date: Mon, 15 Sep 2026 12:00:00 +0000',
    ]);
    const normalized = normalizeImapFlowFetchMessage({
      uid: 42,
      headers,
      envelope: {
        subject: 'Envelope Subject',
        from: [{ name: 'Buyer', address: 'buyer@example.com' }],
        to: [{ address: 'hello@example.com' }],
        date: new Date('2026-09-15T12:00:00.000Z'),
      },
      source: Buffer.from('raw source'),
    }, { now: '2026-09-15T12:00:00.000Z' });

    assert.equal(normalized.subject, 'Envelope Subject');
    assert.equal(normalized.from, 'buyer@example.com');
    assert.equal(normalized.to[0], 'hello@example.com');
    assert.equal(normalized.receivedAt, '2026-09-15T12:00:00.000Z');
    assert.match(normalized.body, /raw source/);
  });

  it('supports Map-like headers for mailparser-style fixtures', () => {
    const headers = new Map([
      ['message-id', '<map-msg@example.com>'],
      ['references', ['<a@example.com>', '<b@example.com>']],
    ]);
    assert.equal(getHeaderValue(headers, 'Message-ID'), '<map-msg@example.com>');
    assert.equal(getHeaderValue(headers, 'References'), '<a@example.com> <b@example.com>');
  });

  it('supports plain-object and array header fixtures', () => {
    assert.equal(getHeaderValue({ 'Message-ID': '<obj@example.com>' }, 'message-id'), '<obj@example.com>');
    assert.equal(
      getHeaderValue([{ name: 'Message-ID', value: '<arr@example.com>' }], 'message-id'),
      '<arr@example.com>'
    );
  });

  it('returns null for malformed or missing headers without throwing', () => {
    assert.equal(getHeaderValue(null, 'message-id'), null);
    assert.equal(getHeaderValue(undefined, 'message-id'), null);
    assert.equal(getHeaderValue(Buffer.from('not-a-header-line'), 'message-id'), null);
    assert.equal(getHeaderValue({ get: 'not-a-function' }, 'message-id'), null);
    assert.doesNotThrow(() => parseRawMailHeaders(123));
  });

  it('skips malformed imapflow fetch records without crashing poll normalization', () => {
    assert.equal(normalizeImapFlowFetchMessage(null), null);
    assert.equal(normalizeImapFlowFetchMessage({ envelope: {} }), null);
    const normalized = normalizeImapFlowFetchMessage({
      uid: 1,
      headers: Buffer.from('Message-ID: <safe@example.com>\r\n'),
      envelope: {},
    });
    assert.equal(normalized.rfcMessageId, '<safe@example.com>');
  });
});

describe('SPEC-253 reply ingestion with imapflow-shaped messages', () => {
  it('associates thread from Buffer-header normalized inbound reply', async () => {
    const store = new MemoryTenantMailboxStore();
    const integration = await store.saveIntegration({
      id: 'tmi_tenant_a',
      tenantId: 'tenant-a',
      providerType: 'GENERIC_SMTP_IMAP',
      mailboxAddress: 'hello@example.com',
      imapHost: 'imap.example.com',
      imapPort: 993,
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

    const sent = await sendTenantEmail({
      tenantId: 'tenant-a',
      sendingIdentityId: identity.id,
      missionId: 'mission-1',
      prospectId: 'prospect-1',
      to: 'buyer@example.com',
      subject: 'Worth a look?',
      body: 'Hello.',
      metadata: { idempotencyKey: 'buffer-header-send' },
    }, {
      store,
      transport: fakeTransport({ messageId: '<outbound@example.com>' }),
      secretResolver: fakeSecretResolver,
    });

    const imapflowRecord = normalizeImapFlowFetchMessage({
      uid: 99,
      headers: imapflowHeaderBuffer([
        `Message-ID: <reply-buffer@example.com>`,
        `In-Reply-To: ${sent.message.rfcMessageId}`,
        `References: ${sent.message.rfcMessageId}`,
        'Subject: Re: Worth a look?',
      ]),
      envelope: {
        subject: 'Re: Worth a look?',
        from: [{ address: 'buyer@example.com' }],
        to: [{ address: 'hello@example.com' }],
        date: new Date('2026-09-15T12:05:00.000Z'),
      },
    });

    const first = await pollTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
    }, {
      store,
      secretResolver: fakeSecretResolver,
      imapAdapter: { async fetchNewMessages() { return [imapflowRecord]; } },
    });
    const second = await pollTenantMailbox({
      tenantId: 'tenant-a',
      integrationId: integration.id,
    }, {
      store,
      secretResolver: fakeSecretResolver,
      imapAdapter: { async fetchNewMessages() { return [imapflowRecord]; } },
    });

    assert.equal(first.inserted, 1);
    assert.equal(first.fetched, 1);
    assert.equal(second.inserted, 0);
    assert.equal(second.duplicates, 1);
    assert.equal(first.results[0].event.eventType, EVENT_TYPES.REPLY_RECEIVED);
    assert.equal(first.results[0].message.threadId, sent.message.threadId);
  });

  it('empty mailbox poll still succeeds', async () => {
    const store = new MemoryTenantMailboxStore();
    const integration = await store.saveIntegration({
      id: 'tmi_tenant_a',
      tenantId: 'tenant-a',
      providerType: 'GENERIC_SMTP_IMAP',
      mailboxAddress: 'hello@example.com',
      imapHost: 'imap.example.com',
      imapPort: 993,
      imapSecretRef: 'IMAP_PASSWORD_REF',
      status: MAILBOX_STATUS.ACTIVE,
    });
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
  });
});
