'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { pollAnchorMailboxOnly, ANCHOR_DEFAULT_INBOX_INTEGRATION_ID } = require('../anchorDailyOutboundCron');
const { MemoryTenantMailboxStore, anchorMailboxConfig, MAILBOX_STATUS } = require('../services/tenantMailbox');

describe('anchorDailyOutbound mailbox-only poll', () => {
  it('polls tmi_10_anchor_jacob when no governed program exists', async () => {
    const originalFetch = global.fetch;
    global.fetch = async () => ({
      ok: true,
      json: async () => ({ access_token: 'oauth-access-token', expires_in: 3600 }),
    });
    try {
    const store = new MemoryTenantMailboxStore();
    await store.saveIntegration({
      ...anchorMailboxConfig('10').integration,
      status: MAILBOX_STATUS.ACTIVE,
    });

    const result = await pollAnchorMailboxOnly(null, {
      store,
      env: {
        GOOGLE_CLIENT_ID: 'id',
        GOOGLE_CLIENT_SECRET: 'secret',
        ANCHOR_GOOGLE_REFRESH_TOKEN: 'refresh',
      },
      imapAdapter: {
        async fetchNewMessages() {
          return [];
        },
      },
      tryAcquireLock: () => true,
      releaseLock: () => {},
    });

    assert.equal(result.mailboxOnly, true);
    assert.equal(result.integrationId, ANCHOR_DEFAULT_INBOX_INTEGRATION_ID);
    assert.equal(result.results[0].success, true);
    assert.equal(result.classification.skipped, 'no_program');
    } finally {
      global.fetch = originalFetch;
    }
  });
});
