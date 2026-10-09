'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');

const { loadCredentials } = require('../getStudioSubstralMailboxToken');
const { captureSelfTestAuth } = require('../scripts/studioSubstralMailboxActivation');
const {
  MemoryTenantMailboxStore,
  studioSubstralMailboxConfig,
} = require('../services/tenantMailbox');
const { deliveredAuthenticationPasses } = require('../utils/mailAuthenticationResults');
const { clearGoogleMailboxAccessTokenCache } = require('../utils/googleMailboxOAuth');

test('Studio Substral OAuth bootstrap prefers the production runtime client pair', () => {
  const prior = {
    GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
    GMAIL_CREDENTIALS: process.env.GMAIL_CREDENTIALS,
  };
  try {
    process.env.GOOGLE_CLIENT_ID = 'runtime-client';
    process.env.GOOGLE_CLIENT_SECRET = 'runtime-secret';
    process.env.GMAIL_CREDENTIALS = JSON.stringify({
      installed: { client_id: 'different-client', client_secret: 'different-secret' },
    });

    assert.deepEqual(loadCredentials(), {
      installed: {
        client_id: 'runtime-client',
        client_secret: 'runtime-secret',
      },
    });
  } finally {
    for (const [key, value] of Object.entries(prior)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test('Studio Substral self-test sends only to itself and captures delivered authentication', async () => {
  const priorFetch = global.fetch;
  clearGoogleMailboxAccessTokenCache();
  global.fetch = async () => ({
    ok: true,
    json: async () => ({ access_token: 'substral-access-token', expires_in: 3600 }),
  });
  const store = new MemoryTenantMailboxStore();
  await store.saveIntegration(studioSubstralMailboxConfig('17').integration);
  const sends = [];
  const delivered = [
    'From: Studio Substral <hello@studiosubstral.com>',
    'Reply-To: hello@studiosubstral.com',
    'Authentication-Results: mx.google.com; spf=pass smtp.mailfrom=studiosubstral.com; dkim=pass header.d=studiosubstral.com; dmarc=pass header.from=studiosubstral.com',
    '',
    'Mailbox authentication check.',
  ].join('\r\n');
  try {
    const result = await captureSelfTestAuth(store, {
      env: {
        GOOGLE_CLIENT_ID: 'shared-runtime-client',
        GOOGLE_CLIENT_SECRET: 'shared-runtime-secret',
        GMAIL_CREDENTIALS: JSON.stringify({
          installed: {
            client_id: 'substral-bootstrap-client',
            client_secret: 'substral-bootstrap-secret',
          },
        }),
        STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN: 'substral-refresh-token',
      },
      transport: {
        async sendMail(message) { sends.push(message); },
        close() {},
      },
      async loadDeliveredMessage() { return delivered; },
    });

    assert.equal(sends.length, 1);
    assert.equal(sends[0].to, 'hello@studiosubstral.com');
    assert.equal(result.selfTest.externalProspectContacted, false);
    assert.deepEqual(result.authentication, {
      spf: 'PASS', dkim: 'PASS', dmarc: 'PASS',
      from: 'hello@studiosubstral.com', replyTo: 'hello@studiosubstral.com',
    });
    const integration = await store.getIntegration('17', 'tmi_17_substral_hello');
    assert.equal(deliveredAuthenticationPasses(integration.verificationState), true);
  } finally {
    global.fetch = priorFetch;
    clearGoogleMailboxAccessTokenCache();
  }
});
