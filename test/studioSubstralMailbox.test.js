'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  studioSubstralMailboxConfig,
  STUDIO_SUBSTRAL_MAILBOX_CONFIG,
  isReplyOnlyMailbox,
  usesGoogleOAuthSmtp,
  normalizeIntegration,
  resolveSmtpAuth,
} = require('../services/tenantMailbox');
const { createGovernedOutboundTenantContext, assertGovernedOutboundTenantId } = require('../services/governedOutboundTenant');
const { clearGoogleMailboxAccessTokenCache } = require('../utils/googleMailboxOAuth');

describe('Studio Substral mailbox config', () => {
  it('binds tenant 17 to hello@studiosubstral.com with isolated OAuth secret ref', () => {
    const cfg = studioSubstralMailboxConfig('17');
    assert.equal(cfg.integration.id, 'tmi_17_substral_hello');
    assert.equal(cfg.identity.senderEmail, 'hello@studiosubstral.com');
    assert.equal(cfg.integration.oauthRefreshSecretRef, 'STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN');
    assert.notEqual(cfg.integration.oauthRefreshSecretRef, 'ANCHOR_GOOGLE_REFRESH_TOKEN');
    assert.match(JSON.stringify(cfg), /STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN/);
    assert.doesNotMatch(JSON.stringify(cfg), /ANCHOR_GOOGLE_REFRESH_TOKEN/);
    assert.doesNotMatch(JSON.stringify(cfg), /BABRUN_MAILBOX/);
  });

  it('is full Google Workspace send+receive (not reply-only)', () => {
    const integration = normalizeIntegration({ ...studioSubstralMailboxConfig('17').integration });
    assert.equal(integration.mailboxAddress, STUDIO_SUBSTRAL_MAILBOX_CONFIG.mailboxAddress);
    assert.ok(usesGoogleOAuthSmtp(integration));
    assert.equal(isReplyOnlyMailbox(integration), false);
  });

  it('registers tenant 17 for governed tenant-mailbox transport', () => {
    assert.equal(assertGovernedOutboundTenantId('17'), '17');
    const ctx = createGovernedOutboundTenantContext('17');
    assert.equal(ctx.tenantId, '17');
    assert.equal(ctx.clientId, 17);
    assert.equal(ctx.usesTenantMailboxTransport, true);
    assert.equal(ctx.usesBrevoTransport, false);
  });

  it('refreshes tenant 17 with the OAuth client used by its bootstrap', async () => {
    const priorFetch = global.fetch;
    let requestBody;
    clearGoogleMailboxAccessTokenCache();
    global.fetch = async (_url, options) => {
      requestBody = new URLSearchParams(options.body);
      return {
        ok: true,
        json: async () => ({ access_token: 'substral-access-token', expires_in: 3600 }),
      };
    };
    try {
      const integration = studioSubstralMailboxConfig('17').integration;
      const auth = await resolveSmtpAuth(integration, {
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
      });

      assert.equal(auth.auth.accessToken, 'substral-access-token');
      assert.equal(requestBody.get('client_id'), 'substral-bootstrap-client');
      assert.equal(requestBody.get('client_secret'), 'substral-bootstrap-secret');
      assert.equal(requestBody.get('refresh_token'), 'substral-refresh-token');
    } finally {
      global.fetch = priorFetch;
      clearGoogleMailboxAccessTokenCache();
    }
  });
});
