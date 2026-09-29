'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  ANCHOR_MAILBOX_CONFIG,
  AUTH_MODES,
  MAILBOX_STATUS,
  MemoryTenantMailboxStore,
  anchorMailboxConfig,
  publicIntegration,
  resolveImapAuth,
  resolveImapAuthMode,
  isReplyOnlyMailbox,
  isImapConfigured,
  canResolveImapCredential,
  verifyTenantMailbox,
  pollTenantMailbox,
} = require('../services/tenantMailbox');
const { clearGoogleMailboxAccessTokenCache } = require('../utils/googleMailboxOAuth');

describe('Anchor mailbox Google OAuth2', () => {
  let originalFetch;

  beforeEach(() => {
    clearGoogleMailboxAccessTokenCache();
    originalFetch = global.fetch;
    global.fetch = async () => ({
      ok: true,
      json: async () => ({ access_token: 'oauth-access-token', expires_in: 3600, token_type: 'Bearer' }),
    });
  });

  afterEach(() => {
    global.fetch = originalFetch;
    clearGoogleMailboxAccessTokenCache();
  });

  function oauthEnv() {
    return {
      GOOGLE_CLIENT_ID: 'google-client-id',
      GOOGLE_CLIENT_SECRET: 'google-client-secret',
      ANCHOR_GOOGLE_REFRESH_TOKEN: 'anchor-refresh-token',
    };
  }

  function secretResolver(ref) {
    return oauthEnv()[ref];
  }

  it('anchorMailboxConfig uses GOOGLE_OAUTH2 and ANCHOR_GOOGLE_REFRESH_TOKEN ref', () => {
    const cfg = anchorMailboxConfig('10');
    assert.equal(cfg.integration.id, 'tmi_10_anchor_jacob');
    assert.equal(cfg.integration.mailboxAddress, ANCHOR_MAILBOX_CONFIG.mailboxAddress);
    assert.equal(cfg.integration.imapAuthMode, AUTH_MODES.GOOGLE_OAUTH2);
    assert.equal(cfg.integration.oauthRefreshSecretRef, 'ANCHOR_GOOGLE_REFRESH_TOKEN');
    assert.equal(cfg.integration.imapHost, 'imap.gmail.com');
    assert.equal(cfg.integration.smtpHost, undefined);
    assert.doesNotMatch(JSON.stringify(cfg), /anchor-refresh-token|dummy-password/);
  });

  it('publicIntegration strips oauth refresh secret ref', async () => {
    const store = new MemoryTenantMailboxStore();
    const integration = await store.saveIntegration(anchorMailboxConfig('10').integration);
    const safe = publicIntegration(integration);
    assert.equal(safe.oauthRefreshSecretRef, undefined);
    assert.equal(safe.hasOAuthRefreshSecretRef, true);
    assert.equal(safe.imapSecretRef, undefined);
  });

  it('resolveImapAuth returns XOAUTH2 accessToken and never password', async () => {
    const integration = anchorMailboxConfig('10').integration;
    const imapAuth = await resolveImapAuth(integration, { env: oauthEnv() });
    assert.equal(imapAuth.mode, AUTH_MODES.GOOGLE_OAUTH2);
    assert.equal(imapAuth.auth.user, 'jacob@goanchorcleaning.com');
    assert.equal(imapAuth.auth.accessToken, 'oauth-access-token');
    assert.equal(imapAuth.auth.pass, undefined);
  });

  it('does not fall back to password when OAuth refresh fails', async () => {
    global.fetch = async () => ({
      ok: false,
      json: async () => ({ error: 'invalid_grant' }),
    });
    const integration = {
      ...anchorMailboxConfig('10').integration,
      imapSecretRef: 'ANCHOR_MAILBOX_PASSWORD',
    };
    await assert.rejects(
      () => resolveImapAuth(integration, {
        env: { ...oauthEnv(), ANCHOR_MAILBOX_PASSWORD: 'legacy-password' },
      }),
      (err) => err.code === 'google_oauth_refresh_failed'
    );
  });

  it('reply-only verification skips SMTP and activates on IMAP success', async () => {
    const store = new MemoryTenantMailboxStore();
    const integration = await store.saveIntegration({
      ...anchorMailboxConfig('10').integration,
      status: MAILBOX_STATUS.UNVERIFIED,
    });
    assert.equal(isReplyOnlyMailbox(integration), true);
    assert.equal(resolveImapAuthMode(integration), AUTH_MODES.GOOGLE_OAUTH2);

    const result = await verifyTenantMailbox({
      tenantId: '10',
      integrationId: integration.id,
    }, {
      store,
      env: oauthEnv(),
      imapVerifier: async ({ imapAuth }) => {
        assert.equal(imapAuth.auth.accessToken, 'oauth-access-token');
      },
    });

    assert.equal(result.verificationState.smtp.status, 'not_required');
    assert.equal(result.verificationState.imap.status, 'verified');
    assert.equal(result.integration.status, MAILBOX_STATUS.ACTIVE);
  });

  it('isImapConfigured and canResolveImapCredential use oauth refresh ref', () => {
    const integration = anchorMailboxConfig('10').integration;
    assert.equal(isImapConfigured(integration), true);
    assert.equal(canResolveImapCredential(integration, { env: oauthEnv() }), true);
    assert.equal(canResolveImapCredential(integration, { env: {} }), false);
  });

  it('pollTenantMailbox uses OAuth adapter auth for Anchor integration', async () => {
    const store = new MemoryTenantMailboxStore();
    const integration = await store.saveIntegration({
      ...anchorMailboxConfig('10').integration,
      status: MAILBOX_STATUS.ACTIVE,
    });
    let capturedAuth;
    const result = await pollTenantMailbox({
      tenantId: '10',
      integrationId: integration.id,
    }, {
      store,
      env: oauthEnv(),
      imapAdapter: {
        async fetchNewMessages({ imapAuth }) {
          capturedAuth = imapAuth;
          return [];
        },
      },
    });
    assert.equal(capturedAuth.mode, AUTH_MODES.GOOGLE_OAUTH2);
    assert.equal(capturedAuth.auth.accessToken, 'oauth-access-token');
    assert.equal(result.fetched, 0);
  });
});
