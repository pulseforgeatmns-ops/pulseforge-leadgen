'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  studioSubstralMailboxConfig,
  STUDIO_SUBSTRAL_MAILBOX_CONFIG,
  isReplyOnlyMailbox,
  usesGoogleOAuthSmtp,
  normalizeIntegration,
} = require('../services/tenantMailbox');
const { createGovernedOutboundTenantContext, assertGovernedOutboundTenantId } = require('../services/governedOutboundTenant');

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
});
