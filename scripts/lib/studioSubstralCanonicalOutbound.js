'use strict';

const TENANT_ID = '17';

const STUDIO_SUBSTRAL_MAILBOX = Object.freeze({
  inboxIntegrationId: 'tmi_17_substral_hello',
  sendingIdentityId: 'tsi_17_substral_hello',
  senderEmail: 'hello@studiosubstral.com',
  senderDisplayName: 'Studio Substral',
  replyToAddress: 'hello@studiosubstral.com',
});

function resolveStudioSubstralClientId(client) {
  return Number(client?.id || process.env.STUDIO_SUBSTRAL_CLIENT_ID || TENANT_ID);
}

module.exports = {
  TENANT_ID,
  STUDIO_SUBSTRAL_MAILBOX,
  resolveStudioSubstralClientId,
};
