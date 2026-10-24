'use strict';

const TENANT_ID = '1';

const PULSEFORGE_MAILBOX = Object.freeze({
  inboxIntegrationId: 'tmi_1_pulseforge_jacob',
  sendingIdentityId: 'tsi_1_pulseforge_jacob',
  senderEmail: 'jacob@pulseforge.com',
  senderDisplayName: 'Jacob Maynard',
  replyToAddress: 'jacob@pulseforge.com',
});

module.exports = { TENANT_ID, PULSEFORGE_MAILBOX };
