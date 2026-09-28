'use strict';

const TENANT_ID = '13';
const CLIENT_ID = 13;

const BABRUN_OBJECTIVE =
  'Book discovery calls with founder-led small business owners in the United States for Babrun\'s 12-week business transformation program.';

const BABRUN_TARGET_SEGMENT = 'Small Business Owners';

const BABRUN_MAILBOX = Object.freeze({
  inboxIntegrationId: 'tmi_13_babrun_hello',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  senderEmail: 'hello@babrun.com',
  senderDisplayName: 'Fedir | Babrun',
  replyToAddress: 'hello@babrun.com',
});

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  BABRUN_OBJECTIVE,
  BABRUN_TARGET_SEGMENT,
  BABRUN_MAILBOX,
};
