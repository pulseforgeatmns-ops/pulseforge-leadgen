'use strict';

/**
 * AUDIT-085 — webhook sender-identity classification unit coverage.
 */

const assert = require('node:assert/strict');
const {
  classifyProviderEventSenderIdentity,
  normalizeSendingDomain,
} = require('../utils/canonicalSenderIdentity');
const {
  senderEmail,
  rootDomainFromEmail,
  normalizeRootDomain,
} = require('../utils/brevoEvents');

assert.equal(normalizeSendingDomain('https://www.GoAnchorCleaning.com/path'), 'goanchorcleaning.com');
assert.equal(rootDomainFromEmail('Ops@GoAnchorCleaning.com'), 'goanchorcleaning.com');
assert.equal(senderEmail({ from: 'Anchor <ops@goanchorcleaning.com>' }), 'ops@goanchorcleaning.com');
assert.equal(normalizeRootDomain('mail.domain-b.com'), 'domain-b.com');

const mismatch = classifyProviderEventSenderIdentity({
  eventSendingDomain: 'domain-b.com',
  tenantSendingDomain: 'domain-a.com',
});
assert.equal(mismatch.status, 'mismatch');
assert.equal(mismatch.reputationExcluded, true);

const matched = classifyProviderEventSenderIdentity({
  eventSenderEmail: 'hello@domain-a.com',
  tenantSendingDomain: 'domain-a.com',
});
assert.equal(matched.status, 'matched');
assert.equal(matched.reputationExcluded, false);

const unknown = classifyProviderEventSenderIdentity({
  tenantSendingDomain: 'domain-a.com',
});
assert.equal(unknown.status, 'unknown');
assert.equal(unknown.reputationExcluded, false);

console.log('canonical sender webhook identity tests passed');
