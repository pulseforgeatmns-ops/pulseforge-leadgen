'use strict';

const assert = require('assert');
const { APPLY_CONFIRMATION, findQueueDuplicates, normalizePhone, parseQueue, validateRow } = require('../scripts/importAnchorVerifiedQueue');

const valid = {
  company: 'Example Property Management',
  phone: '(603) 555-1212',
  vertical: 'property_manager',
  contact_name: 'Alex Example',
  website: 'example.com',
  verification_source: 'owner review',
  verified_at: '2026-07-23T12:00:00.000Z',
  manual_verified: true,
};

assert.strictEqual(APPLY_CONFIRMATION, 'client_10-anchor-verified-queue-2026-07-18');
assert.strictEqual(normalizePhone('(603) 555-1212'), '+16035551212');
assert.strictEqual(normalizePhone('555-1212'), null);
assert.strictEqual(validateRow(valid, 0).valid, true);
assert.match(validateRow({ ...valid, vertical: 'accounting' }, 0).errors.join(' '), /approved Anchor category/);
assert.match(validateRow({ ...valid, manual_verified: false }, 0).errors.join(' '), /manual_verified/);
assert.match(validateRow({ ...valid, extra: 'nope' }, 0).errors.join(' '), /unsupported fields/);
assert.strictEqual(parseQueue(JSON.stringify({ leads: [valid] }))[0].lead.phone, '+16035551212');
assert.deepStrictEqual(findQueueDuplicates([validateRow(valid, 0).lead, validateRow(valid, 1).lead]), [{ index: 1, duplicateOf: 0 }]);

console.log('Anchor verified queue import tests passed');
