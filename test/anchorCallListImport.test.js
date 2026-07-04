'use strict';

const assert = require('assert');
const {
  SERVICE_AREA,
  choosePlace,
  manifest,
  normalizeDomain,
  resolveVertical,
} = require('../scripts/importAnchorCallList');

assert.deepStrictEqual(SERVICE_AREA, ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn']);
assert.strictEqual(manifest.length, 29);
assert.strictEqual(manifest.filter(row => row.group === 'A').length, 15);
assert.strictEqual(manifest.filter(row => row.group === 'B').length, 10);
assert.strictEqual(manifest.filter(row => row.group === 'C').length, 4);
assert.strictEqual(manifest.find(row => row.firm === 'Patrick Kelly').decision, 'skip_retiring');
assert.strictEqual(manifest.find(row => row.firm === 'Manning Zimmerman & Oliveira').disposition, 'gatekeeper_relayed');
assert.strictEqual(manifest.find(row => row.firm === 'Cohen & Winters').disposition, undefined);
assert.strictEqual(manifest.find(row => row.firm === 'Cohen & Winters').special, 'callback_note_only');

assert.strictEqual(normalizeDomain('https://www.Example.com/path'), 'example.com');
assert.strictEqual(resolveVertical({ primaryType: 'lawyer' }, 'accounting').vertical, 'law_firm');
assert.strictEqual(resolveVertical({ primaryTypeDisplayName: { text: 'Tax consultant' } }, 'law_firm').vertical, 'accounting');
assert.strictEqual(resolveVertical({ primaryType: 'business_center' }, 'accounting').vertical, 'accounting');

const row = { firm: 'Example Law', phone: '(603) 555-1212', city: 'Bedford', address: '12 Main St' };
const verified = choosePlace(row, [{
  displayName: { text: 'Example Law' },
  formattedAddress: '12 Main Street, Bedford, NH 03110',
  nationalPhoneNumber: '(603) 555-1212',
}]);
assert.strictEqual(verified.verified, true);

const noAddress = choosePlace({ ...row, address: null }, [{
  displayName: { text: 'Example Law' },
  formattedAddress: '12 Main Street, Bedford, NH 03110',
  nationalPhoneNumber: '(603) 555-1212',
}]);
assert.strictEqual(noAddress.verified, true);
assert.match(noAddress.reason, /omitted address/);

const mismatch = choosePlace(row, [{
  displayName: { text: 'Different Firm' },
  formattedAddress: '99 Other Rd, Bedford, NH 03110',
  nationalPhoneNumber: '(603) 555-9999',
}]);
assert.strictEqual(mismatch.verified, false);

console.log('Anchor call-list import tests passed');
