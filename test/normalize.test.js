const assert = require('node:assert/strict');
const { normalizeVertical } = require('../utils/normalize');

const cases = [
  ['Home Services', 'home_services'],
  [' Med-Spa ', 'med_spa'],
  ['auto repair', 'auto_repair'],
  ['med spa', 'med_spa'],
  ['Property.Management', 'property_management'],
  [null, null],
  ['', null],
  ['home_services', 'home_services'],
];

for (const [input, expected] of cases) {
  assert.equal(normalizeVertical(input), expected);
}

console.log('normalizeVertical tests passed');
