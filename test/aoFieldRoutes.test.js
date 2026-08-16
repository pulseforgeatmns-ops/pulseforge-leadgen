'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');

test('ensureAoFieldSchema exports singleton initializer', () => {
  const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
  assert.equal(typeof ensureAoFieldSchema, 'function');
});

test('getAoProfile does not filter by role', async () => {
  const aoField = require('../services/aoFieldService');
  assert.match(String(aoField.getAoProfile), /FROM users WHERE id = \$1 LIMIT 1/);
  assert.doesNotMatch(String(aoField.getAoProfile), /role = 'ao'/);
});
