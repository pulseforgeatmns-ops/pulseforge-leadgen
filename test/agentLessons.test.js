const assert = require('node:assert/strict');
const test = require('node:test');

const { normalizeScope, parseJsonObject } = require('../utils/agentLessons');

test('agent lessons accept only explicit global scope', () => {
  assert.equal(normalizeScope('global'), 'global');
  assert.equal(normalizeScope('client'), 'client');
  assert.equal(normalizeScope(''), 'client');
});

test('agent lessons parse fenced JSON responses', () => {
  assert.deepEqual(parseJsonObject('```json\n{"lessons": []}\n```'), { lessons: [] });
});
