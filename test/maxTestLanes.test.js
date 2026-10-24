'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { manifests } = require('../scripts/maxTestLanes.cjs');
test('Max lane partition retains every original entry exactly once', () => {
  const { unit, integration, full } = manifests();
  assert.deepEqual(integration, ['test/maxAttentionCronRoute.test.js', 'test/maxAttentionAuthorization.test.js']);
  assert.deepEqual([...unit, ...integration].sort(), [...full].sort());
  assert.equal(new Set([...unit, ...integration]).size, full.length);
  assert.ok(unit.includes('packages/max/workspace/tests/workspaceTestHarness.test.js'));
});
