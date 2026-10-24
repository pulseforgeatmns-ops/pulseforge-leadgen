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

test('required Max CI lane scripts retain their guarded commands', () => {
  const scripts = require('../package.json').scripts;
  assert.equal(scripts['test:max:unit'], 'node scripts/maxTestLanes.cjs unit');
  assert.equal(scripts['test:max:integration'], 'node scripts/maxTestLanes.cjs integration');
  assert.equal(scripts['test:max:fixtures'], 'node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test packages/max/workspace/tests/workspaceTestHarness.test.js test/maxTestLanes.test.js test/operatorObjectiveContextInjection.test.js test/maxWorkspaceDependencyInjection.test.js');
});
