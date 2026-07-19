'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { ANCHOR_QUESTIONS, humanSetterPlaybook } = require('../utils/setterPlaybooks');

test('Anchor receives a category-specific, human-only cleaning playbook', () => {
  const playbook = humanSetterPlaybook({ clientId: 10, clientName: 'Anchor', vertical: 'str_manager' });
  assert.equal(playbook.mode, 'human_only');
  assert.match(playbook.opener, /Anchor Cleaning Services/);
  assert.deepEqual(playbook.qualification_questions, ANCHOR_QUESTIONS.str_manager);
  assert.match(playbook.safety, /does not initiate calls/);
});

test('managed-service clients receive a reusable tenant-branded playbook', () => {
  const playbook = humanSetterPlaybook({ clientId: 42, clientName: 'Northstar', vertical: 'general' });
  assert.equal(playbook.mode, 'human_only');
  assert.match(playbook.title, /Northstar/);
  assert.match(playbook.opener, /Northstar/);
  assert.equal(playbook.qualification_questions.length, 3);
});
