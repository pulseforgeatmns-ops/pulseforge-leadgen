'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  validateMarkDeadInput,
  isValidDeadReason,
  AO_DEAD_REASONS,
} = require('../utils/aoDispositionTypes');
const { accountIsActive } = require('../utils/aoCrmTypes');

test('dead reason catalog matches spec', () => {
  assert.equal(AO_DEAD_REASONS.length, 8);
  assert.equal(isValidDeadReason('not_a_fit'), true);
  assert.equal(isValidDeadReason('invalid'), false);
});

test('validateMarkDeadInput requires reason and other note', () => {
  assert.equal(validateMarkDeadInput({ reason: '', note: '' }).error, 'Valid dead reason required');
  assert.equal(validateMarkDeadInput({ reason: 'other', note: '  ' }).error, 'Note required when reason is Other');
  const ok = validateMarkDeadInput({ reason: 'duplicate', note: '' });
  assert.equal(ok.reason, 'duplicate');
  assert.equal(ok.note, null);
});

test('accountIsActive excludes dead disposition', () => {
  assert.equal(accountIsActive({ disposition_status: 'dead', ao_paused: false }), false);
  assert.equal(accountIsActive({ disposition_status: 'active', ao_current_status: 'ready_to_call', ao_paused: false }), true);
});

test('ao routes expose mark-dead endpoints', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(src, /\/api\/tasks\/:id\/mark-dead/);
  assert.match(src, /\/api\/prospects\/:prospectId\/mark-dead/);
  assert.match(src, /\/api\/mark-dead\/reasons/);
});

test('field dashboard exposes Mark dead queue action', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.match(src, /data-mark-dead/);
  assert.match(src, /mark-dead/);
});

test('queue and route queries exclude dead field leads', () => {
  const field = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoFieldService.js'), 'utf8');
  const route = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoRouteService.js'), 'utf8');
  assert.match(field, /disposition_status/);
  assert.match(route, /disposition_status/);
});
