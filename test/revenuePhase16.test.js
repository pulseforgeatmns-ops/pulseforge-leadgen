'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { REQUIRED_ACTIONS, validateAuthorization } = require('../utils/revenuePhase16');

function authorization() {
  return {
    phase: 'revenue-phase-1.6', client_id: 10, authorized_operator: 'operator@example.test',
    authorized_actions: [...REQUIRED_ACTIONS], maximum_canary_outcomes: 1,
    external_sends_allowed: false, refunds_allowed: false, max_mutations_allowed: false,
    window_start: '2026-07-18T12:00:00Z', window_end: '2026-07-18T13:00:00Z',
    stop_conditions: ['any mismatch'], rollback_owner: 'operator@example.test',
    approved_by: 'approver@example.test', approved_at: '2026-07-18T11:50:00Z',
  };
}

test('Phase 1.6 authorization is exact, bounded, and fails closed outside its window', () => {
  const valid = validateAuthorization(authorization(), new Date('2026-07-18T12:30:00Z'));
  assert.equal(valid.valid, true);
  assert.match(valid.authorizationHash, /^[a-f0-9]{64}$/);
  const invalid = authorization(); invalid.refunds_allowed = true;
  assert.equal(validateAuthorization(invalid, new Date('2026-07-18T12:30:00Z')).valid, false);
  assert.equal(validateAuthorization(authorization(), new Date('2026-07-18T13:01:00Z')).valid, false);
});
