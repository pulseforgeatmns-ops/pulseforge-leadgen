'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { aoWorkingEligibilitySql } = require('../utils/aoWorkingEligibility');

test('AO working predicate includes tenant-specific review gate and universal exclusions', () => {
  const sql = aoWorkingEligibilitySql('p');
  for (const expected of ['ao_outreach_review_required', 'do_not_contact', 'prospect_motion',
    'disposition_status', 'ao_current_status', 'ao_next_action', 'ao_disqualification_reason']) {
    assert.match(sql, new RegExp(expected));
  }
  assert.match(sql, /NOT IN \('SUPPRESS', 'EMAIL_LED'\)/);
  assert.doesNotMatch(sql, /ao_call_suppressed/);
});

test('AO working predicate rejects invalid aliases', () => {
  assert.throws(() => aoWorkingEligibilitySql('p; DROP TABLE prospects'), /Invalid prospect SQL alias/);
});
