'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {
  buildTodayQueue,
  formatAccountRow,
} = require('../services/aoCrmService');
const {
  isAoEligibleForAssignment,
  shouldExcludeFromTodayQueue,
  transferredReviewLabel,
} = require('../utils/aoRosterOperational');

test('SPEC-AO-ROSTER-REASSIGN-001 routes and script exist', () => {
  const aoRoutes = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(aoRoutes, /transfer-review/);
  assert.match(aoRoutes, /roster\/inactive-transfer/);
  const crm = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm.html'), 'utf8');
  assert.match(crm, /needs_reassignment/);
  assert.match(crm, /data-review="keep"/);
  assert.ok(fs.existsSync(path.join(__dirname, '..', 'scripts', 'runAoInactiveRosterReassignment.js')));
});

test('paused or inactive AOs are not eligible for assignment pools', () => {
  assert.equal(isAoEligibleForAssignment({ active: true, ao_operational_status: 'active' }), true);
  assert.equal(isAoEligibleForAssignment({ active: true, ao_operational_status: 'paused' }), false);
  assert.equal(isAoEligibleForAssignment({ active: false, ao_operational_status: 'active' }), false);
});

test('transferred review accounts stay out of Today\'s Queue', () => {
  const account = formatAccountRow({
    prospect_id: 'p1',
    company_name: 'Test Co',
    ao_current_status: 'ready_to_call',
    ao_paused: true,
    ao_review_bucket: 'needs_reassignment',
    ao_reassignment_prior_ao_id: 26,
    ao_reassignment_prior_ao_name: 'Zach',
  }, { dateStr: '2026-10-05', aoNameById: {} });
  assert.equal(shouldExcludeFromTodayQueue(account), true);
  assert.equal(transferredReviewLabel('needs_reassignment'), 'Needs reassignment');
  const queue = buildTodayQueue([account], '2026-10-05', { ownerOperationallyActive: true });
  assert.equal(queue.length, 0);
});

test('inactive AO owner gets an empty Today\'s Queue even with due work', () => {
  const account = formatAccountRow({
    prospect_id: 'p2',
    company_name: 'Due Co',
    ao_current_status: 'follow_up_needed',
    ao_paused: false,
    ao_next_action: 'call',
    next_action_due_at: '2026-10-05T12:00:00Z',
    help_requested: true,
  }, { dateStr: '2026-10-05', aoNameById: {} });
  account.due_today = true;
  account.overdue = false;
  const queue = buildTodayQueue([account], '2026-10-05', { ownerOperationallyActive: false });
  assert.equal(queue.length, 0);
});
