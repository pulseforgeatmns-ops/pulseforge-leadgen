'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  hasObservedInterest,
  normalizeObservedInterestLevel,
  observedInterestRankWeight,
} = require('../utils/aoInterestLevel');
const { computeRankScore, mapAccountRow } = require('../utils/aoAccountPrioritization');
const { formatAccountBriefing } = require('../utils/aoAccountBriefing');
const { buildAssignmentNote } = require('../utils/aoAssignment');
const { BATCH_SLUG } = require('../scripts/data/jakeAoProspectBook');

function assignmentLeadFixture(overrides = {}) {
  const note = buildAssignmentNote({
    batchSlug: BATCH_SLUG,
    company: 'LNH Property Management',
    ownerName: 'Jake',
    lane: 'property_management',
    dueDate: '2026-09-16',
    aoOwnerId: 7,
  });

  return {
    business_name: 'LNH Property Management',
    address: '945 Elm St, Manchester NH',
    business_type: 'property_management',
    status: 'needs_follow_up',
    interest_level: null,
    priority: 'high',
    contact_name: null,
    open_next_action: 'research',
    open_task_due: '2026-09-16',
    waiting_on_jake: false,
    last_interaction_summary: note,
    original_visit_note: note,
    probe_answers: null,
    open_escalation_id: null,
    open_escalation_status: null,
    crm_prospect_id: null,
    ao_owner_id: 7,
    ...overrides,
  };
}

test('normalizeObservedInterestLevel treats null/empty as unassessed', () => {
  assert.equal(normalizeObservedInterestLevel(null), null);
  assert.equal(normalizeObservedInterestLevel(''), null);
  assert.equal(normalizeObservedInterestLevel(undefined), null);
  assert.equal(normalizeObservedInterestLevel('high'), 'high');
});

test('observedInterestRankWeight returns zero for unassessed interest', () => {
  assert.equal(observedInterestRankWeight(null), 0);
  assert.equal(observedInterestRankWeight('low'), 1);
  assert.equal(observedInterestRankWeight('high'), 3);
});

test('fresh assignment briefing does not present low or medium interest', () => {
  const reply = formatAccountBriefing(assignmentLeadFixture(), { today: '2026-09-16' });
  assert.doesNotMatch(reply, /low interest/i);
  assert.doesNotMatch(reply, /medium interest/i);
  assert.doesNotMatch(reply, /High interest recorded/i);
  assert.match(reply, /No decision-maker captured yet/i);
  assert.match(reply, /Research stage/i);
});

test('unassessed interest does not add ranking weight; observed low does', () => {
  const today = '2026-09-16';
  const state = 'follow_up_needed';
  const base = {
    due_date: today,
    priority: 'normal',
    waiting_on_jake: false,
  };

  const unassessed = computeRankScore({ ...base, interest_level: null }, state, today);
  const observedLow = computeRankScore({ ...base, interest_level: 'low' }, state, today);
  const observedHigh = computeRankScore({ ...base, interest_level: 'high' }, state, today);

  assert.ok(unassessed < observedLow);
  assert.ok(observedLow < observedHigh);
  assert.equal(unassessed + 20, observedLow);
  assert.equal(observedLow + 40, observedHigh);
});

test('real logged high interest still affects ranking and briefing', () => {
  const today = '2026-09-16';
  const high = mapAccountRow({
    task_id: 'task-1',
    lead_id: 'lead-1',
    business_name: 'Warm Walkthrough Co',
    lead_status: 'walkthrough_requested',
    priority: 'warm',
    interest_level: 'high',
    due_date: today,
    next_action: 'Book walkthrough',
    waiting_on_jake: false,
    task_status: 'open',
    last_interaction_summary: 'Sarah requested a walkthrough of the common areas.',
    original_visit_note: 'Sarah requested a walkthrough of the common areas.',
    contact_name: 'Sarah Chen',
    contact_title: 'Property Manager',
    is_decision_maker: true,
  }, today);

  assert.equal(hasObservedInterest(high.interest_level), true);
  assert.ok(high.rank_score > mapAccountRow({
    task_id: 'task-2',
    lead_id: 'lead-2',
    business_name: 'Unassessed Co',
    lead_status: 'needs_follow_up',
    priority: 'normal',
    interest_level: null,
    due_date: today,
    next_action: 'research',
    waiting_on_jake: false,
    task_status: 'open',
    last_interaction_summary: assignmentLeadFixture().original_visit_note,
    original_visit_note: assignmentLeadFixture().original_visit_note,
  }, today).rank_score);

  const reply = formatAccountBriefing({
    business_name: 'Warm Walkthrough Co',
    status: 'walkthrough_requested',
    interest_level: 'high',
    contact_name: 'Sarah Chen',
    contact_title: 'Property Manager',
    is_decision_maker: true,
    last_interaction_summary: 'Sarah requested a walkthrough of the common areas.',
    original_visit_note: 'Sarah requested a walkthrough of the common areas.',
    open_next_action: 'Book walkthrough',
    open_task_due: today,
    priority: 'warm',
  }, { today });

  assert.match(reply, /Walkthrough stage/i);
});
