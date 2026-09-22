'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  evaluateDebrief,
  assessDebriefCompleteness,
  shouldBookAssessment,
} = require('../services/aoAdvisoryDebriefEvaluator');

function baseDebrief(overrides = {}) {
  return {
    person_spoken_to: 'Office manager',
    role: 'Office manager',
    decision_maker: 'Facilities director',
    current_cleaning_solution: 'Incumbent vendor',
    stated_context: 'They are covered for now.',
    problem_or_risk: null,
    opportunity_timing: null,
    opportunity_type: null,
    opportunity_strength: null,
    blocker: null,
    recommended_next_step: null,
    recommended_message: null,
    follow_up_due_at: null,
    next_owner: 'ao',
    prescribed_before_diagnosing: false,
    real_reason_to_continue: false,
    specific_dated_next_step: false,
    ...overrides,
  };
}

test('debrief with no diagnosis is incomplete and coaches operator', () => {
  const evaluation = evaluateDebrief(baseDebrief({
    recommended_next_step: 'Book facilities assessment',
    prescribed_before_diagnosing: true,
  }));
  assert.equal(evaluation.debrief_quality, 'weak');
  assert.ok(evaluation.incomplete);
  assert.match(evaluation.coaching_feedback, /diagnosis/i);
  assert.match(evaluation.coaching_feedback, /recommended a next step before/i);
});

test('debrief with vendor backup signal classifies AO_FOLLOW_UP', () => {
  const evaluation = evaluateDebrief(baseDebrief({
    problem_or_risk: 'They sometimes need backup during turnovers.',
    opportunity_timing: 'later',
    opportunity_type: 'backup_overflow',
    opportunity_strength: 'moderate',
    current_cleaning_solution: 'Existing cleaner',
    recommended_next_step: 'Send backup vendor information and follow up in 30 days.',
    recommended_message: 'Good talking with you today. Anchor can be a backup option if your regular cleaner is overloaded.',
    follow_up_due_at: '2026-10-22T00:00:00.000Z',
    real_reason_to_continue: true,
    specific_dated_next_step: true,
  }));
  assert.equal(evaluation.next_action, 'AO_FOLLOW_UP');
  assert.match(evaluation.classification_reason, /backup|follow/i);
});

test('debrief with real active need can classify BOOK_ASSESSMENT', () => {
  const debrief = baseDebrief({
    problem_or_risk: 'Current cleaner missed last turnover and they need coverage now.',
    opportunity_timing: 'now',
    opportunity_type: 'turnover_support',
    opportunity_strength: 'strong',
    recommended_next_step: 'Book facilities assessment this week.',
    follow_up_due_at: '2026-09-25T00:00:00.000Z',
    real_reason_to_continue: true,
    specific_dated_next_step: true,
  });
  assert.equal(shouldBookAssessment(debrief), true);
  const evaluation = evaluateDebrief(debrief);
  assert.ok(['BOOK_ASSESSMENT', 'JAKE_REVIEW'].includes(evaluation.next_action));
});

test('debrief with quote-ready strong need can classify JAKE_REVIEW', () => {
  const evaluation = evaluateDebrief(baseDebrief({
    problem_or_risk: 'Needs quote for recurring office cleaning now.',
    opportunity_timing: 'now',
    opportunity_type: 'recurring_cleaning',
    opportunity_strength: 'strong',
    recommended_next_step: 'Send quote request to Jake.',
    follow_up_due_at: '2026-09-23T00:00:00.000Z',
    real_reason_to_continue: true,
    specific_dated_next_step: true,
  }));
  assert.equal(evaluation.next_action, 'JAKE_REVIEW');
});

test('bad fit debrief classifies SUPPRESS', () => {
  const evaluation = evaluateDebrief(baseDebrief({
    problem_or_risk: 'No cleaning need and they are all set.',
    opportunity_timing: 'not_at_all',
    opportunity_type: 'not_a_fit',
    opportunity_strength: 'weak',
    recommended_next_step: 'Close out and suppress.',
    real_reason_to_continue: false,
    specific_dated_next_step: true,
    follow_up_due_at: '2026-09-23T00:00:00.000Z',
  }));
  assert.equal(evaluation.next_action, 'SUPPRESS');
});

test('unknown decision-maker defaults to NEEDS_RESEARCH', () => {
  const evaluation = evaluateDebrief(baseDebrief({
    person_spoken_to: 'Receptionist',
    role: 'Receptionist',
    decision_maker: null,
    problem_or_risk: 'Might need cleaning later.',
    opportunity_timing: 'later',
    opportunity_type: 'recurring_cleaning',
    opportunity_strength: 'weak',
    recommended_next_step: 'Send proposal',
    follow_up_due_at: '2026-10-01T00:00:00.000Z',
    real_reason_to_continue: true,
    specific_dated_next_step: true,
  }));
  assert.equal(evaluation.next_action, 'NEEDS_RESEARCH');
});

test('assessDebriefCompleteness flags missing dated next step', () => {
  const missing = assessDebriefCompleteness(baseDebrief({
    problem_or_risk: 'Backup need during turnovers.',
    recommended_next_step: 'Follow up later',
    specific_dated_next_step: false,
  }));
  assert.ok(missing.includes('dated_next_step'));
});

test('guardrail blocks facilities assessment without real need', () => {
  const debrief = baseDebrief({
    recommended_next_step: 'Book facilities assessment',
    opportunity_timing: 'later',
    opportunity_strength: 'unclear',
    problem_or_risk: null,
  });
  assert.equal(shouldBookAssessment(debrief), false);
});
