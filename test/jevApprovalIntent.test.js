'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { QUESTIONS } = require('../packages/decision-service/providers/JevProvider');
const { assessJevDecisionForActivePromotion, readActiveRoutingConfig } = require('../packages/decision-service/activeRoutingPromotion');
const fixtures = require('./fixtures/jevProbeCases.json');
const { validateCases } = require('../packages/decision-service/jevProbe');

const config = readActiveRoutingConfig({ JEV_ACTIVE_ROUTING_ENABLED: 'true' });
const candidate = Object.freeze({
  intent: 'status_check', confidence: 0.99,
  mission_bound_probability: 0.9, approval_probability: 0.01,
  inspection_probability: 0.99, requires_human_clarification: false,
  risk_if_misrouted: 'low', recommended_route: 'inspection',
});

test('JEV provider question makes pending-decision binding a prerequisite for approval', () => {
  assert.match(QUESTIONS.recommended_route.instructions, /specific pending decision present in context/i);
  assert.match(QUESTIONS.recommended_route.instructions, /asking whether to approve/i);
  assert.match(QUESTIONS.recommended_route.instructions, /without pending-decision context/i);
  assert.match(QUESTIONS.approval_probability.instructions, /specific pending action shown in supplied context/i);
  assert.match(QUESTIONS.intent.instructions, /not words mentioned in a question/i);
});

test('original and new synthetic routing cases remain unreviewed until independent labeling', () => {
  validateCases(fixtures);
  assert.equal(fixtures.length, 32);
  assert.ok(fixtures.every(row => row.label_status === 'unreviewed'));
  for (const id of ['approval_question', 'approval_reference', 'approve_no_context', 'question_publish', 'unbound_yes', 'explicit_readonly']) {
    assert.ok(fixtures.some(row => row.id === id));
  }
});

test('active promotion cannot use JEV approval, mission, or specialist recommendations', () => {
  for (const recommended_route of ['approval', 'mission', 'specialist', 'clarification', 'unknown']) {
    assert.equal(assessJevDecisionForActivePromotion({ ...candidate, recommended_route }, config).eligible, false);
  }
});

test('active promotion rejects clarification and elevated approval signals even when inspection is suggested', () => {
  assert.equal(assessJevDecisionForActivePromotion({ ...candidate, requires_human_clarification: true }, config).eligible, false);
  assert.equal(assessJevDecisionForActivePromotion({ ...candidate, approval_probability: 0.6 }, config).eligible, false);
});
