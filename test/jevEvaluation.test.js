'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { evaluateCases } = require('../packages/decision-service/jevEvaluation');
const base = Object.freeze({ intent: 'status_check', confidence: 0.99, mission_bound_probability: 0.9, approval_probability: 0.02, inspection_probability: 0.96, requires_human_clarification: false, risk_if_misrouted: 'low', recommended_route: 'inspection' });
const scenario = (id, expected_route, predicted, legacy_route, category = 'status', status = 'human_reviewed') => ({
  id, category, expected_route, label_status: status, provenance: 'synthetic', legacy_route,
  jev_decision: predicted == null ? null : { ...base, recommended_route: predicted },
});
test('independently labeled routing comparisons and high-risk misroutes', () => {
  const r = evaluateCases([scenario('one', 'inspection', 'inspection', 'conversation'), scenario('two', 'clarification', 'approval', 'clarification', 'approval'), scenario('three', 'mission', null, 'mission')]);
  assert.equal(r.reviewed_prediction_count, 2);
  assert.equal(r.jev_accuracy, 0.5);
  assert.equal(r.legacy_accuracy, 2 / 3);
  assert.equal(r.dangerous_misroutes, 1);
  assert.equal(r.unevaluated_predictions, 1);
  assert.equal(r.production_acceptance_eligible, false);
  assert.equal(r.activation_authorized, false);
});
test('missing predictions and unreviewed labels cannot inflate score', () => {
  const r = evaluateCases([scenario('missing', 'inspection', null, 'inspection'), scenario('unreviewed', 'inspection', 'inspection', 'inspection', 'status', 'unreviewed')]);
  assert.equal(r.reviewed_prediction_count, 0);
  assert.equal(r.jev_accuracy, null);
});
test('duplicate and invalid cases fail closed', () => {
  assert.throws(() => evaluateCases([]));
  assert.throws(() => evaluateCases([scenario('same','inspection','inspection'), scenario('same','inspection','inspection')]));
  assert.throws(() => evaluateCases([{ ...scenario('x','inspection','inspection'), provenance: 'production' }]));
  assert.throws(() => evaluateCases([{ ...scenario('x','inspection','inspection'), jev_decision: { ...base, confidence: 2 } }]));
});
