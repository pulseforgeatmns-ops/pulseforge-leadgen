'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { validateCases, probeJev } = require('../packages/decision-service/jevProbe');
const input = [
  { id: 'status01', category: 'status', provenance: 'synthetic', operator_message: 'What is the current status of our mission?', expected_route: 'inspection', label_status: 'unreviewed', legacy_route: 'conversation' },
  { id: 'ambig01', category: 'clarification', provenance: 'synthetic', operator_message: 'Maybe just approve it?', expected_route: 'clarification', label_status: 'unreviewed', legacy_route: 'clarification' },
];
const decision = route => ({
  intent: 'status_check', confidence: 0.99, mission_bound_probability: 0.8,
  approval_probability: 0.05, inspection_probability: 0.92,
  requires_human_clarification: false, risk_if_misrouted: 'low', recommended_route: route,
});
test('isolated provider probe scores no unreviewed decisions and emits no operator text', async () => {
  const observed = [];
  const report = await probeJev(input, { provider: { evaluate: async state => {
    observed.push(state.operator_message);
    return { decision: decision('inspection') };
  } } });
  assert.equal(report.provider_requests, 2);
  assert.equal(report.actual_provider_invoked, true);
  assert.equal(report.reviewed_prediction_count, 0);
  assert.equal(report.production_acceptance_eligible, false);
  assert.equal(report.activation_authorized, false);
  assert.deepEqual(observed, input.map(x => x.operator_message));
  assert.doesNotMatch(JSON.stringify(report), /What is the current status|Maybe just approve/);
});
test('provider exceptions produce missing predictions rather than successes', async () => {
  const report = await probeJev([{ ...input[0], label_status: 'human_reviewed' }], {
    provider: { evaluate: async () => { const e = new Error('token secret'); e.code = 'http_error'; throw e; } },
  });
  assert.equal(report.unevaluated_predictions, 1);
  assert.equal(report.jev_accuracy, null);
  assert.deepEqual(report.failures, [{ id: 'status01', code: 'http_error' }]);
  assert.doesNotMatch(JSON.stringify(report), /token secret/);
});
test('reject non-synthetic and sensitive prompts before provider invocation', () => {
  assert.throws(() => validateCases([{ ...input[0], provenance: 'prospective_shadow' }]));
  assert.throws(() => validateCases([{ ...input[0], operator_message: 'send to someone@example.com' }]));
  assert.throws(() => validateCases([...input, input[0]]));
});

test('timed-out provider that ignores abort cannot stall evaluation', async () => {
  const started = Date.now();
  const report = await probeJev([{ ...input[0], label_status: 'human_reviewed' }], {
    timeoutMs: 100,
    provider: { evaluate: async () => new Promise(() => {}) },
  });
  assert.ok(Date.now() - started < 2000);
  assert.deepEqual(report.failures, [{ id: 'status01', code: 'timeout' }]);
  assert.equal(report.reviewed_prediction_count, 0);
  assert.equal(report.activation_authorized, false);
});
