'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { observedRoute, observedAoRoute } = require('../../../decision-service/observedRoute');
const { classifyShadowMismatch, isDangerousMismatchClassification } = require('../../../decision-service/shadowMismatchClassification');
const { enrichShadowRow } = require('../../../decision-service/shadowRowProjection');
const { buildShadowReview } = require('../../../decision-service/shadowReview');
const { evaluateDeployGates, DEPLOY_GATES, ACTIVE_JEV_ROUTING_ENABLED } = require('../../../decision-service/deployGates');
const {
  classifyPendingDecisionCaptureIntent,
  CAPTURE_INTENTS,
  isHighConfidenceJevInspection,
} = require('../pendingDecisionCaptureGuard');
const { OPERATOR_DECISION_KINDS } = require('../../../acquisition-mission/types');
const fixture = require('../../../../test/fixtures/decisionShadowEvent.json');

test('ACTIVE_JEV_ROUTING_ENABLED remains false and deploy gates default to shadow-only', () => {
  assert.equal(ACTIVE_JEV_ROUTING_ENABLED, false);
  assert.equal(DEPLOY_GATES.ACTIVE_JEV_ROUTING_ENABLED, false);
  const report = buildShadowReview([fixture], { limit: 50, filter: 'all' });
  assert.equal(report.deploy_recommendation, 'shadow_only');
  assert.equal(report.deploy.passed, false);
});

test('AO observedRoute produces comparable intelligence and conversation routes', () => {
  const prioritization = observedRoute({
    intent: 'account_prioritization',
    reply: 'Focus on Acme first.',
    mode: 'conversation',
  }, false, { source: 'ao_ask' });
  assert.equal(prioritization.route, 'intelligence');
  const coaching = observedAoRoute({ intent: 'coaching', reply: 'Try this opener.' }, 'ao_respond');
  assert.equal(coaching.route, 'conversation');
  assert.equal(observedRoute({ reply: 'hello', session_id: 's1' }, false, { source: 'ao_respond' }).route, 'conversation');
});

test('low-confidence JEV approval stays a dangerous mismatch classification only (shadow)', () => {
  const row = enrichShadowRow({
    status: 'evaluated',
    comparison: 'mismatch',
    recommended_route: 'approval',
    confidence: 0.41,
    approval_probability: 0.79,
    current_route: { route: 'clarification', action: 'clarify' },
    intent: 'mission_instruction',
  }, { question: 'maybe continue with the plan changes?' });
  assert.equal(row.mismatch_classification, 'dangerous_approval_vs_clarification');
  assert.equal(isDangerousMismatchClassification(row.mismatch_classification), true);
  assert.equal(row.route_comparable, true);
});

test('JEV approval recommendation does not override pending-decision capture guard for status checks', () => {
  const pending = { kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL, prompt: 'Approve discovery?' };
  const shadowDecision = {
    intent: 'approval',
    recommended_route: 'approval',
    confidence: 0.95,
    approval_probability: 0.92,
    inspection_probability: 0.1,
  };
  const message = 'What is the current status and confidence of the Anchor STR mission?';
  assert.equal(isHighConfidenceJevInspection({ intent: 'status_check', recommended_route: 'inspection',
    inspection_probability: 0.93, approval_probability: 0.07, confidence: 0.99 }), true);
  const captureIntent = classifyPendingDecisionCaptureIntent({
    message,
    pendingDecision: pending,
    shadowDecision,
  });
  assert.equal(captureIntent, CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION);
  assert.notEqual(captureIntent, CAPTURE_INTENTS.DECISION_RESPONSE);
});

test('inspection/status mismatch is flagged for review without activation constants', () => {
  const report = buildShadowReview([fixture], { limit: 50, filter: 'warnings' });
  assert.equal(report.summary.likely_mission_inspections, 1);
  assert.equal(report.deploy_recommendation, 'shadow_only');
  assert.equal(evaluateDeployGates(report).recommendation, 'shadow_only');
});

test('review report surfaces dangerous approval vs clarification pairs', () => {
  const dangerous = enrichShadowRow({
    ...fixture,
    decision_id: '00000000-0000-4000-8000-000000000001',
    recommended_route: 'approval',
    current_route: { route: 'clarification', action: 'clarify' },
    comparison: 'mismatch',
    intent: 'mission_instruction',
    confidence: 0.48,
  }, { question: 'continue?' });
  const report = buildShadowReview([dangerous], { limit: 50, filter: 'all' });
  assert.equal(report.summary.approval_recommended_while_prod_clarification, 1);
  assert.equal(report.dangerous_mismatches.length, 1);
  assert.match(JSON.stringify(report.summary.mismatch_pairs), /clarification->approval/);
});

test('deployment gates fail closed on absent or invalid reliability and accuracy samples', () => {
  const cases = [
    { summary: { total: 0, by_source: {} } },
    { summary: { total: 100, errors: 0, timeouts: 0, inspection_status_match_rate: null, by_source: {} } },
    { summary: { total: 100, errors: 0, timeouts: 0, inspection_status_match_rate: Number.NaN, by_source: {} } },
  ];
  for (const report of cases) {
    const result = evaluateDeployGates(report);
    assert.equal(result.passed, false);
    assert.equal(result.recommendation, 'shadow_only');
    const accuracyGate = result.gates.find(gate => gate.id === 'inspection_status_match_rate');
    assert.equal(accuracyGate.pass, false);
  }
  const noRows = evaluateDeployGates(cases[0]);
  assert.equal(noRows.gates.find(gate => gate.id === 'error_timeout_rate').pass, false);
});

// Synthetic acceptance snapshots only: these tests are not production evidence.
function acceptanceReport() {
  return {
    scope: {
      filter: 'all', summary_basis: 'complete acceptance window', window_complete: true,
      window_start: '2026-09-01T00:00:00.000Z', window_end: '2026-09-08T00:00:00.000Z',
    },
    summary: {
      total: 10000, comparable: 300, errors: 0, timeouts: 0, errors_or_timeouts: 0,
      by_source: { workspace: { comparable: 200 }, ao_ask: { comparable: 100 }, ao_respond: { comparable: 0 } },
      dangerous_approval_clarification_7d: 0, dangerous_mismatches_7d: 0,
      pending_guard_evidence_rows: 1, pending_guard_verified_rows: 1,
      pending_guard_verification_complete: true, inspection_status_match_rate: 0.9,
    },
  };
}
function gate(report, id) {
  return evaluateDeployGates(report).gates.find(item => item.id === id).pass;
}

test('complete synthetic acceptance snapshot is only a read-only candidate, never active authority', () => {
  const result = evaluateDeployGates(acceptanceReport());
  assert.equal(result.passed, true);
  assert.equal(result.recommendation, 'read_only_candidate');
  assert.equal(ACTIVE_JEV_ROUTING_ENABLED, false);
  assert.deepEqual(DEPLOY_GATES.BLOCKED_ACTIVE_ROUTES, ['approval', 'mission', 'specialist']);
});

test('overall 199/200 boundary is separate from existing workspace 200 and AO 100 safeguards', () => {
  const report = acceptanceReport();
  for (const [count, expected] of [[199, false], [200, true]]) {
    report.summary.comparable = count;
    assert.equal(gate(report, 'min_comparable'), expected);
    // A 200-row total cannot also contain 200 workspace + 100 AO rows.
    assert.equal(evaluateDeployGates(report).passed, false);
  }
  report.summary.comparable = 300;
  report.summary.by_source.workspace.comparable = 199;
  assert.equal(gate(report, 'min_workspace_comparable'), false);
  report.summary.by_source.workspace.comparable = 200;
  assert.equal(gate(report, 'min_workspace_comparable'), true);
  report.summary.by_source.ao_ask.comparable = 99;
  assert.equal(gate(report, 'min_ao_comparable'), false);
  report.summary.by_source.ao_respond.comparable = 1;
  assert.equal(gate(report, 'min_ao_comparable'), true);
});

test('error-or-timeout acceptance is strictly below 5%, including 4.99/5% boundary', () => {
  const report = acceptanceReport();
  for (const [count, expected] of [[499, true], [500, false], [501, false]]) {
    report.summary.errors = count;
    report.summary.errors_or_timeouts = count;
    assert.equal(gate(report, 'error_timeout_rate'), expected);
  }
});

test('timeouts overlapping errors are counted once in reliability union', () => {
  const row = { ...fixture, status: 'error', error_reason: 'timeout', errors: [{ code: 'timeout' }] };
  const report = buildShadowReview([row, { ...fixture, errors: [{ code: 'http_error' }] }, fixture]);
  assert.equal(report.summary.errors, 2);
  assert.equal(report.summary.timeouts, 1);
  assert.equal(report.summary.errors_or_timeouts, 2);
  const accepted = acceptanceReport();
  Object.assign(accepted.summary, { errors: 499, timeouts: 499, errors_or_timeouts: 499 });
  assert.equal(gate(accepted, 'error_timeout_rate'), true);
});

test('missing, filtered, truncated, short and invalid seven-day windows fail closed', () => {
  const patches = [
    { window_complete: undefined }, { window_complete: false }, { window_complete: 'true' },
    { filter: 'mismatches' }, { summary_basis: 'returned rows only' },
    { window_start: undefined }, { window_end: undefined }, { window_start: 'invalid' },
    { window_start: '2026-09-01T00:00:00.001Z' },
    { window_start: '2026-08-01T00:00:00.000Z' },
    { window_start: '2026-09-09T00:00:00.000Z' },
    { window_end: '2999-09-08T00:00:00.000Z' },
  ];
  for (const patch of patches) {
    const report = acceptanceReport();
    Object.assign(report.scope, patch);
    assert.equal(gate(report, 'complete_acceptance_window'), false, JSON.stringify(patch));
    assert.equal(evaluateDeployGates(report).recommendation, 'shadow_only');
  }
});

test('zero dangerous mismatches must be explicitly established for all dangerous classes', () => {
  for (const key of ['dangerous_approval_clarification_7d', 'dangerous_mismatches_7d']) {
    for (const value of [undefined, null, -1, 0.1, '0', NaN, Infinity, 1]) {
      const report = acceptanceReport();
      report.summary[key] = value;
      assert.equal(evaluateDeployGates(report).passed, false, `${key}: ${value}`);
    }
  }
});

test('a persisted guard event alone is not verified pending-decision guard evidence', () => {
  for (const patch of [
    { pending_guard_verified_rows: undefined }, { pending_guard_verified_rows: null },
    { pending_guard_verified_rows: 0 }, { pending_guard_verified_rows: 2 },
    { pending_guard_verified_rows: '1' }, { pending_guard_verified_rows: Infinity },
    { pending_guard_evidence_rows: undefined }, { pending_guard_evidence_rows: 2 },
    { pending_guard_verification_complete: undefined },
    { pending_guard_verification_complete: false }, { pending_guard_verification_complete: 'true' },
  ]) {
    const report = acceptanceReport();
    Object.assign(report.summary, patch);
    assert.equal(gate(report, 'pending_guard_evidence'), false, JSON.stringify(patch));
  }
});

test('missing or malformed denominator and reliability evidence cannot become zero errors', () => {
  for (const key of ['total', 'comparable', 'errors', 'timeouts', 'errors_or_timeouts']) {
    for (const value of [undefined, null, -1, 0.5, '0', NaN, Infinity]) {
      const report = acceptanceReport();
      report.summary[key] = value;
      assert.equal(evaluateDeployGates(report).passed, false, `${key}: ${value}`);
    }
  }
  for (const patch of [
    { errors: 3, errors_or_timeouts: 2 }, { errors: 0, errors_or_timeouts: 1 },
    { total: 300, comparable: 301 }, { total: 300, errors: 301, errors_or_timeouts: 301 },
  ]) {
    const report = acceptanceReport();
    Object.assign(report.summary, patch);
    assert.equal(evaluateDeployGates(report).passed, false);
  }
});

test('bounded review cannot certify seven-day safety even with sufficient synthetic sample counts', () => {
  const rows = Array.from({ length: 300 }, (_, index) => ({
    ...fixture, source: index < 200 ? 'workspace' : 'ao_ask',
    comparison: 'match', mismatch_classification: 'match', route_matches: true,
  }));
  const report = buildShadowReview(rows, { limit: 500, filter: 'all' }, { pending_guard: 1 });
  assert.equal(report.summary.comparable, 300);
  assert.equal(report.scope.window_complete, false);
  assert.equal(report.summary.pending_guard_verified_rows, null);
  assert.equal(report.summary.dangerous_mismatches_7d, null);
  assert.equal(report.deploy_recommendation, 'shadow_only');
});

test('source comparable counts honor explicit non-comparable rows just like overall denominator', () => {
  const report = buildShadowReview([{ ...fixture, comparison: 'match', route_comparable: false }]);
  assert.equal(report.summary.comparable, 0);
  assert.equal(report.summary.by_source.workspace.comparable, 0);
});


test('malformed accuracy and absent reports fail closed without throwing', () => {
  assert.equal(evaluateDeployGates(null).passed, false);
  for (const value of [undefined, null, '0.9', -1, 1.1, NaN, Infinity]) {
    const report = acceptanceReport();
    report.summary.inspection_status_match_rate = value;
    assert.equal(gate(report, 'inspection_status_match_rate'), false);
  }
});

test('error union counts events rather than deduplicating reused object references', () => {
  const row = { ...fixture, status: 'error', errors: [{ code: 'timeout' }] };
  assert.equal(buildShadowReview([row, row]).summary.errors_or_timeouts, 2);
});
