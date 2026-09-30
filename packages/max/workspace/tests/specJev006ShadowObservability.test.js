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
