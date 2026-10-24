'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { classifyDecisionMismatch } = require('../packages/decision-service/mismatchClassifier');
const { buildRoutingWarning } = require('../packages/decision-service/shadowRoutingWarning');
const { DecisionService } = require('../packages/decision-service/DecisionService');
const { createWorkspaceEngine } = require('../packages/max/workspace/WorkspaceEngine');
const fixture = require('./fixtures/decisionShadowEvent.json');

const productionMismatch = {
  status: 'evaluated',
  intent: 'status_check',
  confidence: 0.99,
  inspection_probability: 0.93,
  mission_bound_probability: 0.8,
  recommended_route: 'inspection',
  current_route: {
    route: 'conversation',
    raw_route: 'intelligence',
    pipeline: 'ClientIntelligence',
  },
  route_matches: false,
  comparison: 'mismatch',
  errors: [],
};

test('known production mismatch emits warning', () => {
  const warning = classifyDecisionMismatch(productionMismatch);
  assert.ok(warning);
  assert.equal(warning.warning_type, 'likely_mission_inspection_misroute');
  assert.equal(warning.severity, 'review');
  assert.match(warning.reason, /mission inspection\/status routing/);
});

test('no warning when route already matches', () => {
  assert.equal(classifyDecisionMismatch({
    ...productionMismatch,
    current_route: { route: 'inspection' },
    recommended_route: 'inspection',
    route_matches: true,
    comparison: 'match',
  }), null);
});

test('no warning on low confidence', () => {
  assert.equal(classifyDecisionMismatch({
    ...productionMismatch,
    confidence: 0.74,
    inspection_probability: 0.93,
  }), null);
});

test('no warning on low inspection probability', () => {
  assert.equal(classifyDecisionMismatch({
    ...productionMismatch,
    confidence: 0.99,
    inspection_probability: 0.40,
  }), null);
});

test('no warning on provider error', () => {
  assert.equal(classifyDecisionMismatch({
    ...productionMismatch,
    status: 'error',
    errors: [{ code: 'http_error', http_status: 401 }],
  }), null);
});

test('fixture row classifies as warning candidate', () => {
  assert.equal(classifyDecisionMismatch(fixture).warning_type, 'likely_mission_inspection_misroute');
  const payload = buildRoutingWarning(fixture);
  assert.equal(payload.event, 'DECISION_SHADOW_WARNING');
  assert.equal(payload.spec, 'SPEC-JEV-003');
  assert.equal(payload.warning_type, 'likely_mission_inspection_misroute');
});

test('warning generation cannot affect routing when classifier throws', async () => {
  const ENV = { DECISION_SHADOW_ENABLED: 'true', DECISION_PROVIDER: 'jev', JEV_TIMEOUT_MS: '10' };
  const decision = Object.fromEntries(['intent', 'confidence', 'mission_bound_probability', 'approval_probability',
    'inspection_probability', 'requires_human_clarification', 'risk_if_misrouted', 'recommended_route'].map(key => [key, fixture[key]]));
  const input = { question: 'What is the current status and confidence of the Anchor STR mission?', context: { tenantId: '10' } };
  const result = { route: 'intelligence', workspaceOwnership: { owner: 'reasoning' },
    routingTrace: { pipeline: 'ClientIntelligence' }, answer: 'unchanged production answer' };
  const logs = [];
  const service = new DecisionService({
    env: ENV,
    provider: { name: 'jev', evaluate: async () => ({ decision, model: 'jev-test' }) },
    audit: row => logs.push(row),
    warningAudit: () => { throw Error('warning failed'); },
    classifyMismatch: () => { throw Error('classifier failed'); },
  });
  const workspace = createWorkspaceEngine({ decisionService: service, disableLlm: true });
  workspace._askProduction = async () => result;
  assert.strictEqual(await workspace.ask(input), result);
  await service.drain();
  assert.equal(logs.length, 1);
  assert.equal(logs[0].comparison, 'mismatch');
  assert.equal(logs[0].current_route.route, 'conversation');
});

test('shadow disabled writes and surfaces nothing', async () => {
  const ENV = { DECISION_SHADOW_ENABLED: 'false' };
  const logs = [];
  const warnings = [];
  const service = new DecisionService({
    env: ENV,
    audit: row => logs.push(row),
    warningAudit: warning => warnings.push(warning),
    provider: { name: 'jev', evaluate: async () => ({ decision: productionMismatch, model: 'jev-test' }) },
  });
  assert.equal(service.begin({ question: 'status?' }), null);
  await service.drain();
  assert.equal(logs.length, 0);
  assert.equal(warnings.length, 0);
});
