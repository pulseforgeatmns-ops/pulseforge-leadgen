'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  assessJevDecisionForActivePromotion,
  readActiveRoutingConfig,
} = require('../packages/decision-service/activeRoutingPromotion');
const { DecisionService } = require('../packages/decision-service/DecisionService');

const baseDecision = {
  intent: 'status_check',
  confidence: 0.95,
  mission_bound_probability: 0.8,
  approval_probability: 0.1,
  inspection_probability: 0.92,
  requires_human_clarification: false,
  risk_if_misrouted: 'low',
  recommended_route: 'inspection',
};

test('assessJevDecisionForActivePromotion accepts safe inspection status flows', () => {
  const config = readActiveRoutingConfig({ JEV_ACTIVE_ROUTING_ENABLED: 'true' });
  assert.equal(assessJevDecisionForActivePromotion(baseDecision, config).eligible, true);
});

test('assessJevDecisionForActivePromotion blocks execution and approval routes', () => {
  const config = readActiveRoutingConfig({ JEV_ACTIVE_ROUTING_ENABLED: 'true' });
  for (const recommended_route of ['mission', 'approval', 'specialist']) {
    const result = assessJevDecisionForActivePromotion(
      { ...baseDecision, recommended_route },
      config
    );
    assert.equal(result.eligible, false);
    assert.equal(result.reason, 'unsafe_route');
  }
});

test('assessJevDecisionForActivePromotion blocks low confidence and elevated risk', () => {
  const config = readActiveRoutingConfig({ JEV_ACTIVE_ROUTING_ENABLED: 'true' });
  assert.equal(
    assessJevDecisionForActivePromotion({ ...baseDecision, confidence: 0.5 }, config).reason,
    'low_confidence'
  );
  assert.equal(
    assessJevDecisionForActivePromotion(
      { ...baseDecision, risk_if_misrouted: 'high' },
      config
    ).reason,
    'elevated_risk'
  );
});

test('DecisionService.evaluateActiveRouting is fail-closed when disabled', async () => {
  const service = new DecisionService({
    env: { DECISION_SHADOW_ENABLED: 'true', JEV_ACTIVE_ROUTING_ENABLED: 'false' },
    provider: {
      name: 'jev',
      model: 'jev-test',
      evaluate: async () => {
        assert.fail('provider must not run when active routing disabled');
      },
    },
  });
  assert.equal(await service.evaluateActiveRouting({ question: 'status?' }), null);
});

test('DecisionService.evaluateActiveRouting returns parsed decision when enabled', async () => {
  const service = new DecisionService({
    env: {
      DECISION_SHADOW_ENABLED: 'true',
      JEV_ACTIVE_ROUTING_ENABLED: 'true',
      DECISION_PROVIDER: 'jev',
      JEV_ENABLED: 'true',
      JEV_API_KEY: 'test',
    },
    provider: {
      name: 'jev',
      model: 'jev-test',
      evaluate: async () => ({ decision: baseDecision, model: 'jev-test' }),
    },
  });
  const row = await service.evaluateActiveRouting({ question: 'What is mission status?' });
  assert.equal(row.decision.recommended_route, 'inspection');
  assert.equal(row.assessment.eligible, true);
});
