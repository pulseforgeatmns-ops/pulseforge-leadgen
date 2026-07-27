'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createPolicyEngine,
  createMaxReasoningRuntime,
  POLICY_ACTIONS,
  canAutonomousExecute,
} = require('../..');
const { recommendation, AS_OF, AS_OF_MONDAY } = require('./helpers');

function permissivePolicy(overrides = {}) {
  return {
    minimumConfidence: 0.5,
    maximumRisk: 0.9,
    maximumContradictionSeverity: 0.9,
    approvalRequired: [],
    blockedDays: [],
    blockAutonomousOutreach: false,
    cooldownHours: 0,
    requireVerifiedDecisionMakerFor: [],
    maxEvidenceAgeDays: 365,
    dailyOutreachLimit: null,
    ...overrides,
  };
}

describe('PolicyEngine — multi-rule evaluation', () => {
  it('allows a clean recommendation under permissive tenant policy', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant('10', permissivePolicy());

    const decision = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({
        confidence: 90,
        score: 80,
        type: 'nurture',
        recommendedAction: 'nurture_sequence',
      }),
      context: {
        evidenceAgeDays: 5,
        risk: 0.1,
        contradictionSeverity: 0.1,
      },
    });

    assert.equal(decision.blocked, false);
    assert.equal(decision.requiresApproval, false);
    assert.equal(decision.allowed, true);
    assert.equal(decision.outcome, POLICY_ACTIONS.ALLOW);
    assert.equal(canAutonomousExecute(decision), true);
    assert.ok(decision.audit);
    assert.equal(decision.explanation.chain.finalOutcome, POLICY_ACTIONS.ALLOW);
  });

  it('requires approval for low confidence', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant(
      '10',
      permissivePolicy({
        minimumConfidence: 0.75,
        blockAutonomousOutreach: false,
        approvalRequired: [],
      })
    );

    const decision = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({ confidence: 40, type: 'nurture' }),
      context: { evidenceAgeDays: 5, risk: 0.1 },
    });

    assert.equal(decision.blocked, false);
    assert.equal(decision.requiresApproval, true);
    assert.equal(decision.allowed, true);
    assert.equal(decision.outcome, POLICY_ACTIONS.REQUIRE_APPROVAL);
    assert.ok(decision.approvalTicket);
    assert.ok(
      decision.matchedRules.some((r) => r.ruleId === 'confidence')
    );
  });

  it('blocks on high risk and supersedes approval', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant(
      '10',
      permissivePolicy({
        maximumRisk: 0.4,
        minimumConfidence: 0.9, // would also require approval
      })
    );

    const decision = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({ confidence: 50 }),
      context: { risk: 0.85, evidenceAgeDays: 5 },
    });

    assert.equal(decision.blocked, true);
    assert.equal(decision.allowed, false);
    assert.equal(decision.requiresApproval, false);
    assert.equal(decision.outcome, POLICY_ACTIONS.BLOCK);
  });

  it('blocks cooldown and weekend tenant policy', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant(
      '10',
      permissivePolicy({
        blockedDays: ['Sunday'],
        cooldownHours: 48,
      })
    );

    const weekend = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF, // Sunday
      recommendation: recommendation(),
      context: { evidenceAgeDays: 5, risk: 0.1 },
    });
    assert.equal(weekend.blocked, true);
    assert.match(weekend.reason, /blockedDay=Sunday/);

    const cooldown = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation(),
      context: {
        evidenceAgeDays: 5,
        risk: 0.1,
        lastActionAt: '2026-07-20T06:00:00.000Z',
      },
    });
    // Monday policy still has cooldownHours 48 — last action 6h earlier → block
    assert.equal(cooldown.blocked, true);
  });

  it('supports distinct tenant configurations', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant('1', permissivePolicy({ approvalRequired: [] }));
    policy.configureTenant(
      '10',
      permissivePolicy({
        approvalRequired: ['email'],
        blockAutonomousOutreach: false,
      })
    );

    const rec = recommendation({
      type: 'nurture',
      recommendedAction: 'nurture_sequence',
    });
    const ctx = { evidenceAgeDays: 5, risk: 0.1, channel: 'email' };

    const a = await policy.evaluate({
      tenantId: '1',
      asOf: AS_OF_MONDAY,
      recommendation: rec,
      context: ctx,
    });
    const b = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: rec,
      context: ctx,
    });

    assert.equal(a.requiresApproval, false);
    assert.equal(b.requiresApproval, true);
  });

  it('generates immutable audit trail', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant('10', permissivePolicy());

    const d1 = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({ id: 'rec:a' }),
      context: { evidenceAgeDays: 5, risk: 0.1 },
      operator: 'system',
    });
    const d2 = await policy.evaluate({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({ id: 'rec:b' }),
      context: { evidenceAgeDays: 5, risk: 0.1 },
    });

    assert.ok(d1.audit.id);
    assert.equal(d1.audit.operator, 'system');
    assert.equal(policy.auditLog.count('10'), 2);
    const listed = policy.auditLog.list({ tenantId: '10' });
    assert.equal(listed.length, 2);
    assert.equal(listed[0].recommendationId, 'rec:a');
    assert.equal(listed[1].recommendationId, 'rec:b');

    // Immutability
    assert.throws(() => {
      d1.audit.decision.allowed = false;
    }, TypeError);
    void d2;
  });

  it('is deterministic for identical inputs', async () => {
    const policy = createPolicyEngine();
    policy.configureTenant('10', permissivePolicy({ approvalRequired: ['email'] }));

    const input = {
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({ confidence: 60 }),
      context: {
        evidenceAgeDays: 5,
        risk: 0.2,
        channel: 'email',
        contradictionSeverity: 0.1,
      },
    };

    const a = await policy.evaluate(input);
    const b = await policy.evaluate(input);

    assert.equal(a.outcome, b.outcome);
    assert.equal(a.reason, b.reason);
    assert.deepEqual(
      a.matchedRules.map((r) => `${r.ruleId}:${r.action}`),
      b.matchedRules.map((r) => `${r.ruleId}:${r.action}`)
    );
    assert.deepEqual(a.explanation.chain, b.explanation.chain);
  });
});

describe('PolicyEngine — runtime wiring surface', () => {
  it('exposes decide() on createMaxReasoningRuntime without calling evaluate()', async () => {
    const max = createMaxReasoningRuntime({ startIngestor: false });
    max.policy.configureTenant('10', permissivePolicy());

    let evaluateCalls = 0;
    const original = max.engine.evaluate.bind(max.engine);
    max.engine.evaluate = async (...args) => {
      evaluateCalls += 1;
      return original(...args);
    };

    const decision = await max.decide({
      tenantId: '10',
      asOf: AS_OF_MONDAY,
      recommendation: recommendation({ type: 'nurture' }),
      context: { evidenceAgeDays: 5, risk: 0.1 },
    });

    assert.equal(evaluateCalls, 0);
    assert.ok(decision.audit);
    assert.equal(typeof max.policy.evaluate, 'function');
  });
});
