'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  ConfidenceRule,
  ContradictionRule,
  TenantPolicyRule,
  RiskRule,
  CooldownRule,
  ContactRule,
  EvidenceFreshnessRule,
  createDefaultRuleRegistry,
  createPolicyEngine,
  POLICY_ACTIONS,
  RULE_IDS,
  DEFAULT_TENANT_POLICY,
} = require('..');
const { recommendation, AS_OF, AS_OF_MONDAY } = require('./helpers');

function ctx(overrides = {}) {
  return {
    tenantId: '10',
    recommendation: recommendation(overrides.rec),
    policy: { ...DEFAULT_TENANT_POLICY, ...(overrides.policy || {}) },
    asOf: overrides.asOf || AS_OF_MONDAY,
    ...overrides,
    rec: undefined,
  };
}

describe('Individual policy rules', () => {
  it('ConfidenceRule requires approval below minimum', () => {
    const pass = ConfidenceRule.evaluate(
      ctx({ rec: { confidence: 90 }, policy: { minimumConfidence: 0.75 } })
    );
    assert.equal(pass.passed, true);
    assert.equal(pass.action, POLICY_ACTIONS.ALLOW);

    const fail = ConfidenceRule.evaluate(
      ctx({ rec: { confidence: 50 }, policy: { minimumConfidence: 0.75 } })
    );
    assert.equal(fail.passed, false);
    assert.equal(fail.action, POLICY_ACTIONS.REQUIRE_APPROVAL);
  });

  it('ContradictionRule blocks high contradiction severity', () => {
    const pass = ContradictionRule.evaluate(
      ctx({ contradictionSeverity: 0.2, policy: { maximumContradictionSeverity: 0.6 } })
    );
    assert.equal(pass.action, POLICY_ACTIONS.ALLOW);

    const fail = ContradictionRule.evaluate(
      ctx({ contradictionSeverity: 0.9, policy: { maximumContradictionSeverity: 0.6 } })
    );
    assert.equal(fail.action, POLICY_ACTIONS.BLOCK);
  });

  it('TenantPolicyRule blocks configured days and daily limits', () => {
    const sunday = TenantPolicyRule.evaluate(
      ctx({
        asOf: AS_OF, // Sunday
        policy: {
          blockedDays: ['Sunday'],
          blockAutonomousOutreach: false,
          approvalRequired: [],
        },
        channel: 'email',
      })
    );
    assert.equal(sunday.action, POLICY_ACTIONS.BLOCK);
    assert.match(sunday.reason, /blockedDay=Sunday/);

    const volume = TenantPolicyRule.evaluate(
      ctx({
        asOf: AS_OF_MONDAY,
        dailyOutreachCount: 10,
        policy: {
          blockedDays: [],
          dailyOutreachLimit: 10,
          blockAutonomousOutreach: false,
          approvalRequired: [],
        },
      })
    );
    assert.equal(volume.action, POLICY_ACTIONS.BLOCK);
  });

  it('TenantPolicyRule requires approval for email channel', () => {
    const result = TenantPolicyRule.evaluate(
      ctx({
        asOf: AS_OF_MONDAY,
        channel: 'email',
        policy: {
          approvalRequired: ['email'],
          blockAutonomousOutreach: false,
          blockedDays: [],
        },
      })
    );
    assert.equal(result.action, POLICY_ACTIONS.REQUIRE_APPROVAL);
  });

  it('RiskRule blocks when risk exceeds maximum', () => {
    const pass = RiskRule.evaluate(
      ctx({ risk: 0.2, policy: { maximumRisk: 0.4 } })
    );
    assert.equal(pass.action, POLICY_ACTIONS.ALLOW);

    const fail = RiskRule.evaluate(
      ctx({ risk: 0.8, policy: { maximumRisk: 0.4 } })
    );
    assert.equal(fail.action, POLICY_ACTIONS.BLOCK);
  });

  it('CooldownRule blocks repeated actions inside window', () => {
    const pass = CooldownRule.evaluate(
      ctx({
        asOf: '2026-07-22T12:00:00.000Z',
        lastActionAt: '2026-07-20T12:00:00.000Z',
        policy: { cooldownHours: 24 },
      })
    );
    assert.equal(pass.action, POLICY_ACTIONS.ALLOW);

    const fail = CooldownRule.evaluate(
      ctx({
        asOf: '2026-07-20T18:00:00.000Z',
        lastActionAt: '2026-07-20T12:00:00.000Z',
        policy: { cooldownHours: 24 },
      })
    );
    assert.equal(fail.action, POLICY_ACTIONS.BLOCK);
  });

  it('ContactRule requires verified decision-maker for pursue', () => {
    const fail = ContactRule.evaluate(
      ctx({
        rec: {
          type: 'pursue',
          recommendedAction: 'request_intro',
          supportingSignals: [{ kind: 'evidence', id: 'ev-x', summary: 'growth signal' }],
        },
        hasVerifiedDecisionMaker: false,
        people: [],
      })
    );
    assert.equal(fail.action, POLICY_ACTIONS.REQUIRE_APPROVAL);

    const pass = ContactRule.evaluate(
      ctx({
        rec: { type: 'pursue', recommendedAction: 'request_intro' },
        hasVerifiedDecisionMaker: true,
      })
    );
    assert.equal(pass.action, POLICY_ACTIONS.ALLOW);
  });

  it('EvidenceFreshnessRule blocks stale evidence', () => {
    const pass = EvidenceFreshnessRule.evaluate(
      ctx({
        asOf: AS_OF_MONDAY,
        evidenceAgeDays: 10,
        policy: { maxEvidenceAgeDays: 90 },
      })
    );
    assert.equal(pass.action, POLICY_ACTIONS.ALLOW);

    const fail = EvidenceFreshnessRule.evaluate(
      ctx({
        asOf: AS_OF_MONDAY,
        evidenceAgeDays: 120,
        policy: { maxEvidenceAgeDays: 90 },
      })
    );
    assert.equal(fail.action, POLICY_ACTIONS.BLOCK);
  });
});

describe('Rule registry ordering', () => {
  it('lists seven rules by priority then id', () => {
    const registry = createDefaultRuleRegistry();
    assert.deepEqual(registry.ids(), [
      RULE_IDS.CONFIDENCE,
      RULE_IDS.CONTRADICTION,
      RULE_IDS.TENANT_POLICY,
      RULE_IDS.RISK,
      RULE_IDS.COOLDOWN,
      RULE_IDS.CONTACT,
      RULE_IDS.EVIDENCE_FRESHNESS,
    ]);
    const priorities = registry.list().map((r) => r.priority);
    for (let i = 1; i < priorities.length; i += 1) {
      assert.ok(priorities[i] >= priorities[i - 1]);
    }
  });
});
