'use strict';

const {
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  maxSeverity,
  SEVERITY_RANK,
} = require('../PolicyTypes');

/**
 * Aggregate rule results into a single PolicyDecision (deterministic).
 *
 * Precedence: block > requireApproval > warn > allow
 */
function aggregateRuleResults(ruleResults) {
  const results = ruleResults || [];
  let blocked = false;
  let requiresApproval = false;
  let warned = false;
  let severity = POLICY_SEVERITIES.NONE;
  const reasons = [];
  const matchedRules = [];

  for (const r of results) {
    const influences =
      r.action !== POLICY_ACTIONS.ALLOW || r.passed === false;
    if (influences) {
      matchedRules.push({
        ruleId: r.ruleId,
        ruleName: r.ruleName,
        priority: r.priority,
        passed: r.passed,
        action: r.action,
        severity: r.severity,
        reason: r.reason,
        details: r.details || {},
      });
    }

    if (r.action === POLICY_ACTIONS.BLOCK) {
      blocked = true;
      severity = maxSeverity(severity, r.severity || POLICY_SEVERITIES.HIGH);
      if (r.reason) reasons.push(r.reason);
    } else if (r.action === POLICY_ACTIONS.REQUIRE_APPROVAL) {
      requiresApproval = true;
      severity = maxSeverity(severity, r.severity || POLICY_SEVERITIES.MEDIUM);
      if (r.reason) reasons.push(r.reason);
    } else if (r.action === POLICY_ACTIONS.WARN) {
      warned = true;
      severity = maxSeverity(severity, r.severity || POLICY_SEVERITIES.LOW);
      if (r.reason) reasons.push(r.reason);
    }
  }

  // Block supersedes approval
  if (blocked) {
    requiresApproval = false;
  }

  const allowed = !blocked;
  const outcome = blocked
    ? POLICY_ACTIONS.BLOCK
    : requiresApproval
      ? POLICY_ACTIONS.REQUIRE_APPROVAL
      : warned
        ? POLICY_ACTIONS.WARN
        : POLICY_ACTIONS.ALLOW;

  matchedRules.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return String(a.ruleId).localeCompare(String(b.ruleId));
  });

  return {
    allowed,
    requiresApproval,
    blocked,
    warned,
    severity,
    outcome,
    reason: reasons.length ? reasons.sort().join(' || ') : 'all_rules_passed',
    matchedRules,
    ruleResults: results.map(compactResult),
  };
}

function compactResult(r) {
  return {
    ruleId: r.ruleId,
    ruleName: r.ruleName,
    priority: r.priority,
    passed: r.passed,
    action: r.action,
    severity: r.severity,
    reason: r.reason,
    details: r.details || {},
  };
}

/**
 * Explainability chain: Recommendation → Policy Decision → Matched Rules → Outcome
 * @param {object} input
 */
function buildDecisionExplanation(input) {
  const decision = input.decision || {};
  return {
    chain: {
      recommendation: {
        id: input.recommendationId || null,
        type: input.recommendationType || null,
        score: input.score != null ? input.score : null,
        confidence: input.confidence != null ? input.confidence : null,
        recommendedAction: input.recommendedAction || null,
      },
      policyDecision: {
        allowed: decision.allowed,
        requiresApproval: decision.requiresApproval,
        blocked: decision.blocked,
        severity: decision.severity,
        outcome: decision.outcome,
      },
      matchedRules: (decision.matchedRules || []).map((r) => ({
        ruleId: r.ruleId,
        action: r.action,
        reason: r.reason,
      })),
      finalOutcome: decision.outcome,
    },
    summary: decision.reason,
  };
}

module.exports = {
  aggregateRuleResults,
  buildDecisionExplanation,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  SEVERITY_RANK,
};
