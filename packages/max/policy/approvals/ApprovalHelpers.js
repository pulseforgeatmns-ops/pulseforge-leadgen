'use strict';

/**
 * Approval helpers — structured flags only.
 * No UI / human approval screens (out of scope for SPEC-005).
 */

/**
 * @param {object} decision - PolicyDecision
 */
function approvalRequired(decision) {
  return Boolean(decision && decision.requiresApproval && !decision.blocked);
}

/**
 * @param {object} decision
 */
function canAutonomousExecute(decision) {
  return Boolean(
    decision &&
      decision.allowed &&
      !decision.blocked &&
      !decision.requiresApproval
  );
}

/**
 * Structured approval ticket placeholder for future operator surfaces.
 * @param {object} input
 */
function buildApprovalTicket(input) {
  const decision = input.decision || {};
  return {
    status: 'pending_approval',
    recommendationId: input.recommendationId || null,
    tenantId: input.tenantId || null,
    outcome: decision.outcome,
    reason: decision.reason,
    matchedRules: (decision.matchedRules || []).map((r) => r.ruleId),
    createdAt: input.createdAt || new Date().toISOString(),
  };
}

module.exports = {
  approvalRequired,
  canAutonomousExecute,
  buildApprovalTicket,
};
