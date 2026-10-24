'use strict';

const {
  PolicyEngine,
  createPolicyEngine,
  TenantPolicyStore,
  PolicyAuditLog,
  RuleRegistry,
} = require('./engine/PolicyEngine');
const {
  createDefaultRuleRegistry,
  ConfidenceRule,
  ContradictionRule,
  TenantPolicyRule,
  RiskRule,
  CooldownRule,
  ContactRule,
  EvidenceFreshnessRule,
  assertRule,
  ruleResult,
} = require('./rules');
const {
  aggregateRuleResults,
  buildDecisionExplanation,
} = require('./evaluation/DecisionAggregator');
const {
  approvalRequired,
  canAutonomousExecute,
  buildApprovalTicket,
} = require('./approvals/ApprovalHelpers');
const {
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  SEVERITY_RANK,
  RULE_IDS,
  DEFAULT_TENANT_POLICY,
  WEEKDAY_NAMES,
  toHundredScale,
  maxSeverity,
} = require('./PolicyTypes');

module.exports = {
  PolicyEngine,
  createPolicyEngine,
  TenantPolicyStore,
  PolicyAuditLog,
  RuleRegistry,
  createDefaultRuleRegistry,
  ConfidenceRule,
  ContradictionRule,
  TenantPolicyRule,
  RiskRule,
  CooldownRule,
  ContactRule,
  EvidenceFreshnessRule,
  assertRule,
  ruleResult,
  aggregateRuleResults,
  buildDecisionExplanation,
  approvalRequired,
  canAutonomousExecute,
  buildApprovalTicket,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  SEVERITY_RANK,
  RULE_IDS,
  DEFAULT_TENANT_POLICY,
  WEEKDAY_NAMES,
  toHundredScale,
  maxSeverity,
};
