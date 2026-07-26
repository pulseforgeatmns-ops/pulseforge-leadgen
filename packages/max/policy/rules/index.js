'use strict';

const { RuleRegistry } = require('./RuleRegistry');
const { ConfidenceRule } = require('./ConfidenceRule');
const { ContradictionRule } = require('./ContradictionRule');
const { TenantPolicyRule } = require('./TenantPolicyRule');
const { RiskRule } = require('./RiskRule');
const { CooldownRule } = require('./CooldownRule');
const { ContactRule } = require('./ContactRule');
const { EvidenceFreshnessRule } = require('./EvidenceFreshnessRule');
const { assertRule, ruleResult } = require('./RuleInterface');

/**
 * Create registry with the seven initial policy rules.
 * @returns {RuleRegistry}
 */
function createDefaultRuleRegistry() {
  const registry = new RuleRegistry();
  registry
    .register(ConfidenceRule)
    .register(ContradictionRule)
    .register(TenantPolicyRule)
    .register(RiskRule)
    .register(CooldownRule)
    .register(ContactRule)
    .register(EvidenceFreshnessRule);
  return registry;
}

module.exports = {
  RuleRegistry,
  createDefaultRuleRegistry,
  assertRule,
  ruleResult,
  ConfidenceRule,
  ContradictionRule,
  TenantPolicyRule,
  RiskRule,
  CooldownRule,
  ContactRule,
  EvidenceFreshnessRule,
};
