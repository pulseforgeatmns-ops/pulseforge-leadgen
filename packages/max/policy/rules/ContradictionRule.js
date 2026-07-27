'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  toHundredScale,
} = require('../PolicyTypes');

/**
 * Contradiction Rule — high contradiction severity blocks autonomous execution.
 */
const ContradictionRule = Object.freeze({
  id: RULE_IDS.CONTRADICTION,
  name: 'Contradiction Rule',
  priority: 20,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const severity = resolveContradictionSeverity(context);
    const maximum = toHundredScale(
      policy.maximumContradictionSeverity != null
        ? policy.maximumContradictionSeverity
        : 0.6
    );

    if (severity <= maximum) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `contradictionSeverity=${severity}<=maximum=${maximum}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { contradictionSeverity: severity, maximum },
      });
    }

    return ruleResult({
      passed: false,
      severity: POLICY_SEVERITIES.HIGH,
      reason: `contradictionSeverity=${severity}>maximum=${maximum}:block_autonomous`,
      action: POLICY_ACTIONS.BLOCK,
      details: { contradictionSeverity: severity, maximum },
    });
  },
});

/**
 * Prefer explicit context.contradictionSeverity; else derive from opposingSignals.
 * @param {object} context
 */
function resolveContradictionSeverity(context) {
  if (context.contradictionSeverity != null) {
    return toHundredScale(context.contradictionSeverity);
  }
  const rec = context.recommendation || {};
  const opposing = (rec.opposingSignals || []).length;
  const supporting = (rec.supportingSignals || []).length;
  let severity = opposing * 12;
  if (opposing > supporting && opposing > 0) severity += 20;
  return Math.min(100, severity);
}

module.exports = { ContradictionRule, resolveContradictionSeverity };
