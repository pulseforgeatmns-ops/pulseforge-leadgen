'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  toHundredScale,
} = require('../PolicyTypes');

/**
 * Confidence Rule — low-confidence recommendations require approval.
 */
const ConfidenceRule = Object.freeze({
  id: RULE_IDS.CONFIDENCE,
  name: 'Confidence Rule',
  priority: 10,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const rec = context.recommendation || {};
    const confidence = toHundredScale(rec.confidence);
    const minimum = toHundredScale(policy.minimumConfidence);

    if (confidence >= minimum) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `confidence=${confidence}>=minimum=${minimum}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { confidence, minimum },
      });
    }

    return ruleResult({
      passed: false,
      severity: POLICY_SEVERITIES.MEDIUM,
      reason: `confidence=${confidence}<minimum=${minimum}:requires_approval`,
      action: POLICY_ACTIONS.REQUIRE_APPROVAL,
      details: { confidence, minimum },
    });
  },
});

module.exports = { ConfidenceRule };
