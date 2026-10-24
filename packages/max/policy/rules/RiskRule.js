'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  toHundredScale,
} = require('../PolicyTypes');

/**
 * Risk Rule — prevent actions when risk exceeds threshold.
 */
const RiskRule = Object.freeze({
  id: RULE_IDS.RISK,
  name: 'Risk Rule',
  priority: 40,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const risk = resolveRisk(context);
    const maximum = toHundredScale(
      policy.maximumRisk != null ? policy.maximumRisk : 0.4
    );

    if (risk <= maximum) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `risk=${risk}<=maximum=${maximum}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { risk, maximum },
      });
    }

    // Above max risk → block autonomous; high severity
    return ruleResult({
      passed: false,
      severity: risk >= 80 ? POLICY_SEVERITIES.CRITICAL : POLICY_SEVERITIES.HIGH,
      reason: `risk=${risk}>maximum=${maximum}:block`,
      action: POLICY_ACTIONS.BLOCK,
      details: { risk, maximum },
    });
  },
});

/**
 * Prefer context.risk; else derive from recommendation risk fields / opposing load.
 * @param {object} context
 */
function resolveRisk(context) {
  if (context.risk != null) return toHundredScale(context.risk);
  const rec = context.recommendation || {};
  if (rec.risk != null) return toHundredScale(rec.risk);
  if (context.riskScore != null) return toHundredScale(context.riskScore);

  // Structured derivation only — no LLM
  const opposing = (rec.opposingSignals || []).length;
  const score = Number(rec.score);
  let risk = opposing * 10;
  if (Number.isFinite(score) && score < 40) risk += 30;
  if (rec.type === 'deprioritize' || rec.recommendedAction === 'hold') {
    risk += 25;
  }
  if (context.risks && Array.isArray(context.risks) && context.risks.length) {
    const top = Math.max(
      ...context.risks.map((r) => Number(r.severity) || 0)
    );
    risk = Math.max(risk, top);
  }
  return Math.min(100, risk);
}

module.exports = { RiskRule, resolveRisk };
