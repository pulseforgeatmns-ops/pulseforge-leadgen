'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
} = require('../PolicyTypes');

const MS_HOUR = 60 * 60 * 1000;

/**
 * Cooldown Rule — prevent repeated actions inside configured windows.
 */
const CooldownRule = Object.freeze({
  id: RULE_IDS.COOLDOWN,
  name: 'Cooldown Rule',
  priority: 50,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const cooldownHours =
      policy.cooldownHours == null ? 24 : Number(policy.cooldownHours);

    if (!Number.isFinite(cooldownHours) || cooldownHours <= 0) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: 'cooldown_disabled',
        action: POLICY_ACTIONS.ALLOW,
        details: { cooldownHours },
      });
    }

    const asOfMs = Date.parse(
      context.asOf || context.now || new Date().toISOString()
    );
    const lastActionAt = context.lastActionAt || context.lastOutreachAt;
    if (!lastActionAt) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: 'no_prior_action',
        action: POLICY_ACTIONS.ALLOW,
        details: { cooldownHours },
      });
    }

    const lastMs = Date.parse(lastActionAt);
    if (!Number.isFinite(lastMs) || !Number.isFinite(asOfMs)) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.LOW,
        reason: 'cooldown_timestamps_unparseable:warn',
        action: POLICY_ACTIONS.WARN,
        details: { lastActionAt, cooldownHours },
      });
    }

    const elapsedHours = (asOfMs - lastMs) / MS_HOUR;
    if (elapsedHours >= cooldownHours) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `elapsedHours=${round2(elapsedHours)}>=cooldownHours=${cooldownHours}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { elapsedHours: round2(elapsedHours), cooldownHours },
      });
    }

    return ruleResult({
      passed: false,
      severity: POLICY_SEVERITIES.MEDIUM,
      reason: `elapsedHours=${round2(elapsedHours)}<cooldownHours=${cooldownHours}:block`,
      action: POLICY_ACTIONS.BLOCK,
      details: {
        elapsedHours: round2(elapsedHours),
        cooldownHours,
        lastActionAt,
        remainingHours: round2(cooldownHours - elapsedHours),
      },
    });
  },
});

function round2(n) {
  return Math.round(Number(n) * 100) / 100;
}

module.exports = { CooldownRule };
