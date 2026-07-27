'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
} = require('../PolicyTypes');

/**
 * Contact Rule — require verified decision-maker before certain recommendation types.
 */
const ContactRule = Object.freeze({
  id: RULE_IDS.CONTACT,
  name: 'Contact Rule',
  priority: 60,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const rec = context.recommendation || {};
    const requiredFor = new Set(
      (policy.requireVerifiedDecisionMakerFor || []).map((x) =>
        String(x).toLowerCase()
      )
    );

    const type = String(rec.type || '').toLowerCase();
    const action = String(rec.recommendedAction || '').toLowerCase();
    const applies = requiredFor.has(type) || requiredFor.has(action);

    if (!applies) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `contact_check_not_required:type=${type}:action=${action}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { type, action },
      });
    }

    const verified = hasVerifiedDecisionMaker(context);
    if (verified) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: 'verified_decision_maker_present',
        action: POLICY_ACTIONS.ALLOW,
        details: { type, action, verified: true },
      });
    }

    return ruleResult({
      passed: false,
      severity: POLICY_SEVERITIES.HIGH,
      reason: `missing_verified_decision_maker:type=${type}:action=${action}:require_approval`,
      action: POLICY_ACTIONS.REQUIRE_APPROVAL,
      details: { type, action, verified: false },
    });
  },
});

/**
 * @param {object} context
 */
function hasVerifiedDecisionMaker(context) {
  if (context.hasVerifiedDecisionMaker === true) return true;
  if (context.verifiedDecisionMaker === true) return true;

  const people = context.people || context.decisionMakers || [];
  for (const p of people) {
    if (isVerifiedDm(p)) return true;
  }

  const rec = context.recommendation || {};
  const signals = [
    ...(rec.supportingSignals || []),
    ...((context.explanation && context.explanation.supportingSignals) || []),
  ];
  for (const s of signals) {
    if (/decision-?maker/i.test(String(s.summary || ''))) return true;
  }

  return false;
}

function isVerifiedDm(person) {
  if (!person || typeof person !== 'object') return false;
  if (person.verified === true || person.isDecisionMaker === true) return true;
  const title = String(person.title || person.jobTitle || '').toLowerCase();
  const verifiedFlag = person.emailVerified === true || person.verifiedEmail === true;
  if (
    verifiedFlag &&
    /owner|founder|ceo|president|partner|principal|director|manager/.test(title)
  ) {
    return true;
  }
  if (person.role === 'decision_maker' && person.verified !== false) {
    return person.email || person.phone ? true : false;
  }
  return false;
}

module.exports = { ContactRule, hasVerifiedDecisionMaker };
