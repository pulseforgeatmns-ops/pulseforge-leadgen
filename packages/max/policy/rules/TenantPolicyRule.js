'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  WEEKDAY_NAMES,
} = require('../PolicyTypes');

/**
 * Tenant Policy Rule — per-tenant configurable requirements.
 * Examples: never email automatically, require approval for follow-ups,
 * block outreach on weekends, limit daily outreach volume.
 */
const TenantPolicyRule = Object.freeze({
  id: RULE_IDS.TENANT_POLICY,
  name: 'Tenant Policy Rule',
  priority: 30,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const channel = resolveChannel(context);
    const action = resolveActionKey(context);
    const asOf = context.asOf || context.now || new Date().toISOString();
    const dayName = weekdayName(asOf);
    const reasons = [];
    let decisionAction = POLICY_ACTIONS.ALLOW;
    let severity = POLICY_SEVERITIES.NONE;

    // Blocked calendar days (e.g. Sunday)
    const blockedDays = (policy.blockedDays || []).map(String);
    if (blockedDays.includes(dayName)) {
      reasons.push(`blockedDay=${dayName}`);
      decisionAction = POLICY_ACTIONS.BLOCK;
      severity = POLICY_SEVERITIES.HIGH;
    }

    // Daily outreach volume limit
    const dailyLimit = policy.dailyOutreachLimit;
    const dailyCount = Number(context.dailyOutreachCount);
    if (
      decisionAction !== POLICY_ACTIONS.BLOCK &&
      dailyLimit != null &&
      Number.isFinite(Number(dailyLimit)) &&
      Number.isFinite(dailyCount) &&
      dailyCount >= Number(dailyLimit)
    ) {
      reasons.push(`dailyOutreachLimit=${dailyLimit}:count=${dailyCount}`);
      decisionAction = POLICY_ACTIONS.BLOCK;
      severity = POLICY_SEVERITIES.HIGH;
    }

    // Channels that always require approval (email, linkedin, …)
    const approvalRequired = new Set(
      (policy.approvalRequired || []).map((x) => String(x).toLowerCase())
    );
    if (
      decisionAction !== POLICY_ACTIONS.BLOCK &&
      channel &&
      approvalRequired.has(String(channel).toLowerCase())
    ) {
      reasons.push(`approvalRequired.channel=${channel}`);
      decisionAction = POLICY_ACTIONS.REQUIRE_APPROVAL;
      severity = maxSev(severity, POLICY_SEVERITIES.MEDIUM);
    }

    // Never email / outreach automatically
    if (
      decisionAction !== POLICY_ACTIONS.BLOCK &&
      policy.blockAutonomousOutreach === true &&
      isOutreachAction(action, channel)
    ) {
      reasons.push('blockAutonomousOutreach=true');
      decisionAction = POLICY_ACTIONS.REQUIRE_APPROVAL;
      severity = maxSev(severity, POLICY_SEVERITIES.MEDIUM);
    }

    // Follow-ups require approval when listed in approvalRequired as "follow_up"
    if (
      decisionAction !== POLICY_ACTIONS.BLOCK &&
      approvalRequired.has('follow_up') &&
      (action === 'follow_up_outreach' || action === 'follow_up')
    ) {
      reasons.push('approvalRequired.follow_up');
      decisionAction = POLICY_ACTIONS.REQUIRE_APPROVAL;
      severity = maxSev(severity, POLICY_SEVERITIES.MEDIUM);
    }

    if (decisionAction === POLICY_ACTIONS.ALLOW) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `tenant_policy:ok:day=${dayName}:channel=${channel || 'none'}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { dayName, channel, action },
      });
    }

    return ruleResult({
      passed: false,
      severity,
      reason: reasons.sort().join('|'),
      action: decisionAction,
      details: { dayName, channel, action, reasons: reasons.sort() },
    });
  },
});

function resolveChannel(context) {
  if (context.channel) return String(context.channel).toLowerCase();
  const rec = context.recommendation || {};
  if (rec.channel) return String(rec.channel).toLowerCase();
  const action = resolveActionKey(context);
  if (action && action.includes('email')) return 'email';
  if (action && action.includes('linkedin')) return 'linkedin';
  if (action === 'follow_up_outreach' || action === 'request_intro') return 'email';
  if (action === 'nurture_sequence') return 'email';
  return null;
}

function resolveActionKey(context) {
  const rec = context.recommendation || {};
  return String(rec.recommendedAction || rec.type || '').toLowerCase();
}

function isOutreachAction(action, channel) {
  if (channel === 'email' || channel === 'linkedin' || channel === 'sms') {
    return true;
  }
  return [
    'follow_up_outreach',
    'request_intro',
    'nurture_sequence',
    'pursue',
    'follow_up',
  ].includes(action);
}

function weekdayName(iso) {
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return WEEKDAY_NAMES[0];
  return WEEKDAY_NAMES[new Date(ms).getUTCDay()];
}

function maxSev(a, b) {
  const rank = { none: 0, low: 1, medium: 2, high: 3, critical: 4 };
  return (rank[a] || 0) >= (rank[b] || 0) ? a : b;
}

module.exports = { TenantPolicyRule };
