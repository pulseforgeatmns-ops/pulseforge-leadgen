'use strict';

/**
 * Policy & Decision Engine types (SPEC-005 / v0.9.1).
 * Policy never reasons — it evaluates recommendations against explicit rules.
 */

const POLICY_ACTIONS = Object.freeze({
  ALLOW: 'allow',
  REQUIRE_APPROVAL: 'requireApproval',
  BLOCK: 'block',
  WARN: 'warn',
});

const POLICY_SEVERITIES = Object.freeze({
  NONE: 'none',
  LOW: 'low',
  MEDIUM: 'medium',
  HIGH: 'high',
  CRITICAL: 'critical',
});

const SEVERITY_RANK = Object.freeze({
  none: 0,
  low: 1,
  medium: 2,
  high: 3,
  critical: 4,
});

const RULE_IDS = Object.freeze({
  CONFIDENCE: 'confidence',
  CONTRADICTION: 'contradiction',
  TENANT_POLICY: 'tenant_policy',
  RISK: 'risk',
  COOLDOWN: 'cooldown',
  CONTACT: 'contact',
  EVIDENCE_FRESHNESS: 'evidence_freshness',
});

/** Default tenant policy (data-driven; override per tenant without recompile). */
const DEFAULT_TENANT_POLICY = Object.freeze({
  minimumConfidence: 0.75,
  maximumRisk: 0.4,
  maximumContradictionSeverity: 0.6,
  approvalRequired: Object.freeze(['email', 'linkedin']),
  blockedDays: Object.freeze([]),
  blockAutonomousOutreach: true,
  cooldownHours: 24,
  requireVerifiedDecisionMakerFor: Object.freeze([
    'pursue',
    'request_intro',
    'follow_up_outreach',
  ]),
  maxEvidenceAgeDays: 90,
  dailyOutreachLimit: null,
});

const WEEKDAY_NAMES = Object.freeze([
  'Sunday',
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
]);

/**
 * Normalize confidence/risk thresholds that may be 0–1 or 0–100.
 * @param {number} value
 * @returns {number} 0–100 scale
 */
function toHundredScale(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n >= 0 && n <= 1) return n * 100;
  return n;
}

/**
 * @param {string} a
 * @param {string} b
 */
function maxSeverity(a, b) {
  const ra = SEVERITY_RANK[a] != null ? SEVERITY_RANK[a] : 0;
  const rb = SEVERITY_RANK[b] != null ? SEVERITY_RANK[b] : 0;
  return ra >= rb ? a || POLICY_SEVERITIES.NONE : b || POLICY_SEVERITIES.NONE;
}

module.exports = {
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
  SEVERITY_RANK,
  RULE_IDS,
  DEFAULT_TENANT_POLICY,
  WEEKDAY_NAMES,
  toHundredScale,
  maxSeverity,
};
