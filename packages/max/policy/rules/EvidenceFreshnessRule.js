'use strict';

const { ruleResult } = require('./RuleInterface');
const {
  RULE_IDS,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
} = require('../PolicyTypes');

const MS_DAY = 24 * 60 * 60 * 1000;

/**
 * Evidence Freshness Rule — block recommendations based on stale evidence.
 */
const EvidenceFreshnessRule = Object.freeze({
  id: RULE_IDS.EVIDENCE_FRESHNESS,
  name: 'Evidence Freshness Rule',
  priority: 70,
  /**
   * @param {object} context
   */
  evaluate(context) {
    const policy = context.policy || {};
    const maxAgeDays =
      policy.maxEvidenceAgeDays == null
        ? 90
        : Number(policy.maxEvidenceAgeDays);

    if (!Number.isFinite(maxAgeDays) || maxAgeDays <= 0) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: 'evidence_freshness_disabled',
        action: POLICY_ACTIONS.ALLOW,
        details: { maxAgeDays },
      });
    }

    const asOfMs = Date.parse(
      context.asOf || context.now || new Date().toISOString()
    );
    const ageDays = resolveEvidenceAgeDays(context, asOfMs);

    if (ageDays == null) {
      // No evidence timestamps → require approval rather than silent allow
      const hasEvidence =
        ((context.recommendation && context.recommendation.evidence) || [])
          .length > 0 ||
        ((context.recommendation && context.recommendation.supportingSignals) || [])
          .length > 0;

      if (!hasEvidence) {
        return ruleResult({
          passed: false,
          severity: POLICY_SEVERITIES.MEDIUM,
          reason: 'no_evidence_timestamps:require_approval',
          action: POLICY_ACTIONS.REQUIRE_APPROVAL,
          details: { maxAgeDays, ageDays: null },
        });
      }

      return ruleResult({
        passed: false,
        severity: POLICY_SEVERITIES.LOW,
        reason: 'evidence_age_unknown:warn',
        action: POLICY_ACTIONS.WARN,
        details: { maxAgeDays, ageDays: null },
      });
    }

    if (ageDays <= maxAgeDays) {
      return ruleResult({
        passed: true,
        severity: POLICY_SEVERITIES.NONE,
        reason: `evidenceAgeDays=${round2(ageDays)}<=max=${maxAgeDays}`,
        action: POLICY_ACTIONS.ALLOW,
        details: { ageDays: round2(ageDays), maxAgeDays },
      });
    }

    return ruleResult({
      passed: false,
      severity: POLICY_SEVERITIES.HIGH,
      reason: `evidenceAgeDays=${round2(ageDays)}>max=${maxAgeDays}:block`,
      action: POLICY_ACTIONS.BLOCK,
      details: { ageDays: round2(ageDays), maxAgeDays },
    });
  },
});

/**
 * Prefer newest evidence timestamp; fall back to context.evidenceAgeDays.
 * @param {object} context
 * @param {number} asOfMs
 */
function resolveEvidenceAgeDays(context, asOfMs) {
  if (context.evidenceAgeDays != null && Number.isFinite(Number(context.evidenceAgeDays))) {
    return Number(context.evidenceAgeDays);
  }

  const timestamps = [];
  if (context.newestEvidenceAt) timestamps.push(context.newestEvidenceAt);
  if (context.evidenceFreshnessAt) timestamps.push(context.evidenceFreshnessAt);

  for (const e of context.evidence || []) {
    if (e && (e.occurredAt || e.createdAt || e.timestamp)) {
      timestamps.push(e.occurredAt || e.createdAt || e.timestamp);
    }
  }

  const rec = context.recommendation || {};
  for (const s of rec.supportingSignals || []) {
    if (s && (s.occurredAt || s.createdAt || s.timestamp)) {
      timestamps.push(s.occurredAt || s.createdAt || s.timestamp);
    }
  }

  let newest = null;
  for (const t of timestamps) {
    const ms = Date.parse(t);
    if (!Number.isFinite(ms)) continue;
    if (newest == null || ms > newest) newest = ms;
  }

  if (newest == null || !Number.isFinite(asOfMs)) return null;
  return Math.max(0, (asOfMs - newest) / MS_DAY);
}

function round2(n) {
  return Math.round(Number(n) * 100) / 100;
}

module.exports = { EvidenceFreshnessRule, resolveEvidenceAgeDays };
