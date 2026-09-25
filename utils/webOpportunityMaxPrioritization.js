'use strict';

/**
 * Max prioritization helper for website opportunity assessments (SPEC-WEB-001 / SPEC-WEB-001A).
 * Does not sort by raw opportunity score alone.
 * Generic default economics must not manufacture commercial priority.
 */

const { RECOMMENDED_ACTIONS } = require('../packages/capabilities/websiteOpportunityIntelligence');
const { ECONOMIC_CONFIDENCE } = require('../packages/capabilities/websiteOpportunityIntelligence/types');

const DIAGNOSIS_PRIORITY = Object.freeze({
  REDESIGN_CANDIDATE: 12,
  TARGETED_REMEDIATION: 6,
  INSUFFICIENT_EVIDENCE: 0,
  HEALTHY_SITE: -25,
});

function prioritizeWebOpportunities(assessments = [], { capacityHours = 40 } = {}) {
  return [...assessments]
    .map((row) => {
      const payload = row.payload || row;
      const economics = row.economics || payload.economics || {};
      const score = row.opportunity_score ?? payload.opportunity_score ?? 0;
      const confidence = row.confidence ?? payload.confidence ?? 0;
      const action = row.recommended_action || payload.recommended_action;
      const components = row.score_components || payload.score_components || {};
      const diagnosisClass = payload.commercial_diagnosis?.diagnosis_class || row.diagnosis_class || null;
      const economicConfidence = economics.economic_confidence || ECONOMIC_CONFIDENCE.UNKNOWN;

      let priority = score * 0.35;
      priority += confidence * 25;
      priority += (components.website_deficiency?.score || 0) * 0.4;
      priority += (components.contactability?.score || 0) * 0.35;
      priority += (components.buying_signals?.score || 0) * 0.25;

      if (components.buying_signals?.unknown) priority -= 3;

      if (economicConfidence === ECONOMIC_CONFIDENCE.HIGH || economicConfidence === ECONOMIC_CONFIDENCE.MEDIUM) {
        const contribution = economics.prospect_specific_economics?.estimated_contribution
          ?? economics.estimated_contribution;
        if (contribution != null) priority += contribution / 150;
        const econScore = components.project_economics?.score || 0;
        priority += econScore * 0.3;
      }

      if (diagnosisClass && DIAGNOSIS_PRIORITY[diagnosisClass] != null) {
        priority += DIAGNOSIS_PRIORITY[diagnosisClass];
      }

      if (action === RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY) priority += 10;
      if (action === RECOMMENDED_ACTIONS.DO_NOT_PURSUE) priority -= 40;
      if (action === RECOMMENDED_ACTIONS.MONITOR) priority -= 15;
      if ((economics.prospect_specific_economics?.estimated_operator_hours
        ?? economics.estimated_operator_hours ?? 0) >= capacityHours * 0.75) {
        priority -= 15;
      }

      const evidenceQuality = Array.isArray(payload.evidence_refs)
        ? payload.evidence_refs.filter((e) => e.evidence_class === 'MEASURED').length
        : 0;
      priority += evidenceQuality * 2;

      const measuredRatio = Array.isArray(payload.evidence_refs) && payload.evidence_refs.length
        ? evidenceQuality / payload.evidence_refs.length
        : 0;
      priority += measuredRatio * 8;

      return {
        ...row,
        max_priority_score: Math.round(priority * 100) / 100,
        prioritization_factors: {
          opportunity_score: score,
          confidence,
          diagnosis_class: diagnosisClass,
          economic_confidence: economicConfidence,
          website_deficiency: components.website_deficiency?.score,
          estimated_contribution: economicConfidence === ECONOMIC_CONFIDENCE.LOW
            || economicConfidence === ECONOMIC_CONFIDENCE.UNKNOWN
            ? null
            : (economics.prospect_specific_economics?.estimated_contribution ?? economics.estimated_contribution),
          recommended_action: action,
          measured_evidence_count: evidenceQuality,
          buying_signals_unknown: Boolean(components.buying_signals?.unknown),
          operator_hours: economics.prospect_specific_economics?.estimated_operator_hours
            ?? economics.estimated_operator_hours,
          uses_default_economics_only: economicConfidence === ECONOMIC_CONFIDENCE.LOW
            || economicConfidence === ECONOMIC_CONFIDENCE.UNKNOWN,
        },
      };
    })
    .sort((a, b) => b.max_priority_score - a.max_priority_score);
}

function buildMaxWebOpportunityDigest(assessments = []) {
  const ranked = prioritizeWebOpportunities(assessments);
  const top = ranked.slice(0, 5);
  const distribution = {};
  for (const action of Object.values(RECOMMENDED_ACTIONS)) {
    distribution[action] = ranked.filter(
      (r) => (r.recommended_action || r.payload?.recommended_action) === action
    ).length;
  }
  const diagnosisDistribution = {};
  for (const row of ranked) {
    const dc = row.prioritization_factors?.diagnosis_class
      || row.payload?.commercial_diagnosis?.diagnosis_class
      || 'unknown';
    diagnosisDistribution[dc] = (diagnosisDistribution[dc] || 0) + 1;
  }
  return {
    ranked,
    top_five: top,
    distribution,
    diagnosis_distribution: diagnosisDistribution,
    summary: top.length
      ? `Top web opportunity: ${top[0].business_name || top[0].domain} (priority ${top[0].max_priority_score})`
      : 'No website opportunity assessments available',
  };
}

module.exports = {
  prioritizeWebOpportunities,
  buildMaxWebOpportunityDigest,
};
