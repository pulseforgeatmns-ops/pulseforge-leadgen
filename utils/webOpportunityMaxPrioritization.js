'use strict';

/**
 * Max prioritization helper for website opportunity assessments (SPEC-WEB-001).
 * Does not sort by raw opportunity score alone.
 */

const { RECOMMENDED_ACTIONS } = require('../packages/capabilities/websiteOpportunityIntelligence');

function prioritizeWebOpportunities(assessments = [], { capacityHours = 40 } = {}) {
  return [...assessments]
    .map((row) => {
      const payload = row.payload || row;
      const economics = row.economics || payload.economics || {};
      const score = row.opportunity_score ?? payload.opportunity_score ?? 0;
      const confidence = row.confidence ?? payload.confidence ?? 0;
      const action = row.recommended_action || payload.recommended_action;
      const components = row.score_components || payload.score_components || {};

      let priority = score * 0.35;
      priority += confidence * 25;
      priority += (economics.estimated_contribution || 0) / 100;
      priority += (components.contactability?.score || components.project_economics?.score || 0) * 0.5;

      if (action === RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY) priority += 15;
      if (action === RECOMMENDED_ACTIONS.DO_NOT_PURSUE) priority -= 40;
      if ((economics.estimated_operator_hours || 0) >= capacityHours * 0.75) priority -= 20;

      const evidenceQuality = Array.isArray(payload.evidence_refs)
        ? payload.evidence_refs.filter((e) => e.evidence_class === 'MEASURED').length
        : 0;
      priority += evidenceQuality * 2;

      return {
        ...row,
        max_priority_score: Math.round(priority * 100) / 100,
        prioritization_factors: {
          opportunity_score: score,
          confidence,
          estimated_contribution: economics.estimated_contribution,
          recommended_action: action,
          measured_evidence_count: evidenceQuality,
          operator_hours: economics.estimated_operator_hours,
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
  return {
    ranked,
    top_five: top,
    distribution,
    summary: top.length
      ? `Top web opportunity: ${top[0].business_name || top[0].domain} (priority ${top[0].max_priority_score})`
      : 'No website opportunity assessments available',
  };
}

module.exports = {
  prioritizeWebOpportunities,
  buildMaxWebOpportunityDigest,
};
