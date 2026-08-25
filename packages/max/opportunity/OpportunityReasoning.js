'use strict';

/**
 * SPEC-164 — Max Opportunity Reasoning.
 * ADR-084 — Every Max recommendation must include explicit opportunity reasoning.
 *
 * Max consumes Opportunity Intelligence output for executive decisions.
 * Scout answers "What is true?" — Opportunity Intelligence answers "What matters most?"
 */

const {
  explainWhyFirst,
  compareOpportunities,
  explainOvernightChanges,
  buildRecommendationFromOpportunity,
} = require('../../scout/opportunity/OpportunityIntelligenceEngine');

function opportunityReportFrom(missionReport = {}) {
  return (
    missionReport.opportunityIntelligence || {
      topOpportunities: missionReport.topOpportunities || [],
      opportunities: missionReport.topOpportunities || [],
    }
  );
}

/**
 * Explain why an operator should act on a specific company today.
 * @param {object} input
 * @returns {object}
 */
function explainWhyActToday(input = {}) {
  const report = opportunityReportFrom(input.missionReport || input);
  const opportunities = report.opportunities || report.topOpportunities || [];
  const entityName = input.entity || input.entityName;

  let target =
    (entityName &&
      opportunities.find(
        (o) => o.entity.name.toLowerCase() === String(entityName).toLowerCase()
      )) ||
    report.topOpportunity ||
    opportunities[0] ||
    null;

  if (!target) {
    return {
      spec: 'SPEC-164',
      adr: 'ADR-084',
      summary: 'No opportunity intelligence available for this mission.',
      opportunityReasoning: [],
    };
  }

  const alternatives = opportunities.filter((o) => o.entity.name !== target.entity.name);
  return explainWhyFirst(target, alternatives);
}

/**
 * Comparative reasoning — why is ABC ranked above XYZ?
 * @param {object} input
 * @returns {object}
 */
function explainRankingComparison(input = {}) {
  const report = opportunityReportFrom(input.missionReport || input);
  const opportunities = report.opportunities || report.topOpportunities || [];
  const entityA = input.entityA || input.higher;
  const entityB = input.entityB || input.lower;

  const higher =
    opportunities.find((o) => o.entity.name.toLowerCase() === String(entityA).toLowerCase()) ||
    opportunities[0];
  const lower =
    opportunities.find((o) => o.entity.name.toLowerCase() === String(entityB).toLowerCase()) ||
    opportunities[1];

  if (!higher || !lower) {
    return {
      spec: 'SPEC-164',
      adr: 'ADR-084',
      summary: 'Insufficient opportunities for comparative reasoning.',
    };
  }

  return {
    spec: 'SPEC-164',
    adr: 'ADR-084',
    ...compareOpportunities(higher, lower),
  };
}

/**
 * Ensure a Max recommendation includes opportunity reasoning (invariant).
 * @param {object} recommendation
 * @param {object} missionReport
 * @returns {object}
 */
function ensureOpportunityReasoning(recommendation = {}, missionReport = {}) {
  if (recommendation.opportunityReasoning?.length && recommendation.basedOnOpportunityIntelligence) {
    return recommendation;
  }

  const report = opportunityReportFrom(missionReport);
  const top = report.topOpportunity || (report.topOpportunities || [])[0];
  if (!top) return recommendation;

  const enriched = buildRecommendationFromOpportunity(top, recommendation);
  return {
    ...recommendation,
    ...enriched,
    summary: enriched.summary || recommendation.summary,
  };
}

/**
 * What changed overnight? Explains opportunity movement, not just new evidence.
 * @param {object} input
 * @returns {object}
 */
function explainWhatChanged(input = {}) {
  return explainOvernightChanges(input.priorReport || {}, input.currentReport || input);
}

module.exports = {
  explainWhyActToday,
  explainRankingComparison,
  explainWhatChanged,
  ensureOpportunityReasoning,
  explainWhyFirst,
  compareOpportunities,
  explainOvernightChanges,
  buildRecommendationFromOpportunity,
};
