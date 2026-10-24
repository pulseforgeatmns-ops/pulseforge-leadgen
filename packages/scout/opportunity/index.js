'use strict';

/**
 * SPEC-164 — Opportunity Intelligence public exports.
 */

const {
  VALUE_LEVELS,
  URGENCY_LEVELS,
  OPPORTUNITY_CATEGORIES,
  OPPORTUNITY_TIMELINE_STAGES,
  CATEGORY_RANK,
  buildOpportunity,
  buildOpportunityDimension,
} = require('./types');

const {
  evaluateOpportunities,
  evaluateSingleOpportunity,
  rankOpportunities,
  compareOpportunities,
  explainWhyFirst,
  explainOvernightChanges,
  detectOpportunityMovements,
  recalculateForMissionObjectives,
  buildOpportunityIntelligenceReport,
  buildRecommendationFromOpportunity,
} = require('./OpportunityIntelligenceEngine');

module.exports = {
  VALUE_LEVELS,
  URGENCY_LEVELS,
  OPPORTUNITY_CATEGORIES,
  OPPORTUNITY_TIMELINE_STAGES,
  CATEGORY_RANK,
  buildOpportunity,
  buildOpportunityDimension,
  evaluateOpportunities,
  evaluateSingleOpportunity,
  rankOpportunities,
  compareOpportunities,
  explainWhyFirst,
  explainOvernightChanges,
  detectOpportunityMovements,
  recalculateForMissionObjectives,
  buildOpportunityIntelligenceReport,
  buildRecommendationFromOpportunity,
};
