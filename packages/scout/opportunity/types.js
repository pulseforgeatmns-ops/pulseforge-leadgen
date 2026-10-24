'use strict';

/**
 * SPEC-164 — Opportunity Intelligence types.
 * ADR-084 — Businesses grow by pursuing opportunities.
 *
 * Opportunities are evaluated on business dimensions — not lead scores.
 */

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

const VALUE_LEVELS = Object.freeze({
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
});

const URGENCY_LEVELS = Object.freeze({
  IMMEDIATE: 'immediate',
  SOON: 'soon',
  ROUTINE: 'routine',
  DEFER: 'defer',
});

const OPPORTUNITY_CATEGORIES = Object.freeze({
  IMMEDIATE: 'immediate',
  DEVELOPING: 'developing',
  MONITOR: 'monitor',
  LONG_TERM: 'long_term',
  WATCH: 'watch',
  DECLINE: 'decline',
});

const OPPORTUNITY_TIMELINE_STAGES = Object.freeze({
  MONITOR: 'monitor',
  DEVELOPING: 'developing',
  IMMEDIATE: 'immediate',
  ACTIVE: 'active',
  WON: 'won',
  LOST: 'lost',
  CUSTOMER: 'customer',
});

const CATEGORY_RANK = Object.freeze({
  [OPPORTUNITY_CATEGORIES.IMMEDIATE]: 0,
  [OPPORTUNITY_CATEGORIES.DEVELOPING]: 1,
  [OPPORTUNITY_CATEGORIES.MONITOR]: 2,
  [OPPORTUNITY_CATEGORIES.LONG_TERM]: 3,
  [OPPORTUNITY_CATEGORIES.WATCH]: 4,
  [OPPORTUNITY_CATEGORIES.DECLINE]: 5,
});

/**
 * Qualitative dimension with explainable reasoning.
 * @param {object} partial
 * @returns {object}
 */
function buildOpportunityDimension(partial = {}) {
  const level = partial.level || VALUE_LEVELS.MEDIUM;
  const reasoning = Array.isArray(partial.reasoning)
    ? partial.reasoning.map(asText).filter(Boolean)
    : partial.reasoning
      ? [asText(partial.reasoning)]
      : [];
  return Object.freeze({
    level,
    reasoning,
    summary: partial.summary ? asText(partial.summary) : reasoning[0] || null,
  });
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildOpportunity(partial = {}) {
  const entity = partial.entity || {};
  return Object.freeze({
    entity: Object.freeze({
      id: entity.id || entity.entityId || null,
      name: asText(entity.name || entity.entity) || 'Unknown entity',
      kind: entity.kind || null,
    }),
    mission: partial.mission || null,
    timing: partial.timing || buildOpportunityDimension(),
    priority: partial.priority != null ? Number(partial.priority) : null,
    urgency: partial.urgency || URGENCY_LEVELS.ROUTINE,
    category: partial.category || OPPORTUNITY_CATEGORIES.WATCH,
    timelineStage: partial.timelineStage || OPPORTUNITY_TIMELINE_STAGES.MONITOR,
    expectedBusinessValue: partial.expectedBusinessValue || buildOpportunityDimension(),
    expectedDifficulty: partial.expectedDifficulty || buildOpportunityDimension(),
    expectedProbability: partial.expectedProbability || buildOpportunityDimension(),
    expectedLearningValue: partial.expectedLearningValue || buildOpportunityDimension(),
    strategicFit: partial.strategicFit || buildOpportunityDimension(),
    reachability: partial.reachability || buildOpportunityDimension(),
    opportunityReasoning: Array.isArray(partial.opportunityReasoning)
      ? partial.opportunityReasoning.map(asText).filter(Boolean)
      : [],
    supportingUnderstanding: partial.supportingUnderstanding || null,
    recommendedAction: asText(partial.recommendedAction) || null,
    expectedOutcome: asText(partial.expectedOutcome) || null,
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    activatedHeuristics: (partial.activatedHeuristics || []).slice(),
    comparativeNotes: partial.comparativeNotes || null,
  });
}

module.exports = {
  VALUE_LEVELS,
  URGENCY_LEVELS,
  OPPORTUNITY_CATEGORIES,
  OPPORTUNITY_TIMELINE_STAGES,
  CATEGORY_RANK,
  buildOpportunity,
  buildOpportunityDimension,
};
