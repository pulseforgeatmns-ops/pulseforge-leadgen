'use strict';

/**
 * Opportunity Ranking types (SPEC-026).
 * Convert enriched prospects into an explainable priority queue.
 */

const RANKING_PROGRESS_STAGES = Object.freeze({
  SCORING: 'Scoring...',
  BRIEFING: 'Briefing...',
  PRIORITIZING: 'Prioritizing...',
  COMPLETED: 'Completed',
});

const PRIORITY = Object.freeze({
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
});

/** Factor max points — sum = 100. */
const FACTOR_MAX = Object.freeze({
  profile_match: 20,
  buying_signals: 15,
  company_size: 10,
  decision_maker_confidence: 15,
  personalization_opportunities: 10,
  geographic_fit: 10,
  historical_success: 10,
  evidence_confidence: 10,
});

const FACTOR_LABELS = Object.freeze({
  profile_match: 'Profile Match',
  buying_signals: 'Buying Signals',
  company_size: 'Company Size',
  decision_maker_confidence: 'Decision Maker Confidence',
  personalization_opportunities: 'Personalization Opportunities',
  geographic_fit: 'Geographic Fit',
  historical_success: 'Historical Success',
  evidence_confidence: 'Evidence Confidence',
});

const OPERATOR_ACTIONS = Object.freeze([
  'approve',
  're_rank',
  'exclude',
  'lock',
  'continue_to_campaign_builder',
]);

/**
 * @param {number} overallScore
 * @returns {'high'|'medium'|'low'}
 */
function priorityFromScore(overallScore) {
  const score = Number(overallScore) || 0;
  if (score >= 70) return PRIORITY.HIGH;
  if (score >= 45) return PRIORITY.MEDIUM;
  return PRIORITY.LOW;
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildFactorScore(partial = {}) {
  return {
    factor: String(partial.factor || ''),
    label: String(partial.label || FACTOR_LABELS[partial.factor] || partial.factor || ''),
    score: Number.isFinite(Number(partial.score)) ? Number(partial.score) : 0,
    max: Number.isFinite(Number(partial.max))
      ? Number(partial.max)
      : FACTOR_MAX[partial.factor] || 0,
    detail: partial.detail != null ? String(partial.detail) : '',
    evidenceRefs: Array.isArray(partial.evidenceRefs)
      ? partial.evidenceRefs.map(String)
      : [],
    matched: partial.matched === true,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildOpportunityBrief(partial = {}) {
  return {
    whyFit: String(partial.whyFit || ''),
    bestOutreachAngle: String(partial.bestOutreachAngle || ''),
    talkingPoints: Array.isArray(partial.talkingPoints)
      ? partial.talkingPoints.map(String).slice(0, 3)
      : [],
    potentialObjections: Array.isArray(partial.potentialObjections)
      ? partial.potentialObjections.map(String)
      : [],
    suggestedFirstAction: String(partial.suggestedFirstAction || ''),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildRankedOpportunity(partial = {}) {
  const overallScore = clampScore(partial.overallScore);
  return {
    id: String(partial.id || ''),
    companyName: String(partial.companyName || ''),
    website: partial.website != null ? String(partial.website) : null,
    industry: partial.industry != null ? String(partial.industry) : null,
    address: partial.address != null ? String(partial.address) : null,
    email: partial.email != null ? String(partial.email) : null,
    phone: partial.phone != null ? String(partial.phone) : null,
    overallScore,
    priority: partial.priority || priorityFromScore(overallScore),
    confidence: clamp01(partial.confidence),
    topReasons: Array.isArray(partial.topReasons)
      ? partial.topReasons.map(String)
      : [],
    risks: Array.isArray(partial.risks) ? partial.risks.map(String) : [],
    recommendedNextAction: String(partial.recommendedNextAction || ''),
    factorScores: Array.isArray(partial.factorScores) ? partial.factorScores : [],
    opportunityBrief: partial.opportunityBrief || buildOpportunityBrief(),
    discoveryConfidence: Number.isFinite(Number(partial.discoveryConfidence))
      ? Number(partial.discoveryConfidence)
      : null,
    rankingSignals: Array.isArray(partial.rankingSignals)
      ? partial.rankingSignals
      : [],
    source: partial.source || null,
    enriched: partial.enriched === true,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildOpportunityRankingResult(partial = {}) {
  return {
    prospects: Array.isArray(partial.prospects) ? partial.prospects : [],
    rankedCount: Number(partial.rankedCount) || 0,
    summary: {
      high: Number(partial.summary?.high) || 0,
      medium: Number(partial.summary?.medium) || 0,
      low: Number(partial.summary?.low) || 0,
      averageScore: Number(partial.summary?.averageScore) || 0,
    },
    reviewPackage: partial.reviewPackage || null,
    confidence: clamp01(partial.confidence),
    warnings: Array.isArray(partial.warnings) ? partial.warnings.map(String) : [],
  };
}

function clampScore(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(100, Math.round(v)));
}

function clamp01(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, Number(v.toFixed(4))));
}

module.exports = {
  RANKING_PROGRESS_STAGES,
  PRIORITY,
  FACTOR_MAX,
  FACTOR_LABELS,
  OPERATOR_ACTIONS,
  priorityFromScore,
  buildFactorScore,
  buildOpportunityBrief,
  buildRankedOpportunity,
  buildOpportunityRankingResult,
  clampScore,
  clamp01,
};
