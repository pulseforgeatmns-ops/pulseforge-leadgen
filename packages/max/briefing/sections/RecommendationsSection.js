'use strict';

const {
  Prioritizer,
  deriveUrgency,
  deriveContradictionSeverity,
} = require('../priorities/Prioritizer');
const { DEFAULT_RECOMMENDATION_LIMIT } = require('../BriefingTypes');

/**
 * Recommended Actions — consumes existing Recommendation objects.
 * Does not generate new recommendations. Deterministic ordering only.
 */
function buildRecommendationsSection(contexts, options = {}) {
  const prioritizer = options.prioritizer || new Prioritizer();
  const limit =
    options.limit != null
      ? Number(options.limit)
      : DEFAULT_RECOMMENDATION_LIMIT;

  const items = [];
  for (const ctx of contexts || []) {
    if (!ctx.recommendation) continue;
    const rec = ctx.recommendation;
    items.push({
      id: rec.id,
      companyId: ctx.companyId,
      companyName: ctx.companyName,
      type: rec.type,
      priority: rec.priority,
      score: rec.score,
      confidence: rec.confidence,
      recommendedAction: rec.recommendedAction,
      why: (rec.reasoningSummary && rec.reasoningSummary.whyThis) || [],
      whyNot: (rec.reasoningSummary && rec.reasoningSummary.whyNot) || [],
      whyNow: (rec.reasoningSummary && rec.reasoningSummary.whyNow) || [],
      claims: rec.claims || [],
      evidence: rec.evidence || [],
      trend: (ctx.trend && ctx.trend.score) || 'insufficient',
      urgency: deriveUrgency({
        changes: ctx.changes,
        watchHitCount: (ctx.triggeredWatches || []).length,
        scoreDelta: ctx.diff ? ctx.diff.scoreDelta : 0,
      }),
      contradictionSeverity: deriveContradictionSeverity({
        recommendation: rec,
        changes: ctx.changes,
      }),
    });
  }

  return {
    total: items.length,
    items: prioritizer.order(items, { limit }),
  };
}

module.exports = {
  buildRecommendationsSection,
};
