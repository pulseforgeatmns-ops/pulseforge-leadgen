'use strict';

const {
  Prioritizer,
  deriveUrgency,
  deriveContradictionSeverity,
} = require('../priorities/Prioritizer');
const { DEFAULT_PRIORITY_LIMIT } = require('../BriefingTypes');

/**
 * Priority queue — top-ranked recommendations from existing memory.
 * Does not generate recommendations.
 */
function buildPriorityQueue(contexts, options = {}) {
  const prioritizer = options.prioritizer || new Prioritizer();
  const limit =
    options.limit != null ? Number(options.limit) : DEFAULT_PRIORITY_LIMIT;

  const items = [];
  for (const ctx of contexts || []) {
    if (!ctx.recommendation) continue;
    const rec = ctx.recommendation;
    const urgency = deriveUrgency({
      changes: ctx.changes,
      watchHitCount: (ctx.triggeredWatches || []).length,
      scoreDelta: ctx.diff ? ctx.diff.scoreDelta : 0,
    });
    const contradictionSeverity = deriveContradictionSeverity({
      recommendation: rec,
      changes: ctx.changes,
    });

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
      trend: (ctx.trend && ctx.trend.score) || 'insufficient',
      confidenceTrend: (ctx.trend && ctx.trend.confidence) || 'insufficient',
      urgency,
      contradictionSeverity,
      scoreDelta: ctx.diff ? ctx.diff.scoreDelta : 0,
      confidenceDelta: ctx.diff ? ctx.diff.confidenceDelta : 0,
    });
  }

  return prioritizer.order(items, { limit });
}

module.exports = {
  buildPriorityQueue,
};
