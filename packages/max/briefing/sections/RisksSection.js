'use strict';

const { DEFAULT_RISK_LIMIT, RISK_CHANGE_TYPES } = require('../BriefingTypes');
const { round } = require('../../reasoning/ReasoningTypes');

const RISK_KIND = Object.freeze({
  OPPORTUNITY_DECAYED: 'opportunity_decayed',
  CONFIDENCE_COLLAPSE: 'confidence_collapse',
  NEW_CONTRADICTION: 'new_contradiction',
  STRATEGY_DETERIORATION: 'strategy_deterioration',
  SIGNAL_REMOVED: 'signal_removed',
  PRIORITY_REGRESSION: 'priority_regression',
  ENGAGEMENT_GAP: 'engagement_gap',
});

/**
 * Risks — largest deteriorations from Memory changes (assembled, not invented).
 */
function buildRisksSection(contexts, options = {}) {
  const limit =
    options.limit != null ? Number(options.limit) : DEFAULT_RISK_LIMIT;

  /** @type {object[]} */
  const risks = [];

  for (const ctx of contexts || []) {
    for (const change of ctx.changes || []) {
      if (!RISK_CHANGE_TYPES.includes(change.type)) continue;
      const kind = classifyRiskKind(change, ctx);
      const severity = riskSeverity(change, ctx);
      risks.push({
        companyId: ctx.companyId,
        companyName: ctx.companyName,
        kind,
        changeType: change.type,
        severity,
        magnitude: change.magnitude != null ? change.magnitude : null,
        details: change.details || {},
        score: ctx.latest ? ctx.latest.score : null,
        confidence: ctx.latest ? ctx.latest.confidence : null,
        scoreDelta: ctx.diff ? ctx.diff.scoreDelta : null,
        confidenceDelta: ctx.diff ? ctx.diff.confidenceDelta : null,
        why: riskWhy(change, ctx, kind),
      });
    }

    // Engagement gap heuristic from existing recommendation/action only —
    // surfaces hold/deprioritize with downward trend as risk without new reasoning.
    if (
      ctx.recommendation &&
      (ctx.recommendation.recommendedAction === 'hold' ||
        ctx.recommendation.type === 'deprioritize') &&
      ctx.trend &&
      ctx.trend.score === 'down'
    ) {
      risks.push({
        companyId: ctx.companyId,
        companyName: ctx.companyName,
        kind: RISK_KIND.ENGAGEMENT_GAP,
        changeType: 'trend_down_deprioritize',
        severity: round(
          Math.max(40, 100 - (ctx.recommendation.score || 0))
        ),
        magnitude: ctx.diff ? Math.abs(ctx.diff.scoreDelta || 0) : null,
        details: {
          recommendedAction: ctx.recommendation.recommendedAction,
          type: ctx.recommendation.type,
        },
        score: ctx.recommendation.score,
        confidence: ctx.recommendation.confidence,
        scoreDelta: ctx.diff ? ctx.diff.scoreDelta : null,
        confidenceDelta: ctx.diff ? ctx.diff.confidenceDelta : null,
        why: ['trend=down', `action=${ctx.recommendation.recommendedAction}`],
      });
    }
  }

  risks.sort((a, b) => {
    if (b.severity !== a.severity) return b.severity - a.severity;
    const k = String(a.kind).localeCompare(String(b.kind));
    if (k !== 0) return k;
    return String(a.companyId).localeCompare(String(b.companyId));
  });

  // Dedupe by companyId+kind (keep highest severity — already sorted)
  const seen = new Set();
  const deduped = [];
  for (const r of risks) {
    const key = `${r.companyId}::${r.kind}::${r.changeType}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(r);
  }

  return {
    total: deduped.length,
    items: deduped.slice(0, limit),
  };
}

function classifyRiskKind(change, ctx) {
  if (change.type === 'score_decreased') return RISK_KIND.OPPORTUNITY_DECAYED;
  if (change.type === 'confidence_decreased') return RISK_KIND.CONFIDENCE_COLLAPSE;
  if (change.type === 'new_contradiction') return RISK_KIND.NEW_CONTRADICTION;
  if (change.type === 'strategy_score_down') {
    return RISK_KIND.STRATEGY_DETERIORATION;
  }
  if (change.type === 'removed_claim' || change.type === 'removed_evidence') {
    return RISK_KIND.SIGNAL_REMOVED;
  }
  if (
    change.type === 'priority_changed' ||
    change.type === 'type_changed' ||
    change.type === 'action_changed'
  ) {
    return RISK_KIND.PRIORITY_REGRESSION;
  }
  void ctx;
  return change.type;
}

function riskSeverity(change, ctx) {
  const mag = Number(change.magnitude) || 0;
  let severity = Math.min(100, 20 + mag * 3);

  if (change.type === 'confidence_decreased') {
    severity = Math.min(100, 30 + mag * 4);
  }
  if (change.type === 'new_contradiction') {
    severity = Math.min(100, 55 + mag * 5);
  }
  if (change.type === 'score_decreased' && mag >= 15) {
    severity = Math.min(100, 50 + mag * 2);
  }
  if (ctx.diff && ctx.diff.confidenceDelta <= -20) {
    severity = Math.max(severity, 70);
  }
  return round(severity);
}

function riskWhy(change, ctx, kind) {
  const why = [`kind=${kind}`, `change=${change.type}`];
  if (change.magnitude != null) why.push(`magnitude=${change.magnitude}`);
  if (ctx.diff) {
    why.push(`scoreDelta=${ctx.diff.scoreDelta}`);
    why.push(`confidenceDelta=${ctx.diff.confidenceDelta}`);
  }
  return why.sort();
}

module.exports = {
  buildRisksSection,
  RISK_KIND,
};
