'use strict';

const { INTERACTION_TYPES, OUTCOMES } = require('./OperatorTypes');

/**
 * Internal Intelligence Quality Dashboard metrics (SPEC-012).
 * Not customer-facing.
 */

/**
 * @param {object} input
 * @param {object[]} input.events
 * @param {object[]} input.learnings
 * @param {object} [input.preferences]
 */
function buildQualityDashboard(input = {}) {
  const events = input.events || [];
  const learnings = input.learnings || [];

  const typeCounts = {};
  for (const ev of events) {
    typeCounts[ev.type] = (typeCounts[ev.type] || 0) + 1;
  }

  const decided = learnings.filter(
    (l) =>
      l.outcome === OUTCOMES.APPROVED ||
      l.outcome === OUTCOMES.DISMISSED ||
      l.outcome === OUTCOMES.EXECUTED ||
      l.outcome === OUTCOMES.SUCCESSFUL
  );
  const approved = learnings.filter(
    (l) =>
      l.approved > 0 ||
      l.outcome === OUTCOMES.APPROVED ||
      l.outcome === OUTCOMES.EXECUTED ||
      l.outcome === OUTCOMES.SUCCESSFUL
  );
  const viewed = learnings.filter((l) => l.viewed > 0);

  const acceptanceRate =
    decided.length === 0 ? null : round3(approved.length / decided.length);

  const depths = learnings
    .map((l) => Number(l.investigatedDepth) || 0)
    .filter((d) => d > 0);
  const avgInvestigationDepth =
    depths.length === 0
      ? null
      : round3(depths.reduce((a, b) => a + b, 0) / depths.length);

  const times = learnings
    .map((l) => l.timeToDecisionMs)
    .filter((t) => t != null && Number.isFinite(t));
  const avgTimeToDecisionMs =
    times.length === 0
      ? null
      : Math.round(times.reduce((a, b) => a + b, 0) / times.length);

  const maxUsage = typeCounts[INTERACTION_TYPES.ASKED_MAX] || 0;
  const explanationExpansions =
    typeCounts[INTERACTION_TYPES.EXPANDED_REASONING] || 0;
  const evidenceInspections =
    typeCounts[INTERACTION_TYPES.OPENED_EVIDENCE] || 0;

  const explanationExpansionRate =
    viewed.length === 0
      ? null
      : round3(explanationExpansions / Math.max(viewed.length, 1));
  const evidenceInspectionRate =
    viewed.length === 0
      ? null
      : round3(evidenceInspections / Math.max(viewed.length, 1));

  const trustScores = learnings
    .map((l) => (l.trust && l.trust.score != null ? Number(l.trust.score) : null))
    .filter((s) => s != null);
  const avgTrust =
    trustScores.length === 0
      ? null
      : round3(trustScores.reduce((a, b) => a + b, 0) / trustScores.length);

  return {
    generatedAt: new Date().toISOString(),
    recommendationAcceptanceRate: acceptanceRate,
    averageInvestigationDepth: avgInvestigationDepth,
    averageTimeToDecisionMs: avgTimeToDecisionMs,
    maxUsage,
    explanationExpansionRate,
    evidenceInspectionRate,
    averageTrustScore: avgTrust,
    totals: {
      interactionEvents: events.length,
      recommendationsTracked: learnings.length,
      viewed: viewed.length,
      decided: decided.length,
      approved: approved.length,
      askedMax: maxUsage,
      expandedReasoning: explanationExpansions,
      openedEvidence: evidenceInspections,
    },
    interactionBreakdown: typeCounts,
    topIntents:
      (input.preferences && input.preferences.topIntents) || [],
  };
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

module.exports = { buildQualityDashboard };
