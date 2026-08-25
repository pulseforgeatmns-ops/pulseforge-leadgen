'use strict';

/**
 * SPEC-141 — Mission Intelligence Report.
 * SPEC-144 — Credibility briefs on ranked opportunities.
 */

const { buildIntelligenceBriefs } = require('../credibility/CredibilityFramework');

function formatMarketLabel(marketDefinition) {
  const parts = [];
  if (marketDefinition.geography) parts.push(marketDefinition.geography);
  if (marketDefinition.segment) parts.push(marketDefinition.segment);
  return parts.join(' ') || 'Target market';
}

/**
 * Build the Mission Intelligence Report from pipeline outputs.
 *
 * @param {object} input
 * @returns {object}
 */
function buildIntelligenceReport(input = {}) {
  const marketDefinition = input.marketDefinition || {};
  const coverage = input.coverage || {};
  const ranking = input.ranking || {};
  const evidenceCollection = input.evidenceCollection || {};
  const providerStrategy = input.providerStrategy || {};
  const investigationPlan = input.investigationPlan || null;
  const investigationStatus = input.investigationStatus || null;

  const marketLabel = formatMarketLabel(marketDefinition);
  const ranked = ranking.rankedOpportunities || [];

  const immediateOpportunities = ranked
    .filter((r) => r.immediate)
    .slice(0, 5)
    .map((r) => ({
      rank: r.rank,
      name: r.name,
      score: r.rankScore,
      reasons: r.reasons,
    }));

  const topOpportunities = ranked.slice(0, 10).map((r) => ({
    rank: r.rank,
    name: r.name,
    tier: r.tier,
    score: r.rankScore,
    icpScore: r.icpScore,
    evidenceConfidence: r.evidenceConfidence,
  }));

  const intelligenceBriefs = buildIntelligenceBriefs({
    rankedOpportunities: ranked,
    candidateUniverse: input.candidateUniverse || {},
    claims: input.claims || [],
    hypotheses: input.hypotheses || [],
    conflicts: input.conflicts || [],
    missingEvidence: input.missingEvidence || [],
  });

  const conflictResolution = input.conflictResolution || null;
  const evidenceConflicts = conflictResolution?.evidenceConflicts || null;

  const evidenceSources = [
    ...new Set([
      ...(coverage.sourcesUsed || []),
      ...(providerStrategy.providers || []),
    ]),
  ].map((s) => String(s).replace(/_/g, ' '));

  const summary = buildSummary(marketLabel, coverage, ranking);

  const investigationStrategy = investigationPlan
    ? {
        objective: investigationPlan.objective,
        hypotheses: (investigationPlan.hypotheses || []).map((h) => h.text || h),
        evidenceRequired: investigationPlan.evidenceRequired,
        providerSequence: (investigationPlan.providerSequence || []).map((p) => ({
          order: p.order,
          provider: p.providerLabel || p.providerId,
          gap: p.gap,
          estimatedCost: p.estimatedCost,
          confidenceGain: p.confidenceGain,
          status: p.status,
        })),
        stoppingConditions: investigationPlan.stoppingConditions,
        estimatedCoverage: investigationPlan.estimatedCoverage,
        estimatedConfidence: investigationPlan.estimatedConfidence,
        estimatedCost: investigationPlan.estimatedCost,
      }
    : null;

  return {
    kind: 'mission_intelligence_report',
    market: marketLabel,
    marketDefinition: {
      segment: marketDefinition.segment,
      geography: marketDefinition.geography,
      buyer: marketDefinition.buyer,
      industry: marketDefinition.industry,
      missionGoal: marketDefinition.missionGoal,
    },
    investigationStrategy,
    investigationStatus,
    evidenceSummary: {
      sourcesUsed: evidenceSources,
      withEvidence: evidenceCollection.withEvidence || 0,
      avgConfidence: evidenceCollection.avgConfidence || coverage.confidence || 0,
    },
    remainingUnknowns: investigationStatus?.remainingUnknowns || [],
    recommendedNextInvestigation: investigationStatus?.recommendedNextInvestigation || null,
    estimatedMarket: coverage.universeEstimate
      ? {
          minimum: coverage.universeEstimate.minimum,
          expected: coverage.universeEstimate.expected,
          maximum: coverage.universeEstimate.maximum,
          confidence: coverage.universeEstimate.confidence,
          reasoning: coverage.universeEstimate.reasoning || [],
          revisionHistory: coverage.universeEstimate.revisionHistory || [],
        }
      : coverage.estimatedUniverse
        ? { expected: coverage.estimatedUniverse }
        : null,
    estimatedUniverse: coverage.estimatedUniverse,
    coverage: coverage.coveragePct,
    coveragePct: coverage.coveragePct,
    qualified: coverage.qualified,
    strong: coverage.strong,
    immediate: coverage.immediate,
    confidence: coverage.confidence,
    finished: coverage.finished,
    evidenceSources,
    immediateOpportunities,
    topOpportunities,
    intelligenceBriefs,
    evidenceConflicts,
    credibilityFramework: {
      version: 'SPEC-144',
      briefCount: intelligenceBriefs.length,
    },
    summary,
    watchCount: coverage.watch,
    investigated: coverage.investigated,
  };
}

function buildSummary(marketLabel, coverage, ranking) {
  const parts = [];
  parts.push(`Market: ${marketLabel}.`);
  if (coverage.universeEstimate || coverage.estimatedUniverse) {
    const expected =
      (coverage.universeEstimate && coverage.universeEstimate.expected) || coverage.estimatedUniverse;
    parts.push(`Estimated universe ${expected}; investigated ${coverage.investigated || 0}.`);
    if (coverage.coveragePct != null) {
      parts.push(`Coverage ${Math.round(coverage.coveragePct * 100)}%.`);
    } else {
      parts.push('Coverage unavailable — no universe estimate.');
    }
    if (coverage.universeEstimate && coverage.universeEstimate.confidence != null) {
      parts.push(`Estimate confidence ${coverage.universeEstimate.confidence}.`);
    }
  }
  parts.push(
    `Qualified ${coverage.qualified || 0}, strong ${coverage.strong || 0}, immediate ${coverage.immediate || 0}.`
  );
  parts.push(`Confidence ${coverage.confidence || 0}.`);
  if (coverage.finished) {
    parts.push('Investigation coverage threshold met.');
  } else {
    parts.push('Additional discovery may improve coverage.');
  }
  return parts.join(' ');
}

module.exports = {
  buildIntelligenceReport,
  formatMarketLabel,
};
