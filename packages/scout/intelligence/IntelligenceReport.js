'use strict';

/**
 * SPEC-141 — Mission Intelligence Report.
 * Scout's deliverable: evidence-backed market understanding, not "found N companies."
 */

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

  const evidenceSources = [
    ...new Set([
      ...(coverage.sourcesUsed || []),
      ...(providerStrategy.providers || []),
    ]),
  ].map((s) => String(s).replace(/_/g, ' '));

  const summary = buildSummary(marketLabel, coverage, ranking);

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
    estimatedUniverse: coverage.estimatedUniverse,
    coverage: coverage.coveragePct,
    qualified: coverage.qualified,
    strong: coverage.strong,
    immediate: coverage.immediate,
    confidence: coverage.confidence,
    finished: coverage.finished,
    evidenceSources,
    immediateOpportunities,
    topOpportunities,
    summary,
    watchCount: coverage.watch,
    investigated: coverage.investigated,
  };
}

function buildSummary(marketLabel, coverage, ranking) {
  const parts = [];
  parts.push(`Market: ${marketLabel}.`);
  if (coverage.estimatedUniverse) {
    parts.push(
      `Estimated universe ${coverage.estimatedUniverse}; investigated ${coverage.investigated} (${Math.round((coverage.coveragePct || 0) * 100)}% coverage).`
    );
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
