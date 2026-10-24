'use strict';

/**
 * SPEC-141 Stage 8 — Market Coverage.
 * SPEC-155 — Coverage computed only relative to explicit universe estimate.
 */

const {
  computeCoverageFromEstimate,
  extractExpectedValue,
  normalizeCandidateUniverseEstimate,
} = require('../universe/CandidateUniverseEstimate');

/**
 * Compute market coverage metrics.
 *
 * @param {object} input
 * @returns {object}
 */
function analyzeMarketCoverage(input = {}) {
  const universe = input.candidateUniverse || {};
  const qualification = input.qualification || {};
  const ranking = input.ranking || {};
  const evidenceCollection = input.evidenceCollection || {};
  const universeEstimate =
    normalizeCandidateUniverseEstimate(input.universeEstimate) ||
    normalizeCandidateUniverseEstimate(universe.universeEstimate) ||
    normalizeCandidateUniverseEstimate(universe.estimatedMarket);

  const estimatedUniverse = extractExpectedValue(universeEstimate);
  const investigated = universe.discovered || 0;
  const qualified = qualification.qualifiedCount || 0;
  const strong = ranking.strong || 0;
  const immediate = ranking.immediate || 0;

  const coveragePct = computeCoverageFromEstimate(investigated, universeEstimate);

  const confidence = Number(
    Math.min(
      0.98,
      (evidenceCollection.avgConfidence || 0.3) * 0.4 +
        (coveragePct != null ? coveragePct * 0.3 : 0) +
        (qualified > 0 ? 0.2 : 0) +
        (strong > 0 ? 0.1 : 0)
    ).toFixed(2)
  );

  const finished =
    coveragePct != null &&
    coveragePct >= 0.7 &&
    investigated >= 5 &&
    (qualified > 0 || qualification.watchCount > 0);

  return {
    estimatedUniverse,
    universeEstimate,
    investigated,
    qualified,
    strong,
    immediate,
    coveragePct,
    coverage: coveragePct,
    confidence,
    finished,
    watch: qualification.watchCount || 0,
    rejected: qualification.rejectedCount || 0,
    sourcesUsed: evidenceCollection.sourcesUsed || universe.sourceTypesChecked || [],
  };
}

module.exports = {
  analyzeMarketCoverage,
};
