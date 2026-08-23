'use strict';

/**
 * SPEC-141 Stage 7 — Opportunity Ranking.
 * Qualification determines inclusion; ranking determines priority.
 */

const { RANKING_FACTORS } = require('./types');

function scoreFactor(value, weight = 1) {
  return Math.max(0, Math.min(1, Number(value) || 0)) * weight;
}

/**
 * Rank qualified opportunities by composite score.
 *
 * @param {object} input
 * @returns {object}
 */
function rankOpportunities(input = {}) {
  const qualified = input.qualification.qualified || [];
  const evidenceByCandidate = input.evidenceCollection.evidenceByCandidate || [];
  const evidenceMap = new Map(
    evidenceByCandidate.map((e) => [String(e.candidateId), e])
  );
  const candidates = input.candidateUniverse.candidates || [];
  const candidateMap = new Map(candidates.map((c) => [String(c.id), c]));

  const ranked = qualified.map((q) => {
    const candidate = candidateMap.get(String(q.candidateId)) || {};
    const evidence = evidenceMap.get(String(q.candidateId)) || { confidence: 0.3 };

    const scores = {
      [RANKING_FACTORS.REVENUE_POTENTIAL]: scoreFactor(
        (q.icpScore != null ? q.icpScore / 100 : q.fitScore) || 0.5,
        0.2
      ),
      [RANKING_FACTORS.EASE_OF_ACCESS]: scoreFactor(
        q.checks.hasContactPath ? 0.9 : q.checks.hasDecisionMaker ? 0.6 : 0.3,
        0.15
      ),
      [RANKING_FACTORS.BUYING_SIGNALS]: scoreFactor(
        q.checks.hasBuyingSignals ? 0.85 : 0.25,
        0.2
      ),
      [RANKING_FACTORS.RELATIONSHIP_PROBABILITY]: scoreFactor(
        q.checks.hasDecisionMaker ? 0.75 : 0.35,
        0.1
      ),
      [RANKING_FACTORS.GEOGRAPHIC_FIT]: scoreFactor(
        candidate.serviceAreaMatch !== false ? 0.9 : 0.5,
        0.1
      ),
      [RANKING_FACTORS.EVIDENCE_CONFIDENCE]: scoreFactor(evidence.confidence, 0.15),
      [RANKING_FACTORS.STRATEGIC_VALUE]: scoreFactor(
        q.fitClass === 'supported' ? 0.95 : q.fitClass === 'fit' ? 0.75 : 0.5,
        0.1
      ),
    };

    const composite = Number(
      Object.values(scores)
        .reduce((sum, v) => sum + v, 0)
        .toFixed(3)
    );

    const tier =
      composite >= 0.75 ? 'strong' : composite >= 0.55 ? 'moderate' : 'developing';

    return {
      ...q,
      rankScore: composite,
      tier,
      scores,
      immediate: composite >= 0.78 && q.checks.hasBuyingSignals && q.checks.hasContactPath,
    };
  });

  ranked.sort((a, b) => b.rankScore - a.rankScore);
  ranked.forEach((row, index) => {
    row.rank = index + 1;
  });

  return {
    rankedOpportunities: ranked,
    strong: ranked.filter((r) => r.tier === 'strong').length,
    moderate: ranked.filter((r) => r.tier === 'moderate').length,
    immediate: ranked.filter((r) => r.immediate).length,
  };
}

module.exports = {
  rankOpportunities,
};
