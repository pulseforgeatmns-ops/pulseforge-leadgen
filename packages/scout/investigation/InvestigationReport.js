'use strict';

/**
 * SPEC-142 — Investigation Report.
 * Deliverable with six-question answers for every recommendation.
 */

function buildSixQuestions(recommendation, context = {}) {
  const claim = context.claim || {};
  const missing = context.missingEvidence || recommendation.missingEvidence || [];
  const nextStep = context.nextStep || recommendation.nextBestInvestigation || null;

  return {
    whatDoIBelieve: recommendation.claim || recommendation.text || claim.text || recommendation.name,
    whyDoIBelieveIt: (recommendation.reasons || recommendation.supportedBy || claim.supportedBy || [])
      .map((r) => (typeof r === 'string' ? r : r.label || r.source))
      .filter(Boolean)
      .join('; ') || 'Evidence fusion across collected sources.',
    howConfidentAmI: recommendation.confidence != null ? recommendation.confidence : claim.confidence || 0,
    whatEvidenceSupportsIt: (recommendation.supportedBy || claim.supportedBy || []).map((s) =>
      typeof s === 'string' ? s : s.source || s.label
    ),
    whatEvidenceIsStillMissing: missing,
    whatIsTheNextBestInvestigation: nextStep
      ? `${nextStep.providerLabel || nextStep.providerId} → ${nextStep.gap || 'resolve uncertainty'}`
      : missing.length
        ? `Resolve: ${missing.slice(0, 3).join(', ')}`
        : 'No further investigation required.',
  };
}

function buildRecommendation(entry, claims, missingEvidence, rankingEntry) {
  const entityClaims = claims.filter((c) => c.entityId === entry.companyId || c.entityId === entry.id);
  const primaryClaim = entityClaims.sort((a, b) => b.confidence - a.confidence)[0];

  const rec = {
    rank: rankingEntry.rank,
    name: entry.name || rankingEntry.name,
    companyId: entry.companyId || entry.id,
    tier: rankingEntry.tier,
    score: rankingEntry.rankScore,
    confidence: primaryClaim ? primaryClaim.confidence : rankingEntry.evidenceConfidence || 0,
    claim: primaryClaim ? primaryClaim.text : `${entry.name} qualifies for outreach.`,
    supportedBy: primaryClaim ? primaryClaim.supportedBy : [],
    missingEvidence: primaryClaim ? primaryClaim.missingEvidence : missingEvidence.missing || [],
    reasons: rankingEntry.reasons || [],
    icpScore: rankingEntry.icpScore,
  };

  rec.sixQuestions = buildSixQuestions(rec, {
    claim: primaryClaim,
    missingEvidence: rec.missingEvidence,
  });

  return rec;
}

/**
 * Build Investigation Report deliverable.
 * @param {object} input
 * @returns {object}
 */
function buildInvestigationReport(input = {}) {
  const marketDefinition = input.marketDefinition || {};
  const graph = input.graph || { summary: {} };
  const claims = input.claims || [];
  const hypotheses = input.hypotheses || [];
  const missingEvidence = input.missingEvidence || { missing: [], currentConfidence: 0 };
  const overallConfidence = input.overallConfidence != null ? input.overallConfidence : missingEvidence.currentConfidence;
  const conflicts = input.conflicts || [];
  const ranking = input.ranking || {};
  const qualification = input.qualification || {};
  const candidateUniverse = input.candidateUniverse || {};

  const highConfidence = claims.filter((c) => c.confidence >= 0.8).length;
  const needsInvestigation = claims.filter(
    (c) => c.confidence < 0.8 || (c.missingEvidence || []).length > 0
  ).length;

  const ranked = ranking.rankedOpportunities || [];
  const recommendations = ranked.slice(0, 10).map((r) =>
    buildRecommendation(
      { name: r.name, id: r.companyId, companyId: r.companyId },
      claims,
      missingEvidence,
      r
    )
  );

  const marketLabel = [marketDefinition.geography, marketDefinition.segment].filter(Boolean).join(' ');

  const investigated = (candidateUniverse.candidates || []).length;
  const estimated = candidateUniverse.estimatedMarket || candidateUniverse.discovered || investigated;
  const coveragePct = estimated > 0 ? Number((investigated / estimated).toFixed(2)) : 0;

  return {
    kind: 'investigation_report',
    missionIntelligence: {
      market: marketLabel || 'Target market',
      coverage: coveragePct,
      claims: claims.length,
      highConfidence,
      needsInvestigation,
      conflicts: conflicts.length || graph.summary.conflicts || 0,
      recommendations: recommendations.length,
      overallConfidence,
    },
    market: marketLabel,
    hypotheses,
    claims,
    graph,
    missingEvidence,
    conflicts,
    recommendations,
    iterations: input.iterations || [],
    qualified: qualification.qualifiedCount || 0,
    watch: qualification.watchCount || 0,
    rejected: qualification.rejectedCount || 0,
    summary: buildSummary({
      marketLabel,
      missionIntelligence: {
        coverage: coveragePct,
        claims: claims.length,
        highConfidence,
        needsInvestigation,
        conflicts: conflicts.length,
        recommendations: recommendations.length,
        overallConfidence,
      },
    }),
    acceptanceCriteria: {
      sixQuestionsRequired: true,
      allRecommendationsAnswered: recommendations.every((r) => r.sixQuestions && r.sixQuestions.whatDoIBelieve),
    },
  };
}

function buildSummary({ marketLabel, missionIntelligence }) {
  const mi = missionIntelligence;
  return [
    `Market: ${marketLabel}.`,
    `Coverage ${Math.round((mi.coverage || 0) * 100)}%.`,
    `Claims ${mi.claims}: ${mi.highConfidence} high confidence, ${mi.needsInvestigation} need investigation, ${mi.conflicts} conflicts.`,
    `Recommendations ${mi.recommendations}. Overall confidence ${mi.overallConfidence}.`,
  ].join(' ');
}

module.exports = {
  buildInvestigationReport,
  buildSixQuestions,
  buildRecommendation,
};
