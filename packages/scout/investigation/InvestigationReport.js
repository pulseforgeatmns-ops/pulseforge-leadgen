'use strict';

/**
 * SPEC-142 — Investigation Report.
 * SPEC-144 — Intelligence briefs with credibility framework on every recommendation.
 */

const {
  buildIntelligenceBrief,
  buildIntelligenceBriefs,
  validateBriefAcceptance,
} = require('../credibility/CredibilityFramework');
const {
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
} = require('../universe/CandidateUniverseEstimate');

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

function buildRecommendation(entry, claims, missingEvidence, rankingEntry, context = {}) {
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

  rec.intelligenceBrief = buildIntelligenceBrief({
    rankingEntry,
    candidate: entry,
    claims,
    hypotheses: context.hypotheses || [],
    conflicts: context.conflicts || [],
    missingEvidence: rec.missingEvidence,
  });

  rec.credibility = {
    trust: rec.intelligenceBrief.trust,
    confidenceExplanation: rec.intelligenceBrief.confidenceExplanation,
    acceptance: validateBriefAcceptance(rec.intelligenceBrief),
  };

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
  const candidates = candidateUniverse.candidates || [];
  const candidateMap = new Map(candidates.map((c) => [String(c.id), c]));

  const recommendations = ranked.slice(0, 10).map((r) =>
    buildRecommendation(
      candidateMap.get(String(r.companyId || r.candidateId)) || {
        name: r.name,
        id: r.companyId,
        companyId: r.companyId,
      },
      claims,
      missingEvidence,
      r,
      { hypotheses, conflicts }
    )
  );

  const intelligenceBriefs = buildIntelligenceBriefs({
    ranking,
    claims,
    hypotheses,
    conflicts,
    missingEvidence,
    candidateUniverse,
  });

  const marketLabel = [marketDefinition.geography, marketDefinition.segment].filter(Boolean).join(' ');

  const investigated = (candidateUniverse.candidates || []).length;
  const universeEstimate =
    normalizeCandidateUniverseEstimate(input.universeEstimate) ||
    normalizeCandidateUniverseEstimate(candidateUniverse.universeEstimate) ||
    normalizeCandidateUniverseEstimate(candidateUniverse.estimatedMarket);
  const estimated = extractExpectedValue(universeEstimate);
  const coveragePct = computeCoverageFromEstimate(investigated, universeEstimate);

  const investigationPlan = input.investigationPlan || null;
  const investigationStatus = input.investigationStatus || null;

  const investigationStrategy = investigationPlan
    ? {
        objective: investigationPlan.objective,
        hypotheses: (investigationPlan.hypotheses || []).map((h) => ({
          text: h.text,
          status: h.status || 'open',
          gap: h.gap,
        })),
        evidenceRequired: investigationPlan.evidenceRequired,
        providerSequence: (investigationPlan.providerSequence || []).slice(0, 10).map((p) => ({
          order: p.order,
          provider: p.providerLabel || p.providerId,
          gap: p.gap,
          confidenceGain: p.confidenceGain,
          status: p.status,
        })),
        estimatedUniverse: investigationPlan.estimatedCoverage?.estimatedUniverse,
        targetCoverage: investigationPlan.estimatedCoverage?.targetCoverage,
        targetConfidence: investigationPlan.stoppingConditions?.confidenceTarget,
        stoppingCondition: investigationPlan.stoppingConditions?.expression,
      }
    : null;

  const conflictResolution = input.conflictResolution || null;
  const evidenceConflicts = conflictResolution?.evidenceConflicts || null;

  return {
    kind: 'investigation_report',
    investigationStrategy,
    investigationStatus,
    evidenceSummary: {
      claims: claims.length,
      highConfidence,
      needsInvestigation,
      conflicts: (evidenceConflicts?.summary?.detected ?? conflicts.length) || graph.summary?.conflicts || 0,
      conflictsResolved: evidenceConflicts?.summary?.resolved ?? 0,
      conflictsOutstanding: evidenceConflicts?.summary?.outstanding ?? 0,
      overallConfidence,
      coverage: coveragePct,
    },
    remainingUnknowns: investigationStatus?.remainingUnknowns || missingEvidence.missing || [],
    recommendedNextInvestigation: investigationStatus?.recommendedNextInvestigation || null,
    missionIntelligence: {
      market: marketLabel || 'Target market',
      coverage: coveragePct,
      estimatedUniverse: estimated,
      universeEstimate,
      investigated,
      claims: claims.length,
      highConfidence,
      needsInvestigation,
      conflicts: (evidenceConflicts?.summary?.detected ?? conflicts.length) || graph.summary.conflicts || 0,
      recommendations: recommendations.length,
      overallConfidence,
    },
    evidenceConflicts,
    market: marketLabel,
    hypotheses,
    claims,
    graph,
    missingEvidence,
    conflicts,
    recommendations,
    intelligenceBriefs,
    credibilityFramework: {
      version: 'SPEC-144',
      briefCount: intelligenceBriefs.length,
      allBriefsPassAcceptance: intelligenceBriefs.every(
        (brief) => validateBriefAcceptance(brief).passes
      ),
    },
    iterations: input.iterations || [],
    investigationBoard: input.investigationBoard || null,
    investigationJournal: input.investigationJournal || null,
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
      credibilityBriefRequired: true,
      allCredibilityBriefsPass: recommendations.every(
        (r) => r.credibility && r.credibility.acceptance && r.credibility.acceptance.passes
      ),
      adaptivePlanning: Boolean(input.investigationBoard),
      explicitInvestigationPlan: Boolean(investigationPlan),
      canAnswerStopReason: Boolean(input.investigationJournal?.stopExplanation),
    },
  };
}

function buildSummary({ marketLabel, missionIntelligence }) {
  const mi = missionIntelligence;
  const coverageText =
    mi.coverage != null ? `${Math.round(mi.coverage * 100)}%` : 'unavailable (no universe estimate)';
  return [
    `Market: ${marketLabel}.`,
    `Coverage ${coverageText}.`,
    `Claims ${mi.claims}: ${mi.highConfidence} high confidence, ${mi.needsInvestigation} need investigation, ${mi.conflicts} conflicts.`,
    `Recommendations ${mi.recommendations}. Overall confidence ${mi.overallConfidence}.`,
  ].join(' ');
}

module.exports = {
  buildInvestigationReport,
  buildSixQuestions,
  buildRecommendation,
  buildIntelligenceBriefs,
  validateBriefAcceptance,
};
