'use strict';

/**
 * SPEC-159 — Mission Intelligence Report.
 * Understanding before recommendation (ADR-079).
 *
 * Includes: final market definition, universe estimate, hypothesis history,
 * evidence graph summary, remaining unknowns, confidence evolution,
 * recommendation, suggested next investigation.
 */

const { extractExpectedValue } = require('../universe/CandidateUniverseEstimate');
const { buildBusinessUnderstandingReport } = require('../synthesis/EvidenceSynthesisEngine');
const {
  activateHeuristics,
  buildRecommendationFromHeuristics,
  buildBusinessJudgmentReport,
} = require('../heuristics/BusinessHeuristicsEngine');
const { summarizeHypothesisHistory } = require('./HypothesisLifecycle');
const { serializeInvestigationState } = require('./InvestigationState');

function summarizeEvidenceGraph(evidenceGraph = {}) {
  const nodes = evidenceGraph.nodes || [];
  const edges = evidenceGraph.edges || [];

  const byType = {};
  for (const node of nodes) {
    const type = node.type || 'unknown';
    byType[type] = (byType[type] || 0) + 1;
  }

  const relationships = edges.reduce((acc, edge) => {
    const rel = edge.relation || 'related';
    acc[rel] = (acc[rel] || 0) + 1;
    return acc;
  }, {});

  return {
    nodeCount: nodes.length,
    edgeCount: edges.length,
    byType,
    relationships,
    sampleRelationships: edges.slice(0, 5).map((e) => ({
      from: e.from,
      to: e.to,
      relation: e.relation,
    })),
  };
}

function buildRecommendationFromUnderstanding(state = {}, synthesisResult = null, judgmentResult = null) {
  if (judgmentResult?.activatedHeuristics?.length) {
    return buildRecommendationFromHeuristics(judgmentResult);
  }

  const market = state.marketDefinition || {};
  const businessItems = state.businessUnderstandings || [];
  const topBusiness = businessItems[0];

  if (topBusiness) {
    return {
      kind: 'business_understanding',
      summary: `${topBusiness.entity}: ${(topBusiness.assertions || []).join('; ')} (confidence ${topBusiness.confidence}).`,
      entity: topBusiness.entity,
      assertions: topBusiness.assertions,
      confidence: topBusiness.confidence,
      basedOnUnderstanding: true,
      notDirectFromEvidence: true,
      adr: 'ADR-080',
    };
  }

  const supported = (state.activeHypotheses || []).filter((h) => h.lifecycle === 'supported');
  const dominantTerm = supported[0]?.searchTerms?.[0] || market.terminology?.[0] || null;
  const synthesisConfidence = synthesisResult?.summary?.averageConfidence;

  return {
    kind: 'market_understanding',
    summary: dominantTerm
      ? `Market best understood through "${dominantTerm}" terminology (confidence ${synthesisConfidence ?? state.confidence}).`
      : `Market investigation at confidence ${synthesisConfidence ?? state.confidence} — review remaining unknowns before outreach.`,
    dominantTerminology: dominantTerm,
    confidence: synthesisConfidence ?? state.confidence,
    basedOnUnderstanding: true,
    notDirectFromEvidence: true,
    adr: 'ADR-080',
  };
}

function buildSuggestedNextInvestigation(state = {}) {
  const top = (state.nextQuestions || [])[0];
  if (!top) {
    return {
      action: 'none',
      reason: 'No open investigation branches with higher expected value.',
    };
  }

  return {
    action: 'investigate',
    question: top.question,
    priority: top.priority,
    source: top.source,
    hypothesisId: top.hypothesisId || null,
    rationale: 'Highest-priority unanswered question from investigation state.',
  };
}

/**
 * Build the Mission Intelligence Report from finalized InvestigationState.
 * @param {object} input
 * @returns {object}
 */
function buildMissionIntelligenceReport(input = {}) {
  const state = input.state || {};
  const serialized = serializeInvestigationState(state);
  const marketDefinition = state.marketDefinition || {};
  const universeEstimate = state.universeEstimate || null;

  const hypothesisHistory = summarizeHypothesisHistory(state);
  const evidenceGraphSummary = summarizeEvidenceGraph(state.evidenceGraph);
  const synthesisResult = input.synthesisResult || null;
  const businessUnderstandingSection = buildBusinessUnderstandingReport(
    state.businessUnderstandings || [],
    synthesisResult || { summary: state.synthesisSummary }
  );
  const judgmentResult =
    input.judgmentResult ||
    state.businessJudgment ||
    activateHeuristics({
      businessUnderstandings: state.businessUnderstandings || [],
      extraEvidence: input.extraEvidence || [],
      heuristicLibrary: input.heuristicLibrary,
    });
  const businessJudgmentSection = buildBusinessJudgmentReport(judgmentResult);
  const recommendation = buildRecommendationFromUnderstanding(state, synthesisResult, judgmentResult);
  const suggestedNextInvestigation = buildSuggestedNextInvestigation(state);

  const remainingUnknowns = [
    ...(state.uncertainty?.open || []),
    ...(state.uncertainty?.persistent || []),
  ];

  return {
    kind: 'mission_intelligence_report',
    spec: 'SPEC-159',
    synthesisSpec: 'SPEC-160',
    heuristicsSpec: 'SPEC-162',
    adr: 'ADR-079',
    synthesisAdr: 'ADR-080',
    heuristicsAdr: 'ADR-082',
    finalMarketDefinition: {
      market: marketDefinition.market,
      geography: marketDefinition.geography,
      terminology: marketDefinition.terminology,
      customerTypes: marketDefinition.customerTypes,
      decisionMakers: marketDefinition.decisionMakers,
      buyingSignals: marketDefinition.buyingSignals,
      revisionHistory: marketDefinition.revisionHistory || [],
      revised: marketDefinition.revised === true,
    },
    universeEstimate: universeEstimate
      ? {
          minimum: universeEstimate.minimum,
          expected: extractExpectedValue(universeEstimate),
          maximum: universeEstimate.maximum,
          confidence: universeEstimate.confidence,
          reasoning: universeEstimate.reasoning,
          revisionHistory: universeEstimate.revisionHistory || [],
        }
      : null,
    hypothesisHistory,
    evidenceGraphSummary,
    businessUnderstanding: businessUnderstandingSection,
    businessJudgment: businessJudgmentSection,
    judgmentResult,
    remainingUnknowns,
    confidenceEvolution: state.confidenceEvolution || [],
    currentConfidence: state.confidence,
    recommendation,
    suggestedNextInvestigation,
    investigationCycles: input.cycles || [],
    stopCondition: input.stop || null,
    priorUnderstandingLoaded: state.seededFromMemory === true,
    priorUnderstanding: state.priorUnderstanding || null,
    coverage: state.coverage || input.coverageMetrics || null,
    candidateCount: (input.candidates || []).length,
    understandingFirst: true,
    judgmentFromHeuristics: judgmentResult.basedOnHeuristics === true,
    synthesizedNotRaw: businessUnderstandingSection.synthesizedNotRaw === true,
    summary: buildReportSummary({
      marketDefinition,
      confidence: judgmentResult.overallJudgment?.confidence ?? state.confidence,
      remainingUnknowns,
      recommendation,
      businessJudgment: businessJudgmentSection.overallJudgment?.summary,
    }),
    investigationState: serialized,
  };
}

function buildReportSummary({
  marketDefinition,
  confidence,
  remainingUnknowns,
  recommendation,
  businessJudgment,
}) {
  const market = marketDefinition.market || 'Target market';
  const unknownCount = remainingUnknowns.length;
  const parts = [
    `Market: ${market}.`,
    `Confidence ${confidence}.`,
    `${unknownCount} remaining unknown${unknownCount === 1 ? '' : 's'}.`,
  ];
  if (businessJudgment) {
    parts.push(`Business Judgment: ${businessJudgment}`);
  }
  parts.push(recommendation.summary);
  return parts.join(' ');
}

function mergeIntoDiscoveryReport(discoveryReport = {}, missionReport = {}) {
  return {
    ...discoveryReport,
    missionIntelligenceReport: missionReport,
    finalMarketDefinition: missionReport.finalMarketDefinition,
    hypothesisHistory: missionReport.hypothesisHistory,
    evidenceGraphSummary: missionReport.evidenceGraphSummary,
    businessUnderstanding: missionReport.businessUnderstanding,
    businessJudgment: missionReport.businessJudgment,
    judgmentResult: missionReport.judgmentResult,
    remainingUnknowns: missionReport.remainingUnknowns,
    confidenceEvolution: missionReport.confidenceEvolution,
    suggestedNextInvestigation: missionReport.suggestedNextInvestigation,
    understandingFirst: true,
    judgmentFromHeuristics: missionReport.judgmentFromHeuristics === true,
    synthesizedNotRaw: missionReport.synthesizedNotRaw === true,
    recommendation: missionReport.recommendation,
  };
}

module.exports = {
  buildMissionIntelligenceReport,
  summarizeEvidenceGraph,
  buildRecommendationFromUnderstanding,
  buildSuggestedNextInvestigation,
  mergeIntoDiscoveryReport,
  buildReportSummary,
};
