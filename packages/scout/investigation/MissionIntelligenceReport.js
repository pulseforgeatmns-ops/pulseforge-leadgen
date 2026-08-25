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
const {
  buildInvestigativeStrategy,
  buildInvestigativeStrategyReport,
} = require('./InvestigativeStrategyEngine');
const {
  buildOpportunityIntelligenceReport,
  buildRecommendationFromOpportunity,
} = require('../opportunity/OpportunityIntelligenceEngine');
const { buildStrategicDecision } = require('../../max/decision');
const { buildOutcomeReviewSection } = require('../../acquisition-mission/OutcomeLearning');

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

function buildSuggestedNextInvestigation(state = {}, investigativeStrategy = null) {
  const strategySection = investigativeStrategy || state.investigativeStrategy;
  if (strategySection?.selectedInvestigation) {
    const sel = strategySection.selectedInvestigation;
    return {
      action: 'investigate',
      question: sel.objective,
      gap: sel.gap,
      priority: 'high',
      source: 'investigative_strategy',
      recommendedSource: sel.sourceLabel,
      recommendedSourceId: sel.source,
      expectedInformationGain: sel.expectedInformationGain,
      hypothesisId: null,
      rationale: sel.reasoning,
      documentedGain: true,
      adr: 'ADR-083',
    };
  }

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
    expectedInformationGain: top.expectedInformationGain ?? null,
    recommendedSource: top.recommendedSource ?? null,
    rationale: top.rationale || 'Highest-priority unanswered question from investigation state.',
    documentedGain: top.documentedGain === true,
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
  const investigativeStrategy =
    input.investigativeStrategy ||
    state.investigativeStrategy ||
    buildInvestigativeStrategy({
      state,
      judgmentResult,
      memory: input.memory,
      opts: input.opts,
    });
  const investigativeStrategySection = buildInvestigativeStrategyReport(
    investigativeStrategy,
    input.stop
  );
  const suggestedNextInvestigation = buildSuggestedNextInvestigation(state, investigativeStrategy);

  const opportunityIntelligence = buildOpportunityIntelligenceReport({
    mission: input.mission,
    businessUnderstandings: state.businessUnderstandings || [],
    judgmentResult,
    candidates: input.candidates || [],
    acquisitionOpportunities: input.acquisitionOpportunities || [],
  });

  const heuristicRecommendation = buildRecommendationFromUnderstanding(state, synthesisResult, judgmentResult);
  const opportunityRecommendation = opportunityIntelligence.topOpportunity
    ? buildRecommendationFromOpportunity(opportunityIntelligence.topOpportunity, heuristicRecommendation)
    : null;

  const remainingUnknowns = investigativeStrategySection.remainingUnknowns.length
    ? investigativeStrategySection.remainingUnknowns.map((u) => u.label || u.gap)
    : [
        ...(state.uncertainty?.open || []),
        ...(state.uncertainty?.persistent || []),
      ];

  const strategicDecision = buildStrategicDecision({
    mission: input.mission,
    opportunities: opportunityIntelligence.opportunities,
    opportunityIntelligence,
    constraints: input.constraints,
    competingWork: input.competingWork || input.mission?.competingWork,
    pendingProposals: input.pendingProposals,
    scoutDiscoveries: input.scoutDiscoveries,
    remainingUnknowns,
  });

  const recommendation = opportunityRecommendation
    ? { ...opportunityRecommendation, ...strategicDecision.recommendationOverlay }
    : heuristicRecommendation;

  const outcomeReview = buildOutcomeReviewSection({
    predictions: input.priorPredictions || [],
    evaluations: input.priorEvaluations || [],
    outcomeLearnings: input.priorOutcomeLearnings || input.priorLearnings || [],
    allowPending: true,
  });

  return {
    kind: 'mission_intelligence_report',
    spec: 'SPEC-159',
    synthesisSpec: 'SPEC-160',
    heuristicsSpec: 'SPEC-162',
    strategySpec: 'SPEC-163',
    opportunitySpec: 'SPEC-164',
    decisionSpec: 'SPEC-165',
    outcomeLearningSpec: 'SPEC-166',
    adr: 'ADR-079',
    synthesisAdr: 'ADR-080',
    heuristicsAdr: 'ADR-082',
    strategyAdr: 'ADR-083',
    opportunityAdr: 'ADR-084',
    decisionAdr: 'ADR-085',
    outcomeLearningAdr: 'ADR-086',
    opportunityIntelligence,
    strategicDecision,
    topOpportunities: opportunityIntelligence.topOpportunities,
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
    investigativeStrategy: investigativeStrategySection,
    remainingUnknowns,
    confidenceEvolution: state.confidenceEvolution || [],
    currentConfidence: state.confidence,
    recommendation,
    outcomeReview,
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
    strategyDrivenInvestigation: investigativeStrategySection.everyInvestigationDocumented === true,
    opportunityRankedNotScored: opportunityIntelligence.notScoreBased === true,
    basedOnStrategicDecision: true,
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

/** Keys that must never cross the specialist boundary (SPEC-173). */
const INTERNAL_REASONING_FIELD_NAMES = Object.freeze([
  'activeHypotheses',
  'searchHypotheses',
  'hypothesisHistory',
  'investigationGraph',
  'explorationTree',
  'candidateBranches',
  'coveragePlanner',
  'investigationState',
  'investigativeStrategy',
  'confidenceEvolution',
  'investigationCycles',
  'priorUnderstanding',
]);

/** Contract-forbidden keys enforced by assertContract for Scout. */
const SCOUT_CONTRACT_FORBIDDEN_KEYS = Object.freeze(['hypothesis', 'hypotheses']);

function walkObjectKeys(value, acc = []) {
  if (!value || typeof value !== 'object') return acc;
  if (Array.isArray(value)) {
    for (const item of value) walkObjectKeys(item, acc);
    return acc;
  }
  for (const [key, child] of Object.entries(value)) {
    acc.push(key);
    walkObjectKeys(child, acc);
  }
  return acc;
}

function containsForbiddenReasoningKeys(value, extraForbidden = []) {
  const forbidden = new Set([...SCOUT_CONTRACT_FORBIDDEN_KEYS, ...extraForbidden]);
  return walkObjectKeys(value).some((key) => forbidden.has(key));
}

function projectJudgmentResultForBoundary(judgmentResult = null) {
  if (!judgmentResult || typeof judgmentResult !== 'object') return null;
  return {
    activatedHeuristics: (judgmentResult.activatedHeuristics || []).map((item) => ({
      id: item.id,
      heuristicId: item.heuristicId,
      name: item.name,
      category: item.category,
      score: item.score,
      confidence: item.confidence,
    })),
    overallJudgment: judgmentResult.overallJudgment || null,
    basedOnHeuristics: judgmentResult.basedOnHeuristics === true,
  };
}

function projectStrategicDecisionForBoundary(strategicDecision = null) {
  if (!strategicDecision || typeof strategicDecision !== 'object') return null;
  return {
    recommendationOverlay: strategicDecision.recommendationOverlay || null,
    expectedBusinessOutcome: strategicDecision.expectedBusinessOutcome || null,
    tradeoff: strategicDecision.tradeoff || null,
    rationale: strategicDecision.rationale || null,
  };
}

function projectOpportunityIntelligenceForBoundary(opportunityIntelligence = null) {
  if (!opportunityIntelligence || typeof opportunityIntelligence !== 'object') return null;
  return {
    topOpportunity: opportunityIntelligence.topOpportunity || null,
    topOpportunities: opportunityIntelligence.topOpportunities || [],
    opportunities: Array.isArray(opportunityIntelligence.opportunities)
      ? opportunityIntelligence.opportunities.slice(0, 10)
      : [],
    notScoreBased: opportunityIntelligence.notScoreBased === true,
  };
}

/**
 * SPEC-173 — Project internal Mission Intelligence Report to public executive-facing MIR.
 * Explicit boundary projection: never serializes runtime investigation state.
 * @param {object} internalMir
 * @returns {object|null}
 */
function buildPublicMissionIntelligenceReport(internalMir = {}) {
  if (!internalMir || typeof internalMir !== 'object') return null;

  return {
    kind: 'mission_intelligence_report',
    spec: 'SPEC-173',
    projectionOf: internalMir.spec || 'SPEC-159',
    adr: internalMir.adr || 'ADR-079',
    summary: internalMir.summary || null,
    finalMarketDefinition: internalMir.finalMarketDefinition || null,
    universeEstimate: internalMir.universeEstimate || null,
    evidenceGraphSummary: internalMir.evidenceGraphSummary || null,
    businessUnderstanding: internalMir.businessUnderstanding || null,
    businessJudgment: internalMir.businessJudgment || null,
    judgmentResult: projectJudgmentResultForBoundary(internalMir.judgmentResult),
    remainingUnknowns: Array.isArray(internalMir.remainingUnknowns)
      ? internalMir.remainingUnknowns.slice()
      : [],
    currentConfidence: internalMir.currentConfidence ?? null,
    recommendation: internalMir.recommendation || null,
    strategicDecision: projectStrategicDecisionForBoundary(internalMir.strategicDecision),
    opportunityIntelligence: projectOpportunityIntelligenceForBoundary(internalMir.opportunityIntelligence),
    topOpportunities: internalMir.topOpportunities || [],
    suggestedNextInvestigation: internalMir.suggestedNextInvestigation || null,
    outcomeReview: internalMir.outcomeReview || null,
    coverage: internalMir.coverage || null,
    understandingFirst: internalMir.understandingFirst === true,
    judgmentFromHeuristics: internalMir.judgmentFromHeuristics === true,
    synthesizedNotRaw: internalMir.synthesizedNotRaw === true,
    basedOnStrategicDecision: internalMir.basedOnStrategicDecision === true,
    boundaryProjected: true,
  };
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
    investigativeStrategy: missionReport.investigativeStrategy,
    remainingUnknowns: missionReport.remainingUnknowns,
    confidenceEvolution: missionReport.confidenceEvolution,
    suggestedNextInvestigation: missionReport.suggestedNextInvestigation,
    opportunityIntelligence: missionReport.opportunityIntelligence,
    strategicDecision: missionReport.strategicDecision,
    topOpportunities: missionReport.topOpportunities,
    understandingFirst: true,
    judgmentFromHeuristics: missionReport.judgmentFromHeuristics === true,
    synthesizedNotRaw: missionReport.synthesizedNotRaw === true,
    basedOnStrategicDecision: missionReport.basedOnStrategicDecision === true,
    recommendation: missionReport.recommendation,
    outcomeReview: missionReport.outcomeReview,
  };
}

module.exports = {
  INTERNAL_REASONING_FIELD_NAMES,
  SCOUT_CONTRACT_FORBIDDEN_KEYS,
  buildMissionIntelligenceReport,
  buildPublicMissionIntelligenceReport,
  summarizeEvidenceGraph,
  buildRecommendationFromUnderstanding,
  buildSuggestedNextInvestigation,
  mergeIntoDiscoveryReport,
  buildReportSummary,
  containsForbiddenReasoningKeys,
  walkObjectKeys,
};
