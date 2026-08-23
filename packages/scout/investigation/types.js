'use strict';

/**
 * SPEC-142 — Evidence-Driven Investigation Engine types.
 * Scout investigates uncertainty; providers answer questions.
 */

const INVESTIGATION_PHASES = Object.freeze({
  MISSION: 'mission',
  UNDERSTANDING: 'current_understanding',
  HYPOTHESES: 'generate_hypotheses',
  MISSING_EVIDENCE: 'determine_missing_evidence',
  SELECT_INVESTIGATION: 'select_lowest_cost_investigation',
  COLLECT_EVIDENCE: 'collect_evidence',
  FUSE_EVIDENCE: 'fuse_evidence',
  UPDATE_CONFIDENCE: 'update_confidence',
  COMPLETE: 'enough_confidence',
});

const GRAPH_NODE_TYPES = Object.freeze({
  MISSION: 'mission',
  MARKET: 'market',
  CANDIDATE: 'candidate',
  DECISION_MAKER: 'decision_maker',
  EVIDENCE: 'evidence',
  CLAIM: 'claim',
  CONFIDENCE: 'confidence',
  SOURCE: 'source',
  HYPOTHESIS: 'hypothesis',
});

const HYPOTHESIS_STATUS = Object.freeze({
  OPEN: 'open',
  CONFIRMED: 'confirmed',
  REJECTED: 'rejected',
  INCONCLUSIVE: 'inconclusive',
});

const COMPLETION_REASONS = Object.freeze({
  CONFIDENCE_THRESHOLD: 'confidence_threshold_reached',
  COVERAGE_COMPLETE: 'coverage_complete',
  NO_HIGHER_VALUE_EVIDENCE: 'no_higher_value_evidence',
  COST_EXCEEDS_BENEFIT: 'cost_exceeds_benefit',
  DIMINISHING_RETURNS: 'diminishing_returns',
  PERSISTENT_UNKNOWNS: 'persistent_unknowns_only',
  BLOCKED: 'blocked',
});

const EVIDENCE_GAPS = Object.freeze({
  DECISION_MAKER: 'decision_maker',
  PORTFOLIO_SIZE: 'portfolio_size',
  CLEANING_RESPONSIBILITY: 'cleaning_responsibility',
  CONTACT_PATH: 'contact_path',
  BUYING_SIGNALS: 'buying_signals',
  BUSINESS_FIT: 'business_fit',
  GEOGRAPHIC_FIT: 'geographic_fit',
  VENDOR_RELATIONSHIP: 'vendor_relationship',
  COMPANY_SIZE: 'company_size',
  OWNERSHIP: 'ownership',
});

const INVESTIGATION_EVENTS = Object.freeze({
  STARTED: 'SCOUT_INVESTIGATION_STARTED',
  ITERATION: 'SCOUT_INVESTIGATION_ITERATION',
  STEP: 'SCOUT_INVESTIGATION_STEP',
  CONFLICT: 'SCOUT_INVESTIGATION_CONFLICT',
  COMPLETED: 'SCOUT_INVESTIGATION_COMPLETED',
});

const DEFAULT_CONFIDENCE_THRESHOLD = 0.8;
const DEFAULT_MAX_ITERATIONS = 12;
const DEFAULT_MAX_COST_BUDGET = 100;

function buildHypothesis(partial = {}) {
  return {
    id: partial.id || `hyp-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    text: partial.text || '',
    entityId: partial.entityId || null,
    confidence: partial.confidence != null ? partial.confidence : null,
    minConfidence: partial.minConfidence != null ? partial.minConfidence : DEFAULT_CONFIDENCE_THRESHOLD,
    requiredEvidence: Array.isArray(partial.requiredEvidence) ? partial.requiredEvidence : [],
    collectedEvidence: Array.isArray(partial.collectedEvidence) ? partial.collectedEvidence : [],
    missingEvidence: Array.isArray(partial.missingEvidence) ? partial.missingEvidence : [],
    status: partial.status || HYPOTHESIS_STATUS.OPEN,
    claimIds: Array.isArray(partial.claimIds) ? partial.claimIds : [],
  };
}

function buildClaim(partial = {}) {
  return {
    id: partial.id || `claim-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    text: partial.text || '',
    entityId: partial.entityId || null,
    hypothesisId: partial.hypothesisId || null,
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    supportedBy: Array.isArray(partial.supportedBy) ? partial.supportedBy : [],
    missingEvidence: Array.isArray(partial.missingEvidence) ? partial.missingEvidence : [],
    contradictions: Array.isArray(partial.contradictions) ? partial.contradictions : [],
    resolved: partial.resolved === true,
  };
}

function buildInvestigationStep(partial = {}) {
  return {
    id: partial.id || `step-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    gap: partial.gap || null,
    capability: partial.capability || null,
    providerId: partial.providerId || null,
    providerLabel: partial.providerLabel || null,
    costTier: partial.costTier || null,
    costScore: partial.costScore != null ? partial.costScore : 1,
    entityId: partial.entityId || null,
    rationale: partial.rationale || '',
    question: partial.question || null,
    gapImpact: partial.gapImpact != null ? partial.gapImpact : null,
    gapDifficulty: partial.gapDifficulty != null ? partial.gapDifficulty : null,
    expectedInformationGain: partial.expectedInformationGain != null ? partial.expectedInformationGain : null,
    skipped: partial.skipped === true,
    skipReason: partial.skipReason || null,
    belowGainThreshold: partial.belowGainThreshold === true,
    stopRecommendation: partial.stopRecommendation || null,
  };
}

function buildInvestigationResult(partial = {}) {
  return {
    outcome: partial.outcome || 'completed',
    completionReason: partial.completionReason || null,
    stopExplanation: partial.stopExplanation || null,
    iterations: Array.isArray(partial.iterations) ? partial.iterations : [],
    graph: partial.graph || null,
    hypotheses: Array.isArray(partial.hypotheses) ? partial.hypotheses : [],
    claims: Array.isArray(partial.claims) ? partial.claims : [],
    missingEvidence: partial.missingEvidence || null,
    report: partial.report || null,
    marketDefinition: partial.marketDefinition || null,
    candidateUniverse: partial.candidateUniverse || null,
    overallConfidence: partial.overallConfidence != null ? partial.overallConfidence : 0,
    totalCost: partial.totalCost != null ? partial.totalCost : 0,
    qualification: partial.qualification || null,
    ranking: partial.ranking || null,
    evidenceCollection: partial.evidenceCollection || null,
    providerStrategy: partial.providerStrategy || null,
    evidencePlan: partial.evidencePlan || null,
    investigationBoard: partial.investigationBoard || null,
    investigationJournal: partial.investigationJournal || null,
    providerLearning: partial.providerLearning || null,
    stepSelection: partial.stepSelection || null,
  };
}

module.exports = {
  INVESTIGATION_PHASES,
  GRAPH_NODE_TYPES,
  HYPOTHESIS_STATUS,
  COMPLETION_REASONS,
  EVIDENCE_GAPS,
  INVESTIGATION_EVENTS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  DEFAULT_MAX_ITERATIONS,
  DEFAULT_MAX_COST_BUDGET,
  buildHypothesis,
  buildClaim,
  buildInvestigationStep,
  buildInvestigationResult,
};
