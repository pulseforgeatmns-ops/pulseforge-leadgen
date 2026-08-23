'use strict';

/**
 * SPEC-142 — Evidence-Driven Investigation Engine public exports.
 */

const {
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
} = require('./types');

const {
  createInvestigationGraph,
  serializeGraph,
  getNodesByType,
  getClaimsForCandidate,
  addClaimNode,
  addEvidenceNode,
  addHypothesisNode,
} = require('./InvestigationGraph');

const { generateHypotheses, generateCandidateHypotheses } = require('./HypothesisGeneration');
const { determineMissingEvidence, evidenceSatisfiesGap } = require('./MissingEvidence');
const { selectNextInvestigation, planInvestigationChain } = require('./InvestigationPlanner');
const { computeClaimConfidence, fuseAndUpdateClaims } = require('./ClaimConfidence');
const { detectContradictions } = require('./ContradictionDetection');
const { executeInvestigationStep } = require('./EvidenceExecutor');
const { runInvestigationEngine } = require('./InvestigationLoop');
const { buildInvestigationReport, buildSixQuestions, buildIntelligenceBriefs, validateBriefAcceptance } = require('./InvestigationReport');
const {
  emitInvestigationStarted,
  emitInvestigationIteration,
  emitInvestigationStep,
  emitInvestigationConflict,
  emitInvestigationCompleted,
  listInvestigationLog,
  clearInvestigationLog,
} = require('./observability');

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
  createInvestigationGraph,
  serializeGraph,
  getNodesByType,
  getClaimsForCandidate,
  addClaimNode,
  addEvidenceNode,
  addHypothesisNode,
  generateHypotheses,
  generateCandidateHypotheses,
  determineMissingEvidence,
  evidenceSatisfiesGap,
  selectNextInvestigation,
  planInvestigationChain,
  computeClaimConfidence,
  fuseAndUpdateClaims,
  detectContradictions,
  executeInvestigationStep,
  runInvestigationEngine,
  buildInvestigationReport,
  buildSixQuestions,
  buildIntelligenceBriefs,
  validateBriefAcceptance,
  emitInvestigationStarted,
  emitInvestigationIteration,
  emitInvestigationStep,
  emitInvestigationConflict,
  emitInvestigationCompleted,
  listInvestigationLog,
  clearInvestigationLog,
};
