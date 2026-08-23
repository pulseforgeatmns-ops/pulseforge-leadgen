'use strict';

/**
 * SPEC-142 — Evidence-Driven Investigation Engine public exports.
 * SPEC-145 — Adaptive investigation planning exports.
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
const {
  selectNextInvestigation,
  planInvestigationChain,
  explainStepSelection,
  providersForGap,
  DEFAULT_MIN_EXPECTED_GAIN,
} = require('./InvestigationPlanner');
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

const {
  createInvestigationBoard,
  summarizeBoard,
  getTopPriorityUnknown,
  updateBoardAfterStep,
  computeCoverage,
  computeExpectedValue,
  GAP_VALUE_PROFILE,
  DEFAULT_COVERAGE_THRESHOLD,
} = require('./InvestigationBoard');

const {
  createInvestigationJournal,
  recordJournalStep,
  serializeJournal,
  renderJournalTrail,
} = require('./InvestigationJournal');

const {
  createProviderLearningStore,
  estimateInformationGain,
  DEFAULT_PROVIDER_EFFECTIVENESS,
} = require('./ProviderLearning');

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
  DEFAULT_MIN_EXPECTED_GAIN,
  DEFAULT_COVERAGE_THRESHOLD,
  GAP_VALUE_PROFILE,
  DEFAULT_PROVIDER_EFFECTIVENESS,
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
  explainStepSelection,
  providersForGap,
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
  createInvestigationBoard,
  summarizeBoard,
  getTopPriorityUnknown,
  updateBoardAfterStep,
  computeCoverage,
  computeExpectedValue,
  createInvestigationJournal,
  recordJournalStep,
  serializeJournal,
  renderJournalTrail,
  createProviderLearningStore,
  estimateInformationGain,
};
