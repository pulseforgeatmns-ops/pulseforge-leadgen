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

const {
  buildInvestigationPlan,
  buildProviderPlan,
  buildInvestigationStatus,
  createInvestigationPlan,
  createInvestigationPlanWithLearning,
  reviseInvestigationPlan,
  updatePlanAfterStep,
  skipRemainingProviders,
  isStepInPlan,
  buildInvestigationStatusFromPlan,
  findReplacementProviders,
  buildStoppingConditions,
  PROVIDER_STATUS,
} = require('./InvestigationPlanBuilder');

const {
  buildInvestigationState,
  createInvestigationState,
  serializeInvestigationState,
  applyInvestigationPlan,
  updateEvidenceTracking,
} = require('./InvestigationState');

const {
  HYPOTHESIS_LIFECYCLE,
  buildHypothesisLifecycleRecord,
  summarizeHypothesisHistory,
} = require('./HypothesisLifecycle');

const {
  runInvestigativeReasoningLoop,
  runReasoningCycle,
  shouldStopInvestigation,
} = require('./InvestigativeReasoningLoop');

const {
  buildMissionIntelligenceReport,
  buildPublicMissionIntelligenceReport,
  mergeIntoDiscoveryReport,
  containsForbiddenReasoningKeys,
} = require('./MissionIntelligenceReport');

const {
  buildInvestigativeStrategy,
  recalculateStrategyAfterResolution,
  applyInvestigativeStrategy,
  explainInvestigationChoice,
  buildInvestigativeStrategyReport,
  evaluateStrategyStoppingConditions,
} = require('./InvestigativeStrategyEngine');

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
  buildInvestigationPlan,
  buildProviderPlan,
  buildInvestigationStatus,
  createInvestigationPlan,
  createInvestigationPlanWithLearning,
  reviseInvestigationPlan,
  updatePlanAfterStep,
  skipRemainingProviders,
  isStepInPlan,
  buildInvestigationStatusFromPlan,
  findReplacementProviders,
  buildStoppingConditions,
  PROVIDER_STATUS,
  ...require('./SearchHypothesisEngine'),
  ...require('./InvestigationTree'),
  buildInvestigationState,
  createInvestigationState,
  serializeInvestigationState,
  applyInvestigationPlan,
  updateEvidenceTracking,
  HYPOTHESIS_LIFECYCLE,
  buildHypothesisLifecycleRecord,
  summarizeHypothesisHistory,
  runInvestigativeReasoningLoop,
  runReasoningCycle,
  shouldStopInvestigation,
  buildMissionIntelligenceReport,
  buildPublicMissionIntelligenceReport,
  mergeIntoDiscoveryReport,
  containsForbiddenReasoningKeys,
  buildInvestigativeStrategy,
  recalculateStrategyAfterResolution,
  applyInvestigativeStrategy,
  explainInvestigationChoice,
  buildInvestigativeStrategyReport,
  evaluateStrategyStoppingConditions,
};
