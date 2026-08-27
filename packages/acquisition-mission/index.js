'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration.
 * SPEC-136 — Pending operator decision must match executable mission state.
 * Max manages missions. Capabilities contribute.
 */

const types = require('./types');
const { assertContract, contractFor, FORBIDDEN, PRODUCES } = require('./Contracts');
const { createMission, snapshotMission, normalizePriority } = require('./Mission');
const lifecycle = require('./Lifecycle');
const { createEvent, formatTimeline } = require('./Timeline');
const { buildSharedContext, formatSharedContext } = require('./Context');
const { buildWorkspace, formatWorkspace, bar } = require('./Workspace');
const { createBlocker, inferBlockers, currentBlocker } = require('./Blockers');
const { buildHealth, formatHealth } = require('./Health');
const { recordSegmentOutcome, summarizeLearning, formatLearning } = require('./Learning');
const { explainWhy, formatExplain, collectEvidence } = require('./Explain');
const { createObservation, formatMemory } = require('./Memory');
const { createMemoryAmoStore } = require('./Store');
const { createAcquisitionMissionEngine } = require('./Engine');
const inspection = require('./Inspection');
const {
  findLatestDiscoveryContribution,
  presentationFromDiscoveryPayload,
} = require('./DiscoveryPresentation');
const {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
} = require('./DiscoveryPayload');
const {
  deriveMissionTitle,
  inferTargetSegmentFromObjective,
} = require('./MissionNaming');
const structuredMission = require('./StructuredMission');
const missionPlanner = require('./MissionPlanner');
const specialistInputs = require('./SpecialistInputs');
const contextPrecedence = require('./ContextPrecedence');
const executionErrors = require('./ExecutionErrors');
const executionAudit = require('./ExecutionAudit');
const transactionalExecution = require('./TransactionalExecution');
const transactionalPersistence = require('./TransactionalPersistence');
const specialistExecutionContract = require('./SpecialistExecutionContract');
const pendingOperatorDecision = require('./PendingOperatorDecision');
const missionProgression = require('./MissionProgression');
const workspaceMode = require('./WorkspaceMode');
const operatorDecisionPolicy = require('./OperatorDecisionPolicy');
const outcomeLearning = require('./OutcomeLearning');
const canonicalMissionProjection = require('./CanonicalMissionProjection');
const missionRuntimeOwnership = require('./MissionRuntimeOwnership');
const missionExecutionContext = require('./MissionExecutionContext');
const executionRequest = require('./ExecutionRequest');
const executionRouter = require('./ExecutionRouter');
const executionApproval = require('./ExecutionApproval');
const outboundExecution = require('./OutboundExecution');
const communicationObservation = require('./CommunicationObservation');

module.exports = {
  ...types,
  assertContract,
  contractFor,
  FORBIDDEN,
  PRODUCES,
  createMission,
  snapshotMission,
  normalizePriority,
  ...lifecycle,
  createEvent,
  formatTimeline,
  buildSharedContext,
  formatSharedContext,
  buildWorkspace,
  formatWorkspace,
  bar,
  createBlocker,
  inferBlockers,
  currentBlocker,
  buildHealth,
  formatHealth,
  recordSegmentOutcome,
  summarizeLearning,
  formatLearning,
  explainWhy,
  formatExplain,
  collectEvidence,
  createObservation,
  formatMemory,
  createMemoryAmoStore,
  createAcquisitionMissionEngine,
  ...inspection,
  ...require('./DiscoveryPresentation'),
  ...structuredMission,
  ...missionPlanner,
  ...specialistInputs,
  ...contextPrecedence,
  ...executionErrors,
  ...executionAudit,
  ...transactionalExecution,
  ...transactionalPersistence,
  ...specialistExecutionContract,
  ...pendingOperatorDecision,
  ...missionProgression,
  ...workspaceMode,
  ...operatorDecisionPolicy,
  ...outcomeLearning,
  ...canonicalMissionProjection,
  ...missionRuntimeOwnership,
  ...missionExecutionContext,
  ...executionRequest,
  ...executionRouter,
  ...executionApproval,
  ...outboundExecution,
  ...communicationObservation,
};
