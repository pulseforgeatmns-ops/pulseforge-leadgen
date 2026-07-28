'use strict';

/**
 * @pulseforge/mission-engine — Mission Engine (SPEC-022 / ADR-010)
 *
 * Intent → Plan → Capabilities → Review
 * Operators never invoke Scout or other agents directly.
 */

const {
  MISSION_STATUS,
  MISSION_TYPES,
  AUDIT_KINDS,
  REVIEW_ACTIONS,
  ROUTE_KINDS,
  STAGE_LABELS,
  STAGE_OUTCOMES,
  MESSAGE_CLASS,
  RESOLUTION_PATHS,
  MISSION_EVENTS,
  ARTIFACT_BUS_EVENTS,
  TERMINAL_STATUSES,
  isTerminalStatus,
  isActiveMissionStatus,
  newId,
  missionEnabled,
  activeMissionResolverEnabled,
  artifactValidationEnabled,
  artifactBusEnabled,
} = require('./types');
const {
  routeIntent,
  matchMissionType,
  isIntelligenceOnly,
} = require('./IntentRouter');
const {
  MissionPlanner,
  createMissionPlanner,
  TYPE_CAPABILITY_CHAINS,
  deriveTitle,
  PLANNER_VERSION,
  createExecutionGraph,
  replanGraph,
  validateGraph,
  explainPlan,
  insertStage,
  removeStage,
  replaceStage,
} = require('./MissionPlanner');
const {
  STAGE_LIBRARY,
  TYPE_SEED_STAGES,
  getStage,
  listStages,
  seedStagesForType,
  matchOutcomeStages,
} = require('./StageLibrary');
const ExecutionGraph = require('./ExecutionGraph');
const {
  MissionExecutor,
  createMissionExecutor,
} = require('./MissionExecutor');
const {
  InMemoryMissionStore,
  createInMemoryMissionStore,
} = require('./MissionStore');
const {
  PostgresMissionStore,
  createPostgresMissionStore,
  ensureMissionSchema,
} = require('./PostgresMissionStore');
const {
  MissionEngine,
  createMissionEngine,
  isDiscoveryBlocked,
  discoveryRecoveryActions,
} = require('./MissionEngine');
const {
  ActiveMissionResolver,
  createActiveMissionResolver,
} = require('./ActiveMissionResolver');
const {
  InMemoryActiveMissionBindingStore,
  createInMemoryActiveMissionBindingStore,
} = require('./ActiveMissionBindingStore');
const {
  classifyMessage,
  looksLikeNewObjective,
} = require('./classifyMessage');
const {
  evaluatePipelineGate,
  getStageContract,
  STAGE_CONTRACTS,
  STAGE_OUTCOME_LABELS,
  ARTIFACT_VALIDATION_STATUS,
} = require('./PipelineGate');
const {
  ArtifactBus,
  createArtifactBus,
  ARTIFACT_EVENTS,
} = require('./ArtifactBus');
const ArtifactRegistry = require('./ArtifactRegistry');
const OperatorArtifactInjection = require('./OperatorArtifactInjection');
const ArtifactResolver = require('./ArtifactResolver');
const ArtifactValidator = require('./ArtifactValidator');
const CompatibilityResolver = require('./CompatibilityResolver');
const PlanningDiagnostics = require('./PlanningDiagnostics');
const {
  parseIntent,
  classifyUnit,
  PLAN_CATEGORIES,
} = require('./IntentParser');
const {
  understandIntent,
} = require('./IntentUnderstanding');
const {
  MISSION_INTENT_VERSION,
  INTENT_CATEGORIES,
  INTENT_LABELS,
  INTENT_DOMAINS,
  INTENT_MODES,
  INTENT_CONFIDENCE_THRESHOLD,
  buildMissionIntent,
  summarizeMissionIntent,
  intentLabel,
} = require('./MissionIntent');
const {
  INTENT_EXECUTION_MAP,
  planFromIntent,
  planFromOperatorText,
  resolveMissionTypeFromIntent,
} = require('./CapabilityPlanner');
const {
  EVIDENCE_PLAN_VERSION,
  EVIDENCE_TYPES,
  buildEvidencePlan,
  summarizeEvidencePlan,
  isDiagnosticEvidenceType,
} = require('./EvidencePlan');
const {
  INTENT_EVIDENCE_REQUIREMENTS,
  planEvidence,
  acquisitionStages,
  requiredEvidenceForIntent,
} = require('./EvidencePlanner');
const {
  MISSION_PLAN_VERSION,
  RESERVED_RUNTIME_FIELDS,
  buildMissionPlan,
  validateMissionPlan,
  summarizeMissionPlan,
  executableObjectiveText,
  containsOperatorInstructionLeak,
  resolveExecutionRequest,
} = require('./MissionPlan');

module.exports = {
  MISSION_STATUS,
  MISSION_TYPES,
  AUDIT_KINDS,
  REVIEW_ACTIONS,
  ROUTE_KINDS,
  STAGE_LABELS,
  STAGE_OUTCOMES,
  STAGE_OUTCOME_LABELS,
  ARTIFACT_VALIDATION_STATUS,
  MESSAGE_CLASS,
  RESOLUTION_PATHS,
  MISSION_EVENTS,
  ARTIFACT_BUS_EVENTS,
  ARTIFACT_EVENTS,
  TERMINAL_STATUSES,
  isTerminalStatus,
  isActiveMissionStatus,
  newId,
  missionEnabled,
  activeMissionResolverEnabled,
  artifactValidationEnabled,
  artifactBusEnabled,
  evaluatePipelineGate,
  getStageContract,
  STAGE_CONTRACTS,
  routeIntent,
  matchMissionType,
  isIntelligenceOnly,
  classifyMessage,
  looksLikeNewObjective,
  MissionPlanner,
  createMissionPlanner,
  TYPE_CAPABILITY_CHAINS,
  deriveTitle,
  PLANNER_VERSION,
  createExecutionGraph,
  replanGraph,
  validateGraph,
  explainPlan,
  insertStage,
  removeStage,
  replaceStage,
  STAGE_LIBRARY,
  TYPE_SEED_STAGES,
  getStage,
  listStages,
  seedStagesForType,
  matchOutcomeStages,
  ExecutionGraph,
  MissionExecutor,
  createMissionExecutor,
  InMemoryMissionStore,
  createInMemoryMissionStore,
  PostgresMissionStore,
  createPostgresMissionStore,
  ensureMissionSchema,
  MissionEngine,
  createMissionEngine,
  isDiscoveryBlocked,
  discoveryRecoveryActions,
  ActiveMissionResolver,
  createActiveMissionResolver,
  InMemoryActiveMissionBindingStore,
  createInMemoryActiveMissionBindingStore,
  ArtifactBus,
  createArtifactBus,
  ArtifactRegistry,
  OperatorArtifactInjection,
  ArtifactResolver,
  ArtifactValidator,
  CompatibilityResolver,
  PlanningDiagnostics,
  validateArtifactCandidate: ArtifactValidator.validateArtifactCandidate,
  looksLikeNaturalLanguage: ArtifactValidator.looksLikeNaturalLanguage,
  isViableCompanyName: ArtifactValidator.isViableCompanyName,
  resolveArtifacts: ArtifactResolver.resolveArtifacts,
  deriveRequiredArtifacts: ArtifactResolver.deriveRequiredArtifacts,
  acquisitionOptions: ArtifactResolver.acquisitionOptions,
  ARTIFACT_SOURCES: ArtifactResolver.ARTIFACT_SOURCES,
  resolveCompatibleProducer: CompatibilityResolver.resolveCompatibleProducer,
  resolveCompatibleProducers: CompatibilityResolver.resolveCompatibleProducers,
  buildPlanningDiagnostics: PlanningDiagnostics.buildPlanningDiagnostics,
  buildMissingProducerDiagnostic:
    PlanningDiagnostics.buildMissingProducerDiagnostic,
  buildUnknownMissionDiagnostic:
    PlanningDiagnostics.buildUnknownMissionDiagnostic,
  formatDiagnosticMessage: PlanningDiagnostics.formatDiagnosticMessage,
  detectOperatorProspectListInMessage:
    OperatorArtifactInjection.detectOperatorProspectListInMessage,
  // SPEC-050 / ADR-034
  parseIntent,
  classifyUnit,
  PLAN_CATEGORIES,
  MISSION_PLAN_VERSION,
  RESERVED_RUNTIME_FIELDS,
  buildMissionPlan,
  validateMissionPlan,
  summarizeMissionPlan,
  executableObjectiveText,
  containsOperatorInstructionLeak,
  resolveExecutionRequest,
  // SPEC-055 / ADR-039
  understandIntent,
  MISSION_INTENT_VERSION,
  INTENT_CATEGORIES,
  INTENT_LABELS,
  INTENT_DOMAINS,
  INTENT_MODES,
  INTENT_CONFIDENCE_THRESHOLD,
  buildMissionIntent,
  summarizeMissionIntent,
  intentLabel,
  INTENT_EXECUTION_MAP,
  planFromIntent,
  planFromOperatorText,
  resolveMissionTypeFromIntent,
  // SPEC-056 / ADR-040
  EVIDENCE_PLAN_VERSION,
  EVIDENCE_TYPES,
  buildEvidencePlan,
  summarizeEvidencePlan,
  isDiagnosticEvidenceType,
  INTENT_EVIDENCE_REQUIREMENTS,
  planEvidence,
  acquisitionStages,
  requiredEvidenceForIntent,
};
