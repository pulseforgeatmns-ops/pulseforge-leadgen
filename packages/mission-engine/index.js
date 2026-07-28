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
const {
  parseIntent,
  classifyUnit,
  PLAN_CATEGORIES,
} = require('./IntentParser');
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
};
