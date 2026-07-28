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
  TERMINAL_STATUSES,
  isTerminalStatus,
  isActiveMissionStatus,
  newId,
  missionEnabled,
  activeMissionResolverEnabled,
  artifactValidationEnabled,
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
} = require('./MissionPlanner');
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
  TERMINAL_STATUSES,
  isTerminalStatus,
  isActiveMissionStatus,
  newId,
  missionEnabled,
  activeMissionResolverEnabled,
  artifactValidationEnabled,
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
  MissionExecutor,
  createMissionExecutor,
  InMemoryMissionStore,
  createInMemoryMissionStore,
  PostgresMissionStore,
  createPostgresMissionStore,
  ensureMissionSchema,
  MissionEngine,
  createMissionEngine,
  ActiveMissionResolver,
  createActiveMissionResolver,
  InMemoryActiveMissionBindingStore,
  createInMemoryActiveMissionBindingStore,
};
