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
  newId,
  missionEnabled,
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

module.exports = {
  MISSION_STATUS,
  MISSION_TYPES,
  AUDIT_KINDS,
  REVIEW_ACTIONS,
  ROUTE_KINDS,
  STAGE_LABELS,
  newId,
  missionEnabled,
  routeIntent,
  matchMissionType,
  isIntelligenceOnly,
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
};
