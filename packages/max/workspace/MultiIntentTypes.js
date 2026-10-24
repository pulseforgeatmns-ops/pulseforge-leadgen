'use strict';

/**
 * SPEC-151 — Multi-Intent Execution Planner (MIEP) types (ADR-072).
 */

const INTENT_TYPES = Object.freeze({
  SESSION_CONFIGURATION: 'session_configuration',
  SYSTEM_CONFIGURATION: 'system_configuration',
  MISSION_CREATION: 'mission_creation',
  MISSION_EXECUTION: 'mission_execution',
  BUSINESS_OPERATION: 'business_operation',
  BUSINESS_REASONING: 'business_reasoning',
  REFLECTION: 'reflection',
  INSPECTION: 'inspection',
});

const STEP_KINDS = Object.freeze({
  APPLY_SESSION_CONFIGURATION: 'apply_session_configuration',
  PERSIST_SESSION: 'persist_session',
  REFRESH_RUNTIME: 'refresh_runtime',
  SESSION_INSPECTION: 'session_inspection',
  MISSION_CREATION: 'mission_creation',
  MISSION_EXECUTION: 'mission_execution',
  BUSINESS_OPERATION: 'business_operation',
  BUSINESS_REASONING: 'business_reasoning',
  REFLECTION: 'reflection',
});

const STEP_OWNERS = Object.freeze({
  SESSION_STATE_MANAGER: 'session_state_manager',
  WORKSPACE_RUNTIME: 'workspace_runtime',
  MISSION_RUNTIME: 'mission_runtime',
  MAX_REASONING: 'max_reasoning',
});

/** Default precedence when segment order alone cannot resolve ordering. */
const INTENT_PRECEDENCE = Object.freeze({
  [INTENT_TYPES.SESSION_CONFIGURATION]: 1,
  [INTENT_TYPES.SYSTEM_CONFIGURATION]: 2,
  [INTENT_TYPES.MISSION_CREATION]: 3,
  [INTENT_TYPES.MISSION_EXECUTION]: 4,
  [INTENT_TYPES.BUSINESS_OPERATION]: 4,
  [INTENT_TYPES.BUSINESS_REASONING]: 5,
  [INTENT_TYPES.REFLECTION]: 6,
  [INTENT_TYPES.INSPECTION]: 7,
});

const INTENT_DEPENDENCIES = Object.freeze({
  [INTENT_TYPES.SESSION_CONFIGURATION]: {
    requires: [],
    produces: ['session_state'],
  },
  [INTENT_TYPES.SYSTEM_CONFIGURATION]: {
    requires: [],
    produces: ['system_config'],
  },
  [INTENT_TYPES.MISSION_CREATION]: {
    requires: ['session_state'],
    produces: ['mission'],
  },
  [INTENT_TYPES.MISSION_EXECUTION]: {
    requires: ['session_state'],
    produces: [],
  },
  [INTENT_TYPES.BUSINESS_OPERATION]: {
    requires: ['session_state'],
    produces: ['objective'],
  },
  [INTENT_TYPES.BUSINESS_REASONING]: {
    requires: ['session_state'],
    produces: [],
  },
  [INTENT_TYPES.REFLECTION]: {
    requires: ['session_state'],
    produces: [],
  },
  [INTENT_TYPES.INSPECTION]: {
    requires: [],
    produces: [],
  },
});

/**
 * @typedef {object} DetectedIntent
 * @property {string} type
 * @property {number} confidence
 * @property {string} owner
 * @property {string[]} requires
 * @property {string[]} produces
 * @property {boolean} blocking
 * @property {string} segment
 * @property {number} segmentIndex
 * @property {string} [sourceText]
 */

/**
 * @typedef {object} ExecutionPlanStep
 * @property {string} kind
 * @property {string} intentType
 * @property {string} owner
 * @property {string} segment
 * @property {number} order
 * @property {string[]} requires
 * @property {string[]} produces
 */

/**
 * @typedef {object} ExecutionPlan
 * @property {ExecutionPlanStep[]} steps
 * @property {DetectedIntent[]} intents
 * @property {boolean} compound
 */

/**
 * @typedef {object} ExecutionResult
 * @property {boolean} completed
 * @property {boolean} blocked
 * @property {string|null} pauseReason
 * @property {object|null} producedState
 * @property {object|null} response
 */

function mapIntentToOwner(type) {
  switch (type) {
    case INTENT_TYPES.SESSION_CONFIGURATION:
    case INTENT_TYPES.INSPECTION:
      return STEP_OWNERS.SESSION_STATE_MANAGER;
    case INTENT_TYPES.SYSTEM_CONFIGURATION:
      return STEP_OWNERS.WORKSPACE_RUNTIME;
    case INTENT_TYPES.MISSION_CREATION:
    case INTENT_TYPES.MISSION_EXECUTION:
    case INTENT_TYPES.BUSINESS_OPERATION:
      return STEP_OWNERS.MISSION_RUNTIME;
    case INTENT_TYPES.BUSINESS_REASONING:
    case INTENT_TYPES.REFLECTION:
      return STEP_OWNERS.MAX_REASONING;
    default:
      return STEP_OWNERS.WORKSPACE_RUNTIME;
  }
}

/**
 * @param {string} type
 * @param {object} [overrides]
 * @returns {DetectedIntent}
 */
function buildDetectedIntent(type, overrides = {}) {
  const deps = INTENT_DEPENDENCIES[type] || { requires: [], produces: [] };
  return {
    type,
    confidence: overrides.confidence != null ? overrides.confidence : 0.85,
    owner: overrides.owner || mapIntentToOwner(type),
    requires: overrides.requires || [...deps.requires],
    produces: overrides.produces || [...deps.produces],
    blocking: overrides.blocking === true,
    segment: overrides.segment || '',
    segmentIndex: overrides.segmentIndex != null ? overrides.segmentIndex : 0,
    sourceText: overrides.sourceText || overrides.segment || '',
  };
}

module.exports = {
  INTENT_TYPES,
  STEP_KINDS,
  STEP_OWNERS,
  INTENT_PRECEDENCE,
  INTENT_DEPENDENCIES,
  mapIntentToOwner,
  buildDetectedIntent,
};
