'use strict';

/**
 * SPEC-167 — Execution Contract (ADR-087).
 * Downstream runtime consumes this object instead of loose message flags.
 */

const { EXECUTION_POLICIES, REASONING_MODES, CONVERSATION_STYLES } = require('./SessionState');
const {
  EXECUTION_MODIFIERS,
  CONVERSATION_MODIFIERS,
} = require('./PrimaryObjective');

/**
 * @typedef {object} ExecutionPolicy
 * @property {string|null} executionPolicy — SessionState EXECUTION_POLICIES value
 * @property {string[]} modifiers — EXECUTION_MODIFIERS values
 */

/**
 * @typedef {object} ReasoningPolicy
 * @property {string|null} reasoningMode — SessionState REASONING_MODES value
 */

/**
 * @typedef {object} ConversationPolicy
 * @property {string|null} conversationStyle — SessionState CONVERSATION_STYLES value
 * @property {string[]} modifiers — CONVERSATION_MODIFIERS values
 */

function buildExecutionPolicy(executionModifiers = []) {
  const modifiers = Array.isArray(executionModifiers) ? executionModifiers : [];
  let executionPolicy = null;

  if (modifiers.includes(EXECUTION_MODIFIERS.AUTONOMOUS)) {
    executionPolicy = EXECUTION_POLICIES.AUTONOMOUS;
  } else if (modifiers.includes(EXECUTION_MODIFIERS.READ_ONLY)) {
    executionPolicy = EXECUTION_POLICIES.READ_ONLY;
  } else if (modifiers.includes(EXECUTION_MODIFIERS.PAUSE_ON_APPROVAL)) {
    executionPolicy = EXECUTION_POLICIES.OPERATOR_APPROVAL_REQUIRED;
  } else if (modifiers.includes(EXECUTION_MODIFIERS.HUMAN_IN_THE_LOOP)) {
    executionPolicy = EXECUTION_POLICIES.OPERATOR_APPROVAL_REQUIRED;
  }

  return { executionPolicy, modifiers };
}

function buildReasoningPolicy(conversationModifiers = []) {
  const modifiers = Array.isArray(conversationModifiers) ? conversationModifiers : [];
  let reasoningMode = null;

  if (
    modifiers.includes(CONVERSATION_MODIFIERS.SHOW_REASONING) ||
    modifiers.includes(CONVERSATION_MODIFIERS.NATURAL_REASONING)
  ) {
    reasoningMode = REASONING_MODES.ANALYTICAL;
  } else if (modifiers.includes(CONVERSATION_MODIFIERS.STEP_BY_STEP)) {
    reasoningMode = REASONING_MODES.TEACHING;
  } else if (modifiers.includes(CONVERSATION_MODIFIERS.CONCISE)) {
    reasoningMode = REASONING_MODES.CONCISE;
  }

  return { reasoningMode };
}

function buildConversationPolicy(conversationModifiers = []) {
  const modifiers = Array.isArray(conversationModifiers) ? conversationModifiers : [];
  let conversationStyle = null;

  if (
    modifiers.includes(CONVERSATION_MODIFIERS.NATURAL) ||
    modifiers.includes(CONVERSATION_MODIFIERS.NATURAL_REASONING)
  ) {
    conversationStyle = CONVERSATION_STYLES.NATURAL;
  } else if (modifiers.includes(CONVERSATION_MODIFIERS.CONCISE)) {
    conversationStyle = CONVERSATION_STYLES.CONCISE;
  } else if (modifiers.includes(CONVERSATION_MODIFIERS.VERBOSE)) {
    conversationStyle = CONVERSATION_STYLES.TECHNICAL;
  }

  return { conversationStyle, modifiers };
}

/**
 * @param {import('./OperatorObjectiveResolutionEngine').ObjectiveResolution} objectiveResolution
 * @returns {import('./OperatorObjectiveResolutionEngine').ExecutionContract}
 */
function buildExecutionContract(objectiveResolution) {
  const executionModifiers = objectiveResolution.executionModifiers || [];
  const conversationModifiers = objectiveResolution.conversationModifiers || [];

  return {
    objectiveResolution,
    executionPolicy: buildExecutionPolicy(executionModifiers),
    reasoningPolicy: buildReasoningPolicy(conversationModifiers),
    conversationPolicy: buildConversationPolicy(conversationModifiers),
    requiredCapabilities: objectiveResolution.requiredCapabilities || [],
  };
}

module.exports = {
  buildExecutionContract,
  buildExecutionPolicy,
  buildReasoningPolicy,
  buildConversationPolicy,
};
