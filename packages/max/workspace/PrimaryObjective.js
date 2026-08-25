'use strict';

/**
 * SPEC-167 — Operator primary objective vocabulary (ADR-087).
 * Routing is determined exclusively by primaryObjective.
 */

const PRIMARY_OBJECTIVES = Object.freeze({
  MISSION_CREATION: 'mission_creation',
  MISSION_EXECUTION: 'mission_execution',
  BUSINESS_INTELLIGENCE: 'business_intelligence',
  BUSINESS_DECISION: 'business_decision',
  MISSION_INSPECTION: 'mission_inspection',
  WORKSPACE_OPERATION: 'workspace_operation',
  IDENTITY: 'identity',
  GENERAL_CONVERSATION: 'general_conversation',
  EXECUTION_INSPECTION: 'execution_inspection',
  SESSION_INSPECTION: 'session_inspection',
});

const SUPPORTING_OBJECTIVES = Object.freeze({
  REASONING_EXPLANATION: 'reasoning_explanation',
  IDENTITY: 'identity',
  SESSION_CONFIGURATION: 'session_configuration',
});

const EXECUTION_MODIFIERS = Object.freeze({
  AUTONOMOUS: 'autonomous',
  READ_ONLY: 'read_only',
  SIMULATION: 'simulation',
  PRODUCTION: 'production',
  PAUSE_ON_APPROVAL: 'pause_on_approval',
  HUMAN_IN_THE_LOOP: 'human_in_the_loop',
  FAST: 'fast',
  CONSERVATIVE: 'conservative',
});

const CONVERSATION_MODIFIERS = Object.freeze({
  NATURAL: 'natural',
  CONCISE: 'concise',
  VERBOSE: 'verbose',
  SHOW_REASONING: 'show_reasoning',
  NATURAL_REASONING: 'natural_reasoning',
  STEP_BY_STEP: 'step_by_step',
  TEACHING_MODE: 'teaching_mode',
});

const REQUIRED_CAPABILITIES = Object.freeze({
  SCOUT_INTELLIGENCE: 'scout_intelligence',
  OPPORTUNITY_INTELLIGENCE: 'opportunity_intelligence',
  OUTCOME_LEARNING: 'outcome_learning',
});

/** Lower index = higher routing priority when multiple candidates appear in one message. */
const PRIMARY_OBJECTIVE_PRIORITY = Object.freeze([
  PRIMARY_OBJECTIVES.MISSION_CREATION,
  PRIMARY_OBJECTIVES.MISSION_EXECUTION,
  PRIMARY_OBJECTIVES.MISSION_INSPECTION,
  PRIMARY_OBJECTIVES.EXECUTION_INSPECTION,
  PRIMARY_OBJECTIVES.BUSINESS_DECISION,
  PRIMARY_OBJECTIVES.BUSINESS_INTELLIGENCE,
  PRIMARY_OBJECTIVES.SESSION_INSPECTION,
  PRIMARY_OBJECTIVES.WORKSPACE_OPERATION,
  PRIMARY_OBJECTIVES.IDENTITY,
  PRIMARY_OBJECTIVES.GENERAL_CONVERSATION,
]);

function comparePrimaryObjectivePriority(a, b) {
  const ai = PRIMARY_OBJECTIVE_PRIORITY.indexOf(a);
  const bi = PRIMARY_OBJECTIVE_PRIORITY.indexOf(b);
  return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
}

module.exports = {
  PRIMARY_OBJECTIVES,
  SUPPORTING_OBJECTIVES,
  EXECUTION_MODIFIERS,
  CONVERSATION_MODIFIERS,
  REQUIRED_CAPABILITIES,
  PRIMARY_OBJECTIVE_PRIORITY,
  comparePrimaryObjectivePriority,
};
