'use strict';

/**
 * SPEC-167 — Primary objective to workspace owner routing map.
 * Kept separate from WorkspaceOwnershipResolver to avoid circular imports.
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

const ROUTING_OWNERS = Object.freeze({
  ACTIVE_MISSION: 'active_mission',
  MISSION_CREATION: 'mission_creation',
  MISSION_INSPECTION: 'mission_inspection',
  REASONING: 'reasoning',
  CONVERSATION_IDENTITY: 'conversation_identity',
  SESSION_STATE_MANAGER: 'session_state_manager',
  EXECUTION_STATE_MANAGER: 'execution_state_manager',
});

const PIPELINE_BY_OWNER = Object.freeze({
  active_mission: 'MissionRuntime',
  mission_creation: 'MissionRuntime',
  mission_inspection: 'MissionRuntime',
  reasoning: 'ReasoningFallback',
  conversation_identity: 'IdentityConversation',
  session_state_manager: 'SessionStateManager',
  execution_state_manager: 'ExecutionInspectionOperator',
});

/**
 * @param {string} primaryObjective
 * @returns {{ owner: string, pipeline: string, reason: string }}
 */
function resolveRoutingDecision(primaryObjective) {
  switch (primaryObjective) {
    case PRIMARY_OBJECTIVES.MISSION_CREATION:
      return {
        owner: ROUTING_OWNERS.MISSION_CREATION,
        pipeline: PIPELINE_BY_OWNER.mission_creation,
        reason: 'primary_objective:mission_creation',
      };
    case PRIMARY_OBJECTIVES.MISSION_EXECUTION:
      return {
        owner: ROUTING_OWNERS.ACTIVE_MISSION,
        pipeline: PIPELINE_BY_OWNER.active_mission,
        reason: 'primary_objective:mission_execution',
      };
    case PRIMARY_OBJECTIVES.MISSION_INSPECTION:
      return {
        owner: ROUTING_OWNERS.MISSION_INSPECTION,
        pipeline: PIPELINE_BY_OWNER.mission_inspection,
        reason: 'primary_objective:mission_inspection',
      };
    case PRIMARY_OBJECTIVES.BUSINESS_INTELLIGENCE:
      return {
        owner: ROUTING_OWNERS.REASONING,
        pipeline: 'BusinessIntelligence',
        reason: 'primary_objective:business_intelligence',
      };
    case PRIMARY_OBJECTIVES.BUSINESS_DECISION:
      return {
        owner: ROUTING_OWNERS.REASONING,
        pipeline: 'ReasoningFallback',
        reason: 'primary_objective:business_decision',
      };
    case PRIMARY_OBJECTIVES.WORKSPACE_OPERATION:
      return {
        owner: ROUTING_OWNERS.SESSION_STATE_MANAGER,
        pipeline: PIPELINE_BY_OWNER.session_state_manager,
        reason: 'primary_objective:workspace_operation',
      };
    case PRIMARY_OBJECTIVES.SESSION_INSPECTION:
      return {
        owner: ROUTING_OWNERS.SESSION_STATE_MANAGER,
        pipeline: PIPELINE_BY_OWNER.session_state_manager,
        reason: 'primary_objective:session_inspection',
      };
    case PRIMARY_OBJECTIVES.EXECUTION_INSPECTION:
      return {
        owner: ROUTING_OWNERS.EXECUTION_STATE_MANAGER,
        pipeline: PIPELINE_BY_OWNER.execution_state_manager,
        reason: 'primary_objective:execution_inspection',
      };
    case PRIMARY_OBJECTIVES.IDENTITY:
      return {
        owner: ROUTING_OWNERS.CONVERSATION_IDENTITY,
        pipeline: PIPELINE_BY_OWNER.conversation_identity,
        reason: 'primary_objective:identity',
      };
    default:
      return {
        owner: ROUTING_OWNERS.REASONING,
        pipeline: PIPELINE_BY_OWNER.reasoning,
        reason: 'primary_objective:general_conversation',
      };
  }
}

module.exports = {
  ROUTING_OWNERS,
  resolveRoutingDecision,
};
