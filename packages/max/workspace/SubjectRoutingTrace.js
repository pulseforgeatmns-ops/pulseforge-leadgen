'use strict';

/**
 * SPEC-149 — Routing trace for every workspace turn.
 */

const PIPELINE_BY_OWNER = Object.freeze({
  conversation_identity: 'IdentityConversation',
  reflection: 'ReflectionEngine',
  active_mission: 'MissionRuntime',
  mission_creation: 'MissionRuntime',
  mission_inspection: 'MissionRuntime',
  blueprint: 'BusinessIntelligence',
  reasoning: 'ReasoningFallback',
  knowledge_retrieval: 'KnowledgeLayer',
  conversation_layer: 'ConversationLayer',
  specialist_interrogation: 'SpecialistConversation',
  specialist_scout: 'SpecialistConversation',
  specialist_paige: 'SpecialistConversation',
  specialist_cal: 'SpecialistConversation',
  specialist_direction: 'SpecialistConversation',
  objective_persistence: 'ObjectivePersistence',
});

const PIPELINE_BY_SUBJECT = Object.freeze({
  identity: 'IdentityConversation',
  reflection: 'ReflectionEngine',
  mission: 'MissionRuntime',
  specialist: 'SpecialistConversation',
  business: 'BusinessIntelligence',
  knowledge: 'KnowledgeLayer',
  conversation: 'ConversationLayer',
});

function pipelineForOwner(owner, subject) {
  if (owner && PIPELINE_BY_OWNER[owner]) return PIPELINE_BY_OWNER[owner];
  if (subject && PIPELINE_BY_SUBJECT[subject]) return PIPELINE_BY_SUBJECT[subject];
  return 'ReasoningFallback';
}

function claimedByFor(input = {}) {
  if (input.claimedBy) return input.claimedBy;
  if (input.workspaceOwnership && input.workspaceOwnership.reason) {
    return input.workspaceOwnership.reason;
  }
  if (input.conversationSubject && input.conversationSubject.reason) {
    return `${input.conversationSubject.reason}_subject_router`;
  }
  return 'workspace_router';
}

/**
 * @param {object} input
 * @returns {{ subject: string|null, intent: string|null, thinkingMode: string|null, owner: string|null, pipeline: string, claimedBy: string }}
 */
function buildRoutingTrace(input = {}) {
  const conversationSubject = input.conversationSubject || null;
  const conversationIntent = input.conversationIntent || null;
  const workspaceOwnership = input.workspaceOwnership || null;
  const conversationalState = input.conversationalState || null;
  const owner = workspaceOwnership && workspaceOwnership.owner ? workspaceOwnership.owner : null;
  const subject = conversationSubject && conversationSubject.subject
    ? conversationSubject.subject
    : null;

  return {
    subject,
    intent: conversationIntent && conversationIntent.intent ? conversationIntent.intent : null,
    thinkingMode:
      conversationIntent && conversationIntent.thinkingMode
        ? conversationIntent.thinkingMode
        : null,
    owner,
    pipeline: input.pipeline || pipelineForOwner(owner, subject),
    claimedBy: claimedByFor(input),
    activeObject: conversationalState && conversationalState.activeObject
      ? conversationalState.activeObject
      : null,
    mode: conversationalState && conversationalState.mode
      ? conversationalState.mode
      : null,
    depth: conversationalState && conversationalState.depth
      ? conversationalState.depth
      : null,
    resolvedQuestion: input.resolvedQuestion || null,
    continuity: Boolean(conversationIntent && conversationIntent.continuity),
  };
}

function attachRoutingTrace(result, traceInput = {}) {
  if (!result || typeof result !== 'object') return result;
  return {
    ...result,
    routingTrace: buildRoutingTrace(traceInput),
  };
}

module.exports = {
  PIPELINE_BY_OWNER,
  PIPELINE_BY_SUBJECT,
  pipelineForOwner,
  buildRoutingTrace,
  attachRoutingTrace,
};
