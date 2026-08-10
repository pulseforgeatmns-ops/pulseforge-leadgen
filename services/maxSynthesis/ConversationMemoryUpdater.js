'use strict';

/**
 * Max Synthesis Layer — ConversationMemoryUpdater.
 *
 * Updates the correct section of session reasoning memory for approvals,
 * add-ons, corrections, and artifact progression — without treating the
 * active question as the only write target.
 */

const {
  ensureReasoningMemory,
  recordAcceptedFact,
  recordPendingCorrection,
  resolvePendingCorrection,
  markClassification,
  markArtifactGenerated,
  markArtifactApproved,
  setPendingUserRequest,
  resolveNextArtifact,
  resolveCampaignArtifactAction,
  addQuestionDebt,
  clearQuestionDebt,
  setActiveProbe,
  MESSAGE_CLASSES,
} = require('../clientIntelligenceReasoning');

const { MESSAGE_INTENTS } = require('./MessageIntentClassifier');

/**
 * Apply a classified turn to reasoning memory.
 * Does not mutate CIE sectionState / normalizedFacts — callers still run
 * interview extractors after this for fact writes.
 *
 * @param {object} state — interview_state-like object
 * @param {{
 *   messageClass: string,
 *   text?: string,
 *   section?: string|null,
 *   substance?: string|null,
 *   source?: string,
 *   artifactKind?: string|null,
 * }} turn
 * @returns {{ memory: object, state: object }}
 */
function applyConversationMemoryUpdate(state, turn = {}) {
  const nextState = state && typeof state === 'object' ? { ...state } : {};
  let memory = ensureReasoningMemory(nextState);
  const messageClass = turn.messageClass || MESSAGE_INTENTS.DIRECT_ANSWER;

  memory = markClassification(memory, messageClass);
  if (turn.text != null) {
    memory = setPendingUserRequest(memory, {
      text: turn.text,
      messageClass,
    });
  }

  if (
    (messageClass === MESSAGE_INTENTS.ADD_ON ||
      messageClass === MESSAGE_INTENTS.DIRECT_ANSWER) &&
    turn.section &&
    turn.substance
  ) {
    memory = recordAcceptedFact(memory, {
      section: turn.section,
      substance: turn.substance,
      source: turn.source || (messageClass === MESSAGE_INTENTS.ADD_ON ? 'add_on' : 'answer'),
    });
  }

  if (messageClass === MESSAGE_INTENTS.CORRECTION && turn.section && turn.substance) {
    memory = recordPendingCorrection(memory, {
      section: turn.section,
      substance: turn.substance,
    });
    memory = resolvePendingCorrection(memory, turn.section);
    memory = recordAcceptedFact(memory, {
      section: turn.section,
      substance: turn.substance,
      source: 'correction',
    });
  }

  if (
    (messageClass === MESSAGE_INTENTS.APPROVAL ||
      messageClass === MESSAGE_INTENTS.APPROVAL_PLUS_NEXT_REQUEST) &&
    turn.artifactKind
  ) {
    memory = markArtifactApproved(memory, turn.artifactKind);
  }

  if (turn.generatedArtifactKind) {
    memory = markArtifactGenerated(memory, turn.generatedArtifactKind);
  }

  nextState.reasoningMemory = memory;
  return { memory, state: nextState };
}

/**
 * Resolve campaign artifact progression (criteria → build proposal) via
 * shared memory + classifier semantics.
 */
function updateMemoryForCampaignArtifactTurn(opts = {}) {
  return resolveCampaignArtifactAction(opts);
}

module.exports = {
  MESSAGE_CLASSES,
  MESSAGE_INTENTS,
  applyConversationMemoryUpdate,
  updateMemoryForCampaignArtifactTurn,
  ensureReasoningMemory,
  recordAcceptedFact,
  recordPendingCorrection,
  resolvePendingCorrection,
  markClassification,
  markArtifactGenerated,
  markArtifactApproved,
  setPendingUserRequest,
  resolveNextArtifact,
  resolveCampaignArtifactAction,
  addQuestionDebt,
  clearQuestionDebt,
  setActiveProbe,
};
