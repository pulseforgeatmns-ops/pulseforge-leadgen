'use strict';

/**
 * Max Synthesis Layer — MessageIntentClassifier.
 *
 * Shared user-message classification for Growth / Campaign / future Max
 * conversations. Delegates to SPEC-090 classifyReasoningMessage so CIE
 * discovery and campaign loops share one intent vocabulary.
 */

const {
  MESSAGE_CLASSES,
  classifyReasoningMessage,
  looksLikeApproval,
  looksLikeApprovalPlusNextRequest,
  looksLikeArtifactRequest,
  looksLikeClarificationRequest,
  looksLikeSkip,
  looksLikeVagueAnswer,
  looksLikeNextPlanningRequest,
  looksLikeExplicitReplayRequest,
} = require('../clientIntelligenceReasoning');

/** Public intent set required by the synthesis layer contract. */
const MESSAGE_INTENTS = Object.freeze({
  DIRECT_ANSWER: MESSAGE_CLASSES.DIRECT_ANSWER,
  APPROVAL: MESSAGE_CLASSES.APPROVAL,
  APPROVAL_PLUS_NEXT_REQUEST: MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST,
  CORRECTION: MESSAGE_CLASSES.CORRECTION,
  ADD_ON: MESSAGE_CLASSES.ADD_ON,
  CLARIFICATION_REQUEST: MESSAGE_CLASSES.CLARIFICATION_REQUEST,
  ARTIFACT_REQUEST: MESSAGE_CLASSES.ARTIFACT_REQUEST,
  INSUFFICIENT_ANSWER: MESSAGE_CLASSES.INSUFFICIENT_ANSWER,
  OFF_TOPIC: MESSAGE_CLASSES.OFF_TOPIC,
  SKIP: MESSAGE_CLASSES.SKIP,
  REFINEMENT_FEEDBACK: MESSAGE_CLASSES.REFINEMENT_FEEDBACK,
});

/**
 * Classify a user message into a synthesis-layer intent.
 * @param {string} text
 * @param {object} [opts] — CIE detectors / activeQuestion / etc.
 * @returns {string} MESSAGE_INTENTS value
 */
function classifyMessageIntent(text, opts = {}) {
  return classifyReasoningMessage(text, opts);
}

module.exports = {
  MESSAGE_INTENTS,
  MESSAGE_CLASSES,
  classifyMessageIntent,
  classifyReasoningMessage,
  looksLikeApproval,
  looksLikeApprovalPlusNextRequest,
  looksLikeArtifactRequest,
  looksLikeClarificationRequest,
  looksLikeSkip,
  looksLikeVagueAnswer,
  looksLikeNextPlanningRequest,
  looksLikeExplicitReplayRequest,
};
