'use strict';

/**
 * SPEC-146 — Route read-only cognition turns to mission inspection before execution.
 */

const {
  THINKING_MODES,
  isReadOnlyCognition,
  attachSpecialists,
} = require('../operatorCognition');
const { maybeHandleWorkspaceMissionInspection } = require('./WorkspaceMissionInspection');
const askPathTrace = require('./audit/AskPathTrace');

const MISSION_READ_ONLY_MODES = Object.freeze([
  THINKING_MODES.INSPECT,
  THINKING_MODES.EXPLAIN,
  THINKING_MODES.CHALLENGE,
  THINKING_MODES.COMPARE,
  THINKING_MODES.STRATEGY,
  THINKING_MODES.BRAINSTORM,
  THINKING_MODES.TEACH,
  THINKING_MODES.RESUME,
]);

function shouldPreferMissionInspection(conversationIntent) {
  if (!conversationIntent || !conversationIntent.intent) return false;
  return MISSION_READ_ONLY_MODES.includes(conversationIntent.intent);
}

/**
 * Handle read-only operator cognition when an acquisition mission is active.
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleOperatorCognitionTurn(input = {}) {
  const conversationIntent = input.conversationIntent || null;
  if (!conversationIntent || !isReadOnlyCognition(conversationIntent)) {
    return null;
  }
  if (!shouldPreferMissionInspection(conversationIntent)) {
    return null;
  }

  askPathTrace.traceEnter('maybeHandleOperatorCognitionTurn', {
    intent: conversationIntent.intent,
    via: conversationIntent.via,
  });

  const turn = await maybeHandleWorkspaceMissionInspection({
    ...input,
    conversationIntent,
    cognitionRouting: true,
  });

  if (!turn) {
    askPathTrace.traceEarlyReturn('maybeHandleOperatorCognitionTurn', 'inspection_not_claimed');
    return null;
  }

  const enrichedIntent = attachSpecialists(conversationIntent);
  return {
    ...turn,
    conversationIntent: enrichedIntent,
    reason: `operator_cognition_${conversationIntent.intent}`,
  };
}

module.exports = {
  MISSION_READ_ONLY_MODES,
  shouldPreferMissionInspection,
  maybeHandleOperatorCognitionTurn,
};
