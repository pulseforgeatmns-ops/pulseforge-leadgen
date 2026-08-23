'use strict';

/**
 * SPEC-148 — Reflective cognition routing.
 * When subject = reasoning, no business subsystem may claim the turn.
 */

const { assertReadOnlyCognition } = require('../operatorCognition/ExecutionGuard');
const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const { buildReflectionContext } = require('./ReflectionContext');
const {
  composeReflectiveResponse,
  buildReflectionStructured,
} = require('./ReflectionLayer');
const askPathTrace = require('./audit/AskPathTrace');

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleReflectionTurn(input = {}) {
  const conversationIntent = input.conversationIntent || null;
  const conversationSubject = input.conversationSubject || null;

  if (!conversationIntent) return null;
  if (conversationIntent.intent !== THINKING_MODES.REFLECT) {
    if (!conversationSubject || conversationSubject.subject !== 'reasoning') {
      return null;
    }
  }

  assertReadOnlyCognition(conversationIntent, 'reflection');

  askPathTrace.traceEnter('maybeHandleReflectionTurn', {
    intent: conversationIntent.intent,
    subject: conversationSubject && conversationSubject.subject,
    locked: conversationSubject && conversationSubject.locked,
  });

  const reflectionContext = buildReflectionContext({
    question: input.question,
    session: input.session,
    conversationIntent,
    conversationSubject,
    previousTurnContext: input.previousTurnContext || null,
  });

  const conversational = composeReflectiveResponse({
    question: input.question,
    reflectionContext,
  });

  const structured = buildReflectionStructured(
    conversational,
    conversationIntent,
    conversationSubject
  );

  askPathTrace.traceEarlyReturn('maybeHandleReflectionTurn', 'reflection_composed');

  return {
    handled: true,
    prose: conversational.prose,
    structured,
    reflectionContext,
    conversational,
    reason: 'reflective_cognition',
    answered: {
      kind: 'reflection',
      reflectKind: conversational.reflectKind,
    },
  };
}

module.exports = {
  maybeHandleReflectionTurn,
};
