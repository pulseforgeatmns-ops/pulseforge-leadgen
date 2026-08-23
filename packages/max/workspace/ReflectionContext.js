'use strict';

/**
 * SPEC-148 — Reflection context assembly.
 * Reflection always references the immediately preceding turn before long-term memory.
 */

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function findPreviousExchange(messages = []) {
  if (!Array.isArray(messages) || messages.length < 2) {
    return {
      previousOperatorMessage: null,
      previousAssistantResponse: null,
    };
  }

  const prior = messages.slice(0, -1);
  let previousAssistantResponse = null;
  let previousOperatorMessage = null;

  for (let i = prior.length - 1; i >= 0; i -= 1) {
    const row = prior[i];
    if (!previousAssistantResponse && row.role === 'max') {
      previousAssistantResponse = normalizeText(row.text);
      continue;
    }
    if (previousAssistantResponse && row.role === 'operator') {
      previousOperatorMessage = normalizeText(row.text);
      break;
    }
  }

  return { previousOperatorMessage, previousAssistantResponse };
}

/**
 * @param {object} input
 * @returns {object}
 */
function buildReflectionContext(input = {}) {
  const session = input.session || null;
  const messages = session && Array.isArray(session.messages) ? session.messages : [];
  const { previousOperatorMessage, previousAssistantResponse } = findPreviousExchange(messages);
  const previousTurn = input.previousTurnContext || {};
  const conversationIntent = input.conversationIntent || {};
  const conversationSubject = input.conversationSubject || null;

  const priorIntent = previousTurn.conversationIntent || null;
  const priorOwner = previousTurn.workspaceOwner || null;
  const priorOwnerReason = previousTurn.workspaceOwnerReason || null;

  return {
    currentOperatorMessage: normalizeText(input.question),
    previousOperatorMessage,
    previousAssistantResponse,
    intentClassification: {
      current: conversationIntent,
      previous: priorIntent,
    },
    selectedPipeline: {
      current: null,
      previous: priorOwner,
      previousReason: priorOwnerReason,
    },
    conversationSubject,
    conversationHistory: messages.slice(-6).map((row) => ({
      role: row.role,
      text: normalizeText(row.text),
    })),
    hasPrecedingTurn: Boolean(previousOperatorMessage && previousAssistantResponse),
  };
}

module.exports = {
  buildReflectionContext,
  findPreviousExchange,
};
