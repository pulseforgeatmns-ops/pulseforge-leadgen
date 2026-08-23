'use strict';

/**
 * SPEC-146 — Thinking mode determines specialist participation.
 * Explain: Scout context → Max reasons → Operator (no execution).
 * Execute: Scout executes → transaction commits → Operator.
 */

const { THINKING_MODES } = require('./ThinkingModes');

const MODE_SPECIALISTS = Object.freeze({
  [THINKING_MODES.EXECUTE]: ['scout', 'max', 'emmett', 'paige'],
  [THINKING_MODES.EDIT]: ['max'],
  [THINKING_MODES.INSPECT]: ['max'],
  [THINKING_MODES.EXPLAIN]: ['scout', 'max'],
  [THINKING_MODES.CHALLENGE]: ['scout', 'max'],
  [THINKING_MODES.COMPARE]: ['scout', 'max'],
  [THINKING_MODES.STRATEGY]: ['max'],
  [THINKING_MODES.BRAINSTORM]: ['max'],
  [THINKING_MODES.TEACH]: ['max'],
  [THINKING_MODES.RESUME]: ['max'],
});

function selectSpecialists(conversationIntent) {
  if (!conversationIntent || !conversationIntent.intent) return ['max'];
  return MODE_SPECIALISTS[conversationIntent.intent] || ['max'];
}

function primarySpecialist(conversationIntent) {
  const list = selectSpecialists(conversationIntent);
  return list[0] || 'max';
}

module.exports = {
  MODE_SPECIALISTS,
  selectSpecialists,
  primarySpecialist,
};
