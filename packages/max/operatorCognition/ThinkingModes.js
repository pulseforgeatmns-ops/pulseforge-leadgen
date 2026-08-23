'use strict';

/**
 * SPEC-146 — Operator thinking modes.
 * Every turn begins with: what kind of thinking does this operator need?
 */

const THINKING_MODES = Object.freeze({
  EXECUTE: 'execute',
  INSPECT: 'inspect',
  REFLECT: 'reflect',
  EXPLAIN: 'explain',
  CHALLENGE: 'challenge',
  COMPARE: 'compare',
  STRATEGY: 'strategy',
  BRAINSTORM: 'brainstorm',
  TEACH: 'teach',
  EDIT: 'edit',
  RESUME: 'resume',
});

const THINKING_MODE_CATEGORY = Object.freeze({
  [THINKING_MODES.EXECUTE]: 'execution',
  [THINKING_MODES.EDIT]: 'execution',
  [THINKING_MODES.INSPECT]: 'inspection',
  [THINKING_MODES.REFLECT]: 'reflection',
  [THINKING_MODES.EXPLAIN]: 'reasoning',
  [THINKING_MODES.CHALLENGE]: 'reasoning',
  [THINKING_MODES.COMPARE]: 'reasoning',
  [THINKING_MODES.STRATEGY]: 'reasoning',
  [THINKING_MODES.BRAINSTORM]: 'exploration',
  [THINKING_MODES.TEACH]: 'education',
  [THINKING_MODES.RESUME]: 'continuation',
});

/** Only Execute and Edit may mutate mission state (SPEC-146 execution guard). */
const MUTATING_MODES = Object.freeze([
  THINKING_MODES.EXECUTE,
  THINKING_MODES.EDIT,
]);

function thinkingModeCategory(mode) {
  return THINKING_MODE_CATEGORY[mode] || 'reasoning';
}

function modeMutatesMission(mode) {
  return MUTATING_MODES.includes(mode);
}

module.exports = {
  THINKING_MODES,
  THINKING_MODE_CATEGORY,
  MUTATING_MODES,
  thinkingModeCategory,
  modeMutatesMission,
};
