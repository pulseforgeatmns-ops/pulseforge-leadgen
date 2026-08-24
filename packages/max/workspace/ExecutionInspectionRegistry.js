'use strict';

/**
 * BUG-002 / ADR-073 — Execution Inspection Registry.
 *
 * Canonical vocabulary for operator execution introspection. Classification
 * (isExecutionInspectionQuestion, detectInspectionMode) and the inspection
 * operator consume the same registry so recognized phrasing always maps to
 * stored Execution State.
 */

const { normalizeText } = require('./SessionState');

/**
 * @typedef {object} ExecutionInspectionIntent
 * @property {string} id
 * @property {string} mode — inspectExecutionState formatter key
 * @property {RegExp[]} aliases
 */

/** Registry order defines match priority (first match wins). */
/** @type {ExecutionInspectionIntent[]} */
const EXECUTION_INSPECTION_INTENTS = [
  {
    id: 'execution_pause_inspection',
    mode: 'pause_explanation',
    aliases: [
      /\bwhy did you stop\b/i,
      /\bwhy are you paused\b/i,
      /\bwhy aren'?t you continuing\b/i,
      /\bwhy haven'?t you continued\b/i,
      /\bwhy didn'?t you continue\b/i,
      /\bwhy didn'?t you continue autonomous execution\b/i,
      /\bwhy do you require\b.{0,80}\bapproval\b/i,
      /\bwhy do you require mission plan approval\b/i,
      /\bwhy can'?t you proceed\b/i,
      /\bwhy are you waiting\b/i,
      /\bwhat approval are you waiting for\b/i,
      /\bwhat are you waiting for\b/i,
      /\bwhat(?:'s| is) blocking you\b/i,
      /\bwhat is blocking you\b/i,
      /\bwhy are you blocked\b/i,
      /\bwhy is this blocked\b/i,
    ],
  },
  {
    id: 'execution_next_step_inspection',
    mode: 'next_step',
    aliases: [
      /\bwhat(?:'s| is) next\b/i,
      /\bwhat happens next\b/i,
      /\bwhat will you do next\b/i,
      /\bwhat(?:'s| is) your next step\b/i,
      /\bwhat is your next step\b/i,
    ],
  },
  {
    id: 'execution_state_inspection',
    mode: 'full_state',
    aliases: [
      /\bshow me (?:the |your )?execution state\b/i,
      /\bshow (?:the |your )?execution state\b/i,
      /\bwhat(?:'s| is) (?:the |your )?execution state\b/i,
      /\bsummarize (?:your )?execution\b/i,
      /\bsummarize the execution plan\b/i,
      /\bshow (?:the )?execution plan\b/i,
      /\bshow planner state\b/i,
      /\bshow execution history\b/i,
      /\bwhat(?:'s| is) the execution status\b/i,
    ],
  },
  {
    id: 'execution_plan_position_inspection',
    mode: 'plan_position',
    aliases: [/\bwhere are you in the plan\b/i],
  },
  {
    id: 'execution_status_inspection',
    mode: 'current_activity',
    aliases: [
      /\bwhat are you doing\b/i,
      /\bwhat are you working on\b/i,
      /\bwhat are you executing\b/i,
      /\bwhat step are you on\b/i,
      /\bwhat(?:'s| is) your current step\b/i,
    ],
  },
];

function intentMatches(text, intent) {
  return intent.aliases.some((alias) => alias.test(text));
}

/**
 * All intents matched in registry order.
 * @param {string} text
 * @returns {ExecutionInspectionIntent[]}
 */
function matchExecutionInspectionIntents(text) {
  const q = normalizeText(text);
  if (!q) return [];
  return EXECUTION_INSPECTION_INTENTS.filter((intent) => intentMatches(q, intent));
}

/**
 * First matched intent in registry priority order.
 * @param {string} text
 * @returns {ExecutionInspectionIntent|null}
 */
function matchExecutionInspectionIntent(text) {
  const matched = matchExecutionInspectionIntents(text);
  return matched.length ? matched[0] : null;
}

/**
 * @param {string} text
 * @returns {boolean}
 */
function isExecutionInspectionQuestion(text) {
  return matchExecutionInspectionIntent(text) != null;
}

/**
 * @param {string} text
 * @returns {boolean}
 */
function isExecutionExplanationQuestion(text) {
  const intent = matchExecutionInspectionIntent(text);
  return intent != null && intent.mode === 'pause_explanation';
}

/**
 * @param {string} text
 * @returns {string}
 */
function detectInspectionMode(text) {
  const intent = matchExecutionInspectionIntent(text);
  return intent ? intent.mode : 'current_activity';
}

/** @deprecated Use EXECUTION_INSPECTION_INTENTS — kept for backward-compatible test imports. */
const EXECUTION_INSPECTION_RES = EXECUTION_INSPECTION_INTENTS.filter(
  (intent) => intent.mode === 'current_activity' || intent.mode === 'plan_position'
).flatMap((intent) => intent.aliases);

/** @deprecated Use EXECUTION_INSPECTION_INTENTS */
const EXECUTION_PAUSE_EXPLANATION_RES = EXECUTION_INSPECTION_INTENTS.filter(
  (intent) => intent.mode === 'pause_explanation'
).flatMap((intent) => intent.aliases);

/** @deprecated Use EXECUTION_INSPECTION_INTENTS */
const EXECUTION_NEXT_STEP_RES = EXECUTION_INSPECTION_INTENTS.filter(
  (intent) => intent.mode === 'next_step'
).flatMap((intent) => intent.aliases);

module.exports = {
  EXECUTION_INSPECTION_INTENTS,
  matchExecutionInspectionIntents,
  matchExecutionInspectionIntent,
  isExecutionInspectionQuestion,
  isExecutionExplanationQuestion,
  detectInspectionMode,
  EXECUTION_INSPECTION_RES,
  EXECUTION_PAUSE_EXPLANATION_RES,
  EXECUTION_NEXT_STEP_RES,
};
