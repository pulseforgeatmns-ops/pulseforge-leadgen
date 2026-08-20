'use strict';

/**
 * SPEC-126 — Execution language detection for ownership routing.
 * When present, objective persistence must not claim the turn.
 */

const EXECUTION_VERB_RE =
  /\b(create|resume|begin|operate|execute|manage|run|continue|complete)\b/i;

const MISSION_CREATE_COMMAND_RE =
  /\b(create|begin|start)\s+(?:a\s+)?(?:new\s+)?mission\b/i;

const MISSION_OPERATE_COMMAND_RE =
  /\boperate\b.{0,60}\b(?:anchor|client|account|campaign|mission|pulseforge|through)\b/i;

const MISSION_EXECUTION_COMMAND_RE =
  /\b(resume|continue|execute|manage|run|complete)\b/i;

const MISSION_EXECUTION_CONTEXT_RE =
  /\b(mission|acquisition|campaign|anchor|pulseforge)\b/i;

function normalizeText(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * @param {string} text
 * @returns {boolean}
 */
function hasExecutionLanguage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return EXECUTION_VERB_RE.test(q);
}

/**
 * Execution phrasing that should bind to Mission Creation / Continuation.
 * @param {string} text
 * @returns {{ matched: boolean, reason: string|null }}
 */
function detectMissionExecutionLanguage(text) {
  const q = normalizeText(text);
  if (!q || !hasExecutionLanguage(q)) {
    return { matched: false, reason: null };
  }

  if (MISSION_CREATE_COMMAND_RE.test(q)) {
    return { matched: true, reason: 'mission_create_command' };
  }
  if (MISSION_OPERATE_COMMAND_RE.test(q)) {
    return { matched: true, reason: 'mission_operate_command' };
  }
  if (
    MISSION_EXECUTION_COMMAND_RE.test(q) &&
    MISSION_EXECUTION_CONTEXT_RE.test(q)
  ) {
    return { matched: true, reason: 'mission_execution_command' };
  }

  return { matched: false, reason: null };
}

module.exports = {
  EXECUTION_VERB_RE,
  MISSION_CREATE_COMMAND_RE,
  MISSION_OPERATE_COMMAND_RE,
  hasExecutionLanguage,
  detectMissionExecutionLanguage,
  normalizeText,
};
