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

/** SPEC-127 — operator commands that advance the active mission. */
const MISSION_EXECUTION_COMMAND_RES = [
  /\bapprov(e|al|ed)\b/i,
  /\bcontinue\b/i,
  /\bbegin\b/i,
  /\bexecute\b/i,
  /\bproceed\b/i,
  /\bnext\b/i,
  /\bresume\b/i,
  /\brun\b/i,
  /\bstart\b/i,
  /\bdiscovery\b/i,
  /\bprioritization\b/i,
  /\boutreach\b/i,
  /\bsend\b/i,
];

const NEGATED_EXECUTION_RE =
  /\b(?:do not|don't|never|not)\s+(?:run|create|execute|resume|launch|begin|operate|continue|manage|complete)\b|\binstead of creating\b|\bpreparation-only\b|\bnot (?:launching|executing|running|creating)\b/i;

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
  if (NEGATED_EXECUTION_RE.test(q)) return false;
  return EXECUTION_VERB_RE.test(q);
}

/**
 * Positive acquisition-mission execution phrasing for SPEC-126 ownership.
 * Does not claim legacy campaign/direct-mail workspace flows.
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
  if (isMissionExecutionCommand(q)) {
    return { matched: true, reason: 'mission_execution_command' };
  }

  return { matched: false, reason: null };
}

/**
 * SPEC-127 — execution commands always bind to the active mission when one exists.
 * @param {string} text
 * @returns {boolean}
 */
function isMissionExecutionCommand(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (NEGATED_EXECUTION_RE.test(q)) return false;
  return MISSION_EXECUTION_COMMAND_RES.some((re) => re.test(q));
}

module.exports = {
  EXECUTION_VERB_RE,
  MISSION_CREATE_COMMAND_RE,
  MISSION_OPERATE_COMMAND_RE,
  MISSION_EXECUTION_COMMAND_RES,
  hasExecutionLanguage,
  isMissionExecutionCommand,
  detectMissionExecutionLanguage,
  normalizeText,
};
