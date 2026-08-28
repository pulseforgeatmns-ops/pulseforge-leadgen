'use strict';

/**
 * SPEC-126 — Execution language detection for ownership routing.
 * When present, objective persistence must not claim the turn.
 * SPEC-153 — invoked only during analyzeOperatorIntent(); downstream consumers
 * use OperatorIntent.executionRequested instead of re-parsing.
 * SPEC-200 — clause-level negation; lifecycle verbs bind per clause, not globally.
 */

const { guardPostIntentParsing } = require('./audit/OperatorIntentAudit');
const {
  splitClauses,
  isVerbNegatedInClause,
  isPureNegationClause,
  EXECUTION_VERB_RE,
} = require('./MissionLifecycleIntent');

const MISSION_CREATE_COMMAND_RE =
  /\b(create|begin|start)\s+(?:a\s+)?(?:brand[- ]?new\s+)?(?:new\s+)?(?:acquisition\s+)?mission\b/i;

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

function normalizeText(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function clauseHasExecutionLanguage(clause) {
  if (isPureNegationClause(clause)) return false;
  return EXECUTION_VERB_RE.test(clause);
}

function clauseMatchesExecutionCommand(clause, re) {
  if (isPureNegationClause(clause)) return false;
  if (re.source.includes('resume') && isVerbNegatedInClause(clause, 'resume')) return false;
  if (re.source.includes('continue') && isVerbNegatedInClause(clause, 'continue')) return false;
  if (re.source.includes('create') && isVerbNegatedInClause(clause, 'create')) return false;
  if (re.source.includes('begin') && isVerbNegatedInClause(clause, 'begin')) return false;
  if (re.source.includes('start') && isVerbNegatedInClause(clause, 'start')) return false;
  if (re.source.includes('run') && isVerbNegatedInClause(clause, 'run')) return false;
  if (re.source.includes('execute') && isVerbNegatedInClause(clause, 'execute')) return false;
  if (re.source.includes('operate') && isVerbNegatedInClause(clause, 'operate')) return false;
  return re.test(clause);
}

/**
 * @param {string} text
 * @returns {boolean}
 */
function hasExecutionLanguage(text) {
  guardPostIntentParsing('hasExecutionLanguage');
  const q = normalizeText(text);
  if (!q) return false;
  const clauses = splitClauses(q);
  return clauses.some((clause) => clauseHasExecutionLanguage(clause));
}

/**
 * Positive acquisition-mission execution phrasing for SPEC-126 ownership.
 * Does not claim legacy campaign/direct-mail workspace flows.
 * @param {string} text
 * @returns {{ matched: boolean, reason: string|null }}
 */
function detectMissionExecutionLanguage(text) {
  guardPostIntentParsing('detectMissionExecutionLanguage');
  const q = normalizeText(text);
  if (!q || !hasExecutionLanguage(q)) {
    return { matched: false, reason: null };
  }

  const clauses = splitClauses(q);
  for (const clause of clauses) {
    if (clauseMatchesExecutionCommand(clause, MISSION_CREATE_COMMAND_RE)) {
      return { matched: true, reason: 'mission_create_command' };
    }
  }
  for (const clause of clauses) {
    if (clauseMatchesExecutionCommand(clause, MISSION_OPERATE_COMMAND_RE)) {
      return { matched: true, reason: 'mission_operate_command' };
    }
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
  guardPostIntentParsing('isMissionExecutionCommand');
  const q = normalizeText(text);
  if (!q) return false;
  const clauses = splitClauses(q);
  return clauses.some((clause) =>
    MISSION_EXECUTION_COMMAND_RES.some((re) => clauseMatchesExecutionCommand(clause, re))
  );
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
