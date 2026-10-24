'use strict';

/**
 * SPEC-200 — Explicit Mission Lifecycle Intent.
 * Canonical lifecycle decision before findResumableMission() / AMO ownership.
 * Operator utterance → objective/context resolution → MissionLifecycleIntent → AMO ownership.
 */

const MissionLifecycleIntent = Object.freeze({
  CREATE_NEW: 'CREATE_NEW',
  RESUME_EXISTING: 'RESUME_EXISTING',
  CONTINUE_ACTIVE: 'CONTINUE_ACTIVE',
  UNSPECIFIED: 'UNSPECIFIED',
});

const LIFECYCLE_VERBS = Object.freeze([
  'resume',
  'reuse',
  'continue',
  'run',
  'create',
  'execute',
  'launch',
  'begin',
  'operate',
  'manage',
  'complete',
]);

const EXECUTION_VERB_RE =
  /\b(create|resume|begin|operate|execute|manage|run|continue|complete)\b/i;

const CREATE_CLAUSE_RES = [
  /\bcreate\s+(?:a\s+)?(?:brand[- ]?new|new|another)\s+(?:acquisition\s+)?mission\b/i,
  /\bcreate\s+(?:a\s+)?(?:brand[- ]?new|new)\s+one\b/i,
  /\bstart\s+fresh\s+with\s+(?:a\s+)?(?:new\s+)?(?:acquisition\s+)?mission\b/i,
  /\b(?:begin|start)\s+(?:a\s+)?new\s+(?:acquisition\s+)?mission\b/i,
  /\bcreate\s+another\s+mission\b/i,
  /\bcreate\s+(?:a\s+)?(?:new\s+)?mission\b/i,
];

const RESUME_CLAUSE_RES = [
  /\bresume\s+(?:the\s+)?(?:existing\s+)?(?:acquisition\s+)?mission\b/i,
  /\bresume\s+(?:the\s+)?(?:old\s+)?mission\b/i,
  /\bcreate\s+or\s+resume\b.{0,60}\b(?:acquisition\s+)?mission\b/i,
];

const CONTINUE_CLAUSE_RES = [
  /\bcontinue\s+(?:the\s+)?(?:acquisition\s+)?mission\b/i,
  /\bkeep\s+going\s+(?:on|with)\s+(?:the\s+)?mission\b/i,
];

function normalizeText(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Split an utterance into independent clauses for lifecycle parsing.
 * @param {string} text
 * @returns {string[]}
 */
function splitClauses(text) {
  const q = normalizeText(text);
  if (!q) return [];
  const parts = q.split(/(?<=[.!?])\s+/).map((row) => row.trim()).filter(Boolean);
  return parts.length ? parts : [q];
}

/**
 * True when a clause negates a lifecycle verb (do not resume, don't create, etc.).
 * @param {string} clause
 * @param {string|string[]} verbs
 * @returns {boolean}
 */
function isVerbNegatedInClause(clause, verbs) {
  const list = Array.isArray(verbs) ? verbs : [verbs];
  for (const verb of list) {
    const direct = new RegExp(`\\b(?:do not|don't|never|not)\\s+${verb}\\b`, 'i');
    if (direct.test(clause)) return true;
  }
  const grouped = new RegExp(
    `\\b(?:do not|don't|never)\\s+(?:${list.join('|')})(?:\\s*,\\s*(?:or\\s+)?(?:${list.join('|')}))*\\b`,
    'i'
  );
  if (grouped.test(clause)) return true;
  return false;
}

/**
 * Clause is purely prohibitive — no positive lifecycle or execution verb remains.
 * @param {string} clause
 * @returns {boolean}
 */
function isPureNegationClause(clause) {
  if (!/\b(?:do not|don't|never|not)\b/i.test(clause)) return false;
  const stripped = clause
    .replace(
      /\b(?:do not|don't|never|not)\s+(?:resume|reuse|continue|run|create|execute|launch|begin|operate|manage|complete)(?:\s*,?\s*(?:or\s+)?(?:resume|reuse|continue|run|create|execute|launch|begin|operate|manage|complete))*\b/gi,
      ''
    )
    .replace(/\binstead of creating\b/gi, '')
    .replace(/\bpreparation-only\b/gi, '')
    .replace(/\bnot\s+(?:launching|executing|running|creating)\b/gi, '')
    .trim();
  return !EXECUTION_VERB_RE.test(stripped);
}

function clauseMatchesCreate(clause) {
  if (isVerbNegatedInClause(clause, 'create')) return false;
  return CREATE_CLAUSE_RES.some((re) => re.test(clause));
}

function clauseMatchesResume(clause) {
  if (isVerbNegatedInClause(clause, ['resume', 'reuse', 'continue'])) return false;
  return RESUME_CLAUSE_RES.some((re) => re.test(clause));
}

function clauseMatchesContinue(clause) {
  if (isVerbNegatedInClause(clause, 'continue')) return false;
  return CONTINUE_CLAUSE_RES.some((re) => re.test(clause));
}

/**
 * @param {string} text
 * @returns {{ intent: string, reason: string|null, clauses: string[] }}
 */
function resolveMissionLifecycleIntent(text) {
  const q = normalizeText(text);
  if (!q) {
    return {
      intent: MissionLifecycleIntent.UNSPECIFIED,
      reason: 'empty',
      clauses: [],
    };
  }

  const clauses = splitClauses(q);
  let hasCreate = false;
  let hasResume = false;
  let hasContinue = false;

  for (const clause of clauses) {
    if (clauseMatchesCreate(clause)) hasCreate = true;
    if (clauseMatchesResume(clause)) hasResume = true;
    if (clauseMatchesContinue(clause)) hasContinue = true;
  }

  if (hasCreate) {
    return {
      intent: MissionLifecycleIntent.CREATE_NEW,
      reason: 'explicit_create',
      clauses,
    };
  }
  if (hasResume) {
    return {
      intent: MissionLifecycleIntent.RESUME_EXISTING,
      reason: 'explicit_resume',
      clauses,
    };
  }
  if (hasContinue) {
    return {
      intent: MissionLifecycleIntent.CONTINUE_ACTIVE,
      reason: 'explicit_continue',
      clauses,
    };
  }

  return {
    intent: MissionLifecycleIntent.UNSPECIFIED,
    reason: 'no_explicit_lifecycle',
    clauses,
  };
}

function isExplicitLifecycleIntent(intent) {
  return (
    intent === MissionLifecycleIntent.CREATE_NEW ||
    intent === MissionLifecycleIntent.RESUME_EXISTING ||
    intent === MissionLifecycleIntent.CONTINUE_ACTIVE
  );
}

module.exports = {
  MissionLifecycleIntent,
  LIFECYCLE_VERBS,
  EXECUTION_VERB_RE,
  normalizeText,
  splitClauses,
  isVerbNegatedInClause,
  isPureNegationClause,
  resolveMissionLifecycleIntent,
  isExplicitLifecycleIntent,
};
