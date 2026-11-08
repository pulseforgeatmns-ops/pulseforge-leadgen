'use strict';

/**
 * P0 Max reliability — durable authorization constraints.
 *
 * Operator restrictions ("review only", "no outreach", "no spending") become a
 * structured, persisted property of the mission (`mission.authorizationConstraints`)
 * and are enforced at the two places every specialist action must pass:
 *
 *   1. routeExecutionRequest (SPEC-171) — before dispatch, by intent.
 *   2. executeMissionStage   (SPEC-131) — inside the transaction, by specialist
 *      and stage, so autonomous progression and direct callers are covered too.
 *
 * Constraints are monotonic: merging can only add prohibitions. Nothing in
 * conversation, routing confidence or retries can remove one. Classification is
 * lexical and tenant-independent.
 */

const { amoError } = require('./types');

const SPEC = 'SPEC-MAX-RELIABILITY-004';
const AUTHORIZATION_CONSTRAINT_BLOCKED = 'authorization_constraint_blocked';

const ACTION_CLASSES = Object.freeze({
  PLANNING: 'planning',
  PROVIDER_RESEARCH: 'provider_research',
  CONTENT: 'content',
  OUTREACH_PREPARATION: 'outreach_preparation',
  OUTREACH_AUTHORIZATION: 'outreach_authorization',
  OUTREACH: 'outreach',
  PAID_MEDIA: 'paid_media',
});

/** Actions with external effects or spend — all forbidden by "review only". */
const EXTERNAL_ACTION_CLASSES = Object.freeze([
  ACTION_CLASSES.PROVIDER_RESEARCH,
  ACTION_CLASSES.OUTREACH_AUTHORIZATION,
  ACTION_CLASSES.OUTREACH,
  ACTION_CLASSES.PAID_MEDIA,
]);

const REVIEW_ONLY_RE =
  /\b(?:review|preparation|prep|read)[-\s]?only\b|\bfor\s+review\s+only\b|\breview\s+purposes\s+only\b/i;

/** Applied to negated clauses only ("no outreach", "do not mail anything"). */
const PROHIBITION_RULES = Object.freeze([
  {
    id: 'outreach',
    re: /\b(?:outreach|e-?mails?|e-?mailing|mail|mailing|messages?|messaging|calls?|calling|phone|texts?|texting|sms|send|sending|contact(?:ing)?|campaign\s+activation|activat\w*|launch\w*|execut\w*|print\w*)\b/i,
    classes: [ACTION_CLASSES.OUTREACH, ACTION_CLASSES.OUTREACH_AUTHORIZATION],
  },
  {
    id: 'approval',
    re: /\bapprov\w*\b/i,
    classes: [ACTION_CLASSES.OUTREACH_AUTHORIZATION],
  },
  {
    id: 'content',
    re: /\b(?:content|copy|copywriting|drafts?|drafting|posts?|variants?)\b/i,
    classes: [ACTION_CLASSES.CONTENT],
  },
  {
    id: 'spend',
    re: /\b(?:spend\w*|paid|ads?|advertis\w*|budget|money|cost\w*)\b/i,
    classes: [ACTION_CLASSES.PAID_MEDIA, ACTION_CLASSES.PROVIDER_RESEARCH],
  },
  {
    id: 'research',
    re: /\b(?:discovery|research|prospecting|scout\w*|places|searche?s?|enrich\w*)\b/i,
    classes: [ACTION_CLASSES.PROVIDER_RESEARCH],
  },
]);

/**
 * "Do not send without my approval" / "don't launch until approved" make an
 * action conditional on approval — the existing approval gates already enforce
 * that. They are not permanent prohibitions.
 */
const CONDITIONAL_CLAUSE_RE =
  /\b(?:without|until|unless|before)\b.{0,40}\b(?:approv\w*|sign[-\s]?off|confirm\w*|review\w*|authoriz\w*)\b/i;

function negatedClauses(text) {
  // Lazy: the shared negation grammar lives with the Max classifiers.
  return require('../max/workspace/NegatedDirective').extractNegatedClauses(text);
}

function sortedUnique(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

/**
 * Derive constraints from operator language. Returns null when there are none.
 * @param {string} text
 * @param {{ source?: string, at?: string }} [opts]
 */
function extractAuthorizationConstraints(text, opts = {}) {
  const raw = String(text || '');
  if (!raw.trim()) return null;
  const sources = [];
  const prohibited = [];
  const reviewOnlyMatch = raw.match(REVIEW_ONLY_RE);
  if (reviewOnlyMatch) {
    prohibited.push(...EXTERNAL_ACTION_CLASSES);
    sources.push({ phrase: reviewOnlyMatch[0], rule: 'review_only', classes: [...EXTERNAL_ACTION_CLASSES] });
  }
  for (const clause of negatedClauses(raw)) {
    if (CONDITIONAL_CLAUSE_RE.test(clause)) continue;
    for (const rule of PROHIBITION_RULES) {
      if (rule.re.test(clause)) {
        prohibited.push(...rule.classes);
        sources.push({ phrase: clause, rule: rule.id, classes: [...rule.classes] });
      }
    }
  }
  if (!prohibited.length) return null;
  return {
    spec: SPEC,
    version: 1,
    reviewOnly: Boolean(reviewOnlyMatch),
    prohibited: sortedUnique(prohibited),
    sources,
    origin: opts.source || 'operator_instruction',
    establishedAt: opts.at || new Date().toISOString(),
  };
}

/**
 * Monotonic union. Never removes a prohibition.
 * @param {...object|null} parts
 */
function mergeAuthorizationConstraints(...parts) {
  const present = parts.filter((p) => p && typeof p === 'object' && Array.isArray(p.prohibited));
  if (!present.length) return null;
  const establishedAt = present
    .map((p) => p.establishedAt)
    .filter(Boolean)
    .sort()[0] || new Date().toISOString();
  return {
    spec: SPEC,
    version: 1,
    reviewOnly: present.some((p) => p.reviewOnly === true),
    prohibited: sortedUnique(present.flatMap((p) => p.prohibited)),
    sources: present.flatMap((p) => Array.isArray(p.sources) ? p.sources : []),
    origin: present[0].origin || 'operator_instruction',
    establishedAt,
    // Governed amendment history survives later tightening (never rewritten).
    revision: Math.max(0, ...present.map((p) => Number(p.revision) || 0)),
    amendments: uniqueAmendments(present.flatMap((p) => Array.isArray(p.amendments) ? p.amendments : [])),
  };
}

function uniqueAmendments(rows) {
  const seen = new Set();
  return rows.filter((row) => {
    const key = row && row.amendmentId;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** True when `next` adds at least one prohibition to `current`. */
function tightens(current, next) {
  if (!next) return false;
  const have = new Set((current && current.prohibited) || []);
  return next.prohibited.some((cls) => !have.has(cls)) || (next.reviewOnly && !(current && current.reviewOnly));
}

/**
 * @param {string} specialist
 * @param {string} stage
 */
function stageActionClass(specialist, stage) {
  const s = String(specialist || '').toLowerCase();
  const st = String(stage || '').toLowerCase();
  if (s === 'scout') return ACTION_CLASSES.PROVIDER_RESEARCH;
  if (s === 'paige') return ACTION_CLASSES.CONTENT;
  if (s === 'penny') return ACTION_CLASSES.PAID_MEDIA;
  if (s === 'emmett') return st === 'execute' ? ACTION_CLASSES.OUTREACH : ACTION_CLASSES.OUTREACH_PREPARATION;
  if (s === 'operator' && st === 'execution_approval') return ACTION_CLASSES.OUTREACH_AUTHORIZATION;
  return ACTION_CLASSES.PLANNING;
}

const INTENT_ACTION_CLASSES = Object.freeze({
  approve_discovery: ACTION_CLASSES.PROVIDER_RESEARCH,
  start_discovery: ACTION_CLASSES.PROVIDER_RESEARCH,
  continue_investigation: ACTION_CLASSES.PROVIDER_RESEARCH,
  generate_variants: ACTION_CLASSES.CONTENT,
  assess_paid_acquisition: ACTION_CLASSES.PAID_MEDIA,
  generate_capacity: ACTION_CLASSES.OUTREACH_PREPARATION,
  approve_execution: ACTION_CLASSES.OUTREACH_AUTHORIZATION,
  execute_outbound: ACTION_CLASSES.OUTREACH,
});

/** @param {string} intent */
function intentActionClass(intent) {
  // CER intents are upper-case constants (EXECUTE_OUTBOUND); keys are canonical lower-case.
  return INTENT_ACTION_CLASSES[String(intent || '').toLowerCase()] || ACTION_CLASSES.PLANNING;
}

/**
 * @param {object|null} mission
 * @param {string} actionClass
 * @returns {{ allowed: boolean, actionClass: string, constraints: object|null }}
 */
function evaluateAction(mission, actionClass) {
  const constraints = mission && mission.authorizationConstraints;
  const prohibited = new Set((constraints && constraints.prohibited) || []);
  return { allowed: !prohibited.has(actionClass), actionClass, constraints: constraints || null };
}

/**
 * Throw `authorization_constraint_blocked` when the mission forbids the action.
 * @param {object|null} mission
 * @param {string} actionClass
 * @param {object} [extras]
 */
function assertActionAuthorized(mission, actionClass, extras = {}) {
  const verdict = evaluateAction(mission, actionClass);
  if (verdict.allowed) return verdict;
  const err = amoError(
    AUTHORIZATION_CONSTRAINT_BLOCKED,
    `Mission authorization forbids ${actionClass.replace(/_/g, ' ')}` +
      (verdict.constraints && verdict.constraints.reviewOnly ? ' (review-only mission).' : '.')
  );
  err.spec = SPEC;
  err.actionClass = actionClass;
  err.missionId = mission && mission.id;
  err.authorizationConstraints = verdict.constraints;
  Object.assign(err, extras);
  throw err;
}

/**
 * Synthetic (fixture) evidence isolation. A mission whose committed evidence
 * came from synthetic data is stamped `mission.syntheticEvidence` (sticky) and
 * can never authorize or perform outreach, be admitted into the CRM, or feed
 * learning. Internal planning artifacts may still be produced, labelled.
 */
const SYNTHETIC_PROVENANCE = 'synthetic_fixture';
const SYNTHETIC_EVIDENCE_BLOCKED = 'synthetic_evidence_blocked';
const SYNTHETIC_BLOCKED_CLASSES = new Set([ACTION_CLASSES.OUTREACH_AUTHORIZATION, ACTION_CLASSES.OUTREACH]);

/** @param {object|null} output - TME stage output */
function outputIsSynthetic(output) {
  if (!output || typeof output !== 'object') return false;
  const scout = output.scoutResult;
  return Boolean(
    output.provenance === SYNTHETIC_PROVENANCE ||
      (scout && scout.provenance === SYNTHETIC_PROVENANCE) ||
      (scout && scout.payload && scout.payload.provenance === SYNTHETIC_PROVENANCE)
  );
}

/** @param {object|null} mission */
function missionHasSyntheticEvidence(mission) {
  return Boolean(mission && mission.syntheticEvidence && mission.syntheticEvidence.provenance === SYNTHETIC_PROVENANCE);
}

/**
 * Throw `synthetic_evidence_blocked` when a synthetic-evidence mission would
 * authorize or perform outreach.
 * @param {object|null} mission
 * @param {string} actionClass
 * @param {object} [extras]
 */
function assertNotSyntheticForAction(mission, actionClass, extras = {}) {
  if (!missionHasSyntheticEvidence(mission) || !SYNTHETIC_BLOCKED_CLASSES.has(actionClass)) return;
  const err = amoError(
    SYNTHETIC_EVIDENCE_BLOCKED,
    'This mission contains synthetic (fixture) discovery data; it cannot authorize or perform outreach. Start a new mission with live discovery.'
  );
  err.spec = SPEC;
  err.actionClass = actionClass;
  err.missionId = mission && mission.id;
  err.syntheticEvidence = mission.syntheticEvidence;
  Object.assign(err, extras);
  throw err;
}

/** @param {unknown} err */
function isSyntheticEvidenceError(err) {
  if (!err) return false;
  if (err.code === SYNTHETIC_EVIDENCE_BLOCKED) return true;
  return Boolean(err.cause && err.cause.code === SYNTHETIC_EVIDENCE_BLOCKED);
}

/** @param {unknown} err */
function isAuthorizationConstraintError(err) {
  if (!err) return false;
  if (err.code === AUTHORIZATION_CONSTRAINT_BLOCKED) return true;
  return Boolean(err.cause && err.cause.code === AUTHORIZATION_CONSTRAINT_BLOCKED);
}

module.exports = {
  SPEC,
  AUTHORIZATION_CONSTRAINT_BLOCKED,
  ACTION_CLASSES,
  EXTERNAL_ACTION_CLASSES,
  extractAuthorizationConstraints,
  mergeAuthorizationConstraints,
  tightens,
  stageActionClass,
  intentActionClass,
  evaluateAction,
  assertActionAuthorized,
  isAuthorizationConstraintError,
  SYNTHETIC_PROVENANCE,
  SYNTHETIC_EVIDENCE_BLOCKED,
  outputIsSynthetic,
  missionHasSyntheticEvidence,
  assertNotSyntheticForAction,
  isSyntheticEvidenceError,
};
