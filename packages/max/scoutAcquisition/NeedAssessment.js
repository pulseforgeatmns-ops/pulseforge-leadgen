'use strict';

/**
 * SPEC-100 — Max decides whether Scout is needed.
 * Existing durable intelligence is preferred over a new investigation.
 */

const {
  asText,
  DEFAULT_FRESHNESS_MS,
  SCOUT_SPECIALIST,
  SCOUT_CAPABILITY,
} = require('./Types');
const { criteriaFingerprint } = require('./BoundedContext');

const ACQUISITION_NEED_RE =
  /\b(where should we (?:be )?look|find more(?:\s+\w+){0,6}\s+(?:worth pursuing|like|opportunit)|find commercial(?:[ -]cleaning)? opportunit|looking for (?:commercial|more)|stronger opportunit|enough evidence to prioritize|what(?:'s| has) changed in (?:our )?(?:target )?market|where should we look)\b/i;

const EXPLAIN_RE =
  /\bwhy did (?:the )?acquisition (?:move|elevate|change|go up)|why is acquisition elevated|why did acquisition move\b/i;

const FOLLOWUP_RE =
  /\b(which (?:four|\d+)|why is (?:this|that)(?: one)? strongest|what don'?t we know|find more like|pursue these before|number (?:two|2)|more like)\b/i;

const INSPECTION_RE =
  /\b(what did scout(?: actually)? investigate|how thorough|why did (?:he|scout) find nothing|how many compan(?:y|ies)|what eliminated|where was (?:scout'?s? )?coverage weak|do you trust|what would you investigate next|how (?:complete|deep) was (?:the|this) (?:search|investigation)|why couldn'?t (?:scout|he)|what geographic information|what did you give (?:him|scout)|why (?:didn'?t|did) you elevate|why weren'?t (?:those|these) evaluated)\b/i;

function looksLikeAcquisitionQuestion(question, context = {}) {
  const q = String(question || '').trim();
  if (!q) return false;
  const domainId = String(context.domainId || context.domain || '').toLowerCase();
  const action = String(context.action || '').toLowerCase();
  if (domainId === 'acquisition' && (action === 'discuss_with_max' || action === 'explain_elevation')) {
    return true;
  }
  if (context.acquisitionLoop || context.lastScoutEvaluation) return true;
  if (
    EXPLAIN_RE.test(q) ||
    FOLLOWUP_RE.test(q) ||
    INSPECTION_RE.test(q) ||
    ACQUISITION_NEED_RE.test(q)
  ) {
    return true;
  }
  return false;
}

function looksLikeExplainPriority(question) {
  return EXPLAIN_RE.test(String(question || ''));
}

function looksLikeFollowUp(question) {
  return FOLLOWUP_RE.test(String(question || '')) || looksLikeInvestigationInspection(question);
}

function looksLikeInvestigationInspection(question) {
  return INSPECTION_RE.test(String(question || ''));
}

function looksLikeFindMoreLike(question) {
  return /\bfind more like|more like number\b/i.test(String(question || ''));
}

function normalizeObjectiveKey(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function objectivesSimilar(a, b) {
  const left = normalizeObjectiveKey(a);
  const right = normalizeObjectiveKey(b);
  if (!left || !right) return false;
  if (left === right) return true;
  const tokens = (s) => new Set(s.split(/\s+/).filter((t) => t.length > 3));
  const aSet = tokens(left);
  const bSet = tokens(right);
  let overlap = 0;
  for (const t of aSet) {
    if (bSet.has(t)) overlap += 1;
  }
  const denom = Math.max(aSet.size, bSet.size, 1);
  return overlap / denom >= 0.5;
}

/**
 * @param {object} input
 * @returns {{ needed: boolean, reason: string, reuse: object|null }}
 */
function assessScoutNeed(input = {}) {
  const question = String(input.question || '');
  const existing = input.existingIntelligence || null;
  const recent = Array.isArray(input.recentResults) ? input.recentResults : [];
  const freshnessMs = input.freshnessMs != null ? Number(input.freshnessMs) : DEFAULT_FRESHNESS_MS;
  const now = input.now != null ? Number(input.now) : Date.now();
  const fingerprint = criteriaFingerprint(input.targetContext, input.businessContext);
  const objective = asText(input.objective) || question;

  if (looksLikeExplainPriority(question)) {
    return {
      needed: false,
      reason: 'Operator asked why Acquisition moved — explain existing evaluation.',
      reuse: existing,
      kind: 'explain',
    };
  }

  if (looksLikeInvestigationInspection(question)) {
    const latestWithInvestigation = recent.find(
      (row) => row && row.payload && row.payload.investigation
    );
    return {
      needed: false,
      reason: 'Operator asked about prior specialist work — inspect the cognitive trace, do not rerun.',
      reuse: latestWithInvestigation || existing,
      kind: 'inspect',
    };
  }

  if (looksLikeFollowUp(question) && !looksLikeFindMoreLike(question) && existing) {
    return {
      needed: false,
      reason: 'Follow-up can be answered from accepted acquisition intelligence.',
      reuse: existing,
      kind: 'followup',
    };
  }

  const matching = recent.filter((row) => {
    if (!row) return false;
    if (row.specialist && row.specialist !== SCOUT_SPECIALIST) return false;
    if (row.capability && row.capability !== SCOUT_CAPABILITY) return false;
    const completedAt = new Date(row.completedAt || row.createdAt || 0).getTime();
    if (!completedAt || now - completedAt > freshnessMs) return false;
    if (row.status && !['completed', 'partial'].includes(row.status)) return false;
    const rowFp =
      row.criteriaFingerprint ||
      criteriaFingerprint(row.targetContext, row.businessContext);
    if (rowFp && fingerprint && rowFp === fingerprint) return true;
    return objectivesSimilar(row.objective, objective);
  });

  if (matching.length && !looksLikeFindMoreLike(question)) {
    const latest = matching[0];
    const evidenceCount =
      (latest.evidenceRefs && latest.evidenceRefs.length) ||
      (latest.artifactRefs && latest.artifactRefs.length) ||
      0;
    if (evidenceCount > 0 || latest.status === 'completed') {
      return {
        needed: false,
        reason: 'Recent Scout acquisition intelligence is still sufficient.',
        reuse: latest,
        kind: 'reuse',
      };
    }
  }

  if (existing && existing.sufficient === true && !looksLikeFindMoreLike(question)) {
    return {
      needed: false,
      reason: 'Existing durable intelligence already answers the objective.',
      reuse: existing,
      kind: 'reuse',
    };
  }

  if (!looksLikeAcquisitionQuestion(question, input.context) && !input.force) {
    return {
      needed: false,
      reason: 'Question is not an acquisition intelligence need.',
      reuse: existing,
      kind: 'unrelated',
    };
  }

  return {
    needed: true,
    reason:
      asText(input.reason) ||
      'Current pipeline intelligence is insufficient for the operator objective.',
    reuse: null,
    kind: looksLikeFindMoreLike(question) ? 'followup_delegate' : 'investigate',
  };
}

module.exports = {
  ACQUISITION_NEED_RE,
  EXPLAIN_RE,
  FOLLOWUP_RE,
  INSPECTION_RE,
  looksLikeAcquisitionQuestion,
  looksLikeExplainPriority,
  looksLikeFollowUp,
  looksLikeInvestigationInspection,
  looksLikeFindMoreLike,
  objectivesSimilar,
  assessScoutNeed,
};
