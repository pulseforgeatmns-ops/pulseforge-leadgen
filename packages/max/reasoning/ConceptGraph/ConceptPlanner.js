'use strict';

/**
 * SPEC-152 — Concept Planner.
 * Translates operator questions into concept sets and reasoning goals.
 */

const { THINKING_MODES } = require('../../operatorCognition/ThinkingModes');
const { listSpecialistNames } = require('../../identity/OperatingModel');
const { normalizeId } = require('./ConceptGraph');

const REASONING_GOALS = Object.freeze({
  EXPLAIN_IDENTITY: 'explain_identity',
  EXPLAIN_AUTHORITY: 'explain_authority',
  COMPARE_ROLES: 'compare_roles',
  RESOLVE_CONFLICT: 'resolve_conflict',
  EXPLAIN_BOUNDARIES: 'explain_boundaries',
  EXPLAIN_FAILURE_MODES: 'explain_failure_modes',
  EXPLAIN_RELATIONSHIPS: 'explain_relationships',
  EXPLAIN_SPECIALIZATION: 'explain_specialization',
  EXPLAIN_DEPENDENCY: 'explain_dependency',
});

const SPECIALIST_PATTERN = new RegExp(
  String.raw`\b(${['max', ...listSpecialistNames()].join('|')})\b`,
  'gi'
);

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function uniqueConcepts(values) {
  const seen = new Set();
  const out = [];
  for (const value of values || []) {
    const id = normalizeId(value);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(id);
  }
  return out;
}

function mergeConcepts(existing, next) {
  return uniqueConcepts([...(existing || []), ...(next || [])]);
}

function extractSpecialists(question) {
  const q = normalizeText(question);
  const matches = q.match(SPECIALIST_PATTERN);
  if (!matches) return [];
  return uniqueConcepts(matches.map((m) => m.toLowerCase()));
}

function parseResolvedQuestion(resolvedQuestion) {
  const rq = normalizeText(resolvedQuestion);
  const whyMatch = rq.match(/^why\(([^)]+)\)$/i);
  if (whyMatch) {
    return { kind: 'why', subject: whyMatch[1].toLowerCase() };
  }
  const compareMatch = rq.match(/^compare\(([^)]+)\)$/i);
  if (compareMatch) {
    return {
      kind: 'compare',
      objects: compareMatch[1].split(',').map((o) => o.trim().toLowerCase()).filter(Boolean),
    };
  }
  const explainMatch = rq.match(/^explain\(([^)]+)\)$/i);
  if (explainMatch) {
    return { kind: 'explain', subject: explainMatch[1].split(':')[0].toLowerCase() };
  }
  return null;
}

function classifyGoal(question, parsed = null) {
  const q = normalizeText(question).toLowerCase();

  if (/\b(?:disagree|disagreed|disagrees|conflict|who wins|who decides when)\b/.test(q)) {
    return REASONING_GOALS.RESOLVE_CONFLICT;
  }
  if (/\b(?:who ultimately decides|who decides|who can approve|who can)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_AUTHORITY;
  }
  if (/\b(?:can scout approve|can paige approve|can .* approve outreach)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_AUTHORITY;
  }
  if (/\b(?:why not|why can't|why shouldn't)\b/.test(q) && /\b(?:approve|outreach|decide)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_AUTHORITY;
  }
  if (/\b(?:depend on each other|what happens if one fails|relationship between)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_DEPENDENCY;
  }
  if (/\b(?:when should i ignore|should i ignore)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_FAILURE_MODES;
  }
  if (/\b(?:what should never belong|never do|boundaries)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_BOUNDARIES;
  }
  if (/\b(?:why shouldn't scout do|why can't scout do|why not merge scout|separate specialists)\b/.test(q)) {
    return REASONING_GOALS.COMPARE_ROLES;
  }
  if (/\b(?:different from|vs\.?|versus|compare|difference between)\b/.test(q)) {
    return REASONING_GOALS.COMPARE_ROLES;
  }
  if (parsed && parsed.kind === 'compare') {
    return REASONING_GOALS.COMPARE_ROLES;
  }
  if (parsed && parsed.kind === 'why') {
    if (parsed.subject === 'identity' || parsed.subject === 'max') {
      return REASONING_GOALS.EXPLAIN_IDENTITY;
    }
    if (parsed.subject === 'authority') {
      return REASONING_GOALS.EXPLAIN_AUTHORITY;
    }
    return REASONING_GOALS.EXPLAIN_SPECIALIZATION;
  }
  if (/^why\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_IDENTITY;
  }
  if (/\b(?:how do .* depend|depend on)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_DEPENDENCY;
  }
  if (/\b(?:relationship|coordinate|coordinates)\b/.test(q)) {
    return REASONING_GOALS.EXPLAIN_RELATIONSHIPS;
  }

  return REASONING_GOALS.EXPLAIN_IDENTITY;
}

function conceptsForGoal(goal, question, parsed = null, specialists = []) {
  const base = [];

  switch (goal) {
    case REASONING_GOALS.EXPLAIN_AUTHORITY:
      base.push('authority', 'operator', 'max', 'business_decisions', 'outreach_approval');
      break;
    case REASONING_GOALS.COMPARE_ROLES:
      base.push('max', 'specialization', 'purpose', 'authority');
      if (specialists.length) base.push(...specialists);
      else base.push('scout');
      break;
    case REASONING_GOALS.RESOLVE_CONFLICT:
      base.push('conflict', 'governance', 'operator', 'authority', 'mission', 'max');
      if (specialists.length >= 2) base.push(...specialists.slice(0, 2));
      else base.push('scout', 'paige');
      break;
    case REASONING_GOALS.EXPLAIN_BOUNDARIES:
      base.push('boundaries', 'max', 'authority', 'operator');
      break;
    case REASONING_GOALS.EXPLAIN_FAILURE_MODES:
      base.push('boundaries', 'authority', 'operator', 'principles');
      break;
    case REASONING_GOALS.EXPLAIN_DEPENDENCY:
      if (specialists.length >= 2) base.push(...specialists.slice(0, 2));
      else base.push('scout', 'paige');
      base.push('max', 'mission', 'governance');
      break;
    case REASONING_GOALS.EXPLAIN_RELATIONSHIPS:
      if (specialists.length) base.push(...specialists);
      else base.push('scout', 'paige');
      base.push('max', 'operator', 'governance');
      break;
    case REASONING_GOALS.EXPLAIN_SPECIALIZATION:
      if (specialists.length) base.push(...specialists);
      base.push('specialization', 'max', 'purpose');
      break;
    case REASONING_GOALS.EXPLAIN_IDENTITY:
    default:
      base.push('identity', 'purpose', 'max');
      if (parsed && parsed.subject && parsed.subject !== 'identity') {
        base.push(parsed.subject);
      }
      break;
  }

  return uniqueConcepts(base);
}

/**
 * @param {object} input
 * @returns {object|null}
 */
function planConceptQuery(input = {}) {
  const question = normalizeText(input.question);
  const resolvedQuestion = normalizeText(input.resolvedQuestion);
  const conversationIntent = input.conversationIntent || null;
  const activeConcepts = input.activeConcepts || null;
  const continuity = Boolean(conversationIntent && conversationIntent.continuity);

  if (!question && !resolvedQuestion) return null;

  const parsed = resolvedQuestion ? parseResolvedQuestion(resolvedQuestion) : null;
  const specialists = extractSpecialists(question);
  if (parsed && parsed.kind === 'compare' && parsed.objects) {
    specialists.splice(0, specialists.length, ...parsed.objects);
  }
  if (conversationIntent && conversationIntent.compareObjects) {
    for (const obj of conversationIntent.compareObjects) {
      specialists.push(obj);
    }
  }

  const goal = classifyGoal(question || resolvedQuestion, parsed);
  let concepts = conceptsForGoal(goal, question, parsed, uniqueConcepts(specialists));

  if (activeConcepts && activeConcepts.length) {
    concepts = mergeConcepts(activeConcepts, concepts);
  }

  if (continuity && goal === REASONING_GOALS.EXPLAIN_IDENTITY) {
    concepts = mergeConcepts(concepts, ['purpose', 'specialization']);
  }

  if (/\bbusiness decisions\b/i.test(question)) {
    concepts = mergeConcepts(concepts, ['business_decisions', 'operator', 'scout', 'max', 'market_discovery']);
  }

  const plan = {
    concepts,
    goal,
    via: parsed ? 'resolved_question' : continuity ? 'continuity' : 'direct_classification',
    continuity,
    parsed,
    specialists: uniqueConcepts(specialists),
  };

  if (conversationIntent && conversationIntent.intent === THINKING_MODES.COMPARE) {
    plan.goal = REASONING_GOALS.COMPARE_ROLES;
  }
  if (conversationIntent && conversationIntent.intent === THINKING_MODES.CHALLENGE) {
    plan.goal = REASONING_GOALS.EXPLAIN_FAILURE_MODES;
  }

  return plan;
}

function shouldUseConceptGraphReasoning(input = {}) {
  const question = normalizeText(input.question);
  const resolvedQuestion = normalizeText(input.resolvedQuestion);
  const conversationIntent = input.conversationIntent || null;

  if (resolvedQuestion && parseResolvedQuestion(resolvedQuestion)) return true;

  const plan = planConceptQuery(input);
  if (!plan) return false;

  if (conversationIntent && conversationIntent.continuity) return true;
  if (conversationIntent && conversationIntent.thinkingMode === 'operating_model_reflection') return true;
  if (conversationIntent && conversationIntent.thinkingMode === 'concept_graph_reasoning') return true;
  if (/^why\b/i.test(question)) return true;
  if (
    /\b(?:different from|vs\.?|versus|compare|who decides|who ultimately decides|who wins|disagree|depend on|approve outreach|can scout approve|can paige approve|should never belong|ignore your advice|scout disagrees|who can)\b/i.test(
      question
    )
  ) {
    return true;
  }

  return false;
}

module.exports = {
  REASONING_GOALS,
  planConceptQuery,
  shouldUseConceptGraphReasoning,
  parseResolvedQuestion,
  classifyGoal,
  mergeConcepts,
  extractSpecialists,
};
