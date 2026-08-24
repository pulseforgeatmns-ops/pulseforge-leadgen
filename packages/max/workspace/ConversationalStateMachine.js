'use strict';

/**
 * SPEC-150 — Conversational State Machine.
 *
 * Session-scoped conversation continuity — not durable memory.
 * Answers: "What are we talking about?" across turns.
 *
 * Business continuity (mission stage, evidence) is separate from
 * conversational continuity (subject, intent, active object, depth).
 */

const { CONVERSATION_SUBJECTS } = require('./ConversationSubject');
const { WORKSPACE_OWNERS } = require('./WorkspaceOwnershipResolver');
const { THINKING_MODES, thinkingModeCategory } = require('../operatorCognition/ThinkingModes');
const { attachSpecialists } = require('../operatorCognition/OperatorCognition');
const { mergeConcepts } = require('../reasoning/ConceptGraph');

const SPECIALIST_NAMES =
  'scout|paige|emmett|max|riley|sam|link|faye|ivy|cal|vera|rex|penny';

const CONVERSATIONAL_MODES = Object.freeze({
  REFLECTION: 'reflection',
  EXPLANATION: 'explanation',
  COMPARISON: 'comparison',
  INSPECTION: 'inspection',
  STRATEGY: 'strategy',
  EXECUTION: 'execution',
  EDUCATION: 'education',
  EXPLORATION: 'exploration',
  CONTINUATION: 'continuation',
  /** SPEC-151 — operating model reflection (why/how/compare/should/when). */
  OPERATING_MODEL_REFLECTION: 'operating_model_reflection',
  /** SPEC-152 — concept graph reasoning over relationships. */
  CONCEPT_GRAPH_REASONING: 'concept_graph_reasoning',
});

const SUBJECT_TO_OWNER = Object.freeze({
  [CONVERSATION_SUBJECTS.IDENTITY]: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
  [CONVERSATION_SUBJECTS.REFLECTION]: WORKSPACE_OWNERS.REFLECTION,
  [CONVERSATION_SUBJECTS.KNOWLEDGE]: WORKSPACE_OWNERS.KNOWLEDGE_RETRIEVAL,
  [CONVERSATION_SUBJECTS.CONVERSATION]: WORKSPACE_OWNERS.CONVERSATION_LAYER,
  [CONVERSATION_SUBJECTS.MISSION]: WORKSPACE_OWNERS.MISSION_INSPECTION,
  [CONVERSATION_SUBJECTS.SPECIALIST]: WORKSPACE_OWNERS.SPECIALIST_INTERROGATION,
  [CONVERSATION_SUBJECTS.BUSINESS]: WORKSPACE_OWNERS.REASONING,
});

const EXPLICIT_SUBJECT_CHANGE_RES = [
  /\b(?:new question|different topic|unrelated|separate question|change subject|switch to)\b/i,
  /\b(?:what is|what'?s) (?:your|max'?s?) role\b/i,
  /\bwho are you\b/i,
  /\bwhat is our icp\b/i,
  /\bideal customer\b/i,
  /\bwhy did you answer\b/i,
  /\bdid you misunderstand me\b/i,
  /\bteach me\b/i,
  /\bwhere are we\b/i,
  /\bmission status\b/i,
  new RegExp(String.raw`\bwhy did (?:${SPECIALIST_NAMES})\b`, 'i'),
];

const BARE_FOLLOWUP_RES = [
  /^why\b/i,
  /^how\b/i,
  /^what\b/i,
  /^and\b/i,
  /^explain\b/i,
  /^tell me more\b/i,
  /^go on\b/i,
  /^continue talking\b/i,
  /^elaborate\b/i,
];

const COMPARE_FOLLOWUP_RES = [
  /\b(?:different from|differs from|compare(?:d)? to|compared to|vs\.?|versus)\b/i,
  /\bhow is (?:that|this|it) different\b/i,
  /\bwhat(?:'s| is) the difference\b/i,
];

const PRONOUN_REFERENCE_RE = /\b(?:that|this|it|those|these)\b/i;

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function tokenize(text) {
  return normalizeText(text)
    .toLowerCase()
    .replace(/[^a-z0-9'\s]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function getConversationalState(session) {
  if (session && session.conversationalState && typeof session.conversationalState === 'object') {
    return session.conversationalState;
  }
  const ctx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : null;
  const state = ctx && ctx.conversationalState;
  if (!state || typeof state !== 'object') return null;
  return state;
}

function setConversationalState(session, state) {
  if (!session || typeof session !== 'object') return;
  session.conversationalState = state;
  if (session.context && typeof session.context === 'object') {
    session.context.conversationalState = state;
  }
}

function ownerForSubject(subject) {
  return SUBJECT_TO_OWNER[subject] || WORKSPACE_OWNERS.REASONING;
}

function modeFromIntent(intent) {
  switch (intent) {
    case THINKING_MODES.REFLECT:
      return CONVERSATIONAL_MODES.REFLECTION;
    case THINKING_MODES.EXPLAIN:
      return CONVERSATIONAL_MODES.EXPLANATION;
    case THINKING_MODES.COMPARE:
      return CONVERSATIONAL_MODES.COMPARISON;
    case THINKING_MODES.INSPECT:
      return CONVERSATIONAL_MODES.INSPECTION;
    case THINKING_MODES.STRATEGY:
      return CONVERSATIONAL_MODES.STRATEGY;
    case THINKING_MODES.EXECUTE:
    case THINKING_MODES.EDIT:
      return CONVERSATIONAL_MODES.EXECUTION;
    case THINKING_MODES.TEACH:
      return CONVERSATIONAL_MODES.EDUCATION;
    case THINKING_MODES.BRAINSTORM:
      return CONVERSATIONAL_MODES.EXPLORATION;
    case THINKING_MODES.RESUME:
      return CONVERSATIONAL_MODES.CONTINUATION;
    case THINKING_MODES.CHALLENGE:
      return CONVERSATIONAL_MODES.REFLECTION;
    case THINKING_MODES.OPERATING_MODEL:
      return CONVERSATIONAL_MODES.OPERATING_MODEL_REFLECTION;
    case THINKING_MODES.CONCEPT_GRAPH:
      return CONVERSATIONAL_MODES.CONCEPT_GRAPH_REASONING;
    default:
      return CONVERSATIONAL_MODES.EXPLANATION;
  }
}

function deriveActiveObject(subject, question, priorState = null) {
  const q = normalizeText(question).toLowerCase();

  if (subject === CONVERSATION_SUBJECTS.IDENTITY) {
    return 'max';
  }

  const specialistMatch = q.match(
    new RegExp(String.raw`\b(${SPECIALIST_NAMES})\b`, 'i')
  );
  if (specialistMatch) {
    return specialistMatch[1].toLowerCase();
  }

  if (subject === CONVERSATION_SUBJECTS.MISSION) {
    return priorState && priorState.activeObject === 'mission'
      ? 'mission'
      : 'mission';
  }

  if (priorState && priorState.activeObject) {
    return priorState.activeObject;
  }

  return null;
}

function extractCompareObjects(question, priorState) {
  const q = normalizeText(question);
  const objects = [];
  const seen = new Set();

  function addObject(name) {
    const normalized = String(name || '').toLowerCase();
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    objects.push(normalized);
  }

  if (priorState && priorState.activeObject) {
    addObject(priorState.activeObject);
  }

  const specialistMatches = q.match(
    new RegExp(String.raw`\b(${SPECIALIST_NAMES})\b`, 'gi')
  );
  if (specialistMatches) {
    for (const match of specialistMatches) {
      addObject(match);
    }
  }

  const fromMatch = q.match(/\bdifferent from\s+([a-z][a-z0-9]*)/i);
  if (fromMatch) {
    addObject(fromMatch[1]);
  }

  const vsMatch = q.match(/\bvs\.?\s+([a-z][a-z0-9]*)/i);
  if (vsMatch) {
    addObject(vsMatch[1]);
  }

  return objects.length >= 2 ? objects : objects.length ? objects : null;
}

function isExplicitSubjectChange(question) {
  return matchesAny(normalizeText(question), EXPLICIT_SUBJECT_CHANGE_RES);
}

function isIdentityOperatingModelFollowUp(question, priorState) {
  if (!priorState || priorState.subject !== CONVERSATION_SUBJECTS.IDENTITY) {
    return false;
  }
  const q = normalizeText(question);
  if (isExplicitSubjectChange(q)) return false;

  const identityFollowUpPatterns = [
    /^why\b/i,
    /\bwhen should i ignore\b/i,
    /\bwhy shouldn'?t scout\b/i,
    /\bhow is (?:that|this|it|max|you) different\b/i,
    /\bhow are you different\b/i,
    /\bwhat should never belong\b/i,
    /\bwhat decisions require me\b/i,
    /\bwhy (?:not merge|separate specialists)\b/i,
    /\b(?:who decides|who ultimately decides|who wins|who can)\b/i,
    /\bcan scout approve\b/i,
    /\bwhy not\b/i,
    /\bexplain\b/i,
    /\bvs\.?\s+(?:scout|paige|rex|emmett|sam|riley|cal|vera|max)\b/i,
    /\bdifferent from\s+(?:scout|paige|rex|emmett|sam|riley|cal|vera)\b/i,
  ];
  return matchesAny(q, identityFollowUpPatterns);
}

function isContinuityFollowUp(question, priorState) {
  if (!priorState || !priorState.subject) return false;

  const q = normalizeText(question);
  if (!q) return false;
  if (isExplicitSubjectChange(q)) return false;

  if (isIdentityOperatingModelFollowUp(q, priorState)) {
    return true;
  }

  const tokens = tokenize(q);

  if (matchesAny(q, COMPARE_FOLLOWUP_RES)) {
    return PRONOUN_REFERENCE_RE.test(q) || tokens.length <= 12;
  }

  if (tokens.length <= 4 && matchesAny(q, BARE_FOLLOWUP_RES)) {
    return true;
  }

  if (PRONOUN_REFERENCE_RE.test(q) && tokens.length <= 10) {
    return true;
  }

  return false;
}

function resolveContinuityIntent(question, priorState) {
  const q = normalizeText(question);

  if (matchesAny(q, COMPARE_FOLLOWUP_RES)) {
    return {
      intent: THINKING_MODES.COMPARE,
      via: 'conversation_continuity_compare',
      confidence: 0.94,
      objects: extractCompareObjects(q, priorState),
    };
  }

  if (/^why\b/i.test(q) || /\bwhy (?:that|this|it)\b/i.test(q)) {
    if (/\bwhy shouldn'?t scout do (?:your|max'?s?) job\b/i.test(q)) {
      return {
        intent: THINKING_MODES.COMPARE,
        via: 'conversation_continuity_scout_job',
        confidence: 0.93,
        objects: ['max', 'scout'],
      };
    }
    return {
      intent: THINKING_MODES.EXPLAIN,
      via: 'conversation_continuity_why',
      confidence: 0.93,
    };
  }

  if (/^how\b/i.test(q) && !matchesAny(q, COMPARE_FOLLOWUP_RES)) {
    return {
      intent: THINKING_MODES.EXPLAIN,
      via: 'conversation_continuity_how',
      confidence: 0.88,
    };
  }

  if (/^what\b/i.test(q) && PRONOUN_REFERENCE_RE.test(q)) {
    return {
      intent: THINKING_MODES.EXPLAIN,
      via: 'conversation_continuity_what',
      confidence: 0.86,
    };
  }

  return {
    intent: priorState.lastIntent || THINKING_MODES.EXPLAIN,
    via: 'conversation_continuity_inherit',
    confidence: 0.82,
  };
}

function buildSubjectResult(subject, reason, confidence, locked = false) {
  return {
    subject,
    confidence,
    reason,
    locked,
    via: reason,
  };
}

function buildResolvedQuestion(question, priorState, resolved) {
  const q = normalizeText(question);
  const subject = resolved.subject || priorState.subject;
  const intent = resolved.intent;

  if (/^why\b/i.test(q)) {
    return `why(${subject})`;
  }

  if (matchesAny(q, COMPARE_FOLLOWUP_RES)) {
    const objects =
      resolved.objects && resolved.objects.length
        ? resolved.objects.join(',')
        : priorState.activeObject || subject;
    return `compare(${objects})`;
  }

  if (intent === THINKING_MODES.EXPLAIN && PRONOUN_REFERENCE_RE.test(q)) {
    return `explain(${subject}:${priorState.activeObject || subject})`;
  }

  return q;
}

/**
 * Apply conversational continuity before routing.
 * Follow-ups inherit subject/intent instead of being classified from scratch.
 *
 * @param {object} input
 * @returns {object}
 */
function applyConversationalContinuity(input = {}) {
  const question = normalizeText(input.question);
  const session = input.session || null;
  const priorState = getConversationalState(session);
  let conversationSubject = input.conversationSubject || null;
  let conversationIntent = input.conversationIntent || null;

  if (!priorState || !isContinuityFollowUp(question, priorState)) {
    return {
      applied: false,
      conversationSubject,
      conversationIntent,
      resolvedQuestion: question,
      priorState,
    };
  }

  const continuityIntent = resolveContinuityIntent(question, priorState);
  const inheritedSubject = priorState.subject;
  const locked =
    inheritedSubject === CONVERSATION_SUBJECTS.IDENTITY ||
    inheritedSubject === CONVERSATION_SUBJECTS.REFLECTION ||
    inheritedSubject === CONVERSATION_SUBJECTS.KNOWLEDGE ||
    inheritedSubject === CONVERSATION_SUBJECTS.CONVERSATION;

  conversationSubject = buildSubjectResult(
    inheritedSubject,
    'conversation_continuity',
    Math.max(priorState.confidence || 0.85, 0.85),
    locked
  );

  conversationIntent = attachSpecialists({
    intent:
      inheritedSubject === CONVERSATION_SUBJECTS.IDENTITY
        ? THINKING_MODES.OPERATING_MODEL
        : continuityIntent.intent,
    confidence: continuityIntent.confidence,
    mutatesMission: false,
    thinkingMode:
      inheritedSubject === CONVERSATION_SUBJECTS.IDENTITY
        ? 'operating_model_reflection'
        : thinkingModeCategory(continuityIntent.intent),
    via: continuityIntent.via,
    specialists: null,
    continuity: true,
    inheritedFrom: {
      subject: priorState.subject,
      intent: priorState.lastIntent,
      depth: priorState.depth,
    },
    compareObjects: continuityIntent.objects || null,
    underlyingIntent: continuityIntent.intent,
  });

  const resolvedQuestion = buildResolvedQuestion(question, priorState, {
    subject: inheritedSubject,
    intent: continuityIntent.intent,
    objects: continuityIntent.objects,
  });

  if (session && session.context && typeof session.context === 'object') {
    session.context.resolvedQuestion = resolvedQuestion;
    session.context.conversationContinuityApplied = true;
  }

  return {
    applied: true,
    conversationSubject,
    conversationIntent,
    resolvedQuestion,
    priorState,
    compareObjects: continuityIntent.objects || null,
    owner: priorState.owner || ownerForSubject(inheritedSubject),
  };
}

/**
 * Advance conversational state after a turn completes.
 *
 * @param {object} session
 * @param {object} turn
 * @returns {object|null}
 */
function advanceConversationalState(session, turn = {}) {
  if (!session || !session.context || typeof session.context !== 'object') {
    return null;
  }

  const conversationSubject = turn.conversationSubject || null;
  const conversationIntent = turn.conversationIntent || null;
  const workspaceOwnership = turn.workspaceOwnership || null;
  const question = normalizeText(turn.question);
  const priorState = getConversationalState(session);

  if (!conversationSubject || !conversationSubject.subject) {
    return priorState;
  }

  const subject = conversationSubject.subject;
  const subjectChanged = Boolean(
    priorState &&
      priorState.subject &&
      priorState.subject !== subject &&
      !turn.continuityApplied
  );
  const intent = conversationIntent && conversationIntent.intent
    ? conversationIntent.intent
    : priorState && priorState.lastIntent
      ? priorState.lastIntent
      : THINKING_MODES.EXPLAIN;

  const activeObject =
    (conversationIntent &&
      conversationIntent.compareObjects &&
      conversationIntent.compareObjects[0]) ||
    deriveActiveObject(subject, question, priorState);

  const activeConcepts = mergeConcepts(
    priorState && priorState.activeConcepts,
    (turn.structured &&
      turn.structured.metadata &&
      turn.structured.metadata.operatingModelReasoning &&
      turn.structured.metadata.operatingModelReasoning.activeConcepts) ||
      (turn.structured &&
        turn.structured.metadata &&
        turn.structured.metadata.operatingModelReasoning &&
        turn.structured.metadata.operatingModelReasoning.concepts) ||
      null
  );

  const next = {
    subject,
    owner:
      (workspaceOwnership && workspaceOwnership.owner) ||
      (priorState && priorState.owner) ||
      ownerForSubject(subject),
    activeObject,
    activeConcepts: activeConcepts.length ? activeConcepts : priorState && priorState.activeConcepts || null,
    mode: modeFromIntent(intent),
    depth: subjectChanged ? 1 : (priorState && priorState.depth ? priorState.depth + 1 : 1),
    objects:
      (conversationIntent && conversationIntent.compareObjects) ||
      (activeObject ? [activeObject] : priorState && priorState.objects) ||
      null,
    lastQuestion: question,
    lastIntent: intent,
    lastResolvedQuestion: turn.resolvedQuestion || question,
    confidence: conversationSubject.confidence || 0.85,
    updatedAt: new Date().toISOString(),
  };

  setConversationalState(session, next);
  return next;
}

module.exports = {
  CONVERSATIONAL_MODES,
  SUBJECT_TO_OWNER,
  getConversationalState,
  setConversationalState,
  isContinuityFollowUp,
  isExplicitSubjectChange,
  resolveContinuityIntent,
  extractCompareObjects,
  buildResolvedQuestion,
  applyConversationalContinuity,
  advanceConversationalState,
  deriveActiveObject,
  ownerForSubject,
  modeFromIntent,
};
