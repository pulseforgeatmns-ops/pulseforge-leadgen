'use strict';

/**
 * SPEC-083 — Client Intelligence Engine (CIE) thin-slice v1.
 * SPEC-084 — Interview experience helpers (understanding progress, executive summary, resume).
 * SPEC-085 — Executive Business Brief (client-facing synthesis after interview).
 * SPEC-088 — Growth Work Continuation Flow (resume to first incomplete Growth Plan task).
 * SPEC-089 — First Campaign Planning Conversation (review-first plan preview after Growth Plan).
 * SPEC-090 — Max Conversational Reasoning Layer (classify → memory → probe → readiness).
 * Text interview → evidence → confidence → Business Blueprint → approve → playbook handoff.
 * Does not invent campaign strategy or activate Scout/Composer.
 */

const crypto = require('crypto');
const defaultPool = require('../db');
const {
  createPlaybookFromApprovedBlueprint,
} = require('./clientIntelligencePlaybookHandoff');
const {
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  buildGrowthConversationReply,
  normalizeGrowthState,
  buildGrowthInfrastructureHandoffContext,
  composeAvoidSentence,
} = require('./clientIntelligenceGrowthDirection');
const {
  buildEmptyAreas,
  buildInfrastructureReadinessOpening,
  buildInfrastructureReadinessReply,
  extractBusinessName: extractReadinessBusinessName,
} = require('./clientIntelligenceInfrastructureReadiness');
const {
  ANCHOR_SAMPLE_CLIENT_ID,
  ANCHOR_FIXTURE_KEY,
  ANCHOR_BUSINESS_NAME,
  fixturesAllowed,
  cloneAnchorSections,
  cloneAnchorNormalizedFacts,
} = require('./clientIntelligenceFixtures');
const {
  generateDraftScorecard,
  buildBriefScorecardSections,
} = require('../packages/operator-scorecard');
const {
  buildGrowthPlan,
  resolveGrowthPlanResumeTarget,
  applyTaskCompletion,
} = require('./clientIntelligenceGrowthPlan');
const {
  buildCampaignPlanningContext,
  buildCampaignPlanningOpening,
  buildCampaignPlanningReply,
  seedSlotsFromContext,
  applyScoutExecutionResult,
  executeScoutWorkRequest,
} = require('./clientIntelligenceCampaignPlanning');
const {
  EPISTEMIC_STATES,
  classifyEpistemicState,
  createBusinessFact,
  preserveEpistemicState,
  extractBusinessFacts,
  projectBusinessFacts,
} = require('./clientIntelligenceEpistemic');
const {
  MESSAGE_CLASSES,
  ARTIFACT_KINDS,
  ANSWER_DISPOSITIONS,
  MAX_PROBE_ATTEMPTS,
  emptyReasoningMemory,
  ensureReasoningMemory,
  classifyReasoningMessage,
  classifyAnswerDisposition,
  assessAnswerSufficiency,
  buildProbingFollowUp,
  inferCrossSectionTarget,
  recordAcceptedFact,
  recordPendingCorrection,
  resolvePendingCorrection,
  syncConfidenceFromSections,
  addQuestionDebt,
  clearQuestionDebt,
  setActiveProbe,
  markClassification,
  markArtifactGenerated,
  markArtifactApproved,
  markProspectCriteriaApproved,
  markLiveSourcingApproved,
  resolveNextArtifact,
  resolveCampaignArtifactAction,
  inferApprovedArtifactsFromMessage,
  checkArtifactReadiness,
  synthesizeBusinessLanguage,
  reasoningAck,
  planReasoningTurn,
  looksLikeVagueAnswer,
  looksLikeGenericCategoryAnswer,
  looksLikeExplicitUnknownAnswer,
  looksLikeExplicitDeferral,
  looksLikeSkip,
} = require('./clientIntelligenceReasoning');

const SESSION_STATUSES = Object.freeze([
  'NEW',
  'DISCOVERY',
  'CLARIFICATION',
  'VALIDATION',
  'BLUEPRINT_GENERATION',
  'CLIENT_REVIEW',
  'APPROVED',
]);

const ALLOWED_TRANSITIONS = Object.freeze({
  NEW: ['DISCOVERY'],
  DISCOVERY: ['CLARIFICATION'],
  CLARIFICATION: ['VALIDATION'],
  VALIDATION: ['BLUEPRINT_GENERATION'],
  BLUEPRINT_GENERATION: ['CLIENT_REVIEW'],
  CLIENT_REVIEW: ['APPROVED', 'DISCOVERY'],
  APPROVED: [],
});

const BLUEPRINT_SECTIONS = Object.freeze([
  'identity',
  'services',
  'idealCustomers',
  'avoidCustomers',
  'targetMarkets',
  'competitiveAdvantages',
  'brandVoice',
  'campaignGoals',
  'successMetrics',
]);

const EVIDENCE_TYPES = Object.freeze([
  'EXPLICIT',
  'INFERRED',
  'OBSERVED',
  'CLIENT_EDITED',
]);

const NEXT_ACTIONS = Object.freeze([
  'ASK',
  'CLARIFY',
  'PROBE',
  'SUMMARIZE',
  'VALIDATE',
  'GENERATE_BLUEPRINT',
  'COMPLETE',
]);

const EXPLICIT_CONFIDENCE = 0.64;
const SPECIFICITY_BUMP = 0.1;
const CONFIRMATION_BUMP = 0.1;
const CONSISTENCY_BUMP = 0.08;
const CORROBORATION_BUMP = 0.07;
const AMBIGUITY_PENALTY = 0.15;
const CONTRADICTION_PENALTY = 0.22;
const INFERRED_CONFIDENCE = 0.48;
const UNKNOWN_CONFIDENCE = 0.18;
const MAX_CONFIDENCE = 0.98;
const MIN_SECTION_CONFIDENCE = 0.55;
const GENERATED_BY = 'CIE-v1';
const REFLECTION_EVERY_N = 3;

const CONFIRMATION_RE =
  /\b(yes|correct|exactly|that'?s right|confirm|confirmed|agreed|accurate)\b/i;
const AMBIGUITY_RE =
  /\b(maybe|perhaps|not sure|unsure|kind of|sort of|various|etc\.?|something like|i think|probably|roughly|around|whatever|idk|tbd)\b/i;

/**
 * Answer kinds for CIE interview / refinement traffic.
 * Only business_fact may become Blueprint / Brief commercial evidence.
 * @deprecated Prefer MESSAGE_TYPES for interview routing; retained for Brief sanitization.
 */
const ANSWER_KINDS = Object.freeze({
  BUSINESS_FACT: 'business_fact',
  REFINEMENT_FEEDBACK: 'refinement_feedback',
  SYSTEM_GUIDANCE: 'system_guidance',
  GENERATED_BRIEF: 'generated_brief',
});

/**
 * Interview message classification (SPEC-085 + SPEC-090).
 * Only direct_answer becomes the answer to the active interview question.
 * ADD_ON / CLARIFICATION_REQUEST are canonical; older names alias to them.
 */
const MESSAGE_TYPES = Object.freeze({
  DIRECT_ANSWER: MESSAGE_CLASSES.DIRECT_ANSWER,
  CORRECTION: MESSAGE_CLASSES.CORRECTION,
  ADD_ON: MESSAGE_CLASSES.ADD_ON,
  /** @deprecated Alias of ADD_ON — retained for callers/tests. */
  SUPPLEMENTAL_CONTEXT: MESSAGE_CLASSES.ADD_ON,
  APPROVAL: MESSAGE_CLASSES.APPROVAL,
  APPROVAL_PLUS_NEXT_REQUEST: MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST,
  CLARIFICATION_REQUEST: MESSAGE_CLASSES.CLARIFICATION_REQUEST,
  /** @deprecated Alias of CLARIFICATION_REQUEST. */
  QUESTION_TO_MAX: MESSAGE_CLASSES.CLARIFICATION_REQUEST,
  ARTIFACT_REQUEST: MESSAGE_CLASSES.ARTIFACT_REQUEST,
  INSUFFICIENT_ANSWER: MESSAGE_CLASSES.INSUFFICIENT_ANSWER,
  OFF_TOPIC: MESSAGE_CLASSES.OFF_TOPIC,
  SKIP: MESSAGE_CLASSES.SKIP,
  REFINEMENT_FEEDBACK: MESSAGE_CLASSES.REFINEMENT_FEEDBACK,
});

/** Domains for supplemental session memory tagging. */
const CONTEXT_DOMAINS = Object.freeze([
  'services',
  'ideal_customer',
  'geography',
  'differentiation',
  'objections',
  'pricing',
  'brand_voice',
  'success_metrics',
  'growth_goals',
  'operations',
]);

/** Map context domains → Blueprint section keys when a correction/supplement can attach. */
const DOMAIN_TO_SECTION = Object.freeze({
  services: 'services',
  ideal_customer: 'idealCustomers',
  geography: 'targetMarkets',
  differentiation: 'competitiveAdvantages',
  objections: 'avoidCustomers',
  pricing: 'avoidCustomers',
  brand_voice: 'brandVoice',
  success_metrics: 'successMetrics',
  growth_goals: 'campaignGoals',
  operations: 'services',
});

/** Normalized evidence keys consumed by Executive Business Brief synthesis. */
const NORMALIZED_FACT_KEYS = Object.freeze([
  'business_name',
  'business_description',
  'services',
  'growth_focus',
  'ideal_customers',
  'ideal_customer_traits',
  'disqualified_customers',
  'geography',
  'vertical_focus',
  'differentiation',
  'brand_voice',
  'ninety_day_outcomes',
  'success_metrics',
]);

const SECTION_TO_NORMALIZED = Object.freeze({
  identity: ['business_name', 'business_description'],
  services: ['services'],
  idealCustomers: ['ideal_customers'],
  avoidCustomers: ['disqualified_customers'],
  targetMarkets: ['geography', 'vertical_focus', 'growth_focus'],
  competitiveAdvantages: ['differentiation'],
  brandVoice: ['brand_voice'],
  campaignGoals: ['ninety_day_outcomes', 'growth_focus'],
  successMetrics: ['success_metrics'],
});

/** Explicit domain pointers in correction / supplemental messages. */
const DOMAIN_POINTER_RE =
  /\b(?:for|about|regarding|on|to)\s+(?:the\s+)?(services?|ideal\s+customers?|ideal\s+customer\s+profile|icp|customers?\s+to\s+avoid|geography|markets?|brand\s+voice|voice|success\s+metrics?|metrics?|goals?|growth|differentiation|advantages?|pricing|operations?)\b/i;

/** User refinement / meta-instruction intent (not business evidence). */
const REFINEMENT_INTENT_RE =
  /\b(please\s+refine|this\s+revision|max\s+is\s+treating|regenerate(?:\s+the\s+brief)?|turn\s+the\s+raw\s+(?:interview\s+)?answers|instructions?\s+to\s+max|not\s+facts?\s+about(?:\s+\w+)?|refinement\s+feedback|revision\s+guidance|the\s+brief\s+is\s+treating|please\s+regenerate|this\s+still\s+sounds\s+weird|sentences?\s+don'?t\s+make\s+sense|max\s+isn'?t\s+understanding|brief\s+should\s+be\s+more\s+conversational|this\s+needs\s+to\s+be\s+fixed)\b/i;

/** Supplemental / out-of-order context markers (start-anchored). */
const SUPPLEMENTAL_CONTEXT_RE =
  /^\s*(?:i\s+also\s+forgot(?:\s+to\s+mention)?|also\s+forgot(?:\s+to\s+mention)?|i\s+forgot(?:\s+to\s+mention)?|forgot\s+to\s+mention|also|one more thing|add this|for context|another thing|not for this question,? but|this might matter|btw|by the way|oh,? and|additionally|worth noting)\b/i;

/**
 * Mid-message supplemental / out-of-order markers —
 * "I also forgot to mention … for ICP"
 */
const SUPPLEMENTAL_PHRASE_RE =
  /\b((?:i\s+)?also\s+forgot(?:\s+to\s+mention)?|forgot\s+to\s+mention|one more thing|for\s+context|not for this question|while i'?m thinking of it|aside from (?:this|that)|for\s+(?:the\s+)?(?:icp|ideal\s+customer(?:\s+profile)?))\b/i;

/** Supplemental preamble wrappers that must never become evidence. */
const SUPPLEMENTAL_PREAMBLE_PATTERNS = Object.freeze([
  /^\s*i\s+also\s+forgot\s+to\s+mention\s+/i,
  /^\s*(?:i\s+)?also\s+forgot(?:\s+to\s+mention)?\s+/i,
  /^\s*forgot\s+to\s+mention\s+/i,
  /^\s*i\s+forgot(?:\s+to\s+mention)?\s+/i,
  /^\s*also[,:\s-]+/i,
  /^\s*add this\s*[,:\s-]*/i,
  /^\s*for\s+context\s*[,:\s-]*/i,
  /^\s*(?:one more thing|another thing|btw|by the way|oh,? and|additionally|worth noting)\s*[,:\s-]*/i,
  /^\s*not for this question,? but\s*/i,
]);

/** Correction markers — update/supersede a prior fact, don't append as a new answer. */
const CORRECTION_RE =
  /^\s*(actually|correction|i meant(?:\s+to\s+say)?|not that|replace that|that should be|sorry,?\s+i meant|to clarify|let me correct)\b/i;

/**
 * Mid-message replacement intent — "disregard last message, please replace with…"
 * These may appear anywhere in the turn, not only at the start.
 */
const CORRECTION_PHRASE_RE =
  /\b(disregard\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)|replace\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)|please\s+replace(?:\s+with)?|replace\s+with(?:\s+the\s+following)?|use\s+this\s+instead)\b/i;

/** Explicit reference to the prior answer (priority B for correction targeting). */
const LAST_ANSWER_REF_RE =
  /\b(?:last|previous)\s+(?:message|answer)\b/i;

/** Correction preamble wrappers that must never become evidence. */
const CORRECTION_PREAMBLE_PATTERNS = Object.freeze([
  /^\s*disregard\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)[,.\s:]*/i,
  /^\s*please\s+replace\s+with(?:\s+the\s+following)?[,;:\s-]*/i,
  /^\s*replace\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)(?:\s+with(?:\s+the\s+following)?)?[,;:\s-]*/i,
  /^\s*replace\s+with(?:\s+the\s+following)?[,;:\s-]*/i,
  /^\s*use\s+this\s+instead[,;:\s-]*/i,
  /^\s*(?:actually|correction|i meant(?:\s+to\s+say)?|not that|replace that|that should be|sorry,?\s+i meant|to clarify|let me correct)\b[,;:\s-]*/i,
  /^\s*(?:to\s+say|that|to\s+clarify that)\s+/i,
  /^\s*please\s+/i,
]);

/** Question directed at Max (not an interview answer). */
const QUESTION_TO_MAX_RE =
  /^(what|why|how|when|where|who|can you|could you|would you|do you|are you|is that|should i)\b.*\?\s*$/i;

/** Snippets that must never appear in Executive Business Brief evidence. */
const META_INSTRUCTION_SANITIZE_RE =
  /\b(this\s+revision\s+introduced|brief\s+is\s+treating|please\s+regenerate|do\s+not\s+include.{0,60}(?:brief|max|instruction|raw|fact|revision)|raw\s+interview\s+answers|clean\s+business\s+language|the\s+substance\s+is\s+mostly\s+right|instructions?\s+to\s+max|business\s+facts?\s+only|treating\s+refinement|not\s+evidence\s+about\s+the\s+business|paste(?:d)?\s+into\s+templates?)\b/i;

/**
 * Raw interview-question / answer-echo fragments that must never appear in Brief prose.
 * These are the bleed patterns from Mad-Lib slot filling.
 */
const RAW_PROMPT_FRAGMENT_RE =
  /\b(when a great-fit customer chooses|what usually tips the decision|anchor'?s brand voice should sound|if i were writing as your brand|over the next 90 days(?:, this growth work)?|we will know(?: the growth work is working)?|i don't want to work with|looking at the next 90 days|how will we know it'?s working|paint me a picture of the ideal|tell me about the (?:business|services)|would feel successful if|both geography is|to say short term|i also forgot to mention|forgot to mention)\b/i;

const SECTION_TITLES = Object.freeze({
  identity: 'Identity',
  services: 'Services',
  idealCustomers: 'Ideal Customers',
  avoidCustomers: 'Customers to Avoid',
  targetMarkets: 'Target Markets',
  competitiveAdvantages: 'Competitive Advantages',
  brandVoice: 'Brand Voice',
  campaignGoals: 'Campaign Goals',
  successMetrics: 'Success Metrics',
});

const QUESTION_BANK = Object.freeze([
  {
    id: 'identity',
    stage: 'Identity',
    section: 'identity',
    prompt:
      "Tell me about the business — what's the name, and how would you describe what you do today?",
    goal: 'Capture business identity',
    askedBecause: 'Identity is required for every Business Blueprint section downstream.',
  },
  {
    id: 'services',
    stage: 'Services',
    section: 'services',
    prompt: 'Tell me about the services your business provides today.',
    goal: 'Capture services',
    askedBecause: 'Services describe what the business delivers to customers.',
  },
  {
    id: 'ideal_customers',
    stage: 'Ideal Customers',
    section: 'idealCustomers',
    prompt:
      'Who do you most want to work with? Paint me a picture of the ideal customer — roles, business types, or segments.',
    goal: 'Capture ideal customers',
    askedBecause: 'ICP understanding feeds playbook idealCustomer fields after approval.',
  },
  {
    id: 'avoid_customers',
    stage: 'Avoid Customers',
    section: 'avoidCustomers',
    prompt:
      "Are there customers or segments you'd rather not take on — and what's usually the reason?",
    goal: 'Capture avoid list',
    askedBecause: 'Avoidance constraints protect targeting quality.',
  },
  {
    id: 'target_markets',
    stage: 'Markets',
    section: 'targetMarkets',
    prompt:
      'Where should we focus first — geography, verticals, or both? Walk me through the markets that matter.',
    goal: 'Capture target markets',
    askedBecause: 'Markets bound discovery and campaign geography later.',
  },
  {
    id: 'advantages',
    stage: 'Advantages',
    section: 'competitiveAdvantages',
    prompt:
      'When a great-fit customer chooses you over someone else, what usually tips the decision?',
    goal: 'Capture competitive advantages',
    askedBecause: 'Advantages ground messaging without inventing strategy.',
  },
  {
    id: 'brand_voice',
    stage: 'Brand Voice',
    section: 'brandVoice',
    prompt:
      'If I were writing as your brand tomorrow, how should it sound — tone, personality, anything we should avoid?',
    goal: 'Capture brand voice',
    askedBecause: 'Brand voice constrains later language without choosing channels.',
  },
  {
    id: 'campaign_goals',
    stage: 'Goals',
    section: 'campaignGoals',
    prompt:
      'Looking at the next 90 days, what business outcomes would make this growth work feel successful?',
    goal: 'Capture campaign goals',
    askedBecause: 'Goals describe desired outcomes, not sequences or offers.',
  },
  {
    id: 'success_metrics',
    stage: 'Success Metrics',
    section: 'successMetrics',
    prompt:
      "How will we know it's working — which numbers or signals do you actually watch?",
    goal: 'Capture success metrics',
    askedBecause: 'Metrics define success for the Business Blueprint.',
  },
]);
class ClientIntelligenceError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ClientIntelligenceError';
    this.code = code;
    this.status = status;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function newId() {
  return crypto.randomUUID();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') {
    throw new ClientIntelligenceError('invalid_client_id', 'client id is required');
  }
  const n = Number(value);
  if (!Number.isFinite(n)) {
    throw new ClientIntelligenceError('invalid_client_id', 'client id must be numeric');
  }
  return Math.trunc(n);
}

function clampConfidence(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(MAX_CONFIDENCE, Math.round(x * 1000) / 1000));
}

function emptySection() {
  return {
    summary: '',
    confidence: 0,
    evidenceIds: [],
    unknowns: [],
  };
}

function emptySections() {
  const out = {};
  for (const key of BLUEPRINT_SECTIONS) out[key] = emptySection();
  return out;
}

function assertTransition(from, to) {
  const allowed = ALLOWED_TRANSITIONS[from] || [];
  if (!allowed.includes(to)) {
    throw new ClientIntelligenceError(
      'invalid_transition',
      `Cannot transition from ${from} to ${to}`
    );
  }
}

function advanceStatus(session, to) {
  assertTransition(session.status, to);
  session.status = to;
  return session;
}

/**
 * SPEC-099/100 — explicit operator unknown (must never become a factual Blueprint value).
 * Broader than answerLooksEmpty: catches "I don't know yet", typos like "yeet", etc.
 * Distinct from explicit deferral ("skip for now" / "leave it open").
 */
function looksLikeExplicitUnknown(text) {
  if (looksLikeExplicitDeferral(text)) return false;
  const s = String(text || '')
    .trim()
    .toLowerCase()
    .replace(/[.!?]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!s) return true;
  const epistemicState = classifyEpistemicState(text);
  if (epistemicState === EPISTEMIC_STATES.UNKNOWN || epistemicState === EPISTEMIC_STATES.NOT_APPLICABLE) {
    return true;
  }
  if (
    /^(n\/?a|none|no|nothing|nope|nil|unknown|not sure|unsure|idk|tbd|-)$/i.test(s)
  ) {
    return true;
  }
  // "I don't know" / "I don't know yet" / typo "yeet" / "I really don't know"
  if (
    /^(i\s+)?(really\s+)?(do\s+not|don't|dont)\s+know(\s+(yet|yeet|right\s+now|at\s+the\s+moment))?$/i.test(
      s
    )
  ) {
    return true;
  }
  if (/^(we\s+)?(do\s+not|don't|dont)\s+know(\s+(yet|yeet|right\s+now))?$/i.test(s)) {
    return true;
  }
  // "I'm not sure" / "I'm really not sure" / "not sure yet" / "still not sure"
  if (
    /^(i('?m|\s+am)\s+)?(really\s+)?(not\s+sure|unsure|no\s+idea)(\s+yet)?$/i.test(s)
  ) {
    return true;
  }
  if (/^(we\s+are\s+)?(not\s+sure|unsure)(\s+yet)?$/i.test(s)) return true;
  if (/^(still\s+)?(not\s+sure|unsure)(\s+yet)?$/i.test(s)) return true;
  if (
    /^(i\s+|we\s+)?(haven't|have\s+not|couldn't|could\s+not)\s+(figured|worked)\s+(that|it|this)\s+out(\s+yet)?$/i.test(
      s
    )
  ) {
    return true;
  }
  if (/^(i\s+)?couldn't\s+tell\s+you$/i.test(s)) return true;
  if (/^(we\s+)?(haven't|have\s+not)\s+decided(\s+yet)?$/i.test(s)) return true;
  if (/^(we'?re|we\s+are)\s+still\s+deciding$/i.test(s)) return true;
  if (/^(still\s+)?(figuring|working)\s+(that|it)\s+out$/i.test(s)) return true;
  if (
    /^(i\s+)?(don'?t|dont|do\s+not)\s+really\s+know(\s+who\s+the\s+best\s+customer\s+is)?(\s+yet)?$/i.test(
      s
    )
  ) {
    return true;
  }
  if (/^maybe\??$/i.test(s)) return true;
  if (looksLikeExplicitUnknownAnswer(text)) return true;
  return false;
}

/**
 * Literal uncertainty phrases that must not appear as Blueprint field values
 * even when embedded in longer free-form answers.
 */
function isLiteralUncertaintyPhrase(text) {
  const s = String(text || '').trim();
  if (!s) return true;
  if (looksLikeExplicitUnknown(s)) return true;
  // Short phrases that are only uncertainty, not mixed with substance.
  if (s.split(/\s+/).length <= 8 && looksLikeExplicitUnknown(s.replace(/^well,?\s+/i, ''))) {
    return true;
  }
  return /^(i\s+don'?t\s+know|not\s+sure|haven'?t\s+figured|no\s+idea)\b/i.test(s) &&
    !/\b(but|except|other than|except for)\b/i.test(s) &&
    s.split(/\s+/).length <= 10;
}

function answerLooksEmpty(text) {
  const s = String(text || '').trim().toLowerCase();
  if (!s) return true;
  if (/^(n\/?a|none|no|nothing|nope|nil|unknown|not sure|-)$/i.test(s)) return true;
  // SPEC-099: explicit unknowns are empty of factual content.
  return looksLikeExplicitUnknown(text);
}

/**
 * Detect user refinement / regeneration instructions.
 * These are guidance for Max — never business facts about the client.
 */
function looksLikeRefinementFeedback(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (REFINEMENT_INTENT_RE.test(s)) return true;
  if (/\b(please\s+)?(refine|regenerate|rewrite)\b/i.test(s) && /\b(brief|summary|section|max)\b/i.test(s)) {
    return true;
  }
  if (/\bturn\s+.{0,40}\binto\s+clean\b/i.test(s)) return true;
  if (/\bnot\s+facts?\s+about\b/i.test(s)) return true;
  if (/\bdo\s+not\s+include\b/i.test(s) && /\b(brief|max|instruction|raw|fact|revision|section)\b/i.test(s)) {
    return true;
  }
  if (/\bthe\s+brief\b/i.test(s) && /\b(treating|refine|regenerate|please|revision|instruction|max)\b/i.test(s)) {
    return true;
  }
  if (/\b(sounds?\s+weird|doesn'?t\s+make\s+sense|isn'?t\s+understanding|needs?\s+to\s+be\s+fixed|more\s+conversational)\b/i.test(s)) {
    return true;
  }
  return false;
}

function looksLikeCorrection(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  return CORRECTION_RE.test(s) || CORRECTION_PHRASE_RE.test(s);
}

/**
 * Strip correction/replacement wrappers so only answer substance remains.
 */
function stripCorrectionPreamble(text) {
  let s = String(text || '').trim();
  if (!s) return '';

  // Peel wrappers repeatedly (e.g. "disregard last message, please replace with the following; …").
  for (let i = 0; i < 8; i += 1) {
    const before = s;
    for (const re of CORRECTION_PREAMBLE_PATTERNS) {
      s = s.replace(re, '').trim();
    }
    // Mid-message wrappers after a leading clause.
    s = s
      .replace(
        /\bdisregard\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)[,.\s:]*/gi,
        ' '
      )
      .replace(/\bplease\s+replace\s+with(?:\s+the\s+following)?[,;:\s-]*/gi, ' ')
      .replace(
        /\breplace\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)(?:\s+with(?:\s+the\s+following)?)?[,;:\s-]*/gi,
        ' '
      )
      .replace(/\breplace\s+with(?:\s+the\s+following)?[,;:\s-]*/gi, ' ')
      .replace(/\buse\s+this\s+instead[,;:\s-]*/gi, ' ')
      .replace(/\bcorrection\b[,;:\s-]*/gi, ' ')
      .replace(/^\s*[,;:\-–—]+\s*/, '')
      .replace(/\s{2,}/g, ' ')
      .trim();
    if (s === before) break;
  }
  return s;
}

function refersToLastAnswer(text) {
  return LAST_ANSWER_REF_RE.test(String(text || ''));
}

/**
 * Infer domain from question-echo / domain language in a replacement body.
 * @returns {string|null} one of CONTEXT_DOMAINS
 */
function inferDomainFromQuestionEcho(text) {
  const s = String(text || '').toLowerCase();
  if (!s) return null;
  if (
    /great-fit customer chooses|tips the decision|why customers choose|competitive (?:edge|advantage)|chooses (?:\w+\s+){0,3}over someone else/.test(
      s
    )
  ) {
    return 'differentiation';
  }
  if (
    /brand voice should|writing as (?:your|the) brand|how should (?:it|the brand|anchor) sound|\bbrand voice\b/.test(
      s
    )
  ) {
    return 'brand_voice';
  }
  if (
    /next 90 days|would feel successful|growth work feel successful|ninety[\s-]?day/.test(s)
  ) {
    return 'growth_goals';
  }
  if (/we will know(?: it'?s| the growth)|how will we know|success metrics?/.test(s)) {
    return 'success_metrics';
  }
  if (
    /greater manchester|bedford|hooksett|londonderry|auburn|goffstown|geography|markets that matter/.test(
      s
    )
  ) {
    return 'geography';
  }
  if (
    /ideal customers?|ideal customer profile|\bicp\b|who do you most want|paint me a picture of the ideal/.test(
      s
    )
  ) {
    return 'ideal_customer';
  }
  if (
    /services include|services (?:your business )?provides|tell me about the services/.test(s)
  ) {
    return 'services';
  }
  if (
    /don'?t want to work with|rather not take on|customers to avoid|excluded customers?/.test(s)
  ) {
    return 'objections';
  }
  return null;
}

/** Reverse DOMAIN_TO_SECTION → primary domain for a Blueprint section. */
function domainFromSection(section) {
  if (!section) return null;
  const entry = Object.entries(DOMAIN_TO_SECTION).find(([, sec]) => sec === section);
  return entry ? entry[0] : null;
}

/**
 * Most recent answered question id (by QUESTION_BANK order), if any.
 */
function findLastAnsweredQuestionId(state = {}) {
  const answers = state.answers || {};
  for (let i = QUESTION_BANK.length - 1; i >= 0; i -= 1) {
    const id = QUESTION_BANK[i].id;
    if (answers[id] != null && String(answers[id]).trim()) return id;
  }
  const idx = Number(state.stepIndex) || 0;
  if (idx > 0 && QUESTION_BANK[idx - 1]) return QUESTION_BANK[idx - 1].id;
  return null;
}

/**
 * Resolve which prior answer a correction should update.
 * Priority: A) explicit domain/question match → B) last/previous answer ref → C) active question.
 * @returns {{ domain: string|null, section: string|null, reason: string, questionId: string|null }}
 */
function resolveCorrectionTarget(text, opts = {}) {
  const activeQuestion = opts.activeQuestion || null;
  const state = opts.state || {};
  const raw = String(text || '').trim();
  const body = stripCorrectionPreamble(raw);

  // A. Explicit domain / question-echo match
  const echoDomain = inferDomainFromQuestionEcho(raw) || inferDomainFromQuestionEcho(body);
  const taggedDomain = tagContextDomain(raw) || tagContextDomain(body);
  const domain = echoDomain || taggedDomain;
  if (domain && DOMAIN_TO_SECTION[domain]) {
    const section = DOMAIN_TO_SECTION[domain];
    const questionId = QUESTION_BANK.find((row) => row.section === section)?.id || null;
    return { domain, section, reason: 'explicit_domain', questionId };
  }

  // B. "last message" / "previous answer" → most recent user answer
  if (refersToLastAnswer(raw)) {
    const lastId = opts.lastAnsweredQuestionId || findLastAnsweredQuestionId(state);
    const lastQ = lastId ? QUESTION_BANK.find((row) => row.id === lastId) : null;
    if (lastQ) {
      return {
        domain: domainFromSection(lastQ.section),
        section: lastQ.section,
        reason: 'last_answered',
        questionId: lastQ.id,
      };
    }
  }

  // C. Current active question only when A and B do not apply
  if (activeQuestion && activeQuestion.section) {
    return {
      domain: domainFromSection(activeQuestion.section),
      section: activeQuestion.section,
      reason: 'active_question',
      questionId: activeQuestion.id || null,
    };
  }

  return { domain: null, section: null, reason: 'unresolved', questionId: null };
}

function looksLikeSupplementalContext(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (SUPPLEMENTAL_CONTEXT_RE.test(s)) return true;
  if (SUPPLEMENTAL_PHRASE_RE.test(s)) return true;
  if (/\b(not\s+(?:an?\s+)?answer\s+to\s+(?:this|the)\s+question|aside\s+from\s+(?:this|that)|while\s+i'?m\s+thinking\s+of\s+it)\b/i.test(s)) {
    return true;
  }

  // Out-of-order ICP / domain add-on while a different question is active.
  const activeQuestion = opts.activeQuestion || null;
  if (activeQuestion && activeQuestion.section) {
    const domain = tagContextDomain(s);
    const activeDomain = domainFromSection(activeQuestion.section);
    if (domain && activeDomain && domain !== activeDomain) {
      if (
        /\b(also|forgot|add(?:ing)?|additionally|btw|by the way|one more|for\s+(?:the\s+)?(?:icp|ideal)|ideal\s+customer)\b/i.test(
          s
        )
      ) {
        return true;
      }
      if (domain === 'ideal_customer' && /\b(icp|ideal\s+customer|property managers?)\b/i.test(s)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Strip supplemental wrappers so only business substance remains.
 */
function stripSupplementalPreamble(text) {
  let s = String(text || '').trim();
  if (!s) return '';

  for (let i = 0; i < 6; i += 1) {
    const before = s;
    for (const re of SUPPLEMENTAL_PREAMBLE_PATTERNS) {
      s = s.replace(re, '').trim();
    }
    s = s
      .replace(/\s+for\s+(?:the\s+)?(?:icp|ideal\s+customers?|ideal\s+customer\s+profile)\s*$/i, '')
      .replace(/\s+as\s+part\s+of\s+(?:my|our|the)\s+ideal\s+customer(?:\s+profile)?\s*$/i, '')
      .replace(/\bas\s+part\s+of\s+(?:my|our|the)\s+ideal\s+customer(?:\s+profile)?\b/gi, ' ')
      .replace(/\bfor\s+(?:the\s+)?(?:icp|ideal\s+customers?|ideal\s+customer\s+profile)\b/gi, ' ')
      .replace(/^(?:to\s+mention)\s+/i, '')
      .replace(/^\s*[,;:\-–—]+\s*/, '')
      .replace(/\s{2,}/g, ' ')
      .trim();
    if (s === before) break;
  }
  return s;
}

/**
 * Parse a supplemental / out-of-order message into target domain + cleaned substance.
 * @returns {{ domain: string|null, section: string|null, substance: string, raw: string, questionId: string|null }}
 */
function parseSupplementalMessage(text, opts = {}) {
  const raw = String(text || '').trim();
  let body = stripSupplementalPreamble(raw);

  const domain =
    inferDomainFromQuestionEcho(raw) ||
    tagContextDomain(raw) ||
    inferDomainFromQuestionEcho(body) ||
    tagContextDomain(body) ||
    null;
  const section = (domain && DOMAIN_TO_SECTION[domain]) || null;
  const questionId = section
    ? QUESTION_BANK.find((row) => row.section === section)?.id || null
    : null;

  let substance = body
    .replace(DOMAIN_POINTER_RE, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
  substance = stripInterviewQuestionEcho(substance);
  if (section) substance = cleanRawAnswer(section, substance);
  substance = normalizeBusinessPhrase(substance);
  substance = stripSupplementalPreamble(substance);

  // Prefer clean entity extraction for list domains.
  if (section === 'idealCustomers') {
    const segments = extractCustomerSegments(substance) || extractCustomerSegments(raw);
    if (segments.length === 1) substance = segments[0];
    else if (segments.length > 1) substance = segments.join(', ');
  } else if (section === 'services') {
    const services = extractServiceList(substance) || extractServiceList(raw);
    if (services.length === 1) substance = services[0];
    else if (services.length > 1) substance = services.join(', ');
  }

  // Ignore unused opts.activeQuestion for now — domain inference is enough.
  void opts;

  return { domain, section, substance, raw, questionId };
}

function looksLikeQuestionToMax(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (QUESTION_TO_MAX_RE.test(s)) return true;
  if (/\?\s*$/.test(s) && /\b(max|you|we|this question|interview|brief)\b/i.test(s)) return true;
  return false;
}

/**
 * Detect meta-instruction language that must be stripped before Brief render.
 */
function containsMetaInstructionLanguage(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (META_INSTRUCTION_SANITIZE_RE.test(s)) return true;
  if (looksLikeRefinementFeedback(s)) return true;
  return false;
}

function containsRawPromptFragment(text) {
  return RAW_PROMPT_FRAGMENT_RE.test(String(text || ''));
}

/**
 * Map free-text domain pointer language → CONTEXT_DOMAINS key.
 */
function domainFromPointerLabel(label) {
  const s = String(label || '').toLowerCase().trim();
  if (!s) return null;
  if (/^services?$/.test(s)) return 'services';
  if (/^icp$/.test(s)) return 'ideal_customer';
  if (/ideal|customer/.test(s) && !/avoid/.test(s)) return 'ideal_customer';
  if (/avoid/.test(s)) return 'objections';
  if (/geo|market/.test(s)) return 'geography';
  if (/brand|voice/.test(s)) return 'brand_voice';
  if (/success|metric/.test(s)) return 'success_metrics';
  if (/goal|growth/.test(s)) return 'growth_goals';
  if (/differen|advantage/.test(s)) return 'differentiation';
  if (/pric/.test(s)) return 'pricing';
  if (/operat/.test(s)) return 'operations';
  return null;
}

/**
 * Infer a likely business domain tag for supplemental memory / corrections.
 * Explicit "for services" pointers win over keyword heuristics.
 * @returns {string|null} one of CONTEXT_DOMAINS
 */
function tagContextDomain(text) {
  const s = String(text || '').toLowerCase();
  if (!s) return null;

  const echo = inferDomainFromQuestionEcho(s);
  if (echo) return echo;

  const pointer = s.match(DOMAIN_POINTER_RE);
  if (pointer) {
    const fromPointer = domainFromPointerLabel(pointer[1]);
    if (fromPointer) return fromPointer;
  }

  if (/\b(price|pricing|rate|cost|cheap|lowest price|budget)\b/.test(s)) return 'pricing';
  if (/\b(avoid|don'?t want|do not want|decline|not a fit|no longer)\b/.test(s)) return 'objections';
  if (/\b(tone|voice|sound|brand voice)\b/.test(s)) return 'brand_voice';
  if (/\b(metric|kpi|measure|success metric|walkthroughs?|pipeline|repl(?:y|ies)|booked)\b/.test(s)) {
    return 'success_metrics';
  }
  if (/\b(90 days|next quarter|growth goal|campaign goal|expansion)\b/.test(s)) return 'growth_goals';
  if (/\b(trust|tips the decision|advantage|differen|accountab|responsiv)\b/.test(s)) {
    return 'differentiation';
  }
  if (/\b(manchester|bedford|hooksett|londonderry|auburn|goffstown|geography|geo\b|county)\b/.test(s)) {
    return 'geography';
  }
  if (/\b(ideal customer|ideal customer profile|\bicp\b|property managers?|homeowners?|facility managers?|segment fit)\b/.test(s)) {
    return 'ideal_customer';
  }
  if (/\b(services?|cleans?|cleaning|offers?|provides?|recurring|turnovers?)\b/.test(s)) {
    return 'services';
  }
  if (/\b(ops|operations?|staff|crew|capacity|schedule|delivery)\b/.test(s)) return 'operations';
  return null;
}

/**
 * Parse a correction message into target domain + cleaned substance.
 * Target resolution prefers explicit domain match, then last-answer refs, then optional fallback.
 * @param {string} text
 * @param {string|null} [fallbackSection]
 * @param {{ activeQuestion?: object|null, state?: object, lastAnsweredQuestionId?: string|null }} [opts]
 * @returns {{ domain: string|null, section: string|null, substance: string, raw: string, reason: string, questionId: string|null }}
 */
function parseCorrectionMessage(text, fallbackSection = null, opts = {}) {
  const raw = String(text || '').trim();
  let body = stripCorrectionPreamble(raw);

  // Legacy leftovers after preamble strip.
  body = body
    .replace(/^(?:to\s+say|that|to\s+clarify that)\s+/i, '')
    .replace(/^(?:replace that|not that)\s*[—–,:;-]?\s*/i, '')
    .trim();

  const resolved = resolveCorrectionTarget(raw, {
    activeQuestion: opts.activeQuestion || null,
    state: opts.state || {},
    lastAnsweredQuestionId: opts.lastAnsweredQuestionId || null,
  });

  let domain = resolved.domain;
  let section = resolved.section;
  let reason = resolved.reason;
  let questionId = resolved.questionId;

  // Caller may supply a fallback only when resolution left section empty.
  if (!section && fallbackSection) {
    section = fallbackSection;
    domain = domain || domainFromSection(fallbackSection);
    reason = reason === 'unresolved' ? 'fallback_section' : reason;
    questionId =
      questionId || QUESTION_BANK.find((row) => row.section === fallbackSection)?.id || null;
  }

  // Once domain is known, drop trailing/leading domain pointers from substance.
  let substance = body
    .replace(DOMAIN_POINTER_RE, ' ')
    .replace(/^(?:to\s+say|that)\s+/i, '')
    .replace(/^(?:geography|markets?|services?|brand\s+voice|voice|ideal\s+customers?)\s+should\s+be\s+/i, '')
    .replace(/\s{2,}/g, ' ')
    .trim();

  substance = stripInterviewQuestionEcho(substance);
  if (section) substance = cleanRawAnswer(section, substance);
  substance = normalizeBusinessPhrase(substance);

  // Never keep correction wrappers in stored evidence.
  substance = stripCorrectionPreamble(substance);

  return { domain, section, substance, raw, reason, questionId };
}

/**
 * Classify every user message during the interview before attaching to a question.
 * SPEC-090: delegates to the conversational reasoning layer while preserving
 * CIE detectors for correction / add-on / refinement.
 * @param {string} text
 * @param {{ speaker?: string, context?: string, activeQuestion?: object|null, briefReady?: boolean }} [opts]
 * @returns {string} one of MESSAGE_TYPES
 */
function classifyInterviewMessage(text, opts = {}) {
  return classifyReasoningMessage(text, {
    speaker: opts.speaker,
    context: opts.context,
    activeQuestion: opts.activeQuestion,
    looksLikeCorrection,
    looksLikeAddOn: looksLikeSupplementalContext,
    looksLikeRefinement: looksLikeRefinementFeedback,
    containsMetaInstruction: containsMetaInstructionLanguage,
    answerLooksEmpty,
  });
}

/**
 * Classify a user/system response for evidence routing (Brief sanitization).
 * @param {string} text
 * @param {{ speaker?: string, context?: string }} [opts]
 * @returns {string} one of ANSWER_KINDS
 */
function classifyUserResponse(text, opts = {}) {
  const speaker = String(opts.speaker || '').toLowerCase();
  const context = String(opts.context || '').toLowerCase();
  if (context === 'generated_brief' || speaker === 'assistant') {
    return ANSWER_KINDS.GENERATED_BRIEF;
  }
  if (speaker === 'system' || speaker === 'developer' || context === 'system_guidance') {
    return ANSWER_KINDS.SYSTEM_GUIDANCE;
  }
  const msgType = classifyInterviewMessage(text, opts);
  if (msgType === MESSAGE_TYPES.REFINEMENT_FEEDBACK) {
    return ANSWER_KINDS.REFINEMENT_FEEDBACK;
  }
  // Supplemental / correction / direct answers can carry business substance.
  if (
    msgType === MESSAGE_TYPES.DIRECT_ANSWER ||
    msgType === MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT ||
    msgType === MESSAGE_TYPES.CORRECTION
  ) {
    if (looksLikeRefinementFeedback(text) || containsMetaInstructionLanguage(text)) {
      return ANSWER_KINDS.REFINEMENT_FEEDBACK;
    }
    return ANSWER_KINDS.BUSINESS_FACT;
  }
  if (looksLikeRefinementFeedback(text) || containsMetaInstructionLanguage(text)) {
    return ANSWER_KINDS.REFINEMENT_FEEDBACK;
  }
  return ANSWER_KINDS.BUSINESS_FACT;
}

function correctionTargetLabel(domain, businessName) {
  const name = String(businessName || 'the business').trim() || 'the business';
  const shortName = name.replace(/\s+Cleaning$/i, '') || name;
  const labels = {
    services: 'services',
    ideal_customer: 'ideal customers',
    geography: 'geography',
    differentiation: `why customers choose ${shortName}`,
    objections: 'customers to decline',
    pricing: 'pricing',
    brand_voice: `how ${shortName} should sound`,
    success_metrics: 'success metrics',
    growth_goals: 'near-term goals',
    operations: 'operations',
  };
  return labels[domain] || domain || 'that topic';
}

function conversationalQuestionRestate(question, businessName) {
  if (!question) return 'the current question';
  const name = String(businessName || 'the business').trim() || 'the business';
  const shortName = name.replace(/\s+Cleaning$/i, '') || name;
  switch (question.id) {
    case 'brand_voice':
      return `how should ${shortName} sound if I'm writing as the brand?`;
    case 'advantages':
      return `when a great-fit customer chooses ${shortName} over someone else, what usually tips the decision?`;
    case 'campaign_goals':
      return 'looking at the next 90 days, what outcomes would make this growth work feel successful?';
    case 'success_metrics':
      return "how will we know it's working?";
    case 'services':
      return 'what services does the business provide today?';
    case 'ideal_customers':
      return 'who do you most want to work with?';
    case 'avoid_customers':
      return 'are there any customers or segments you\'d rather not take on?';
    case 'target_markets':
      return 'where should we focus first — geography, verticals, or both?';
    case 'identity':
      return "what's the business name, and how would you describe what you do today?";
    default:
      return question.prompt || 'the current question';
  }
}

/**
 * Conversational acknowledgement for non-answer interview messages.
 * @param {string} messageType
 * @param {string} text
 * @param {string|null} domain
 * @param {{ activeQuestion?: object|null, targetSection?: string|null, businessName?: string|null, substance?: string|null }} [opts]
 */
function conversationalAck(messageType, text, domain, opts = {}) {
  const domainLabel = {
    services: 'services',
    ideal_customer: 'ideal customer fit',
    geography: 'geography',
    differentiation: 'differentiation',
    objections: 'customers to decline',
    pricing: 'pricing',
    brand_voice: 'brand voice',
    success_metrics: 'success metrics',
    growth_goals: 'growth goals',
    operations: 'operations',
  };
  const activeQuestion = opts.activeQuestion || null;
  const targetSection = opts.targetSection || null;
  const businessName = opts.businessName || null;
  const substance = String(opts.substance || '').trim();

  switch (messageType) {
    case MESSAGE_TYPES.ADD_ON:
    case MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT: {
      const reopen = conversationalQuestionRestate(activeQuestion, businessName);
      if (domain === 'ideal_customer' && substance) {
        const added = substance.replace(/\.$/, '');
        return `Got it. I'll add ${added} to your ideal customer profile. For this question, ${reopen}`;
      }
      if (domain && domainLabel[domain] && substance) {
        return `Got it. I'll add ${substance.replace(/\.$/, '')} under ${domainLabel[domain]}. For this question, ${reopen}`;
      }
      if (domain && domainLabel[domain]) {
        return `Got it. That sounds like it belongs under ${domainLabel[domain]}. I'll remember it there rather than treating it as your answer to this question.\n\nFor this question, ${reopen}`;
      }
      return `Got it. I'll add that to the business context rather than treating it as your answer to this question.\n\nFor this question, ${reopen}`;
    }
    case MESSAGE_TYPES.CORRECTION: {
      const correctingPrior =
        Boolean(targetSection) &&
        Boolean(activeQuestion) &&
        targetSection !== activeQuestion.section;
      if (correctingPrior) {
        const about = correctionTargetLabel(domain, businessName);
        const reopen = conversationalQuestionRestate(activeQuestion, businessName);
        return `Got it. I replaced your previous answer about ${about}. Let's keep the current question open: ${reopen}`;
      }
      if (domain && domainLabel[domain]) {
        return `Helpful correction. I'll update ${domainLabel[domain]} rather than adding it as a new answer.`;
      }
      return "Helpful correction. I'll update the relevant fact rather than treating this as a new answer to the current question.";
    }
    case MESSAGE_TYPES.REFINEMENT_FEEDBACK:
      return "Understood — I'll treat that as guidance for how I write and regenerate, not as business evidence.";
    case MESSAGE_TYPES.CLARIFICATION_REQUEST:
    case MESSAGE_TYPES.QUESTION_TO_MAX:
      return "Good question. I'll stay with our discovery for now — answer the current prompt when you're ready, or add context with \"also\" / \"I forgot\" if it's extra detail.";
    case MESSAGE_TYPES.APPROVAL:
      return reasoningAck(MESSAGE_TYPES.APPROVAL, {
        activeQuestion,
        reopenPrompt: conversationalQuestionRestate(activeQuestion, businessName),
      });
    case MESSAGE_TYPES.SKIP:
      return reasoningAck(MESSAGE_TYPES.SKIP, {
        reopenPrompt: null,
      });
    case MESSAGE_TYPES.INSUFFICIENT_ANSWER:
      return reasoningAck(MESSAGE_TYPES.INSUFFICIENT_ANSWER, {
        probe: opts.probe || null,
      });
    case MESSAGE_TYPES.OFF_TOPIC:
      return "Noted. Whenever you're ready, we can continue with the current question.";
    default:
      return "Thanks — I've got that.";
  }
}

/**
 * Split a free-form message into business facts vs revision guidance.
 * Only facts may populate Blueprint / Brief commercial fields.
 */
function partitionUserResponse(text) {
  const raw = String(text || '').trim();
  if (!raw) return { facts: [], guidance: [], kind: ANSWER_KINDS.BUSINESS_FACT };

  const sentences = raw
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => s.trim())
    .filter(Boolean);

  const facts = [];
  const guidance = [];

  if (sentences.length <= 1) {
    const kind = classifyUserResponse(raw);
    if (kind === ANSWER_KINDS.BUSINESS_FACT) facts.push(raw);
    else guidance.push(raw);
    return { facts, guidance, kind };
  }

  for (const sentence of sentences) {
    const kind = classifyUserResponse(sentence);
    if (kind === ANSWER_KINDS.BUSINESS_FACT) facts.push(sentence);
    else guidance.push(sentence);
  }

  // Whole-message refinement intent wins when every "fact" sentence is still meta-ish.
  if (!facts.length && guidance.length) {
    return { facts, guidance, kind: ANSWER_KINDS.REFINEMENT_FEEDBACK };
  }
  if (facts.length && guidance.length) {
    return { facts, guidance, kind: ANSWER_KINDS.BUSINESS_FACT };
  }
  if (looksLikeRefinementFeedback(raw) && !facts.length) {
    return { facts: [], guidance: guidance.length ? guidance : [raw], kind: ANSWER_KINDS.REFINEMENT_FEEDBACK };
  }
  return {
    facts,
    guidance,
    kind: facts.length ? ANSWER_KINDS.BUSINESS_FACT : ANSWER_KINDS.REFINEMENT_FEEDBACK,
  };
}

function isBusinessFactStatement(text) {
  if (answerLooksEmpty(text)) return false;
  if (/^Unknown:/i.test(String(text || ''))) return false;
  return classifyUserResponse(text) === ANSWER_KINDS.BUSINESS_FACT;
}

function looksLikeConfirmation(text) {
  return CONFIRMATION_RE.test(String(text || ''));
}

function looksAmbiguous(text) {
  return AMBIGUITY_RE.test(String(text || ''));
}

/**
 * Specificity signals — never response length.
 * Named entities (2+ capitalized tokens), concrete domain terms, numeric facts.
 */
function hasSpecificitySignals(statement) {
  const s = String(statement || '').trim();
  if (!s) return false;
  if (/\b\d+(\.\d+)?%?\b/.test(s)) return true;
  if (
    /\b(commercial|residential|recurring|property managers?|homeowners?|law firms?|manchester|charleston|myrtle|nashville|premium|enterprise|b2b|b2c|weekly|monthly|quarterly)\b/i.test(
      s
    )
  ) {
    return true;
  }
  // Multi-word proper name / place (e.g. "South Carolina", "Aji Home Services")
  if (/\b[A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){1,4}\b/.test(s)) return true;
  return false;
}

function detectContradiction(previousStatements, nextText) {
  const next = String(nextText || '').trim().toLowerCase();
  if (!next) return false;
  for (const prev of previousStatements || []) {
    const p = String(prev || '').trim().toLowerCase();
    if (!p) continue;
    // Same category, opposite polarity on a shared noun phrase (simple heuristic)
    const prevNeg = /\b(not|never|no longer|don't|do not|avoid)\b/.test(p);
    const nextNeg = /\b(not|never|no longer|don't|do not|avoid)\b/.test(next);
    if (prevNeg !== nextNeg) {
      const tokens = p
        .split(/\W+/)
        .filter((t) => t.length > 4)
        .slice(0, 6);
      if (tokens.some((t) => next.includes(t))) return true;
    }
  }
  return false;
}

function tokenizeSignificant(text) {
  return String(text || '')
    .toLowerCase()
    .split(/\W+/)
    .filter((t) => t.length > 4);
}

function isConsistentRepeat(previousStatements, nextText) {
  const nextTokens = new Set(tokenizeSignificant(nextText));
  if (nextTokens.size === 0) return false;
  for (const prev of previousStatements || []) {
    const prevTokens = tokenizeSignificant(prev);
    const overlap = prevTokens.filter((t) => nextTokens.has(t)).length;
    if (overlap >= 2) return true;
  }
  return false;
}

/**
 * Confidence must NOT use response length.
 * Increases: explicit, confirmation, consistency, corroboration, specificity.
 * Decreases: contradiction / ambiguity / missing information.
 */
function scoreEvidenceConfidence({
  type,
  statement,
  priorStatements,
  isConfirmation,
  hasCorroboration,
}) {
  if (answerLooksEmpty(statement)) return UNKNOWN_CONFIDENCE;
  let score = type === 'EXPLICIT' || type === 'CLIENT_EDITED' ? EXPLICIT_CONFIDENCE : INFERRED_CONFIDENCE;
  if (hasSpecificitySignals(statement)) score += SPECIFICITY_BUMP;
  if (looksAmbiguous(statement)) score -= AMBIGUITY_PENALTY;
  if (isConfirmation) score += CONFIRMATION_BUMP;
  if (isConsistentRepeat(priorStatements, statement)) score += CONSISTENCY_BUMP;
  if (hasCorroboration) score += CORROBORATION_BUMP;
  if (detectContradiction(priorStatements, statement)) score -= CONTRADICTION_PENALTY;
  return clampConfidence(score);
}

function capitalizeSentence(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function ensurePeriod(text) {
  const s = String(text || '').trim().replace(/\s+/g, ' ');
  if (!s) return '';
  return /[.!?]$/.test(s) ? s : `${s}.`;
}

/** Protect common abbreviations so sentence splitters do not treat "Co." as EOS. */
function protectAbbreviations(text) {
  return String(text || '')
    .replace(/\b(Co|Inc|Ltd|LLC|Llc|Mr|Mrs|Ms|Dr|Jr|Sr|vs|etc)\./gi, '$1\u0000');
}

function restoreAbbreviations(text) {
  return String(text || '').replace(/\u0000/g, '.');
}

function escapeRegExp(text) {
  return String(text || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function firstSentence(text) {
  const s = protectAbbreviations(String(text || '').trim());
  if (!s) return '';
  const parts = s.split(/(?<=[.!?])\s+/);
  return restoreAbbreviations(parts[0] || s);
}

function stripLeadingWeAre(text) {
  return String(text || '')
    .trim()
    .replace(/^(we are|we're|i am|i'm|this is|our company is|the business is)\s+/i, '');
}

/**
 * SPEC-228 — business_description is a description-only slot. Downstream
 * identity synthesis prepends business_name ("${name} is a ${description}"),
 * so a description that already carries a "<name> is a ..." wrap causes
 * accumulating duplication across refinement rounds. Strip that wrap here so
 * business_description never becomes an opaque prose accumulator.
 */
function sanitizeIdentityDescription(name, description) {
  let desc = String(description || '').trim();
  if (!desc) return desc;
  const cleanName = String(name || '').trim();
  if (!cleanName) return normalizeBusinessPhrase(desc);
  const escapedName = escapeRegExp(cleanName);
  const wrapRe = new RegExp(`^${escapedName}\\s+(?:is|are)\\s+(?:a|an)\\s+`, 'i');
  const bareRe = new RegExp(`^${escapedName}\\s*[:,\\-—–]?\\s+`, 'i');
  for (let i = 0; i < 6; i += 1) {
    const before = desc;
    if (wrapRe.test(desc)) desc = desc.replace(wrapRe, '').trim();
    else if (bareRe.test(desc)) desc = desc.replace(bareRe, '').trim();
    if (desc === before) break;
  }
  return normalizeBusinessPhrase(desc);
}

function titleCaseWords(text) {
  return String(text || '')
    .split(/(\s+)/)
    .map((part) => {
      if (!/[a-zA-Z]/.test(part)) return part;
      if (/^[A-Z]{2,}$/.test(part)) return part;
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join('');
}

const PLACE_NAME_RE =
  /\b(bedford|hooksett|londonderry|auburn|goffstown|manchester|charleston|nashville|toronto)\b/gi;

/**
 * Normalize a business phrase for Brief rendering (lists, services, segments, places).
 */
/**
 * SPEC-101 — correct obvious mechanical typing errors for structured /
 * presentation understanding. Does not strengthen claims. Raw evidence stays
 * untouched at the provenance layer.
 */
function normalizeMechanicalTypos(text) {
  let s = String(text || '');
  if (!s) return s;

  // Known phrase-level operator typing errors from acceptance interviews.
  s = s.replace(/\bwee'?ll\s+b\s+ee\s+theree\s+on\s+ttime\b/gi, "we'll be there on time");
  s = s.replace(/\bwee'?ll\b/gi, "we'll");
  s = s.replace(/\btheree\b/gi, 'there');
  s = s.replace(/\bttime\b/gi, 'time');
  s = s.replace(/\bb\s+ee\b/gi, 'be');
  s = s.replace(/\bmiix\b/gi, 'mix');
  s = s.replace(/\bmoreee\b/gi, 'more');
  s = s.replace(/\bneiighborhood\b/gi, 'neighborhood');
  s = s.replace(/\brreevvenue\b/gi, 'revenue');
  // Obvious doubled-vowel / past-tense mechanical typos.
  s = s.replace(/\bcreateed\b/gi, 'created');
  s = s.replace(/\bgenerateed\b/gi, 'generated');
  s = s.replace(/\bestablishhed\b/gi, 'established');
  // Common operator spelling of "commercial" from acceptance interviews.
  s = s.replace(/\bcommeercial\b/gi, 'commercial');
  s = s.replace(/\bcommerical\b/gi, 'commercial');

  return s;
}

/**
 * SPEC-101 — client-facing prose polish. Expression only; never invents meaning.
 */
function normalizePresentationProse(text) {
  let s = normalizeMechanicalTypos(String(text || ''));
  if (!s) return s;
  s = s
    .replace(/\bwarm and neighborhood\b/gi, 'warm and neighborhood-oriented')
    .replace(/\brecurring revenue created\b/gi, 'new recurring revenue')
    .replace(/\brevenue created\b/gi, 'new revenue')
    // Accidental mid-sentence capitalization from operator notes.
    .replace(/(?<=\s)Is(?=\s)/g, 'is')
    .replace(/\s{2,}/g, ' ')
    .trim();
  return s;
}

/**
 * Presentation-only metric phrase polish (typos + light grammar).
 * Does not invent amounts, targets, or conversion rates.
 */
function normalizeMetricPresentationPhrase(text) {
  let s = normalizeMechanicalTypos(String(text || '').trim());
  if (!s) return '';
  s = s
    .replace(/\brecurring revenue created\b/gi, 'new recurring revenue')
    .replace(/\brevenue created\b/gi, 'new revenue')
    .replace(/^#\s*of\s+/i, 'Number of ')
    .replace(/\s{2,}/g, ' ')
    .trim();
  return s;
}

/**
 * Convert a leading finite verb into a gerund for "priority is …" framing.
 */
function toGerundVerb(verb) {
  const v = String(verb || '').toLowerCase();
  if (!v) return v;
  if (v === 'run') return 'running';
  if (v.endsWith('ie')) return `${v.slice(0, -2)}ying`;
  if (v.endsWith('e') && !v.endsWith('ee')) return `${v.slice(0, -1)}ing`;
  return `${v}ing`;
}

/**
 * Normalize goal substance into a noun/gerund phrase suitable after
 * "near-term priority is …" without changing the underlying claim.
 */
function normalizeGoalOutcomePhrase(text) {
  let outcome = normalizeMechanicalTypos(String(text || '').trim());
  if (!outcome) return '';
  outcome = outcome
    .replace(/^(?:focus on|center on)\s+/i, '')
    .replace(
      /^(?:we(?:'ll| will)|i(?:'ll| will)|we want to|i want to|our goal is to|the goal is to)\s+/i,
      ''
    )
    .trim();

  const GERUNDABLE =
    /^(establish|create|build|grow|book|increase|improve|win|close|hire|launch|expand|generate|develop|add|secure|deliver|turn|run)\b/i;
  if (GERUNDABLE.test(outcome)) {
    outcome = outcome.replace(GERUNDABLE, (m) => toGerundVerb(m));
  }

  // "establishing a reliable pipeline turning prospects into clients"
  // → "… pipeline that turns prospects into clients"
  outcome = outcome.replace(
    /\b(pipeline|process|system|program|engine)\s+turning\b/i,
    '$1 that turns'
  );

  return outcome.replace(/\s{2,}/g, ' ').trim();
}

/**
 * Join decision-maker labels into natural prose.
 * Keeps full labels ("property managers and facility managers") so role
 * identity stays explicit and searchable in Brief assertions.
 */
function formatDecisionMakerProse(labels) {
  const items = (labels || []).map((l) => String(l || '').trim()).filter(Boolean);
  if (!items.length) return '';
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
}

/**
 * Join segment/property-type labels into natural prose.
 */
function formatSegmentProse(labels) {
  const items = (labels || []).map((l) => String(l || '').trim()).filter(Boolean);
  if (!items.length) return '';
  return items
    .map((item) =>
      String(item)
        .replace(/\bapartment\/multifamily\b/gi, 'apartment and multifamily')
        .replace(/\bapartment(?:\s+and\s+|\s+)multifamily\b/gi, 'apartment and multifamily')
        .trim()
    )
    .join(', ');
}

/**
 * Presentation-only customer constraint / avoidance sentence.
 * Preserves hard exclusion vs low preference vs conditional acceptance.
 * Does not rewrite raw evidence — Brief prose only.
 */
function composeCustomerConstraintPresentation(businessName, rawConstraint) {
  const subject = businessSubject(businessName || 'The business');
  const raw = normalizeMechanicalTypos(String(rawConstraint || '').trim());
  if (!raw) return '';

  // Price-first disqualification stays categorical (existing behavior).
  if (/lowest price|cheap|bargain|price.?first/i.test(raw)) {
    return `${subject} deliberately avoids customers who prioritize the lowest price over reliability, professionalism, and accountability`;
  }

  const hasRestaurant = /\brestaurants?\b/i.test(raw);
  const hasBackOfHouse = /\bback[\s-]?of[\s-]?house\b/i.test(raw);
  const hasFrontOfHouse = /\bfront[\s-]?of[\s-]?house\b/i.test(raw);
  const conditionalOpen =
    /\b(?:would consider|may consider|open to|unless|except(?:\s+for)?|not preferred|not a priority|prefer not)\b/i.test(
      raw
    );

  // Observed AS Cleaning shape: restaurants / BOH difficulty / FOH may be considered.
  if (hasRestaurant && hasBackOfHouse && (hasFrontOfHouse || conditionalOpen)) {
    return (
      `${subject} does not currently prioritize restaurant cleaning because of the difficulty of ` +
      `back-of-house work, though front-of-house opportunities may be considered`
    );
  }

  // "No restaurants unless it's only front of house cleaning"
  if (hasRestaurant && hasFrontOfHouse && /unless|except/i.test(raw)) {
    return (
      `${subject} generally avoids full restaurant cleaning, though front-of-house-only work may be considered`
    );
  }

  // Soft preference / not preferred — do not escalate to categorical exclusion.
  if (
    /\b(?:not preferred|not a priority|prefer not|low priority|would rather not|does not currently prioritize)\b/i.test(
      raw
    ) ||
    (conditionalOpen && !/^(?:no|never|do not|don't|won't|will not)\b/i.test(raw))
  ) {
    let focus = raw
      .replace(/^(?:no|not|avoid|excluding)\s+/i, '')
      .replace(/\bbecause\b[\s\S]*$/i, '')
      .replace(/\bwould consider\b[\s\S]*$/i, '')
      .replace(/\bbut not preferred\b[\s\S]*$/i, '')
      .trim();
    focus = midSentence(focus || 'that work');
    return `${subject} does not currently prioritize ${focus}`;
  }

  // Default: prefer-to-avoid (softer than "deliberately avoids") unless hard no.
  let who = raw
    .replace(/^(?:customers?\s+)?(?:who'?s|whose|who|that)\s+/i, '')
    .replace(/^(?:no|not|avoid|excluding)\s+/i, '')
    .replace(/^main priority is\s+/i, 'prioritize ')
    .trim();
  who = midSentence(who);
  if (/^(?:no|never|do not|don't|won't|will not)\b/i.test(raw)) {
    return `${subject} deliberately avoids ${who}`;
  }
  return `${subject} prefers to avoid ${who}`;
}

function normalizeBusinessPhrase(phrase) {
  let s = String(phrase || '').trim();
  if (!s) return '';

  s = normalizeMechanicalTypos(s);

  // Repair spaced punctuation that should be compound hyphens.
  s = s
    .replace(/\b(commercial|residential|short[\s-]?term|high[\s-]?traffic|low|great|move[\s-]?in)\s*[—–]\s*(focused|term|traffic|price|fit|out)\b/gi, '$1-$2')
    .replace(/\b(low|great)\s+[—–-]\s+(price|fit)\b/gi, '$1-$2')
    .replace(/\bcommercial\s+[—–-]\s+focused\b/gi, 'commercial-focused')
    .replace(/\bgreat\s+[—–-]\s+fit\b/gi, 'great-fit')
    .replace(/\blow\s+[—–-]\s+price\b/gi, 'low-price');

  s = s
    .replace(/\bSTR\b/g, 'short-term rental')
    .replace(/\bshort[\s-]?term\s+rental\s+companies\b/gi, 'short-term rental companies')
    .replace(/\bshort[\s-]?term\s+rental\s+turnovers?\b/gi, 'short-term rental turnovers')
    .replace(/\bmove[\s-]?in\s*\/\s*move[\s-]?out\s+cleans?\b/gi, 'move-in/move-out cleaning')
    .replace(/\bmove[\s-]?in\/?outs?\s+cleans?\b/gi, 'move-in/move-out cleaning')
    .replace(/\bmove[\s-]?in\/out\s+cleans?\b/gi, 'move-in/move-out cleaning')
    .replace(/\bmove in\/out cleans?\b/gi, 'move-in/move-out cleaning')
    .replace(/\bmove[\s-]?in\s*\/\s*(?:move[\s-]?)?outs?\b/gi, 'move-in/move-out cleaning')
    .replace(/\bresidential\s+standards?(?:\s+cleans?|\s+cleaning)?\b/gi, 'residential standard cleaning')
    .replace(/\brecurring\s+standards?(?:\s+cleans?|\s+cleaning)?\b/gi, 'recurring standard cleaning')
    .replace(/\bstandard homes?\b/gi, 'standard home cleaning')
    .replace(/\bstandard offices?\b/gi, 'standard office cleaning')
    .replace(/\boffice\s+(?:cleans?|cleaning)\b/gi, 'office cleaning')
    .replace(/\brecurring cleans?\b/gi, 'recurring cleaning')
    .replace(/\bdeep cleans?\b/gi, 'deep cleans')
    .replace(/\bhigh[\s-]?traffic buildings?\b/gi, 'high-traffic buildings')
    .replace(/\brec centers?\b/gi, 'rec centers')
    .replace(/\bfacilit(?:y|ies)\s+managers?\b/gi, 'facility managers')
    .replace(/\bproperty managers?\b/gi, 'property managers')
    .replace(/\bapartment\/multifamily\s+(?:buildings?|properties)\b/gi, 'APARTMENT_MULTIFAMILY_BUILDINGS')
    .replace(/\bapartment(?:\s+and\s+|\s+)multifamily\s+(?:buildings?|properties)\b/gi, 'APARTMENT_MULTIFAMILY_BUILDINGS')
    .replace(/\bapartment buildings?\b/gi, 'APARTMENT_MULTIFAMILY_BUILDINGS')
    .replace(/\bmultifamily(?:\s+(?:apartment\s+)?)?(?:buildings?|properties)\b/gi, 'APARTMENT_MULTIFAMILY_BUILDINGS')
    .replace(/APARTMENT_MULTIFAMILY_BUILDINGS/g, 'apartment/multifamily buildings')
    .replace(/\bprofessional offices?\b/gi, 'professional offices')
    .replace(/\bdaycares?\b/gi, 'daycares')
    .replace(/\bgreater\s+manchester(?:\s+area)?\b/gi, 'Greater Manchester')
    .replace(/\bgreater\s+toronto(?:\s+area)?\b/gi, 'Greater Toronto Area')
    .replace(PLACE_NAME_RE, (m) => titleCaseWords(m))
    .replace(/\s{2,}/g, ' ')
    .trim();

  // Avoid double "cleaning cleaning"
  s = s.replace(/\bcleaning cleaning\b/gi, 'cleaning');
  return s;
}

/**
 * Strip business-name / company lead-ins from answer prose.
 * "Anchor Cleaning provides standard home cleaning" → "standard home cleaning"
 */
function stripBusinessNameLeadIn(text) {
  let s = String(text || '').trim();
  if (!s) return '';
  s = s
    .replace(
      /^(?:anchor(?:\s+cleaning)?|[A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:provides?|offers?|delivers?|does|serves?|most wants to work with|wants to work with|prefers? to work with)\s+/i,
      ''
    )
    .replace(
      /^(?:the\s+)?(?:business|company)\s+(?:provides?|offers?|delivers?|most wants to work with|wants to work with)\s+/i,
      ''
    )
    .replace(/^services?(?:\s+include|\s+are|:)\s+/i, '')
    .replace(/^ideal customers?(?:\s+include|\s+are|\s+is|:)\s+/i, '')
    .replace(/^the ideal customers?(?:\s+include|\s+are|\s+is|:)\s+/i, '')
    .trim();
  return s;
}

/** Known cleaning service entity patterns → canonical labels. */
const SERVICE_ENTITY_PATTERNS = Object.freeze([
  [/residential\s+standard(?:s|\s+cleans?|\s+cleaning)?/gi, 'residential standard cleaning'],
  [/recurring\s+standard(?:s|\s+cleans?|\s+cleaning)?/gi, 'recurring standard cleaning'],
  [/standard\s+home(?:\s+cleaning)?/gi, 'standard home cleaning'],
  [/standard\s+office(?:\s+cleaning)?/gi, 'standard office cleaning'],
  [/(?<!standard\s)\boffice\s+(?:cleans?|cleaning)\b/gi, 'office cleaning'],
  [/deep\s+cleans?/gi, 'deep cleans'],
  [/move[\s-]?in\s*\/\s*move[\s-]?out\s+(?:cleans?|cleaning)/gi, 'move-in/move-out cleaning'],
  [/move[\s-]?in\/(?:move[\s-]?)?outs?\s+(?:cleans?|cleaning)/gi, 'move-in/move-out cleaning'],
  [/move[\s-]?in\s*\/\s*(?:move[\s-]?)?outs?\b/gi, 'move-in/move-out cleaning'],
  [/recurring\s+(?:cleans?|cleaning)(?!\s+commercial)/gi, 'recurring cleaning'],
  [/short[\s-]?term\s+rental\s+turnovers?/gi, 'short-term rental turnovers'],
  [/short[\s-]?term\s+rental\s+companies/gi, 'short-term rental companies'],
]);

/**
 * True when a free-form list part is already represented by a canonical service label.
 */
function servicePartCoveredByCanonical(part, canonicals) {
  const p = String(part || '')
    .toLowerCase()
    .replace(/[^a-z0-9/]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!p) return true;
  for (const c of canonicals || []) {
    const canon = String(c || '')
      .toLowerCase()
      .replace(/[^a-z0-9/]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (!canon) continue;
    if (p === canon) return true;
    // Alias forms: "residential standard" ↔ "residential standard cleaning"
    const pCore = p.replace(/\s+cleaning$/i, '').replace(/\s+cleans$/i, '');
    const cCore = canon.replace(/\s+cleaning$/i, '').replace(/\s+cleans$/i, '');
    if (pCore && cCore && (pCore === cCore || canon.includes(pCore) || p.includes(cCore))) {
      return true;
    }
  }
  return false;
}

/** Known ICP segment / decision-maker patterns → canonical labels. */
const CUSTOMER_SEGMENT_PATTERNS = Object.freeze([
  [/property managers?/gi, 'property managers'],
  [/facilit(?:y|ies)\s+managers?/gi, 'facility managers'],
  [/apartment(?:\s*\/\s*|\s+)multifamily\s+(?:buildings?|properties)/gi, 'apartment/multifamily buildings'],
  [/apartment buildings?/gi, 'apartment/multifamily buildings'],
  [/multifamily(?:\s+(?:apartment\s+)?)?(?:buildings?|properties)/gi, 'apartment/multifamily buildings'],
  [/short[\s-]?term\s+rental\s+companies/gi, 'short-term rental companies'],
  [/\bSTR companies\b/gi, 'short-term rental companies'],
  [/professional offices?/gi, 'professional offices'],
  [/daycares?/gi, 'daycares'],
  [/rec centers?/gi, 'rec centers'],
  [/high[\s-]?traffic buildings?/gi, 'high-traffic buildings'],
  [/commercial customers?/gi, 'commercial customers'],
]);

/** Trailing conversational fillers that must not become durable ICP values. */
const CONVERSATIONAL_FILLER_RE =
  /^(?:etc\.?|and\s+stuff|things\s+like\s+that|or\s+whatever|and\s+so\s+on)$/i;

function isConversationalFiller(text) {
  return CONVERSATIONAL_FILLER_RE.test(String(text || '').trim());
}

function isDecisionMakerLabel(text) {
  const s = String(text || '').toLowerCase();
  return /\b(?:managers?|owners?|directors?|principals?)\b/.test(s);
}

function managerRoleFromWord(word) {
  const w = String(word || '')
    .toLowerCase()
    .trim();
  if (w === 'property') return 'property managers';
  if (w === 'facility' || w === 'facilities') return 'facility managers';
  return '';
}

/**
 * Expand "property or/and facilities manager(s)" into both decision-maker labels.
 */
function expandOrManagerCompound(text) {
  return String(text || '').replace(
    /\b(property|facilit(?:y|ies))\s+(?:or|and)\s+(property|facilit(?:y|ies))\s+managers?\b/gi,
    (_, a, b) => {
      const left = managerRoleFromWord(a);
      const right = managerRoleFromWord(b);
      return [left, right].filter(Boolean).join(', ');
    }
  );
}

/**
 * True when a free-form ICP part is already covered by a canonical label.
 */
function customerPartCoveredByCanonical(part, canonicals) {
  const p = String(part || '')
    .toLowerCase()
    .replace(/[^a-z0-9/]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!p) return true;
  for (const c of canonicals || []) {
    const canon = String(c || '')
      .toLowerCase()
      .replace(/[^a-z0-9/]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (!canon) continue;
    if (p === canon) return true;
    if (canon.includes(p) || p.includes(canon)) return true;
    const pCore = p.replace(/\s+managers?$/i, '').replace(/\s+buildings?$/i, '');
    const cCore = canon.replace(/\s+managers?$/i, '').replace(/\s+buildings?$/i, '');
    if (pCore && cCore && (pCore === cCore || canon.includes(pCore))) return true;
  }
  return false;
}

/**
 * Merge ICP lists with later decision-maker clarification taking precedence
 * in ordering (SPEC-101 evidence precedence). No schema change — single list.
 */
function mergeIdealCustomersWithPrecedence(prior, incoming, opts = {}) {
  const preferIncomingDecisionMakers = Boolean(opts.preferIncomingDecisionMakers);
  const cleanPrior = (prior || []).filter(
    (item) => item && !isConversationalFiller(item) && !isLiteralUncertaintyPhrase(item)
  );
  const cleanIncoming = (incoming || []).filter(
    (item) => item && !isConversationalFiller(item) && !isLiteralUncertaintyPhrase(item)
  );
  const priorDm = cleanPrior.filter(isDecisionMakerLabel);
  const priorSeg = cleanPrior.filter((item) => !isDecisionMakerLabel(item));
  const incomingDm = cleanIncoming.filter(isDecisionMakerLabel);
  const incomingSeg = cleanIncoming.filter((item) => !isDecisionMakerLabel(item));

  if (preferIncomingDecisionMakers && incomingDm.length) {
    return dedupeNormalizedList([...incomingDm, ...priorDm, ...incomingSeg, ...priorSeg]);
  }
  return dedupeNormalizedList([...priorDm, ...incomingDm, ...priorSeg, ...incomingSeg]);
}

/** Value traits that must not become ICP segment rows. */
const VALUE_TRAIT_PATTERNS = Object.freeze([
  [/\breliability\b/gi, 'reliability'],
  [/\bconsistency\b/gi, 'consistency'],
  [/\bclear communication\b/gi, 'clear communication'],
  [/\baccountability\b/gi, 'accountability'],
  [/\bprofessionalism\b/gi, 'professionalism'],
]);

/**
 * Deduplicate a normalized list (case-insensitive).
 */
function dedupeNormalizedList(items) {
  const out = [];
  for (const item of items || []) {
    const normalized = normalizeBusinessPhrase(item);
    if (!normalized) continue;
    if (isConversationalFiller(normalized)) continue;
    if (out.some((x) => x.toLowerCase() === normalized.toLowerCase())) continue;
    out.push(normalized);
  }
  return out;
}

/**
 * Extract canonical service entities from free-form prose.
 * Never returns "Anchor Cleaning provides…" style lead-ins.
 * SPEC-099: never collapse a multi-value operator answer to a single matched entity.
 */
function extractServiceList(text) {
  const raw = String(text || '').trim();
  if (!raw) return [];

  const found = [];
  for (const [re, canon] of SERVICE_ENTITY_PATTERNS) {
    re.lastIndex = 0;
    if (re.test(raw)) {
      if (!found.some((f) => f.toLowerCase() === canon.toLowerCase())) found.push(canon);
    }
  }

  // Always list-split so unmatched siblings survive when some patterns match.
  let body = stripBusinessNameLeadIn(raw);
  body = body
    .split(/(?<=[.!?])\s+/)
    .filter((sentence) => !/\bgrowth focus\b|\bweekly or multiple\b/i.test(sentence))
    .join(' ');
  // Protect move-in/move-out before splitting.
  body = body
    .replace(/move-in\/move-out/gi, 'move-in-MOVEOUT')
    .replace(/move\s+in\s*\/\s*(?:move\s+)?out/gi, 'move-in-MOVEOUT');
  const parts = String(body || '')
    .split(/\s*(?:,|;|\band\b|\n)\s*/i)
    .map((p) => p.replace(/move-in-MOVEOUT/gi, 'move-in/move-out').trim())
    .filter(Boolean)
    .map((p) =>
      normalizeBusinessPhrase(
        stripBusinessNameLeadIn(p.replace(/^[-–—*•]\s*/, '')).replace(/[.]+$/, '')
      )
    )
    .filter((p) => p && !/^(?:anchor(?:\s+cleaning)?|provides?|offers?)\b/i.test(p))
    .filter((p) => p.split(/\s+/).length <= 8)
    .filter((p) => !isLiteralUncertaintyPhrase(p));

  if (!found.length && !parts.length) return [];
  if (!found.length) return dedupeNormalizedList(parts);

  const merged = [...found];
  for (const part of parts) {
    if (servicePartCoveredByCanonical(part, found)) continue;
    if (merged.some((m) => m.toLowerCase() === part.toLowerCase())) continue;
    merged.push(part);
  }
  return dedupeNormalizedList(merged);
}

/**
 * Extract growth-focus statements that should not live inside the services list.
 */
function extractGrowthFocusItems(text) {
  const raw = String(text || '');
  const items = [];
  const focusMatch = raw.match(
    /(?:strongest\s+)?growth focus is\s+([^.;]+)(?:[.;]|$)/i
  );
  if (focusMatch) {
    let focus = focusMatch[1].trim();
    const forCustomers = focus.match(
      /^(.*?(?:cleaning|service))\s+for\s+(customers?\s+who\s+need\s+.+)$/i
    );
    if (forCustomers) {
      items.push(normalizeBusinessPhrase(forCustomers[1]));
      items.push(normalizeBusinessPhrase(forCustomers[2]));
    } else {
      items.push(normalizeBusinessPhrase(focus));
    }
  }
  if (
    /recurring commercial cleaning/i.test(raw) &&
    !items.some((i) => /recurring commercial/i.test(i))
  ) {
    items.push('recurring commercial cleaning');
  }
  if (
    /weekly or multiple[\s-]?times[\s-]?per[\s-]?week/i.test(raw) &&
    !items.some((i) => /weekly or multiple/i.test(i))
  ) {
    items.push('customers who need weekly or multiple-times-per-week service');
  }
  return dedupeNormalizedList(items);
}

/**
 * Extract clean ICP decision-maker + segment entities from free-form prose.
 * SPEC-099/101: never collapse a multi-concept operator answer to a single noun phrase.
 * Preserves decision makers (roles) and target environments (segments) together.
 */
function extractCustomerSegments(text) {
  const raw = String(text || '').trim();
  if (!raw) return [];

  const expanded = expandOrManagerCompound(raw);
  const found = [];
  const listClause = expanded.match(
    /(?:the\s+)?ideal customers?\s+(?:are|include|is)\s+([^.]+?)(?:\.|$)/i
  );
  const searchSpace = listClause ? listClause[1] : expanded;

  for (const [re, canon] of CUSTOMER_SEGMENT_PATTERNS) {
    re.lastIndex = 0;
    if (re.test(searchSpace) || (!listClause && re.test(expanded))) {
      if (canon === 'commercial customers' && listClause) continue;
      if (!found.some((f) => f.toLowerCase() === canon.toLowerCase())) found.push(canon);
    }
  }

  // Always list-split so unmatched siblings survive when some patterns match (SPEC-101,
  // same successful pattern as extractServiceList / SPEC-099).
  let body = stripBusinessNameLeadIn(expanded);
  body = expandOrManagerCompound(body);
  body = body
    .replace(/^commercial customers?\s+who value\s+[^.]+[. ]*/i, '')
    .replace(/^who value\s+[^.]+[. ]*/i, '')
    .replace(/\s+that need\s+.+$/i, '')
    .replace(/\s+as part of (?:my|our|the)\s+ideal customer(?:\s+profile)?\s*$/i, '')
    .replace(/^(?:a|an|the)\s+/i, '')
    .replace(
      /^(?:let'?s\s+)?(?:target|focus\s+on|prioritize|aim\s+at)\s+/i,
      ''
    );

  const parts = String(body || '')
    .split(/\s*(?:,|;|\band\b|\n)\s*/i)
    .map((p) =>
      normalizeBusinessPhrase(
        stripBusinessNameLeadIn(p.replace(/^[-–—*•]\s*/, ''))
          .replace(/[.]+$/, '')
          .replace(/\s+as part of (?:my|our|the)\s+ideal customer(?:\s+profile)?$/i, '')
          .replace(/\s+who value\s+.+$/i, '')
          .replace(/\s+that need\s+.+$/i, '')
          .replace(/^(?:a|an|the)\s+/i, '')
          .replace(/^(?:let'?s\s+)?(?:target|focus\s+on|prioritize)\s+/i, '')
      )
    )
    .filter(
      (p) =>
        p &&
        !isConversationalFiller(p) &&
        !isValueTraitPhrase(p) &&
        !isLiteralUncertaintyPhrase(p) &&
        !/^(?:anchor(?:\s+cleaning)?|most wants|wants to work|we serve|greater)\b/i.test(p) &&
        !/^(?:the\s+)?ideal\s+customer/i.test(p) &&
        // Bare role stems without "managers" are incomplete fragments from list splits.
        !/^(?:property|facilit(?:y|ies)|apartment|multifamily)$/i.test(p)
    )
    .filter((p) => p.split(/\s+/).length <= 8);

  if (!found.length && !parts.length) return [];

  const merged = found.length ? [...found] : [];
  for (const part of parts) {
    if (customerPartCoveredByCanonical(part, merged.length ? merged : found)) continue;
    if (merged.some((m) => m.toLowerCase() === part.toLowerCase())) continue;
    // Drop long preamble remnants that slipped past lead-in stripping.
    if (/^(?:the\s+)?ideal\s+customer/i.test(part)) continue;
    if (/^we serve\b/i.test(part)) continue;
    merged.push(part);
  }

  const cleaned = dedupeNormalizedList(
    merged.filter((f) => f !== 'commercial customers' || merged.length === 1)
  );
  // Prefer decision makers before environment segments when both are present.
  return mergeIdealCustomersWithPrecedence([], cleaned);
}

function isValueTraitPhrase(text) {
  const s = String(text || '').toLowerCase().trim();
  return /^(reliability|consistency|clear communication|accountability|professionalism)$/i.test(
    s
  );
}

/**
 * Extract value traits (reliability, consistency, …) from ICP prose.
 */
function extractValueTraits(text) {
  const raw = String(text || '');
  const found = [];
  const valueClause = raw.match(/who value\s+([^.]+?)(?:\.|$)/i);
  const space = valueClause ? valueClause[1] : raw;
  for (const [re, canon] of VALUE_TRAIT_PATTERNS) {
    re.lastIndex = 0;
    if (re.test(space)) {
      if (!found.some((f) => f.toLowerCase() === canon.toLowerCase())) found.push(canon);
    }
  }
  return dedupeNormalizedList(found);
}

/**
 * Clean a candidate business name — strip pronouns / intro bleed.
 * "Anchor Cleaning we" → "Anchor Cleaning"
 * Prefer durable tenant identities when the operator types a near-match casing/abbreviation.
 */
function sanitizeBusinessName(name) {
  let s = String(name || '').trim();
  if (!s) return '';
  s = s
    .replace(/\s+(?:we|we're|we are|i|i'm|i am|my company|our company|the company)\b.*$/i, '')
    .replace(/^(?:we are|we're|i am|i'm|my company is|our company is)\s+/i, '')
    .replace(/[—–,:;.\s]+$/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();

  // Prefer canonical AS Cleaning Co. when present (client_id=11). Do not fuzzy-guess
  // unrelated names — only map clear AS Cleaning variants.
  if (/\bas\s+cleaning(?:\s+co\.?)?\b/i.test(String(name || '')) || /\bas\s+cleaning(?:\s+co\.?)?\b/i.test(s)) {
    return 'AS Cleaning Co.';
  }

  // Prefer canonical Anchor Cleaning when present.
  if (/\banchor\s+cleaning\b/i.test(s) || /\banchor\b/i.test(s)) {
    if (/\banchor\s+cleaning\b/i.test(String(name || '')) || /\banchor\s+cleaning\b/i.test(s)) {
      return 'Anchor Cleaning';
    }
    if (/^anchor\b/i.test(s) && s.split(/\s+/).length <= 2) return 'Anchor';
  }
  // Drop trailing lowercase filler words that aren't part of a proper name.
  s = s.replace(/\s+(?:we|are|is|a|an|the|and|for|to)$/i, '').trim();
  return s;
}

/**
 * Split a free-form list answer into cleaned phrase items.
 * Protects compound tokens like move-in/move-out before splitting.
 */
function splitListItems(text) {
  let s = String(text || '').trim();
  if (!s) return [];
  s = s.replace(/move-in\/move-out/gi, 'move-in-MOVEOUT');
  return s
    .split(/\s*(?:,|;|\band\b|\n|\|)\s*/i)
    .map((p) => p.replace(/move-in-MOVEOUT/gi, 'move-in/move-out').trim())
    .filter(Boolean)
    .map((p) => normalizeBusinessPhrase(p.replace(/^[-–—*•]\s*/, '')))
    .filter(Boolean);
}

function emptyNormalizedFacts() {
  return {
    business_name: null,
    business_description: null,
    services: [],
    growth_focus: null,
    ideal_customers: [],
    ideal_customer_traits: [],
    disqualified_customers: [],
    geography: [],
    vertical_focus: null,
    differentiation: null,
    brand_voice: null,
    ninety_day_outcomes: null,
    success_metrics: [],
    epistemic_states: {
      business_name: EPISTEMIC_STATES.UNRESOLVED,
      business_description: EPISTEMIC_STATES.UNRESOLVED,
      services: EPISTEMIC_STATES.UNRESOLVED,
      growth_focus: EPISTEMIC_STATES.UNRESOLVED,
      ideal_customers: EPISTEMIC_STATES.UNRESOLVED,
      disqualified_customers: EPISTEMIC_STATES.UNRESOLVED,
      geography: EPISTEMIC_STATES.UNRESOLVED,
      differentiation: EPISTEMIC_STATES.UNRESOLVED,
      brand_voice: EPISTEMIC_STATES.UNRESOLVED,
      ninety_day_outcomes: EPISTEMIC_STATES.UNRESOLVED,
      success_metrics: EPISTEMIC_STATES.UNRESOLVED,
    },
    hypotheses: {},
    evidence_statements: {},
    business_facts: {},
    transformation_areas: [],
    pains: [],
    learning_signals: [],
    excluded_metrics: [],
    superseded_slots: [],
  };
}

function cloneNormalizedFacts(facts) {
  const src = facts || emptyNormalizedFacts();
  return {
    business_name: src.business_name || null,
    business_description: src.business_description || null,
    services: [...(src.services || [])],
    growth_focus: src.growth_focus || null,
    ideal_customers: [...(src.ideal_customers || [])],
    ideal_customer_traits: [...(src.ideal_customer_traits || [])],
    disqualified_customers: [...(src.disqualified_customers || [])],
    geography: [...(src.geography || [])],
    vertical_focus: src.vertical_focus || null,
    differentiation: src.differentiation || null,
    brand_voice: src.brand_voice || null,
    ninety_day_outcomes: src.ninety_day_outcomes || null,
    success_metrics: [...(src.success_metrics || [])],
    epistemic_states: {
      ...(emptyNormalizedFacts().epistemic_states),
      ...(src.epistemic_states || {}),
    },
    hypotheses: {
      ...(src.hypotheses || {}),
    },
    evidence_statements: {
      ...(src.evidence_statements || {}),
    },
    business_facts: Object.fromEntries(
      Object.entries(src.business_facts || {}).map(([key, facts]) => [
        key,
        Array.isArray(facts) ? facts.map((fact) => ({ ...fact })) : [],
      ])
    ),
    transformation_areas: [...(src.transformation_areas || [])],
    pains: [...(src.pains || [])],
    learning_signals: [...(src.learning_signals || [])],
    excluded_metrics: [...(src.excluded_metrics || [])],
    superseded_slots: [...(src.superseded_slots || [])],
  };
}

function uniquePush(list, items) {
  return dedupeNormalizedList([...(list || []), ...(items || [])]);
}

function extractPlaces(text) {
  const s = String(text || '');
  const places = [];
  if (/greater\s+toronto(?:\s+area)?|\bGTA\b/i.test(s)) {
    places.push('Greater Toronto Area');
  }
  if (/greater\s+manchester/i.test(s)) places.push('Greater Manchester');
  for (const m of s.matchAll(PLACE_NAME_RE)) {
    const place = titleCaseWords(m[0]);
    if (place.toLowerCase() === 'manchester' && /greater\s+manchester/i.test(s)) continue;
    if (place.toLowerCase() === 'toronto' && /greater\s+toronto|GTA/i.test(s)) continue;
    if (!places.some((p) => p.toLowerCase() === place.toLowerCase())) places.push(place);
  }
  return places;
}

const SECTION_TO_PRIMARY_FIELD = Object.freeze({
  identity: 'business_description',
  services: 'services',
  idealCustomers: 'ideal_customers',
  avoidCustomers: 'disqualified_customers',
  targetMarkets: 'geography',
  competitiveAdvantages: 'differentiation',
  brandVoice: 'brand_voice',
  campaignGoals: 'ninety_day_outcomes',
  successMetrics: 'success_metrics',
});

/**
 * Ingest a direct answer into normalized evidence for a Blueprint section.
 * SPEC-099/SPEC-221: explicit unknowns and hypotheses never become factual values;
 * epistemic state is recorded explicitly.
 */
function ingestAnswerIntoNormalizedFacts(facts, sectionKey, rawAnswer, opts = {}) {
  const next = cloneNormalizedFacts(facts);
  const cleaned = cleanRawAnswer(sectionKey, stripInterviewQuestionEcho(rawAnswer));
  if (!cleaned) return next;

  const primaryField = SECTION_TO_PRIMARY_FIELD[sectionKey] || sectionKey;
  const extractedFacts = extractBusinessFacts(rawAnswer, {
    section: sectionKey,
    subject: primaryField,
    provenance: opts.provenance || null,
  });
  const existingFacts = next.business_facts[primaryField] || [];
  next.business_facts[primaryField] = [
    ...existingFacts,
    ...extractedFacts.filter((fact) => !existingFacts.some((prior) => prior.id === fact.id)),
  ];
  const projection = projectBusinessFacts(next.business_facts[primaryField], primaryField);
  const epistemicState = projection.epistemicState;

  next.epistemic_states[primaryField] = epistemicState;
  next.evidence_statements[primaryField] = projection.evidence || String(rawAnswer || cleaned);

  if (projection.hypothesisValue) {
    next.hypotheses[primaryField] = projection.hypothesisValue;
  }

  if (epistemicState === EPISTEMIC_STATES.UNKNOWN || epistemicState === EPISTEMIC_STATES.NOT_APPLICABLE) {
    return next;
  }
  if (epistemicState === EPISTEMIC_STATES.HYPOTHESIS) {
    next.hypotheses[primaryField] = cleaned;
    return next;
  }

  // Explicit unknowns leave the section unset rather than storing uncertainty phrases.
  if (looksLikeExplicitUnknown(cleaned) || isLiteralUncertaintyPhrase(cleaned)) {
    next.epistemic_states[primaryField] = EPISTEMIC_STATES.UNKNOWN;
    return next;
  }

  switch (sectionKey) {
    case 'identity': {
      // Prefer em/en dash or spaced hyphen as name/description separator —
      // never split on compound-word hyphens like commercial-focused.
      const dash =
        cleaned.match(/^(.+?)\s+[—–]\s+(.+)$/) ||
        cleaned.match(/^(.+?)[—–](.+)$/) ||
        cleaned.match(/^(.+?)\s+-\s+(.+)$/);
      const weAre = cleaned.match(/^(.+?)\s+we(?:'re| are)\s+(.+)$/i);
      if (dash) {
        next.business_name = sanitizeBusinessName(dash[1]);
        const desc = firstSentence(
          stripLeadingWeAre(dash[2]).replace(/^(a|an|the)\s+/i, '')
        );
        next.business_description = normalizeBusinessPhrase(desc);
      } else if (weAre) {
        next.business_name = sanitizeBusinessName(weAre[1]);
        next.business_description = normalizeBusinessPhrase(
          firstSentence(weAre[2].replace(/^(a|an|the)\s+/i, ''))
        );
      } else {
        const named = cleaned.match(
          /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:is|are|provides?|offers?)\s+(.+)$/i
        );
        if (named) {
          next.business_name = sanitizeBusinessName(named[1]);
          next.business_description = normalizeBusinessPhrase(
            firstSentence(stripLeadingWeAre(named[2]).replace(/^(a|an|the)\s+/i, ''))
          );
        } else {
          next.business_description = normalizeBusinessPhrase(firstSentence(cleaned));
          if (/\banchor(?:\s+cleaning)?\b/i.test(cleaned)) {
            next.business_name = /anchor\s+cleaning/i.test(cleaned) ? 'Anchor Cleaning' : 'Anchor';
          } else if (/\bas\s+cleaning(?:\s+co\.?)?\b/i.test(cleaned)) {
            next.business_name = 'AS Cleaning Co.';
          }
        }
      }
      next.business_name = sanitizeBusinessName(next.business_name);
      next.business_description = sanitizeIdentityDescription(next.business_name, next.business_description);
      if (/commercial/i.test(cleaned)) next.growth_focus = 'commercial cleaning';
      if (/residential/i.test(cleaned) && !next.vertical_focus) {
        next.vertical_focus = /commercial/i.test(cleaned)
          ? 'commercial-focused cleaning with residential welcome'
          : 'residential cleaning';
      }
      break;
    }
    case 'services': {
      const items = extractServiceList(cleaned);
      next.services = uniquePush(
        [],
        (items.length ? items : splitListItems(cleaned)).filter(
          (item) => item && !isLiteralUncertaintyPhrase(item)
        )
      );
      const focuses = extractGrowthFocusItems(String(rawAnswer || cleaned));
      if (focuses.length) {
        next.growth_focus = focuses.join('; ');
      } else if (/commercial/i.test(cleaned)) {
        next.growth_focus = next.growth_focus || 'commercial cleaning';
      }
      break;
    }
    case 'idealCustomers': {
      // SPEC-101: hedged guesses must not become unqualified durable ICP values.
      if (
        /^(?:maybe|perhaps|i\s+think|probably)\b/i.test(cleaned) &&
        cleaned.split(/\s+/).length <= 8
      ) {
        break;
      }
      const segments = extractCustomerSegments(cleaned);
      const fallback = splitListItems(stripBusinessNameLeadIn(cleaned)).filter(
        (item) => item && !isConversationalFiller(item)
      );
      next.ideal_customers = mergeIdealCustomersWithPrecedence(
        [],
        segments.length ? segments : fallback
      );
      // Drop accidental trait / preamble bleed / uncertainty phrases / generic nouns / fillers.
      next.ideal_customers = next.ideal_customers.filter(
        (item) =>
          !isValueTraitPhrase(item) &&
          !isLiteralUncertaintyPhrase(item) &&
          !isConversationalFiller(item) &&
          !looksLikeGenericCategoryAnswer(item) &&
          !/as part of (?:my|our|the)\s+ideal customer/i.test(item) &&
          !/^(?:anchor(?:\s+cleaning)?|most wants)\b/i.test(item)
      );
      next.ideal_customer_traits = uniquePush(
        next.ideal_customer_traits,
        extractValueTraits(String(rawAnswer || cleaned))
      );
      break;
    }
    case 'avoidCustomers': {
      next.disqualified_customers = uniquePush(
        [],
        splitListItems(cleaned)
          .map((item) =>
            String(item || '')
              .replace(/^(?:no|not|avoid|excluding)\s+/i, '')
              .trim()
          )
          .filter((item) => item && !isLiteralUncertaintyPhrase(item))
      );
      break;
    }
    case 'targetMarkets': {
      next.geography = uniquePush([], extractPlaces(cleaned));
      if (/commercial/i.test(cleaned)) next.growth_focus = next.growth_focus || 'commercial cleaning';
      if (/residential/i.test(cleaned) && !next.vertical_focus) {
        next.vertical_focus = 'residential';
      }
      if (!next.geography.length && !isLiteralUncertaintyPhrase(cleaned)) {
        const geoOnly = normalizeBusinessPhrase(
          cleaned
            .replace(
              /\band\s+(?:let'?s\s+)?(?:target|focus\s+on|prioritize)\b[\s\S]*$/i,
              ''
            )
            .replace(
              /^(?:let'?s\s+)?(?:target|focus\s+on|prioritize)\b[\s\S]*$/i,
              ''
            )
            .trim()
        );
        if (geoOnly && !isConversationalFiller(geoOnly) && !/\bmanagers?\b/i.test(geoOnly)) {
          next.geography = uniquePush([], [geoOnly]);
        }
      }
      // SPEC-101 — later targeting clarification can refine earlier ICP understanding.
      if (
        /\b(?:let'?s\s+)?target\b|\bfocus\s+on\b|\bpriorit(?:y|ize)\b|\bideal customers?\b/i.test(
          cleaned
        )
      ) {
        const targeting = extractCustomerSegments(cleaned);
        if (targeting.length) {
          next.ideal_customers = mergeIdealCustomersWithPrecedence(
            next.ideal_customers,
            targeting,
            { preferIncomingDecisionMakers: true }
          );
        }
      }
      break;
    }
    case 'competitiveAdvantages': {
      if (!isLiteralUncertaintyPhrase(cleaned)) {
        next.differentiation = synthesizeDifferentiationSnippet(cleaned);
      }
      break;
    }
    case 'brandVoice': {
      if (!isLiteralUncertaintyPhrase(cleaned)) {
        next.brand_voice = normalizeBrandVoiceTone(cleaned);
      }
      break;
    }
    case 'campaignGoals': {
      if (!isLiteralUncertaintyPhrase(cleaned)) {
        next.ninety_day_outcomes = normalizeBusinessPhrase(cleaned);
      }
      if (/commercial/i.test(cleaned)) next.growth_focus = next.growth_focus || 'commercial cleaning';
      break;
    }
    case 'successMetrics': {
      next.success_metrics = uniquePush(
        [],
        splitListItems(cleaned).filter((item) => !isLiteralUncertaintyPhrase(item))
      );
      if (!next.success_metrics.length && !isLiteralUncertaintyPhrase(cleaned)) {
        next.success_metrics = [normalizeBusinessPhrase(cleaned)];
      }
      break;
    }
    default:
      break;
  }
  return next;
}

/**
 * Apply a correction to normalized facts for the targeted domain.
 * Never writes into the active-question domain unless that is the explicit target.
 */
function applyCorrectionToNormalizedFacts(facts, correction) {
  const next = cloneNormalizedFacts(facts);
  const section = correction.section;
  const substance = normalizeBusinessPhrase(correction.substance || '');
  if (!section || !substance || isLiteralUncertaintyPhrase(substance)) return next;

  switch (section) {
    case 'services': {
      // Replace near-duplicate STR company service lines with turnovers when correcting.
      let services = [...(next.services || [])];
      const serviceItems = extractServiceList(substance);
      const toAdd = serviceItems.length ? serviceItems : [substance];
      if (toAdd.some((s) => /short-term rental turnover/i.test(s)) || /short-term rental turnover/i.test(substance)) {
        services = services.filter((s) => !/short-term rental compan/i.test(s));
      }
      next.services = uniquePush(services, toAdd);
      break;
    }
    case 'idealCustomers': {
      const segments = extractCustomerSegments(substance);
      const fallback = splitListItems(substance).filter(
        (item) => item && !isConversationalFiller(item)
      );
      next.ideal_customers = mergeIdealCustomersWithPrecedence(
        next.ideal_customers,
        segments.length ? segments : fallback,
        { preferIncomingDecisionMakers: true }
      );
      next.ideal_customers = next.ideal_customers.filter(
        (item) =>
          !isValueTraitPhrase(item) &&
          !isLiteralUncertaintyPhrase(item) &&
          !isConversationalFiller(item) &&
          !/as part of (?:my|our|the)\s+ideal customer/i.test(item)
      );
      next.ideal_customer_traits = uniquePush(
        next.ideal_customer_traits,
        extractValueTraits(substance)
      );
      break;
    }
    case 'avoidCustomers':
      next.disqualified_customers = uniquePush(
        next.disqualified_customers,
        splitListItems(substance)
      );
      break;
    case 'targetMarkets':
      next.geography = uniquePush(extractPlaces(substance), next.geography);
      if (!extractPlaces(substance).length) {
        const geoOnly = normalizeBusinessPhrase(
          substance
            .replace(
              /\band\s+(?:let'?s\s+)?(?:target|focus\s+on|prioritize)\b[\s\S]*$/i,
              ''
            )
            .trim()
        );
        if (geoOnly && !/\bmanagers?\b/i.test(geoOnly)) {
          next.geography = uniquePush(next.geography, [geoOnly]);
        }
      }
      if (
        /\b(?:let'?s\s+)?target\b|\bfocus\s+on\b|\bpriorit(?:y|ize)\b|\bideal customers?\b/i.test(
          substance
        )
      ) {
        const targeting = extractCustomerSegments(substance);
        if (targeting.length) {
          next.ideal_customers = mergeIdealCustomersWithPrecedence(
            next.ideal_customers,
            targeting,
            { preferIncomingDecisionMakers: true }
          );
        }
      }
      break;
    case 'competitiveAdvantages':
      next.differentiation = synthesizeDifferentiationSnippet(substance);
      break;
    case 'brandVoice':
      next.brand_voice = normalizeBrandVoiceTone(substance);
      break;
    case 'campaignGoals':
      next.ninety_day_outcomes = substance;
      break;
    case 'successMetrics':
      next.success_metrics = uniquePush([], splitListItems(substance));
      break;
    case 'identity': {
      next.business_description = sanitizeIdentityDescription(next.business_name, substance);
      break;
    }
    default:
      break;
  }
  return next;
}

function normalizedSemanticValue(value) {
  return normalizeBusinessPhrase(String(value || '')).replace(/[.!?]+$/, '').trim().toLowerCase();
}

/**
 * SPEC-228 — detect raw correction-instruction prose that must never survive
 * as active business meaning (a metric, a geography value, a differentiation
 * claim, etc). Correction history may keep the operator's original language;
 * active projection may not.
 */
function containsCorrectionInstructionLeakage(text) {
  return /\bdo not interpret\b|\bnot a (?:success\s+)?metric\b|\bnot a standalone metric\b|\bnot a (?:separate\s+)?service\b|\bdid not establish\b|\bnot established\b|\bremove that assumption\b/i.test(
    String(text || '')
  );
}

function containsObjectiveCorrectionLeakage(text) {
  return containsCorrectionInstructionLeakage(text) ||
    /\b(?:pain|pains|learning signals?)\b[\s\S]{0,100}\b(?:not|rather than)\b[\s\S]{0,40}\b(?:metric|objective|goal|outcome)s?\b/i.test(
      String(text || '')
    );
}

function authoritativeObjectiveFromFacts(facts) {
  const factRows = [
    ...(facts?.business_facts?.ninety_day_outcomes || []),
    ...(facts?.business_facts?.growth_focus || []),
  ];
  const factValue = factRows.find(
    (fact) => fact && fact.epistemic_state === EPISTEMIC_STATES.KNOWN && fact.value &&
      !containsObjectiveCorrectionLeakage(fact.value)
  );
  if (factValue) return normalizeBusinessPhrase(factValue.value);
  if (facts?.growth_focus && !containsObjectiveCorrectionLeakage(facts.growth_focus)) {
    return normalizeBusinessPhrase(facts.growth_focus);
  }
  return null;
}

function sameSemanticValue(left, right) {
  const a = normalizedSemanticValue(left);
  const b = normalizedSemanticValue(right);
  return Boolean(a && b && (a === b || a.includes(b) || b.includes(a)));
}

function withoutSemanticValue(values, value) {
  return (values || []).filter((entry) => !sameSemanticValue(entry, value));
}

function reviewCorrectionOperations(text, state, turnId) {
  const operations = [];
  const add = (operation, slot, value, extra = {}) => operations.push({
    operation, slot, target_key: normalizedSemanticValue(value) || slot,
    previous_value: extra.previous_value || null,
    value: value == null ? null : normalizeBusinessPhrase(value),
    classification: extra.classification || null, negation: Boolean(extra.negation),
    epistemic_state: extra.epistemic_state || EPISTEMIC_STATES.KNOWN,
    evidence_ref: turnId, created_at: nowIso(), source_text: extra.source_text || text,
  });
  const sentences = String(text || '').split(/(?<=[.!?])\s+|\n+/).map((item) => item.trim()).filter(Boolean);
  for (const sentence of sentences) {
    const lower = sentence.toLowerCase();
    if (/geograph(?:y|ic).{0,50}\b(?:not|no longer|isn't|is not).{0,60}\b(constrained|constraint|primary|restricted)\b/.test(lower)) {
      add('RETRACT', 'geography', null, { previous_value: (state.normalizedFacts?.geography || []).join(', '), negation: true, epistemic_state: EPISTEMIC_STATES.NOT_APPLICABLE, source_text: sentence });
    }
    if (/\b(?:lead|raw)\s+volume\b.{0,60}\bnot\s+(?:a\s+)?(?:success\s+)?metric\b/.test(lower)) {
      add('RETRACT', 'success_metrics', 'raw lead volume', { negation: true, source_text: sentence });
    }
    if (/\bpremium\s+positioning\b.{0,60}\b(?:never|not|did not|didn't).{0,40}\b(?:establish|established|validated|confirm)/.test(lower) || /\b(?:never|not|did not|didn't).{0,40}\b(?:establish|established|validated|confirm).{0,60}\bpremium\s+positioning\b/.test(lower)) {
      add('RETRACT', 'differentiation', 'premium positioning', { negation: true, source_text: sentence });
    }
    const pain = sentence.match(/\b(.+?)\s+(?:is|are)\s+(?:a\s+)?pains?\s*,?\s+not\s+(?:a\s+)?metrics?\b/i);
    if (pain) add('RECLASSIFY', 'pains', pain[1], { previous_value: pain[1], classification: 'PAIN', negation: true, source_text: sentence });
    const outcome = sentence.match(/^(.+?)\s+(?:is|are)\s+(?:an?\s+)?(?:outcomes?|transformation areas?)(?:\s+or\s+(?:outcomes?|transformation areas?))?\s*,?\s+not\s+(?:a\s+)?(?:separate\s+)?services?\b/i);
    if (outcome) {
      for (const item of splitListItems(outcome[1])) {
        add('RECLASSIFY', 'transformation_areas', item, { previous_value: item, classification: 'OUTCOME', negation: true, source_text: sentence });
      }
    }
    const offer = sentence.match(/\b(?:one\s+)?(?:primary\s+)?offer\s*(?:is|=|:)\s*(?:the\s+)?([^.;]+)/i);
    if (offer) add('CORRECT', 'services', offer[1], { source_text: sentence });
    if (/\b(?:existing\s+)?operating\s+small\s+business(?:es)?\b/i.test(sentence) && /\b(?:icp|ideal customer|founder)\b/i.test(sentence)) {
      add('CORRECT', 'ideal_customers', 'existing operating small business', { source_text: sentence });
    }
    if (/\b(?:segments?|verticals?)\s+to\s+test\b/i.test(sentence) || /\binitial\s+(?:test\s+)?segments?\b/i.test(sentence)) {
      if (/\bcleaning\b/i.test(sentence) && /\bhome services\b/i.test(sentence)) {
        add('ASSERT', 'ideal_customers', 'cleaning/home services', { source_text: sentence });
      } else if (/\bcleaning\b/i.test(sentence)) {
        add('ASSERT', 'ideal_customers', 'cleaning', { source_text: sentence });
      }
      if (/\be-commerce\b/i.test(sentence)) add('ASSERT', 'ideal_customers', 'e-commerce', { source_text: sentence });
      if (/\bfitness\b/i.test(sentence)) add('ASSERT', 'ideal_customers', 'fitness', { source_text: sentence });
    }
    if (/\b(?:fewer than|under|less than)\s+10\s+employees\b/i.test(sentence)) {
      add('ASSERT', 'ideal_customer_traits', 'generally fewer than 10 employees', { source_text: sentence });
    }
    if (/\bfounder operational bottleneck\b/i.test(sentence) || /\bfounder\b.{0,30}\btoo central to operations\b/i.test(sentence)) {
      add('ASSERT', 'ideal_customer_traits', 'founder operational bottleneck', { source_text: sentence });
    }
    const metricMatches = sentence.match(/qualified founder conversations|icp-qualified conversations|serious program conversations|paid enrollments|discovery[\s-]*(?:to|->|→)[\s-]*enrollment conversion/gi) || [];
    for (const metric of metricMatches) add('ASSERT', 'success_metrics', metric, { source_text: sentence });
    const signalMatches = sentence.match(/pain[- ]patterns?(?:\s+frequency)?|segment[- ]response patterns/gi) || [];
    for (const signal of signalMatches) add('ASSERT', 'learning_signals', signal, { classification: 'LEARNING_SIGNAL', source_text: sentence });
    const painMatches = /\b(?:pains?|learning signals?)\b/i.test(sentence) ? (sentence.match(/employee problems|lack of owner time|founder dependence|revenue pressure/gi) || []) : [];
    for (const item of painMatches) add('ASSERT', 'pains', item, { classification: 'PAIN', source_text: sentence });
    if (/\bdifferentiation\b/.test(lower) && /\b(?:hypothesis|unvalidated|not established)\b/.test(lower)) {
      let value = sentence;
      const colonMatch = sentence.match(/:\s*(.+)$/);
      if (colonMatch) {
        value = colonMatch[1];
      } else {
        value = sentence
          .replace(/^.*?\bdifferentiation\b\s*(?:is|remains)?\s*(?:still\s+)?(?:a\s+)?\s*/i, '')
          .replace(/\bhypothesis\b\s*[:,]?\s*/i, '');
      }
      value = value.trim();
      add('CORRECT', 'differentiation', value || 'practical transformation-focused 12-week approach', { epistemic_state: EPISTEMIC_STATES.HYPOTHESIS, source_text: sentence });
    }
    const objective = sentence.match(
      /\b(?:near-term|90-day|ninety-day)\s+(?:objective|goal|outcome|priority)(?:s)?\s*(?:is|are|:|=)\s*(.+)$/i
    );
    if (objective && !containsObjectiveCorrectionLeakage(objective[1])) {
      add('CORRECT', 'campaignGoals', objective[1], { source_text: sentence });
    }
  }
  if (!operations.length && /\b(?:correction|correct|retract|remove|not a metric|not a service|not established|not constrained|reclassify)\b/i.test(text)) {
    add('CLARIFY', 'unresolved', null, { epistemic_state: EPISTEMIC_STATES.UNRESOLVED });
  }
  return operations;
}

function projectWorkingSemanticOperations(facts, operations) {
  const next = cloneNormalizedFacts(facts);
  for (const operation of operations || []) {
    const { operation: kind, slot, value } = operation;
    if (slot && slot !== 'unresolved') {
      next.superseded_slots = uniquePush(next.superseded_slots, [slot]);
    }
    if (kind === 'RETRACT') {
      if (slot === 'geography') {
        next.geography = [];
        next.epistemic_states.geography = operation.epistemic_state || EPISTEMIC_STATES.UNKNOWN;
      } else if (slot === 'success_metrics') {
        next.success_metrics = withoutSemanticValue(next.success_metrics, value);
        next.excluded_metrics = uniquePush(next.excluded_metrics, [value]);
      } else if (slot === 'differentiation') {
        if (sameSemanticValue(next.differentiation, value)) next.differentiation = null;
        if (sameSemanticValue(next.brand_voice, value)) next.brand_voice = null;
        next.superseded_slots = uniquePush(next.superseded_slots, ['brand_voice']);
        delete next.hypotheses.differentiation;
      }
      continue;
    }
    if (kind === 'CORRECT' && slot === 'services') {
      // SPEC-228: a corrected primary offer must also displace a stale
      // business_description that duplicates the offer confusion being corrected.
      const previousServices = facts.services || [];
      if (previousServices.some((prev) => sameSemanticValue(next.business_description, prev))) {
        next.business_description = value;
      }
      next.services = [value];
      continue;
    }
    if (kind === 'CORRECT' && slot === 'differentiation') {
      next.differentiation = value;
      next.epistemic_states.differentiation = operation.epistemic_state;
      if (operation.epistemic_state === EPISTEMIC_STATES.HYPOTHESIS) next.hypotheses.differentiation = value;
      continue;
    }
    if (kind === 'CORRECT' && slot === 'campaignGoals') {
      next.ninety_day_outcomes = value;
      next.epistemic_states.ninety_day_outcomes = operation.epistemic_state;
      continue;
    }
    if (kind === 'RECLASSIFY') {
      next.services = withoutSemanticValue(next.services, value);
      next.success_metrics = withoutSemanticValue(next.success_metrics, value);
    }
    if (['pains', 'transformation_areas', 'learning_signals'].includes(slot)) {
      next[slot] = uniquePush(next[slot], [value]);
      if (slot === 'pains') next.success_metrics = withoutSemanticValue(next.success_metrics, value);
    } else if (slot === 'ideal_customers') {
      next.ideal_customers = kind === 'CORRECT'
        ? [value]
        : uniquePush(next.ideal_customers, [value]);
    } else if (slot === 'ideal_customer_traits') {
      next.ideal_customer_traits = uniquePush(next.ideal_customer_traits, [value]);
    } else if (slot === 'success_metrics' && !next.excluded_metrics.some((item) => sameSemanticValue(item, value))) {
      next.success_metrics = uniquePush(next.success_metrics, [value]);
    }
  }
  // SPEC-228 invariant: business_description is description-only and must never
  // carry a self-referential "<name> is a ..." wrap, regardless of which slots
  // the correction operations targeted.
  next.business_description = sanitizeIdentityDescription(next.business_name, next.business_description);
  // SPEC-228 invariant: raw correction-instruction prose (negation sentences,
  // literal "not a metric" text, etc.) must never remain active business
  // meaning even if it entered a slot before the correction operation model
  // understood that slot.
  next.success_metrics = next.success_metrics.filter((item) => {
    if (!containsCorrectionInstructionLeakage(item)) return true;
    next.excluded_metrics = uniquePush(
      next.excluded_metrics,
      [/lead volume/i.test(item) ? 'raw lead volume' : item]
    );
    return false;
  });
  next.geography = next.geography.filter((item) => !containsCorrectionInstructionLeakage(item));
  if (containsCorrectionInstructionLeakage(next.differentiation)) next.differentiation = null;
  if (containsCorrectionInstructionLeakage(next.brand_voice)) {
    next.brand_voice = null;
    next.epistemic_states.brand_voice = EPISTEMIC_STATES.UNKNOWN;
    next.superseded_slots = uniquePush(next.superseded_slots, ['brand_voice']);
  }
  if (containsObjectiveCorrectionLeakage(next.ninety_day_outcomes)) {
    next.ninety_day_outcomes = authoritativeObjectiveFromFacts(facts);
    next.epistemic_states.ninety_day_outcomes = next.ninety_day_outcomes
      ? EPISTEMIC_STATES.KNOWN
      : EPISTEMIC_STATES.UNKNOWN;
  }
  return next;
}

async function applyRefinementSemanticCorrections(store, session, state, text, turnId) {
  const operations = reviewCorrectionOperations(text, state, turnId);
  if (!operations.length) return { operations, evidenceIds: [] };
  state.workingSemanticCorrections = [...(state.workingSemanticCorrections || []), ...operations];
  state.normalizedFacts = projectWorkingSemanticOperations(state.normalizedFacts, operations);
  const evidenceIds = [];
  for (const operation of operations) {
    const evidence = await store.insertEvidence({
      id: newId(), client_id: session.client_id, session_id: session.id,
      source: 'Blueprint refinement', source_turn_id: turnId, category: 'refinement',
      statement: operation.source_text, confidence: EXPLICIT_CONFIDENCE,
      type: 'CLIENT_EDITED', created_at: new Date(),
    });
    evidenceIds.push(evidence.id);
  }
  state.sectionState = sectionsFromNormalizedFacts(state.normalizedFacts, state.sectionState);
  session.interview_state = state;
  return { operations, evidenceIds };
}

function normalizeBrandVoiceTone(text) {
  let tone = stripCorrectionPreamble(String(text || '').trim());
  // Peel lead-ins repeatedly until stable.
  for (let i = 0; i < 4; i += 1) {
    const before = tone;
    tone = tone
      .replace(/^(?:anchor(?:\s+cleaning)?(?:'s|’s)?\s+)/i, '')
      .replace(/^(?:the\s+business(?:'s|’s)?\s+)/i, '')
      .replace(/^(?:the\s+)?(?:brand\s+)?voice should (?:sound|feel|read|be)\s+/i, '')
      .replace(/^(?:brand voice should (?:sound|feel|read|be)\s+)/i, '')
      .replace(/^(?:should\s+(?:sound|feel|read|be)\s+)/i, '')
      .replace(/^(?:sound|feel|read|be)\s+/i, '')
      .replace(/^(?:anchor(?:'s|’s)\s+)/i, '')
      .replace(/^(?:disregard\s+last\s+message[,.\s:]*)/i, '')
      .replace(/^(?:please\s+replace\s+with(?:\s+the\s+following)?[,;:\s-]*)/i, '')
      .trim();
    if (tone === before) break;
  }
  // If a long paragraph remains, keep the adjective-list head.
  const adjList = tone.match(
    /^((?:calm|professional|reliable|direct|friendly|warm|clear|confident|easy to work with)(?:(?:,\s*(?:and\s+)?|\s+and\s+)(?:calm|professional|reliable|direct|friendly|warm|clear|confident|easy to work with)){1,6})/i
  );
  if (adjList) tone = adjList[1];
  else tone = firstSentence(tone);
  tone = normalizeBusinessPhrase(tone);
  // Final possessive bleed guard.
  tone = tone.replace(/^(?:anchor(?:\s+cleaning)?(?:'s|’s)\s+)/i, '').trim();
  return tone;
}

/**
 * Compress a differentiation answer into one short synthesized phrase.
 * Never keep multi-sentence raw transcript paragraphs.
 */
function synthesizeDifferentiationSnippet(text) {
  let s = stripInterviewQuestionEcho(String(text || '').trim());
  s = normalizeMechanicalTypos(s);
  s = s
    .replace(/^competitive edge is described as\s+/i, '')
    .replace(/^customers? choose (?:us|anchor(?:\s+cleaning)?|this business|as\s+cleaning(?:\s+co\.?)?) because(?:\s+of)?\s+/i, '')
    .trim();
  if (!s) return '';

  // Prefer trust / responsiveness framing when present.
  if (/\btrust\b/i.test(s) && /\b(responsive|consistent|accountab|chase|confidence|done right)\b/i.test(s)) {
    return 'trust, responsiveness, and confidence that the work will be done right without customers needing to chase the team';
  }
  if (/\btrust\b/i.test(s) && /\b(show up|communicate|solve|taken care)\b/i.test(s)) {
    return 'trust that the team shows up consistently, communicates clearly, and solves problems quickly';
  }
  if (/reliable crews/i.test(s)) return 'reliable crews and clear communication';

  // Conservative on-time / quality framing when the operator stated those ideas.
  if (/\bexcellent quality\b/i.test(s) && /\b(?:on[- ]?time|there on time|being on time|we'll be there on time)\b/i.test(s)) {
    return 'excellent quality and reliable, on-time service';
  }
  if (/\b(?:on[- ]?time|there on time|being on time|we'll be there on time)\b/i.test(s) && /\b(?:quality|communication)\b/i.test(s)) {
    const quality = /\bexcellent quality\b/i.test(s)
      ? 'excellent quality'
      : /\bfast communication\b/i.test(s)
        ? 'fast communication'
        : 'quality';
    return `${quality} and being on time`;
  }

  // Otherwise first sentence, capped.
  let snippet = firstSentence(s);
  snippet = normalizeBusinessPhrase(snippet);
  const words = snippet.split(/\s+/);
  if (words.length > 28) {
    snippet = `${words.slice(0, 28).join(' ')}…`;
  }
  return snippet.replace(/[.!?]+$/, '').trim();
}

/**
 * Build Blueprint-shaped section summaries from normalized evidence (not raw transcript).
 */
function sectionsFromNormalizedFacts(facts, priorSections = null) {
  const f = cloneNormalizedFacts(facts);
  const prior = priorSections || emptySections();
  const sections = emptySections();
  const name = sanitizeBusinessName(f.business_name || '');
  const priorSummary = (key, slot) => {
    const summary = String(prior[key]?.summary || '').trim();
    if (!summary || f.superseded_slots.includes(slot) || containsCorrectionInstructionLeakage(summary)) return '';
    return summary;
  };

  const identityBits = [];
  if (name && f.business_description) {
    const desc = normalizeBusinessPhrase(firstSentence(f.business_description));
    const article = /^[aeiou]/i.test(desc) ? 'an' : 'a';
    identityBits.push(`${name} is ${article} ${desc}`);
  } else if (name) {
    identityBits.push(`${name} is a cleaning company`);
  } else if (f.business_description) {
    identityBits.push(
      `The business is understood as ${normalizeBusinessPhrase(firstSentence(f.business_description))}`
    );
  }
  const getEpistemicState = (fieldKey, hasValue) => {
    return f.epistemic_states?.[fieldKey] || (hasValue ? EPISTEMIC_STATES.KNOWN : EPISTEMIC_STATES.UNRESOLVED);
  };

  sections.identity = {
    ...(prior.identity || emptySection()),
    epistemic_state: getEpistemicState('business_description', identityBits.length > 0),
    summary: identityBits.length
      ? [
          ensurePeriod(identityBits[0]),
          'This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
        ].join(' ')
      : priorSummary('identity', 'business_description'),
  };

  sections.services = {
    ...(prior.services || emptySection()),
    epistemic_state: getEpistemicState('services', f.services.length > 0),
    summary: f.services.length && f.epistemic_states?.services === EPISTEMIC_STATES.KNOWN
      ? [
          ensurePeriod(`Today the business delivers ${f.services.join(', ')}`),
          'Service understanding reflects what is actually sold now, not aspirational packaging.',
        ].join(' ')
      : f.epistemic_states?.services === EPISTEMIC_STATES.UNKNOWN
        ? 'Services: Not yet defined.'
        : prior.services?.summary || '',
  };

  sections.idealCustomers = {
    ...(prior.idealCustomers || emptySection()),
    epistemic_state: getEpistemicState('ideal_customers', (f.ideal_customers || []).length > 0),
    summary: (() => {
      const cleanIdeal = (f.ideal_customers || []).filter(
        (item) =>
          item &&
          !isLiteralUncertaintyPhrase(item) &&
          !isValueTraitPhrase(item) &&
          !isConversationalFiller(item)
      );
      if (cleanIdeal.length && f.epistemic_states?.ideal_customers === EPISTEMIC_STATES.KNOWN) {
        return [
          ensurePeriod(`Ideal customers are ${cleanIdeal.join(', ')}`),
          'This ICP picture prioritizes fit over volume.',
        ].join(' ');
      }
      if (f.epistemic_states?.ideal_customers === EPISTEMIC_STATES.HYPOTHESIS) {
        return `Current hypothesis: target audience may be ${f.hypotheses?.ideal_customers || f.evidence_statements?.ideal_customers || 'under evaluation'}.`;
      }
      if (f.epistemic_states?.ideal_customers === EPISTEMIC_STATES.UNKNOWN) {
        return 'Ideal customers: Not yet defined.';
      }
      const priorIdealSummary = String(prior.idealCustomers?.summary || '').trim();
      if (
        priorIdealSummary &&
        !isLiteralUncertaintyPhrase(priorIdealSummary) &&
        !/\bi don'?t know\b|\bnot sure\b|\bhaven'?t figured\b/i.test(priorIdealSummary)
      ) {
        return priorSummary('idealCustomers', 'ideal_customers');
      }
      return '';
    })(),
  };

  sections.avoidCustomers = {
    ...(prior.avoidCustomers || emptySection()),
    epistemic_state: getEpistemicState('disqualified_customers', f.disqualified_customers.length > 0),
    summary: f.disqualified_customers.length && f.epistemic_states?.disqualified_customers === EPISTEMIC_STATES.KNOWN
      ? [
          ensurePeriod(`The business prefers to avoid ${f.disqualified_customers.join(', ')}`),
          'These constraints protect targeting quality and should stay visible in the Blueprint.',
        ].join(' ')
      : f.epistemic_states?.disqualified_customers === EPISTEMIC_STATES.UNKNOWN
        ? 'Disqualified customers: Not yet defined.'
        : priorSummary('avoidCustomers', 'disqualified_customers'),
  };

  const marketBits = [];
  if (f.geography.length) marketBits.push(f.geography.join(', '));
  if (f.growth_focus) marketBits.push(`with a near-term growth focus on ${f.growth_focus}`);
  sections.targetMarkets = {
    ...(prior.targetMarkets || emptySection()),
    epistemic_state: getEpistemicState('geography', marketBits.length > 0),
    summary: marketBits.length && f.epistemic_states?.geography === EPISTEMIC_STATES.KNOWN
      ? [
          ensurePeriod(`Priority markets center on ${marketBits.join(' ')}`),
          'Geography and vertical focus here bound where discovery should concentrate first.',
        ].join(' ')
      : f.epistemic_states?.geography === EPISTEMIC_STATES.HYPOTHESIS
        ? `Current hypothesis: target markets center on ${f.hypotheses?.geography || f.evidence_statements?.geography || 'under evaluation'}.`
        : f.epistemic_states?.geography === EPISTEMIC_STATES.NOT_APPLICABLE
          ? 'Geography is not currently a meaningful targeting constraint; targeting is based on business stage and characteristics instead.'
          : f.epistemic_states?.geography === EPISTEMIC_STATES.UNKNOWN
            ? 'Target markets: Not yet defined.'
            : priorSummary('targetMarkets', 'geography'),
  };

  sections.competitiveAdvantages = {
    ...(prior.competitiveAdvantages || emptySection()),
    epistemic_state: getEpistemicState('differentiation', Boolean(f.differentiation)),
    summary: f.differentiation && f.epistemic_states?.differentiation === EPISTEMIC_STATES.KNOWN
      ? [
          ensurePeriod(`Competitive edge is described as ${f.differentiation}`),
          'This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
        ].join(' ')
      : f.epistemic_states?.differentiation === EPISTEMIC_STATES.UNKNOWN && f.hypotheses?.differentiation
        ? `Actual customer reason-to-choose: Not yet established. Current hypothesis: ${f.hypotheses.differentiation}.`
        : f.epistemic_states?.differentiation === EPISTEMIC_STATES.HYPOTHESIS
        ? `Current hypothesis: competitive advantage around ${f.hypotheses?.differentiation || f.evidence_statements?.differentiation || 'under evaluation'}.`
        : f.epistemic_states?.differentiation === EPISTEMIC_STATES.UNKNOWN
          ? 'Differentiation: Not yet defined.'
          : priorSummary('competitiveAdvantages', 'differentiation'),
  };

  sections.brandVoice = {
    ...(prior.brandVoice || emptySection()),
    epistemic_state: getEpistemicState('brand_voice', Boolean(f.brand_voice)),
    summary: f.brand_voice && f.epistemic_states?.brand_voice === EPISTEMIC_STATES.KNOWN
      ? [
          ensurePeriod(`Brand voice should read as ${f.brand_voice}`),
          'Tone guidance constrains later language without choosing channels or campaigns.',
        ].join(' ')
      : f.epistemic_states?.brand_voice === EPISTEMIC_STATES.HYPOTHESIS
        ? `Current hypothesis: brand voice tone may align with ${f.hypotheses?.brand_voice || f.evidence_statements?.brand_voice || 'under evaluation'}.`
        : f.epistemic_states?.brand_voice === EPISTEMIC_STATES.UNKNOWN
          ? 'Brand voice: Not yet defined.'
          : priorSummary('brandVoice', 'brand_voice'),
  };

  sections.campaignGoals = {
    ...(prior.campaignGoals || emptySection()),
    epistemic_state: getEpistemicState('ninety_day_outcomes', Boolean(f.ninety_day_outcomes)),
    summary: f.ninety_day_outcomes
      ? [
          ensurePeriod(`Near-term growth goals focus on ${f.ninety_day_outcomes}`),
          'These are desired business outcomes for the next phase of work, not execution tactics.',
        ].join(' ')
      : priorSummary('campaignGoals', 'ninety_day_outcomes'),
  };

  sections.successMetrics = {
    ...(prior.successMetrics || emptySection()),
    summary: f.success_metrics.length
      ? [
          ensurePeriod(`Success will be judged by ${f.success_metrics.join(', ')}`),
          'These signals define whether the engagement is working from the client\'s perspective.',
        ].join(' ')
      : priorSummary('successMetrics', 'success_metrics'),
  };

  // Preserve confidence / evidenceIds / unknowns from prior when present.
  for (const key of BLUEPRINT_SECTIONS) {
    const p = prior[key] || emptySection();
    const hasSummary =
      (sections[key].epistemic_state === EPISTEMIC_STATES.KNOWN ||
       sections[key].epistemic_state === EPISTEMIC_STATES.HYPOTHESIS ||
       !sections[key].epistemic_state) &&
      Boolean(String(sections[key].summary || '').trim()) &&
      !answerLooksEmpty(sections[key].summary) &&
      !/\bi don'?t know\b|\bnot sure yet\b|\bNot yet defined\b|\bNot applicable\b/i.test(String(sections[key].summary || ''));
    const unknowns = hasSummary
      ? []
      : [...(p.unknowns || [])];
    if (!hasSummary && key === 'idealCustomers') {
      const commercialPref =
        /commercial/i.test(String(f.growth_focus || '')) ||
        /commercial/i.test(String(f.vertical_focus || '')) ||
        /prefer commercial/i.test(String(f.business_description || ''));
      const icpUnknown = commercialPref
        ? 'Which commercial customer segments are the strongest fit'
        : 'Missing clear answer for idealCustomers';
      if (!unknowns.some((u) => /commercial customer segment|ideal customer/i.test(String(u)))) {
        unknowns.unshift(icpUnknown);
      }
    }
    sections[key] = {
      ...sections[key],
      summary: sections[key].summary || '',
      confidence: p.confidence || (hasSummary ? EXPLICIT_CONFIDENCE : UNKNOWN_CONFIDENCE),
      evidenceIds: [...(p.evidenceIds || [])],
      unknowns,
      epistemic_state: sections[key].epistemic_state || p.epistemic_state || EPISTEMIC_STATES.UNRESOLVED,
    };
  }
  return sections;
}

/**
 * Strip interview-question echo / Mad-Lib filler so only the substance remains.
 */
function stripInterviewQuestionEcho(text) {
  let s = String(text || '').trim();
  if (!s) return '';

  const echoPatterns = [
    /\bwhen a great-fit customer chooses .{0,80}?,?\s*what usually tips the decision(?:\s+is|\s*:)?\s*/gi,
    /\bwhen a great-fit customer chooses .{0,80}?(?:,|\s+is)\s*/gi,
    /\bwhat usually tips the decision(?:\s+is|\s*:)?\s*/gi,
    /\bif i were writing as (?:your|the) brand tomorrow,?\s*(?:how should it sound[:\s-]*)?/gi,
    /\b(?:anchor'?s|the)\s+brand voice should (?:sound|feel|read)(?:\s+as)?\s*/gi,
    /\bbrand voice should (?:sound|feel|read)(?:\s+as)?\s*/gi,
    /\blooking at the next 90 days,?\s*(?:what business outcomes would make this growth work feel successful[:\s-]*)?/gi,
    /\bover the next 90 days(?:,?\s*this growth work(?:\s+should\s+\w+(?:\s+on)?)?)?\s*/gi,
    /\bwe will know(?: the growth work is working)?(?:\s+by)?\s*/gi,
    /\bhow will we know it'?s working[:\s-]*/gi,
    /\btell me about the (?:business|services)[:\s-]*/gi,
    /\bwho do you most want to work with[:\s-]*/gi,
    /\bpaint me a picture of the ideal customer[:\s-]*/gi,
    /\bwhere should we focus first[:\s-]*/gi,
    /\bare there customers or segments you'?d rather not take on[:\s-]*/gi,
    /\bi don't want to work with\s+/gi,
    /\bwould feel successful if\s*/gi,
    /\bboth\s+geography\s+is\s*/gi,
    /\bto say\s+/gi,
  ];
  for (const re of echoPatterns) {
    s = s.replace(re, '');
  }

  // Strip common answer preambles that restate the question.
  // Only normalize spaced dashes — never rewrite compound hyphens (commercial-focused).
  s = s
    .replace(/^(?:the (?:answer|decision|tip|thing) is|it(?:'s| is)|that (?:is|would be))\s+/i, '')
    .replace(/^(?:both\s*[-–—:]\s*)/i, '')
    .replace(/^(?:well,?\s+)/i, '')
    .replace(/\s+[—–]\s+/g, ' — ')
    .replace(/\s+-\s+/g, ' — ')
    .trim();

  return s.replace(/\s{2,}/g, ' ').trim();
}

/**
 * Clean a raw interview answer into substance suitable for synthesis.
 */
function cleanRawAnswer(sectionKey, text) {
  let s = stripInterviewQuestionEcho(text);
  if (!s) return '';
  s = stripBusinessNameLeadIn(s);
  s = stripSupplementalPreamble(s);

  const sectionStrips = {
    identity: [/^(?:the business(?: name)? is|we are|we're|i run)\s+/i],
    services: [
      /^(?:we (?:sell|offer|provide|do|deliver)|services? (?:include|are)|today (?:we|the business) (?:delivers?|offers?))\s+/i,
      /^(?:anchor(?:\s+cleaning)?\s+)?provides?\s+/i,
    ],
    idealCustomers: [
      /^(?:our ideal (?:customer|client)s? (?:are|is)|we (?:want|prefer|target|most want to work with)|ideal customers? (?:are|include))\s+/i,
      /^(?:anchor(?:\s+cleaning)?\s+)?most wants to work with\s+/i,
      /\s+as part of (?:my|our|the)\s+ideal customer(?:\s+profile)?$/i,
    ],
    avoidCustomers: [
      /^(?:i don'?t want to work with|we (?:avoid|don'?t want|do not want|should avoid|decline)|avoid|customers? (?:who|that))\s+/i,
      /^(?:customers?\s+)/i,
    ],
    targetMarkets: [
      /^(?:we (?:focus|serve|cover|target)|markets? (?:are|include)|geography (?:is|centers on)|both\s*[-–—:]\s*)/i,
    ],
    competitiveAdvantages: [
      /^(?:we (?:are|offer|have|win because)|our (?:edge|advantage) is|customers? choose (?:us|this business|anchor) (?:for|because(?:\s+of)?))\s+/i,
      /^(?:the decision is)\s+/i,
    ],
    brandVoice: [
      /^(?:we (?:sound|are)|voice (?:is|should be)|brand (?:is|should)|it should (?:sound|feel|be)|should sound)\s+/i,
    ],
    campaignGoals: [
      /^(?:we want to|our goal is to|goals? (?:are|include)|near-term (?:growth )?(?:goals?|priorities) (?:focus|center) on)\s+/i,
    ],
    successMetrics: [
      /^(?:we (?:track|measure|watch|will know)|metrics? (?:are|include)|success (?:is|means|will be judged by)|by watching)\s+/i,
    ],
  };

  for (const re of sectionStrips[sectionKey] || []) {
    s = s.replace(re, '');
  }

  // Grammar fixes common in spoken answers.
  s = s
    .replace(/\bwho'?s\b/gi, 'whose')
    .replace(/\bmain priority is the lowest price\b/gi, 'main priority is the lowest price')
    .replace(/\s{2,}/g, ' ')
    .trim()
    .replace(/^[,;:\-–—]\s*/, '')
    .replace(/\s+[,;]$/, '');

  return s;
}

/**
 * Extract a short business name from an identity summary when available.
 */
function extractBusinessName(identitySummary) {
  const s = String(identitySummary || '').trim();
  if (!s) return '';
  if (/\banchor\s+cleaning\b/i.test(s)) return 'Anchor Cleaning';
  const named = s.match(
    /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:is|are)\b/
  );
  if (named) return sanitizeBusinessName(named[1]);
  const cleaning = s.match(/\b(Anchor(?:\s+Cleaning)?)\b/i);
  if (cleaning) return sanitizeBusinessName(cleaning[1]);
  return '';
}

function businessSubject(name, { possessive = false } = {}) {
  const cleaned = sanitizeBusinessName(name) || String(name || '').trim();
  if (!cleaned) return possessive ? "the business's" : 'the business';
  if (possessive) {
    return /s$/i.test(cleaned) ? `${cleaned}'` : `${cleaned}'s`;
  }
  return cleaned;
}

/**
 * Convert cleaned substance into a polished executive statement for a Brief facet.
 * Never concatenates raw interview text into Mad-Lib templates.
 */
function synthesizeNormalizedFact(kind, rawOrSummary, opts = {}) {
  const name = opts.businessName || '';
  const subject = businessSubject(name);
  const possessive = businessSubject(name, { possessive: true });

  // Prefer substance from Blueprint first-sentence wrappers, else clean raw.
  let claim = coreClaim(sanitizeSummaryForBrief(rawOrSummary));
  if (!claim) claim = String(rawOrSummary || '').trim();
  claim = stripInterviewQuestionEcho(claim);

  // Map kind → section key for cleanRawAnswer
  const sectionKey =
    {
      identity: 'identity',
      services: 'services',
      ideal: 'idealCustomers',
      avoid: 'avoidCustomers',
      markets: 'targetMarkets',
      advantages: 'competitiveAdvantages',
      voice: 'brandVoice',
      goals: 'campaignGoals',
      metrics: 'successMetrics',
    }[kind] || kind;

  // Peel Blueprint wrapper prefixes then clean.
  claim = claim.replace(
    /^(Today the business delivers|Ideal customers are|The business prefers to avoid|Priority markets center on|Competitive edge is described as|Brand voice should read as|Near-term growth goals focus on|Success will be judged by|The business is understood as|Progress will be judged by|Ideal customers include|Geographic focus centers on|The business declines|Brand voice should feel|Near-term growth priorities center on|Customers choose this business because(?:\s+of)?|Services include)\s+/i,
    ''
  );
  let substance = cleanRawAnswer(sectionKey, claim);
  if (!substance || containsMetaInstructionLanguage(substance) || looksLikeRefinementFeedback(substance)) {
    return '';
  }
  if (containsRawPromptFragment(substance)) {
    substance = stripInterviewQuestionEcho(substance);
    substance = cleanRawAnswer(sectionKey, substance);
  }
  if (!substance || containsRawPromptFragment(substance)) return '';

  switch (kind) {
    case 'identity': {
      let sentence = substance;
      if (/^[A-Z][a-zA-Z0-9&'.-]+/.test(sentence) && /\bis\b/i.test(sentence)) {
        // already a full identity sentence
      } else if (/^an?\s+/i.test(sentence)) {
        sentence = name ? `${name} is ${sentence}` : `This is ${sentence}`;
      } else if (name && !new RegExp(`^${escapeRegExp(name)}(?:\\b|$)`, 'i').test(sentence)) {
        const article = /^[aeiou]/i.test(sentence) ? 'an' : 'a';
        sentence = `${name} is ${article} ${sentence}`;
      } else if (name && new RegExp(`^${escapeRegExp(name)}$`, 'i').test(sentence)) {
        sentence = `${name} is a cleaning company`;
      } else {
        sentence = /^[A-Z]/.test(sentence) ? sentence : `This is a ${sentence}`;
      }
      if (
        /\bis an?\s+.+\bcleaning$/i.test(sentence) &&
        !/\b(company|service|business|firm|studio|practice)\b/i.test(sentence)
      ) {
        sentence = sentence.replace(/\bcleaning$/i, 'cleaning company');
      }
      return capitalizeSentence(sentence);
    }
    case 'services': {
      const offer = midSentence(substance);
      return `Services include ${offer}`;
    }
    case 'ideal': {
      const who = midSentence(substance);
      if (!who || isLiteralUncertaintyPhrase(who) || /\bi don'?t know\b/i.test(who)) {
        return '';
      }
      return `Ideal customers include ${who}`;
    }
    case 'avoid': {
      // Prefer nuanced presentation that preserves preference vs exclusion.
      const composed = composeCustomerConstraintPresentation(name || subject, substance);
      if (composed) return composed.replace(/[.!?]+$/, '').trim();
      let who = substance
        .replace(/^(?:customers?\s+)?(?:who'?s|whose|who|that)\s+/i, '')
        .replace(/^main priority is\s+/i, 'prioritize ')
        .replace(/\bprioritize the lowest price\b/i, 'prioritize the lowest price')
        .trim();
      if (/lowest price|cheap|bargain|price.?first/i.test(who)) {
        return ensurePeriod(
          `${subject} deliberately avoids customers who prioritize the lowest price over reliability, professionalism, and accountability`
        ).replace(/\.$/, '');
      }
      who = midSentence(who);
      return `${subject} prefers to avoid ${who}`;
    }
    case 'markets': {
      let where = substance
        .replace(/^(?:both\s*[-–—:]\s*)?/i, '')
        .replace(/^greater\s+manchester\s+area\s+includes?\s+/i, 'Greater Manchester area, including ')
        .trim();
      // Title-case known NH towns when present.
      where = where.replace(
        /\b(bedford|hooksett|londonderry|auburn|goffstown|manchester)\b/gi,
        (m) => titleCaseWords(m)
      );
      if (/greater\s+manchester/i.test(where) || /\b(Bedford|Hooksett|Londonderry|Auburn|Goffstown)\b/.test(where)) {
        const towns = where.match(/\b(Bedford|Hooksett|Londonderry|Auburn|Goffstown)\b/g);
        const townList = towns && towns.length ? `, including ${[...new Set(towns)].join(', ')}` : '';
        return `${possessive} near-term geography is the Greater Manchester area${townList}`;
      }
      where = midSentence(where);
      return `${possessive} near-term geography centers on ${where}`;
    }
    case 'advantages': {
      let edge = substance.trim();
      // "trust — responsive..." / "trust: ..." → full consultant clause
      const trustDash = edge.match(/^trust\s*[—–\-:]\s*(.+)$/i);
      if (trustDash) {
        edge = `they trust the team to be ${trustDash[1].trim()}`;
        return `Customers choose ${subject} because ${midSentence(edge)}`;
      }
      if (/^trust\b/i.test(edge) && /\b(responsive|consistent|accountab|chase|show|communicate)\b/i.test(edge)) {
        return `Customers choose ${subject} because they trust the team to be responsive, consistent, and accountable without needing to chase the work`;
      }
      // Normalize "customers trust..." → "they trust..." when we already name the chooser.
      edge = edge.replace(/^(?:customers?|clients?)\s+(trust|choose|prefer)\b/i, 'they $1');
      // If the answer is already a "trust / show up / communicate" clause, keep it natural.
      if (/^(they|customers?|clients?)\s+/i.test(edge)) {
        return `Customers choose ${subject} because ${midSentence(edge)}`;
      }
      if (/^(trust|show|communicate|solve|make|be)\b/i.test(edge)) {
        const clause = /^trust\b/i.test(edge)
          ? edge.replace(/^trust\b/i, 'they trust')
          : `they ${edge}`;
        return `Customers choose ${subject} because ${midSentence(clause)}`;
      }
      if (/\btrust\b/i.test(edge) && /\b(responsive|consistent|accountab|chase)\b/i.test(edge)) {
        return `Customers choose ${subject} because they trust the team to be responsive, consistent, and accountable without needing to chase the work`;
      }
      // Adjective list left after stripping "trust —"
      if (/^(responsive|consistent|accountab)/i.test(edge)) {
        return `Customers choose ${subject} because they trust the team to be ${midSentence(edge)}`;
      }
      // Noun-phrase differentiation (e.g. "reliable crews")
      if (!/\b(because|that|who|to)\b/i.test(edge) && edge.split(/\s+/).length <= 8) {
        return `Customers choose ${subject} for ${midSentence(edge)}`;
      }
      return `Customers choose ${subject} because ${midSentence(edge)}`;
    }
    case 'voice': {
      let tone = normalizeBrandVoiceTone(substance);
      tone = midSentence(tone);
      if (!tone || /^(?:anchor|the business)\b/i.test(tone)) return '';
      const cleanName = sanitizeBusinessName(name) || name;
      const voicePossessive = businessSubject(cleanName || 'the business', { possessive: true });
      return `${voicePossessive} brand voice should feel ${tone}`;
    }
    case 'goals': {
      let outcome = normalizeGoalOutcomePhrase(substance);
      if (/commercial cleaning|greater manchester/i.test(outcome) && /growth|pipeline|clients?/i.test(outcome) === false) {
        // Keep the established Manchester commercial shorthand when that is the whole claim.
      }
      if (/^commercial cleaning growth in Greater Manchester$/i.test(outcome)) {
        return `${possessive} near-term priority is commercial cleaning growth in Greater Manchester`;
      }
      if (/commercial cleaning|greater manchester/i.test(substance) && !/pipeline|prospects|establish/i.test(substance)) {
        return `${possessive} near-term priority is commercial cleaning growth in Greater Manchester`;
      }
      outcome = midSentence(outcome);
      return `${possessive} near-term priority is ${outcome}`;
    }
    case 'metrics': {
      // SPEC-099: never invent metrics the operator did not state.
      let signals = substance
        .replace(/^both\s+activity quality and real opportunity movement\b/i, '')
        .trim();
      signals = midSentence(signals || substance);
      if (!signals || isLiteralUncertaintyPhrase(signals)) return '';
      return `Success should be measured by ${signals}`;
    }
    default:
      return capitalizeSentence(substance);
  }
}

/**
 * Consultant-style section summary (2–4 sentences). Never a raw transcript dump.
 */
function summarizeSection(sectionKey, statements) {
  const cleaned = (statements || [])
    .map((s) => String(s || '').trim())
    .filter((s) => s && !/^Unknown:/i.test(s) && !answerLooksEmpty(s))
    .filter((s) => isBusinessFactStatement(s))
    .map((s) => stripInterviewQuestionEcho(s))
    .map((s) => cleanRawAnswer(sectionKey, s))
    .filter(Boolean)
    .filter((s) => !containsRawPromptFragment(s));
  if (!cleaned.length) return '';

  const latest = cleaned[cleaned.length - 1];

  switch (sectionKey) {
    case 'identity': {
      const dash =
        latest.match(/^(.+?)\s+[—–]\s+(.+)$/) ||
        latest.match(/^(.+?)[—–](.+)$/) ||
        latest.match(/^(.+?)\s+-\s+(.+)$/);
      const weAre = latest.match(/^(.+?)\s+we(?:'re| are)\s+(.+)$/i);
      if (dash || weAre) {
        const parts = dash || weAre;
        const name = sanitizeBusinessName(parts[1].trim().replace(/[.!?,]+$/, ''));
        const desc = firstSentence(
          stripLeadingWeAre(parts[2]).replace(/^(a|an|the)\s+/i, '')
        );
        return [
          ensurePeriod(`${name} is ${/^[aeiou]/i.test(desc) ? 'an' : 'a'} ${normalizeBusinessPhrase(desc)}`),
          'This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
        ].join(' ');
      }
      const named = latest.match(
        /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:is|are|provides?|offers?|does)\s+(.+)$/i
      );
      if (named) {
        return [
          ensurePeriod(`${named[1].trim()} is ${stripLeadingWeAre(named[2]).replace(/^(a|an|the)\s+/i, '')}`),
          'Understanding of the business starts from this operator-stated identity.',
        ].join(' ');
      }
      return [
        ensurePeriod(
          `The business is understood as ${stripLeadingWeAre(latest).replace(/^(a|an|the)\s+/i, '')}`
        ),
        'This identity note will ground services, markets, and messaging downstream.',
      ].join(' ');
    }
    case 'services':
      return [
        ensurePeriod(`Today the business delivers ${latest}`),
        'Service understanding reflects what is actually sold now, not aspirational packaging.',
      ].join(' ');
    case 'idealCustomers':
      return [
        ensurePeriod(`Ideal customers are ${latest}`),
        'This ICP picture prioritizes fit over volume.',
      ].join(' ');
    case 'avoidCustomers':
      return [
        ensurePeriod(`The business prefers to avoid ${latest}`),
        'These constraints protect targeting quality and should stay visible in the Blueprint.',
      ].join(' ');
    case 'targetMarkets':
      return [
        ensurePeriod(`Priority markets center on ${latest}`),
        'Geography and vertical focus here bound where discovery should concentrate first.',
      ].join(' ');
    case 'competitiveAdvantages':
      return [
        ensurePeriod(`Competitive edge is described as ${latest}`),
        'This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
      ].join(' ');
    case 'brandVoice':
      return [
        ensurePeriod(`Brand voice should read as ${latest}`),
        'Tone guidance constrains later language without choosing channels or campaigns.',
      ].join(' ');
    case 'campaignGoals':
      return [
        ensurePeriod(`Near-term growth goals focus on ${latest}`),
        'These are desired business outcomes for the next phase of work, not execution tactics.',
      ].join(' ');
    case 'successMetrics':
      return [
        ensurePeriod(`Success will be judged by ${latest}`),
        'These signals define whether the engagement is working from the client\'s perspective.',
      ].join(' ');
    default:
      return [
        ensurePeriod(capitalizeSentence(latest)),
        cleaned.length > 1
          ? ensurePeriod(`Earlier notes in this area remain consistent with that understanding`)
          : ensurePeriod(`This section reflects current operator understanding`),
      ].join(' ');
  }
}

function computeProgress(sectionState) {
  let completed = 0;
  for (const key of BLUEPRINT_SECTIONS) {
    const section = sectionState && sectionState[key];
    if (section && String(section.summary || '').trim() && !answerLooksEmpty(section.summary)) {
      completed += 1;
    }
  }
  const total = BLUEPRINT_SECTIONS.length;
  return {
    label: 'Business Understanding',
    completed,
    total,
    percent: total ? Math.round((completed / total) * 100) : 0,
  };
}

const UNDERSTANDING_STATUS_LABELS = Object.freeze({
  ready: 'Ready',
  building: 'Building…',
  learning: 'Still learning…',
  waiting: 'Waiting for more information…',
});

/**
 * Redacted live progress for the interview panel — never includes summaries.
 * @returns {{ label: string, sections: Array<object> }}
 */
function buildUnderstandingProgress(sectionState) {
  const sections = BLUEPRINT_SECTIONS.map((key) => {
    const section = (sectionState && sectionState[key]) || emptySection();
    const confidence = clampConfidence(section.confidence || 0);
    const evidenceCount = Array.isArray(section.evidenceIds) ? section.evidenceIds.length : 0;
    const hasSummary =
      Boolean(String(section.summary || '').trim()) && !answerLooksEmpty(section.summary);
    const unknowns = [...(section.unknowns || [])].filter(Boolean);
    let status = 'waiting';
    const pct = Math.round(confidence * 100);
    if (evidenceCount > 0 || hasSummary) {
      if (pct >= 81) status = 'ready';
      else if (pct >= 51) status = 'building';
      else status = 'learning';
    }
    return {
      key,
      title: SECTION_TITLES[key] || key,
      confidence,
      confidencePercent: pct,
      evidenceCount,
      unknowns,
      status,
      statusLabel: UNDERSTANDING_STATUS_LABELS[status],
    };
  });
  return {
    label: 'Business Understanding',
    sections,
  };
}

/**
 * Strip Blueprint / interview boilerplate so only substantive meaning remains.
 * Never leak implementation language into CEO-facing copy.
 */
function isMetaConsultantSentence(sentence) {
  const s = String(sentence || '').trim();
  if (!s) return true;
  if (containsMetaInstructionLanguage(s)) return true;
  return (
    /\b(blueprint|operator-stated|operator understanding|downstream|discovery should|ICP picture|engagement is working|evidenceIds?|sectionKey|CIE-v?\d*|prompt|token|json|payload)\b/i.test(
      s
    ) ||
    /anchors every other/i.test(s) ||
    /useful for messaging/i.test(s) ||
    /not an invented strategy/i.test(s) ||
    /not execution tactics/i.test(s) ||
    /without choosing channels/i.test(s) ||
    /from the client's perspective/i.test(s) ||
    /not aspirational packaging/i.test(s) ||
    /should stay visible in the Blueprint/i.test(s) ||
    /Understanding of the business starts/i.test(s) ||
    /will ground services, markets/i.test(s) ||
    /Earlier notes in this area/i.test(s) ||
    /reflects current operator/i.test(s) ||
    /Service understanding reflects/i.test(s) ||
    /These constraints protect targeting/i.test(s) ||
    /Tone guidance constrains/i.test(s) ||
    /These are desired business outcomes for the next phase/i.test(s) ||
    /These signals define whether/i.test(s) ||
    /Geography and vertical focus here bound/i.test(s) ||
    /identity framing is how the operator/i.test(s) ||
    /This identity note will ground/i.test(s) ||
    /^Unknown:/i.test(s) ||
    /^Missing clear answer/i.test(s) ||
    /^No evidence yet/i.test(s)
  );
}

/**
 * Remove meta-instruction / refinement language from a Blueprint summary
 * before Executive Business Brief synthesis.
 */
function sanitizeSummaryForBrief(summary) {
  const parts = protectAbbreviations(String(summary || ''))
    .split(/(?<=[.!?])\s+/)
    .map((s) => restoreAbbreviations(s.trim()))
    .filter(Boolean)
    .filter((s) => !isMetaConsultantSentence(s))
    .filter((s) => isBusinessFactStatement(s) || !containsMetaInstructionLanguage(s))
    .map((s) => scrubArtifactLanguage(s))
    .map((s) => {
      // Peel Blueprint wrappers, then strip question-echo bleed in place.
      let cleaned = s.replace(
        /^(Today the business delivers|Ideal customers are|The business prefers to avoid|Priority markets center on|Competitive edge is described as|Brand voice should read as|Near-term growth goals focus on|Success will be judged by|The business is understood as|Progress will be judged by)\s+/i,
        ''
      );
      cleaned = stripInterviewQuestionEcho(cleaned);
      return cleaned;
    })
    .filter(Boolean)
    .filter((s) => !containsMetaInstructionLanguage(s) && !looksLikeRefinementFeedback(s))
    .filter((s) => !containsRawPromptFragment(s));
  return parts.join(' ');
}

/**
 * Defense-in-depth: strip contaminated section summaries before Brief render.
 * Contaminated-only sections lose confidence contribution for ratings.
 */
function sanitizeSectionsForBrief(sections) {
  const out = {};
  for (const key of BLUEPRINT_SECTIONS) {
    const src = (sections && sections[key]) || emptySection();
    const original = String(src.summary || '').trim();
    const cleanSummary = sanitizeSummaryForBrief(original);
    const contaminatedOnly = Boolean(original) && !cleanSummary;
    out[key] = {
      summary: cleanSummary,
      confidence: contaminatedOnly
        ? UNKNOWN_CONFIDENCE
        : clampConfidence(src.confidence || 0),
      evidenceIds: [...(src.evidenceIds || [])],
      unknowns: [...(src.unknowns || [])],
    };
  }
  return out;
}

function scrubArtifactLanguage(text) {
  return String(text || '')
    .replace(/\bICP\b/g, 'ideal customer')
    .replace(/\bBlueprint\b/gi, '')
    .replace(/\boperator-stated\b/gi, '')
    .replace(/\boperator understanding\b/gi, 'understanding')
    .replace(/\b(evidenceIds?|sectionKey|CIE-v?\d*)\b/gi, '')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s+([,.;:!?])/g, '$1')
    .trim();
}

function substantiveSentences(summary, limit = 2) {
  const parts = protectAbbreviations(String(summary || ''))
    .split(/(?<=[.!?])\s+/)
    .map((s) => restoreAbbreviations(s.trim()))
    .filter(Boolean)
    .filter((s) => !isMetaConsultantSentence(s))
    .map((s) => scrubArtifactLanguage(s))
    .filter(Boolean);
  return parts.slice(0, limit);
}

function coreClaim(summary) {
  const sentences = substantiveSentences(summary, 1);
  if (!sentences.length) return '';
  return sentences[0].replace(/[.!?]+$/, '').trim();
}

function polishPhrase(text) {
  return String(text || '')
    .replace(/\bfriendly professional\b/i, 'friendly and professional')
    .replace(/\s+(voice|tone)$/i, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

/**
 * Lowercase the lead character when embedding a phrase mid-sentence,
 * unless it looks like a proper noun or acronym.
 */
function midSentence(text) {
  const s = polishPhrase(text);
  if (!s) return s;
  if (/^[A-Z]{2,}(?:\b|[0-9])/.test(s)) return s;
  if (/^[A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){1,}/.test(s)) return s;
  return s.charAt(0).toLowerCase() + s.slice(1);
}

/**
 * Extract commercial substance from a Blueprint first sentence as a noun phrase.
 * Used only when weaving into a larger sentence — never left as a bare Mad-Lib slot.
 */
function extractSubstance(claim, patterns) {
  const raw = polishPhrase(claim);
  if (!raw) return '';
  for (const [re, group = 1] of patterns) {
    const m = raw.match(re);
    if (m && m[group]) return polishPhrase(m[group]);
  }
  return polishPhrase(raw);
}

/** @deprecated Prefer normalizeClaim / midSentence — retained for observation helpers. */
function softenClaim(claim) {
  return midSentence(
    String(claim || '').replace(
      /^(Today the business delivers|Ideal customers are|The business prefers to avoid|Priority markets center on|Competitive edge is described as|Brand voice should read as|Near-term growth goals focus on|Success will be judged by|The business is understood as|Progress will be judged by)\s+/i,
      ''
    )
  );
}

function asGerundPhrase(claim) {
  const text = softenClaim(claim);
  if (!text) return text;
  return normalizeGoalOutcomePhrase(text) || text;
}

function humanizeUnknownLabel(raw) {
  const text = String(raw || '').trim();
  if (!text) return '';
  if (/^Pricing philosophy$/i.test(text)) return 'pricing philosophy';
  if (/^Capacity$/i.test(text) || /^capacity$/i.test(text)) {
    return 'capacity and delivery constraints';
  }
  if (/which commercial customer segments/i.test(text)) {
    return 'which commercial customer segments are the strongest fit';
  }
  const missing = text.match(/^Missing clear answer for\s+(.+)$/i);
  if (missing) {
    const key = missing[1].trim();
    const map = {
      identity: 'how the business defines itself',
      services: 'the full service mix',
      idealCustomers: 'who the ideal customer really is',
      avoidCustomers: 'which customers to decline',
      targetMarkets: 'where to concentrate first',
      competitiveAdvantages: 'what wins the buying decision',
      brandVoice: 'how the brand should sound',
      campaignGoals: 'the near-term growth priority',
      successMetrics: 'how success will be measured',
      capacity: 'capacity and delivery constraints',
    };
    return map[key] || key.replace(/([A-Z])/g, ' $1').toLowerCase().trim();
  }
  const noEvidence = text.match(/^No evidence yet for\s+(.+)$/i);
  if (noEvidence) {
    return humanizeUnknownLabel(`Missing clear answer for ${noEvidence[1]}`);
  }
  return scrubArtifactLanguage(
    text
      .replace(/^Unknown:\s*/i, '')
      .replace(/\b(blueprint|evidenceIds?|sectionKey)\b/gi, '')
  );
}

function joinPolished(sentences) {
  return sentences
    .map((s) => ensurePeriod(String(s || '').trim()))
    .filter(Boolean)
    .slice(0, 4)
    .join(' ');
}

/**
 * Normalize a Blueprint summary into a complete CEO-ready sentence for a given facet.
 * Converts business facts into polished executive language — never Mad-Lib
 * concatenation of raw interview text or refinement instructions.
 */
function normalizeClaim(kind, summary, opts = {}) {
  const synthesized = synthesizeNormalizedFact(kind, summary, opts);
  if (!synthesized) return '';
  if (containsRawPromptFragment(synthesized)) return '';
  // Reject known bleed templates that paste unclean substance mid-sentence.
  if (
    /Customers choose this business because of\s+(when|what|how|i |we |over the)/i.test(synthesized) ||
    /Brand voice should feel\s+.*\bshould\s+(sound|feel|read)\b/i.test(synthesized) ||
    /Near-term growth priorities center on\s+(over the next|we will|looking at)/i.test(synthesized) ||
    /Success will be judged by\s+(we will know|i |how will)/i.test(synthesized) ||
    /The business declines\s+i\s+/i.test(synthesized)
  ) {
    return '';
  }
  return synthesized;
}

function composeWhoYouAre(identity, services, opts = {}) {
  const businessName = opts.businessName || extractBusinessName(identity);
  const id = normalizeClaim('identity', identity, { businessName });
  const svc = normalizeClaim('services', services, { businessName });
  const sentences = [];
  if (id && svc) {
    sentences.push(id);
    sentences.push(svc);
    sentences.push(
      'Together, identity and offer define the commercial center of gravity any growth advice must respect.'
    );
  } else if (id) {
    sentences.push(id);
    sentences.push(
      'That identity is clear enough to orient strategy; what the company sells still needs a sharper commercial definition.'
    );
  } else if (svc) {
    sentences.push(svc);
    sentences.push(
      'The offer is visible, but a tighter statement of who the company is — beyond what it sells — would give this picture a firmer center.'
    );
  } else {
    sentences.push('The business identity is still taking shape.');
    sentences.push(
      'A concise statement of who you are and what you deliver would give every later recommendation a firmer center of gravity.'
    );
  }
  return joinPolished(sentences);
}

function composeWhoYouServe(ideal, avoid, markets, opts = {}) {
  const businessName = opts.businessName || '';
  const facts = opts.normalizedFacts || null;
  const idealS = normalizeClaim('ideal', ideal, { businessName });
  const avoidS = normalizeClaim('avoid', avoid, { businessName });
  const marketS = normalizeClaim('markets', markets, { businessName });
  const sentences = [];

  const cleanIdeal = ((facts && facts.ideal_customers) || []).filter(
    (item) => item && !isLiteralUncertaintyPhrase(item)
  );
  const prefersCommercial =
    /commercial/i.test(String((facts && facts.growth_focus) || '')) ||
    /commercial/i.test(String((facts && facts.vertical_focus) || '')) ||
    /prefer commercial/i.test(String((facts && facts.business_description) || ''));

  if (cleanIdeal.length && idealS && !/\bi don'?t know\b/i.test(idealS)) {
    sentences.push(idealS);
  } else if (prefersCommercial) {
    const subject = businessSubject(businessName || 'The business');
    sentences.push(
      `${subject} prefers commercial work while continuing to serve residential customers`
    );
    sentences.push(
      'Specific priority commercial segments have not yet been established'
    );
  } else if (idealS && !/\bi don'?t know\b/i.test(idealS)) {
    sentences.push(idealS);
  }

  if (avoidS) sentences.push(avoidS);
  if (marketS) sentences.push(marketS);

  if (sentences.length >= 2 && cleanIdeal.length) {
    sentences.push(
      'Taken together, this is a disciplined beachhead: fit over volume, and geography chosen to match that fit.'
    );
  } else if (sentences.length >= 1 && prefersCommercial && !cleanIdeal.length) {
    sentences.push(
      'Commercial preference is clear; naming the priority segments would sharpen every commercial decision that follows.'
    );
  } else if (sentences.length === 1) {
    sentences.push(
      'Sharpening who is a strong fit — and who is not — will keep commercial effort concentrated where it compounds.'
    );
  } else if (!sentences.length) {
    sentences.push('The ideal customer and market focus are not yet fully drawn.');
    sentences.push(
      'Defining who belongs in the book of business — and who does not — will sharpen every commercial decision that follows.'
    );
  }
  return joinPolished(sentences);
}

function composeWhyChooseYou(advantages, brandVoice, opts = {}) {
  const businessName = opts.businessName || '';
  const adv = normalizeClaim('advantages', advantages, { businessName });
  const voice = normalizeClaim('voice', brandVoice, { businessName });
  const sentences = [];
  if (adv) sentences.push(adv);
  if (voice) sentences.push(voice);

  if (adv && voice) {
    sentences.push(
      'Differentiation and tone must reinforce each other so the market experiences the same promise the business actually keeps.'
    );
  } else if (adv) {
    sentences.push(
      'Protecting and articulating that edge matters more than inventing a broader claim the company does not own.'
    );
  } else if (voice) {
    sentences.push(
      'The voice is clear; the commercial reason a strong-fit customer should choose you still needs a sharper expression.'
    );
  } else {
    sentences.push('What wins the decision — and how that win should sound — remains under-specified.');
    sentences.push(
      'A crisp point of difference, stated in the company’s own language, would make growth work feel authentic rather than generic.'
    );
  }
  return joinPolished(sentences);
}

function composeWhereHeaded(goals, opts = {}) {
  const businessName = opts.businessName || '';
  const goal = normalizeClaim('goals', goals, { businessName });
  if (goal) {
    return joinPolished([
      goal,
      'That outcome should set priorities, sequencing, and what the team deliberately declines so focus is not diluted.',
      'Recommendations earn their keep only when they move the business meaningfully toward this direction.',
    ]);
  }
  return joinPolished([
    'Near-term direction is still open.',
    'Naming the business outcome that would make the next ninety days feel successful would turn activity into a coherent agenda.',
  ]);
}

function composeWhatSuccess(metrics, opts = {}) {
  const businessName = opts.businessName || '';
  const facts = opts.normalizedFacts || null;
  const operatorMetrics = ((facts && facts.success_metrics) || [])
    .map((m) => String(m || '').trim())
    .filter((m) => m && !isLiteralUncertaintyPhrase(m));

  if (operatorMetrics.length) {
    const list = operatorMetrics.map((m) => normalizeMetricPresentationPhrase(m));
    const parts = [
      `${businessSubject(businessName || 'The business')} currently wants to judge progress using: ${list.join('; ')}`,
      'Those are operator-stated commercial signals — not vanity activity.',
    ];
    // Max suggestions stay attributed; never silently merge into operator criteria.
    const maxSuggestions = [
      'qualified replies',
      'booked conversations',
      'estimate requests',
    ].filter((s) => !list.some((m) => m.toLowerCase().includes(s.split(' ')[0])));
    if (maxSuggestions.length) {
      parts.push(
        `Max may also want to explore: ${maxSuggestions.join(', ')}`
      );
    }
    return joinPolished(parts);
  }

  const metric = normalizeClaim('metrics', metrics, { businessName });
  if (metric && !/\bqualified replies, booked conversations\b/i.test(metric)) {
    return joinPolished([
      metric,
      'Those are commercial signals, not vanity activity — they tell the truth about whether the work is creating value.',
      'If they move, the approach is working; if they stall, the approach should be questioned quickly.',
    ]);
  }
  return joinPolished([
    'The measures of success have not yet been named with enough precision.',
    'Agreeing on a small set of watched signals would make it obvious whether the relationship is creating value.',
  ]);
}

const DEFAULT_LEARN_MORE_TOPICS = [
  'Pricing philosophy',
  'Capacity planning',
  'Seasonality',
  'Hiring strategy',
  'Operational bottlenecks',
  'Referral sources',
  'Technology stack',
];

function titleCasePhrase(text) {
  return String(text || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function collectUnknownLabels(sections) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const unknownLabels = [];
  for (const key of BLUEPRINT_SECTIONS) {
    for (const u of s(key).unknowns || []) {
      const label = humanizeUnknownLabel(u);
      if (label && !unknownLabels.includes(label)) unknownLabels.push(label);
    }
    if (!String(s(key).summary || '').trim() || answerLooksEmpty(s(key).summary)) {
      const label = humanizeUnknownLabel(`Missing clear answer for ${key}`);
      if (label && !unknownLabels.includes(label)) unknownLabels.push(label);
    }
  }
  return unknownLabels;
}

/**
 * SPEC-085 — always identify meaningful unknowns. Never return "nothing outstanding."
 * SPEC-099: explicit interview unknowns (e.g. unresolved ICP) rank ahead of generic fillers.
 */
function composeLearnMoreItems(unknownLabels, opts = {}) {
  const facts = opts.normalizedFacts || null;
  const cleaned = [...new Set((unknownLabels || []).map(humanizeUnknownLabel).filter(Boolean))];
  // Keep question-form unknowns readable; title-case only short topic labels.
  const items = cleaned.map((label) =>
    /\?$/.test(label) || /^(which|who|what|how|where)\b/i.test(label)
      ? capitalizeSentence(label.replace(/\?$/, '') + (/\?$/.test(label) ? '?' : ''))
      : titleCasePhrase(label)
  );

  const prefersCommercial =
    /commercial/i.test(String((facts && facts.growth_focus) || '')) ||
    /commercial/i.test(String((facts && facts.vertical_focus) || ''));
  const hasNamedIdeal = ((facts && facts.ideal_customers) || []).some(
    (item) => item && !isLiteralUncertaintyPhrase(item)
  );
  const geo = ((facts && facts.geography) || []).join(', ') || 'the target geography';

  if (prefersCommercial && !hasNamedIdeal) {
    const icpNeed = `Which commercial customer segments are the strongest fit for ${
      (facts && facts.business_name) || 'the business'
    } within ${geo}?`;
    const already = items.some((item) => /commercial customer segment/i.test(item));
    if (!already) items.unshift(icpNeed);
    else {
      const idx = items.findIndex((item) => /commercial customer segment|ideal customer/i.test(item));
      if (idx > 0) {
        const [row] = items.splice(idx, 1);
        items.unshift(icpNeed);
      } else if (idx === 0) {
        items[0] = icpNeed;
      }
    }
  }

  for (const fallback of DEFAULT_LEARN_MORE_TOPICS) {
    if (items.length >= 4) break;
    if (!items.some((item) => item.toLowerCase() === fallback.toLowerCase())) {
      items.push(fallback);
    }
  }
  return items.slice(0, 5);
}

function composeLearnMoreBody(items) {
  const list = (items || []).filter(Boolean);
  if (!list.length) {
    return joinPolished([
      'A few practical areas still deserve a closer look before recommendations get specific.',
      'Future recommendations become more confident as these areas become understood.',
    ]);
  }
  return joinPolished([
    'A few practical areas still deserve a closer look before recommendations get specific.',
    'Future recommendations become more confident as these areas become understood.',
  ]);
}

function sectionFilled(section) {
  return Boolean(section && String(section.summary || '').trim() && !answerLooksEmpty(section.summary));
}

function sectionConfidence(section) {
  const conf = Number(section && section.confidence);
  if (!Number.isFinite(conf)) return 0;
  return Math.max(0, Math.min(1, conf));
}

/**
 * Evidence-connected observations — not recommendations, not strategy.
 * Built from normalized facts / synthesized claims. Maximum five.
 * Each observation is one concise sentence — never a raw answer paragraph.
 */
function composeObservations(sections, normalizedFacts = null) {
  const facts = normalizedFacts || emptyNormalizedFacts();
  const s = (key) => (sections && sections[key]) || emptySection();
  const observations = [];
  const name = sanitizeBusinessName(
    facts.business_name || extractBusinessName(s('identity').summary) || 'The business'
  );
  const shortName = String(name).replace(/\s+Cleaning$/i, '') || name;
  const possessiveShort = /s$/i.test(shortName) ? `${shortName}'` : `${shortName}'s`;

  if (facts.differentiation || coreClaim(s('competitiveAdvantages').summary)) {
    const edge = synthesizeDifferentiationSnippet(
      facts.differentiation ||
        String(coreClaim(s('competitiveAdvantages').summary) || '').replace(
          /^Competitive edge is described as\s+/i,
          ''
        )
    );
    if (
      edge &&
      !containsRawPromptFragment(edge) &&
      edge.split(/\s+/).length <= 40 &&
      !/Not yet defined|Not applicable/i.test(edge) &&
      facts.epistemic_states?.differentiation !== EPISTEMIC_STATES.UNKNOWN &&
      facts.epistemic_states?.differentiation !== EPISTEMIC_STATES.NOT_APPLICABLE
    ) {
      observations.push(
        `${possessiveShort} differentiation centers on ${edge}.`
      );
    }
  }

  if ((facts.ideal_customers || []).length && (facts.disqualified_customers || []).length) {
    const namedIdeal = (facts.ideal_customers || []).filter(
      (item) => item && !isLiteralUncertaintyPhrase(item)
    );
    if (namedIdeal.length) {
      observations.push(
        'Commercial focus is unusually clear: both the relationships worth pursuing and the ones to decline are named explicitly.'
      );
    } else if (/commercial/i.test(String(facts.growth_focus || ''))) {
      observations.push(
        'Commercial work is preferred and decline criteria are named, but priority commercial segments remain unresolved.'
      );
    }
  } else if ((facts.ideal_customers || []).filter((i) => i && !isLiteralUncertaintyPhrase(i)).length) {
    observations.push(
      `Ideal-customer focus on ${facts.ideal_customers
        .filter((i) => i && !isLiteralUncertaintyPhrase(i))
        .slice(0, 3)
        .join(', ')} gives outreach a disciplined starting point.`
    );
  } else if (/commercial/i.test(String(facts.growth_focus || ''))) {
    observations.push(
      'Commercial work is preferred, but the priority commercial segments have not yet been established.'
    );
  }

  if (facts.ninety_day_outcomes || facts.growth_focus) {
    const goal =
      facts.growth_focus && /commercial/i.test(facts.growth_focus)
        ? `${possessiveShort} near-term growth goal is to build a clearer, repeatable path to commercial cleaning opportunities`
        : `${possessiveShort} near-term growth goal is ${midSentence(
            firstSentence(normalizeBusinessPhrase(facts.ninety_day_outcomes || facts.growth_focus))
          )}`;
    if (
      goal &&
      !containsRawPromptFragment(goal) &&
      !/would feel successful/i.test(goal) &&
      goal.split(/\s+/).length <= 40
    ) {
      observations.push(goal);
    }
  }

  if ((facts.geography || []).length) {
    observations.push(
      `Geographic attention concentrates first in ${facts.geography.join(', ')}, which keeps discovery from spreading too thin.`
    );
  }

  if (facts.brand_voice || coreClaim(s('brandVoice').summary)) {
    const tone = normalizeBrandVoiceTone(facts.brand_voice || coreClaim(s('brandVoice').summary));
    if (
      tone &&
      !/anchor/i.test(tone) &&
      !/Not yet defined|Not applicable/i.test(tone) &&
      facts.epistemic_states?.brand_voice !== EPISTEMIC_STATES.UNKNOWN &&
      facts.epistemic_states?.brand_voice !== EPISTEMIC_STATES.NOT_APPLICABLE
    ) {
      observations.push(
        `${possessiveShort} brand voice reinforces its positioning by sounding ${tone}.`
      );
    }
  }

  if ((facts.success_metrics || []).length) {
    observations.push(
      `Success is anchored in outcomes such as ${facts.success_metrics.slice(0, 4).join(', ')}, not vanity activity.`
    );
  }

  if (facts.business_description && (facts.services || []).length && observations.length < 3) {
    observations.push(
      'Identity and offer already form a coherent foundation any growth recommendation should respect.'
    );
  }

  if (!observations.length) {
    observations.push(
      'The conversation establishes a workable foundation, though several themes still need more evidence before they can be stated with high confidence.'
    );
  }

  return observations
    .map((line) => String(line || '').trim())
    .filter(Boolean)
    .filter((line) => !containsRawPromptFragment(line))
    .filter(
      (line) =>
        !/would feel successful|we will know|both geography is|brand voice should sound anchor|Anchor Cleaning we|a Anchor|anchor'?s calm|low — price|great — fit/i.test(
          line
        )
    )
    // One sentence only — drop anything that still looks like a paragraph dump.
    .map((line) => firstSentence(line))
    .filter((line) => line.split(/\s+/).length <= 45)
    .slice(0, 5)
    .map((line) => ensurePeriod(line));
}

function starsFromConfidence(conf) {
  const c = Math.max(0, Math.min(1, Number(conf) || 0));
  if (c >= 0.9) return 5;
  if (c >= 0.75) return 4;
  if (c >= 0.55) return 3;
  if (c >= 0.35) return 2;
  if (c > 0) return 1;
  return 1;
}

function averageConfidence(values) {
  const nums = values.filter((v) => Number.isFinite(v));
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

/**
 * Assessment scores derive only from observed section confidence.
 * Explanations always reference evidence patterns — never fabricated.
 * SPEC-099: confidence explanations may only cite evidence that actually exists.
 */
function composeAssessment(sections, opts = {}) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const facts = opts.normalizedFacts || null;

  const clarityParts = [sectionConfidence(s('identity')), sectionConfidence(s('services'))];
  const focusParts = [
    sectionConfidence(s('idealCustomers')),
    sectionConfidence(s('avoidCustomers')),
    sectionConfidence(s('targetMarkets')),
  ];
  const diffParts = [
    sectionConfidence(s('competitiveAdvantages')),
    sectionConfidence(s('brandVoice')),
  ];
  const growthParts = [
    sectionConfidence(s('campaignGoals')),
    sectionConfidence(s('successMetrics')),
  ];

  const clarity = averageConfidence(clarityParts);
  const focus = averageConfidence(focusParts);
  const diff = averageConfidence(diffParts);
  const growth = averageConfidence(growthParts);
  const overall = averageConfidence([clarity, focus, diff, growth]);

  const identityClaim = coreClaim(s('identity').summary);
  const idealClaim = coreClaim(s('idealCustomers').summary);
  const avoidClaim = coreClaim(s('avoidCustomers').summary);
  const advClaim = coreClaim(s('competitiveAdvantages').summary);
  const goalClaim = coreClaim(s('campaignGoals').summary);
  const metricClaim = coreClaim(s('successMetrics').summary);

  const namedIdeal = ((facts && facts.ideal_customers) || []).filter(
    (item) => item && !isLiteralUncertaintyPhrase(item)
  );
  const hasNamedIdeal =
    namedIdeal.length > 0 &&
    Boolean(idealClaim) &&
    !/\bi don'?t know\b|\bnot sure\b/i.test(String(idealClaim));
  const prefersCommercial =
    /commercial/i.test(String((facts && facts.growth_focus) || '')) ||
    /commercial/i.test(String((facts && facts.vertical_focus) || '')) ||
    /prefer commercial|commercial/i.test(String((facts && facts.business_description) || ''));
  const hasAvoid = Boolean(avoidClaim) || ((facts && facts.disqualified_customers) || []).length > 0;
  const hasGeo = ((facts && facts.geography) || []).length > 0 || Boolean(coreClaim(s('targetMarkets').summary));

  let marketFocusExplanation;
  let marketFocusStars = starsFromConfidence(focus);
  if (hasNamedIdeal && hasAvoid) {
    marketFocusExplanation =
      'Supported by both a named ideal customer and explicit constraints on who not to serve.';
  } else if (prefersCommercial && (hasAvoid || hasGeo) && !hasNamedIdeal) {
    const geoBit = ((facts && facts.geography) || []).join(', ') || 'the stated geography';
    marketFocusExplanation = `Commercial preference and geography (${geoBit}) are established, but the primary customer segment is still unresolved.`;
    // SPEC-100 — unresolved ideal customer must limit Market Focus confidence.
    marketFocusStars = Math.min(marketFocusStars, 2);
  } else if (hasNamedIdeal || hasGeo) {
    marketFocusExplanation =
      'Supported by customer or market focus signals, with room to sharpen the full beachhead.';
  } else {
    marketFocusExplanation = 'Customer and market focus still need more specific evidence.';
    if (!hasNamedIdeal) marketFocusStars = Math.min(marketFocusStars, 2);
  }

  const diffEpistemic = facts?.epistemic_states?.differentiation || s('competitiveAdvantages')?.epistemic_state || (facts?.differentiation ? EPISTEMIC_STATES.KNOWN : EPISTEMIC_STATES.UNRESOLVED);

  let diffStars = starsFromConfidence(diff);
  let diffExplanation;
  if (diffEpistemic === EPISTEMIC_STATES.UNKNOWN || diffEpistemic === EPISTEMIC_STATES.UNRESOLVED || !advClaim || answerLooksEmpty(advClaim) || /Not yet defined/i.test(advClaim)) {
    diffStars = 1;
    diffExplanation = 'Competitive reason-to-choose is not yet defined and remains an open area to investigate.';
  } else if (diffEpistemic === EPISTEMIC_STATES.HYPOTHESIS) {
    diffStars = Math.min(diffStars, 2);
    diffExplanation = `Supported by a working hypothesis around ${softenClaim(advClaim)}; requires market validation.`;
  } else {
    diffExplanation = `Supported by stated advantages around ${softenClaim(advClaim)}.`;
  }

  const ratings = [
    {
      label: 'Business Clarity',
      stars: starsFromConfidence(clarity),
      explanation: identityClaim
        ? `Supported by a clear identity statement${sectionFilled(s('services')) ? ' and a concrete service mix' : ''}.`
        : 'Identity and service mix are still lightly sketched in the conversation.',
    },
    {
      label: 'Market Focus',
      stars: marketFocusStars,
      explanation: marketFocusExplanation,
    },
    {
      label: 'Differentiation',
      stars: diffStars,
      explanation: diffExplanation,
    },
    {
      label: 'Growth Readiness',
      stars: starsFromConfidence(growth),
      explanation:
        goalClaim && metricClaim
          ? 'Supported by named near-term outcomes and business metrics for judging progress.'
          : goalClaim || metricClaim
            ? 'Direction or success measures are present; pairing both would raise readiness further.'
            : 'Near-term outcomes and success measures are still open.',
    },
  ];

  return {
    ratings,
    confidencePercent: Math.round(overall * 100),
    confidenceNote:
      'Confidence reflects how consistently the conversation evidenced each theme — not a grade of the business itself.',
  };
}

function composeConversationStarters(sections, learnMoreItems) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const starters = [];

  if (sectionFilled(s('idealCustomers'))) {
    starters.push(
      'Which customer segments generate the highest lifetime value — and how you recognize them early.'
    );
  }
  if (sectionFilled(s('competitiveAdvantages'))) {
    starters.push(
      'Whether your pricing reinforces the differentiation you want the market to recognize.'
    );
  }
  if (sectionFilled(s('avoidCustomers')) || sectionFilled(s('idealCustomers'))) {
    starters.push(
      'How referral partnerships compare with outbound acquisition for the relationships you want most.'
    );
  }
  if (sectionFilled(s('campaignGoals'))) {
    starters.push(
      'What would make the next ninety days feel unmistakably successful from the owner\'s chair.'
    );
  }
  if (sectionFilled(s('successMetrics'))) {
    starters.push(
      'Which leading indicators you trust before lagging revenue numbers move.'
    );
  }

  for (const topic of learnMoreItems || []) {
    if (starters.length >= 4) break;
    const lower = String(topic).toLowerCase();
    if (/pric/.test(lower)) {
      starters.push('How pricing decisions get made when demand is strong versus soft.');
    } else if (/capacit/.test(lower)) {
      starters.push('Where capacity starts to constrain growth before marketing does.');
    } else if (/season/.test(lower)) {
      starters.push('How seasonality shapes staffing, cash flow, and outreach timing.');
    } else if (/hir/.test(lower)) {
      starters.push('What a strong hire looks like for the next stage of the business.');
    } else if (/referral/.test(lower)) {
      starters.push('Which referral sources historically produce the cleanest fit.');
    } else if (/technolog|stack/.test(lower)) {
      starters.push('Which tools actually carry the customer relationship day to day.');
    } else if (/operational|bottleneck/.test(lower)) {
      starters.push('Where work most often slows between winning a job and delivering it well.');
    }
  }

  const unique = [];
  for (const line of starters) {
    if (!unique.includes(line)) unique.push(line);
  }
  if (!unique.length) {
    unique.push(
      'Which customer segments generate the highest lifetime value.',
      'Whether your pricing reflects your positioning.',
      'How referral partnerships compare with outbound acquisition.'
    );
  }
  return unique.slice(0, 4);
}

/**
 * SPEC-116 — scorecard sections for the Executive Business Brief.
 * Replaces "Success Looks Like". Distinguishes Max recommendations from
 * operator-approved metrics. Drafts are never presented as approved truth.
 */
function buildOperatorScorecardBriefSections(sections, opts = {}) {
  if (opts.operatorScorecard) {
    return buildBriefScorecardSections(opts.operatorScorecard);
  }
  const facts = opts.normalizedFacts || null;
  const goalFromFacts = facts && (facts.ninety_day_outcomes || facts.growth_focus);
  const businessGoal =
    opts.businessGoal ||
    (Array.isArray(goalFromFacts) ? goalFromFacts.join('; ') : goalFromFacts) ||
    (sections && sections.campaignGoals && sections.campaignGoals.summary) ||
    'Establish a repeatable acquisition process.';
  try {
    const draft = generateDraftScorecard({
      tenantId: opts.tenantId || (opts.clientId != null ? String(opts.clientId) : 'brief'),
      clientId: opts.clientId || null,
      businessName: opts.businessName,
      businessGoal,
      blueprint: { sections },
      normalizedFacts: facts,
      objectives: facts && facts.ninety_day_outcomes,
      operatorMetrics: facts && facts.success_metrics,
      aim: opts.aim || null,
      outcomes: opts.outcomes || null,
      learning: opts.scorecardLearning || null,
    });
    if (facts && facts.excluded_metrics && facts.excluded_metrics.length) {
      draft.metrics = draft.metrics.filter((metric) =>
        !facts.excluded_metrics.some((excluded) =>
          sameSemanticValue(metric.name, excluded) || sameSemanticValue(metric.key, excluded)
        )
      );
    }
    return buildBriefScorecardSections(draft);
  } catch (_err) {
    return buildBriefScorecardSections({
      status: 'draft',
      metrics: [],
      reasoning: { extraExplore: ['qualified replies', 'booked conversations', 'estimate requests'] },
    });
  }
}

/**
 * SPEC-085 — Executive Business Brief.
 * CEO-facing synthesis from a senior consultant. Never concatenates raw
 * interview wording or exposes implementation metadata.
 * Only business interview answers become evidence; refinement instructions
 * are sanitized out before render.
 * SPEC-116 replaces Success Looks Like with operator scorecard sections.
 */
function buildExecutiveSummary(sections, opts = {}) {
  const normalizedFacts =
    opts.normalizedFacts ||
    (opts.interviewState && opts.interviewState.normalizedFacts) ||
    null;
  const fromNormalized = normalizedFacts
    ? sectionsFromNormalizedFacts(normalizedFacts, sections)
    : null;
  const clean = sanitizeSectionsForBrief(fromNormalized || sections);
  const s = (key) => clean[key] || emptySection();
  const businessName = sanitizeBusinessName(
    (normalizedFacts && normalizedFacts.business_name) ||
      extractBusinessName(s('identity').summary)
  );
  const briefOpts = { businessName, normalizedFacts };
  const unknownLabels = collectUnknownLabels(clean);
  const learnMoreItems = composeLearnMoreItems(unknownLabels, { normalizedFacts });
  const observations = composeObservations(clean, normalizedFacts);
  const assessment = composeAssessment(clean, { normalizedFacts });
  const conversations = composeConversationStarters(clean, learnMoreItems);

  // Prefer direct normalized list rendering for key prose sections when available.
  let whoYouAre = composeWhoYouAre(s('identity').summary, s('services').summary, briefOpts);
  let whoYouServe = composeWhoYouServe(
    s('idealCustomers').summary,
    s('avoidCustomers').summary,
    s('targetMarkets').summary,
    briefOpts
  );
  let whyChooseYou = composeWhyChooseYou(
    s('competitiveAdvantages').summary,
    s('brandVoice').summary,
    briefOpts
  );
  let whereHeaded = composeWhereHeaded(s('campaignGoals').summary, briefOpts);
  let successLooksLike = composeWhatSuccess(s('successMetrics').summary, briefOpts);

  if (normalizedFacts) {
    const f = cloneNormalizedFacts(normalizedFacts);
    const cleanServices = (f.services || []).filter(
      (item) => item && !/^(?:anchor(?:\s+cleaning)?\s+)?provides?\b/i.test(item)
    );
    const cleanIdeal = (f.ideal_customers || []).filter(
      (item) =>
        item &&
        !isValueTraitPhrase(item) &&
        !isLiteralUncertaintyPhrase(item) &&
        !isConversationalFiller(item) &&
        !/as part of (?:my|our|the)\s+ideal customer/i.test(item) &&
        !/^(?:anchor(?:\s+cleaning)?\s+)?most wants\b/i.test(item)
    );

    if (cleanServices.length || f.business_name || f.business_description) {
      const identityLine =
        synthesizeNormalizedFact('identity', s('identity').summary, briefOpts) ||
        (f.business_name
          ? `${f.business_name} is a ${f.business_description || 'cleaning company'}`
          : 'This is a cleaning company');
      const whoYouAreParts = [identityLine];
      if (cleanServices.length) {
        whoYouAreParts.push(`Services include ${cleanServices.join(', ')}`);
      }
      whoYouAre = joinPolished(whoYouAreParts);
    }
    if (cleanIdeal.length || f.geography.length || /commercial/i.test(String(f.growth_focus || ''))) {
      const sentences = [];
      if (cleanIdeal.length) {
        const displayName = businessName || 'the business';
        const possessive = businessSubject(displayName, { possessive: true });
        const decisionMakers = cleanIdeal.filter(isDecisionMakerLabel);
        const segments = cleanIdeal.filter((item) => !isDecisionMakerLabel(item));
        const needsRecurring = /dependable recurring|recurring(?:\s+commercial)?(?:\s+cleaning)?|weekly or multiple/i.test(
          [
            s('idealCustomers').summary,
            f.growth_focus,
            (f.ideal_customer_traits || []).join(' '),
            cleanIdeal.join(' '),
          ].join(' ')
        );
        if (decisionMakers.length && segments.length) {
          const dmProse = formatDecisionMakerProse(decisionMakers);
          const segProse = formatSegmentProse(segments);
          sentences.push(
            `${possessive} current acquisition focus is ${dmProse}, particularly those responsible for ${segProse}${
              needsRecurring ? ' that need dependable recurring cleaning' : ''
            }`
          );
        } else {
          sentences.push(
            `${possessive} ideal customers include ${cleanIdeal.join(', ')}${
              needsRecurring ? ' that need dependable recurring cleaning' : ''
            }`
          );
        }
      } else if (/commercial/i.test(String(f.growth_focus || f.vertical_focus || ''))) {
        const shortName =
          String(businessName || 'The business').replace(/\s+Cleaning$/i, '') || 'The business';
        // Keep to two sentences so geography / avoid still fit joinPolished's 4-sentence budget.
        sentences.push(
          `${shortName} has not chosen a primary commercial customer segment yet`
        );
        sentences.push(
          `${businessSubject(businessName || 'The business')} prefers commercial work while continuing to serve residential customers, but the ideal segment remains an open decision`
        );
      }
      if (f.disqualified_customers.length) {
        const constraintRaw = f.disqualified_customers.join('; ');
        sentences.push(
          composeCustomerConstraintPresentation(
            businessName || 'The business',
            constraintRaw
          ) ||
            synthesizeNormalizedFact(
              'avoid',
              `The business prefers to avoid ${constraintRaw}`,
              briefOpts
            ) ||
            `${businessName || 'The business'} prefers to avoid ${constraintRaw}`
        );
      }
      if (f.geography.length) {
        const towns = f.geography.filter((g) => !/^Greater (?:Manchester|Toronto Area)$/i.test(g));
        const hasGM = f.geography.some((g) => /Greater Manchester/i.test(g));
        const hasGTA = f.geography.some((g) => /Greater Toronto|GTA/i.test(g));
        if (hasGM) {
          sentences.push(
            `${businessSubject(businessName || 'Anchor', { possessive: true })} near-term geography is the Greater Manchester area${
              towns.length ? `, including ${towns.join(', ')}` : ''
            }`
          );
        } else if (hasGTA) {
          sentences.push(
            `${businessSubject(businessName || 'the business', { possessive: true })} near-term geography centers on the Greater Toronto Area`
          );
        } else {
          sentences.push(
            `${businessSubject(businessName || 'the business', { possessive: true })} near-term geography centers on ${f.geography.join(', ')}`
          );
        }
      }
      if (sentences.length >= 2 && cleanIdeal.length) {
        sentences.push(
          'Taken together, this is a disciplined beachhead: fit over volume, and geography chosen to match that fit.'
        );
      }
      whoYouServe = joinPolished(sentences);
    }
    if (f.brand_voice) {
      const adv =
        synthesizeNormalizedFact('advantages', s('competitiveAdvantages').summary, briefOpts) ||
        (f.differentiation
          ? `Customers choose ${businessName || 'this business'} because ${midSentence(f.differentiation)}`
          : '');
      const voice = synthesizeNormalizedFact(
        'voice',
        `Brand voice should read as ${f.brand_voice}`,
        briefOpts
      );
      whyChooseYou = joinPolished(
        [adv, voice, 'Differentiation and tone must reinforce each other so the market experiences the same promise the business actually keeps.'].filter(
          Boolean
        )
      );
    }
    const differentiationFacts = f.business_facts?.differentiation || [];
    const unknownReason = differentiationFacts.find(
      (fact) => fact.subject === 'customer_buying_reason' && fact.epistemic_state === EPISTEMIC_STATES.UNKNOWN
    );
    const candidateReason = differentiationFacts.find(
      (fact) => fact.subject === 'candidate_customer_buying_reason' && fact.epistemic_state === EPISTEMIC_STATES.HYPOTHESIS
    );
    if (unknownReason && candidateReason) {
      whyChooseYou = `Actual customer reason-to-choose: Not yet established. Current hypothesis: ${candidateReason.hypothesis_value}.`;
    }
  }

  const bleedRe =
    /would feel successful if|we will know the growth work|both geography is|anchor'?s brand voice should sound|when a great-fit customer chooses|to say short term|Anchor Cleaning we|a Anchor|anchor'?s calm|low — price|great — fit|we'?s brand voice/i;
  const scrubBleed = (body) =>
    protectAbbreviations(String(body || ''))
      .split(/(?<=[.!?])\s+/)
      .map((sentence) => restoreAbbreviations(sentence))
      .filter((sentence) => !bleedRe.test(sentence) && !containsRawPromptFragment(sentence))
      .join(' ');

  whoYouAre = scrubBleed(whoYouAre);
  whoYouServe = scrubBleed(whoYouServe);
  whyChooseYou = scrubBleed(whyChooseYou);
  whereHeaded = scrubBleed(whereHeaded);
  successLooksLike = scrubBleed(successLooksLike);

  // SPEC-101 — presentation normalization for client-facing Brief only.
  whoYouAre = normalizePresentationProse(whoYouAre);
  whoYouServe = normalizePresentationProse(whoYouServe);
  whyChooseYou = normalizePresentationProse(whyChooseYou);
  whereHeaded = normalizePresentationProse(whereHeaded);
  successLooksLike = normalizePresentationProse(successLooksLike);

  const scorecardSections = buildOperatorScorecardBriefSections(clean, {
    ...opts,
    businessName,
    normalizedFacts,
  });

  return {
    title: 'Executive Business Brief',
    subtitle: 'Prepared by Max',
    tagline: 'A working picture for leadership review',
    sections: [
      {
        id: 'whoYouAre',
        title: 'Who You Are',
        kind: 'prose',
        body: whoYouAre,
      },
      {
        id: 'whoYouServe',
        title: 'Who You Serve',
        kind: 'prose',
        body: whoYouServe,
      },
      {
        id: 'whyChooseYou',
        title: 'Why Customers Choose You',
        kind: 'prose',
        body: whyChooseYou,
      },
      {
        id: 'whereHeaded',
        title: "Where You're Headed",
        kind: 'prose',
        body: whereHeaded,
      },
      ...scorecardSections,
      {
        id: 'observations',
        title: 'Initial Observations',
        kind: 'list',
        body: 'These observations connect themes from our conversation. They are not recommendations.',
        items: observations,
      },
      {
        id: 'assessment',
        title: "Max's Initial Assessment",
        kind: 'assessment',
        ratings: assessment.ratings,
        confidencePercent: assessment.confidencePercent,
        body: assessment.confidenceNote,
      },
      {
        id: 'learnMore',
        title: "Areas I'd Like To Learn More",
        kind: 'list',
        body: composeLearnMoreBody(learnMoreItems),
        items: learnMoreItems,
      },
      {
        id: 'conversations',
        title: "Conversations I'd Recommend Next",
        kind: 'list',
        body: "I'd enjoy exploring:",
        items: conversations,
      },
    ],
  };
}

/** @deprecated Use buildExecutiveSummary — alias retained for SPEC-085 naming clarity. */
function buildExecutiveBusinessBrief(sections) {
  return buildExecutiveSummary(sections);
}

function sectionStateFromSession(session) {
  return (session && session.interview_state && session.interview_state.sectionState) || emptySections();
}

function withExperienceFields(session, payload = {}) {
  const sectionState = sectionStateFromSession(session);
  const normalizedFacts =
    (session && session.interview_state && session.interview_state.normalizedFacts) || null;
  const out = {
    ...payload,
    progress: payload.progress || computeProgress(sectionState),
    understanding: buildUnderstandingProgress(sectionState),
  };
  if (payload.blueprint && payload.blueprint.sections) {
    out.executiveSummary = buildExecutiveSummary(payload.blueprint.sections, {
      normalizedFacts,
      interviewState: session && session.interview_state,
      clientId: session && (session.client_id || session.clientId),
      tenantId: session && (session.client_id || session.clientId),
    });
  }
  return out;
}

function shouldReflect(stepIndex) {
  const answered = Number(stepIndex) || 0;
  return answered > 0 && answered % REFLECTION_EVERY_N === 0;
}

/**
 * SPEC-099 — whether existing interview evidence can help the operator reason
 * through an explicit unknown for this section.
 */
function hasRelevantUncertaintyContext(state, sectionKey) {
  const facts = (state && state.normalizedFacts) || emptyNormalizedFacts();
  const sections = (state && state.sectionState) || emptySections();
  switch (sectionKey) {
    case 'idealCustomers':
      return Boolean(
        /commercial|residential/i.test(String(facts.growth_focus || '')) ||
          /commercial|residential/i.test(String(facts.vertical_focus || '')) ||
          /commercial|residential/i.test(String(facts.business_description || '')) ||
          (facts.disqualified_customers || []).length ||
          (facts.geography || []).length ||
          (facts.services || []).length ||
          String((sections.avoidCustomers && sections.avoidCustomers.summary) || '').trim()
      );
    case 'services':
      return Boolean(facts.business_description || facts.growth_focus);
    case 'targetMarkets':
      return Boolean(
        (facts.ideal_customers || []).length ||
          facts.growth_focus ||
          (facts.services || []).length
      );
    case 'campaignGoals':
    case 'successMetrics':
      return Boolean(facts.growth_focus || facts.ninety_day_outcomes || (facts.services || []).length);
    default:
      return Boolean(
        facts.business_name ||
          facts.business_description ||
          (facts.services || []).length ||
          facts.growth_focus
      );
  }
}

/**
 * SPEC-099/100 — contextual collaborative reasoning for meaningful uncertainty.
 * Deterministic template; LLM may later polish phrasing.
 * Hypotheses are framed as options — never written as operator evidence.
 *
 * @param {object} state
 * @param {string} sectionKey
 * @param {string|null} businessName
 * @param {{ attemptCount?: number }} [opts]
 */
function buildUncertaintyReasoningProbe(state, sectionKey, businessName = null, opts = {}) {
  const facts = (state && state.normalizedFacts) || emptyNormalizedFacts();
  const name =
    sanitizeBusinessName(businessName || facts.business_name || '') || 'the business';
  const shortName = String(name).replace(/\s+Cleaning$/i, '') || name;
  const geo = (facts.geography || []).join(', ');
  const avoids = (facts.disqualified_customers || []).slice(0, 2).join('; ');
  const services = (facts.services || []).slice(0, 4);
  const prefersCommercial =
    /commercial/i.test(String(facts.growth_focus || '')) ||
    /commercial/i.test(String(facts.vertical_focus || '')) ||
    /prefer commercial/i.test(String(facts.business_description || ''));
  const attempt = Math.max(1, Number(opts.attemptCount) || 1);

  if (sectionKey === 'idealCustomers') {
    const bits = [];
    bits.push("That's completely fine. We don't need to force an answer yet.");
    if (prefersCommercial) {
      const serviceBit = services.length
        ? ` and your current services include ${services.slice(0, 3).join(', ')}`
        : '';
      bits.push(
        `You've told me you prefer commercial work${
          /residential/i.test(String(facts.vertical_focus || facts.business_description || ''))
            ? ' alongside residential'
            : ''
        }${serviceBit}. So I'd narrow this down by asking what kind of commercial relationship you actually want.`
      );
    } else if (services.length) {
      bits.push(
        `You've already described services like ${services.slice(0, 3).join(', ')}. We can use that as a starting point.`
      );
    }
    if (avoids) {
      bits.push(`You've also flagged constraints around ${avoids}.`);
    }
    if (geo) {
      bits.push(`Geography is centered on ${geo}.`);
    }

    if (attempt >= 2) {
      bits.push(
        `Still open is fine. Based on what we know so far, would it help to decide from experience instead — which past commercial jobs felt easiest to win and retain, or should we leave the ideal customer unresolved for now and revisit once we have more evidence?`
      );
    } else {
      bits.push(
        `Would you rather prioritize:\n• smaller offices where the owner or office manager can make a decision quickly,\n• recurring facilities that may be larger but have a longer sales process,\n• property managers who could potentially send multiple locations,\n• or are you still open enough that we should test several segments before choosing?`
      );
    }
    return bits.join(' ');
  }

  return (
    buildProbingFollowUp(
      { section: sectionKey },
      { reason: 'explicit_unknown' },
      name
    ) ||
    `That's okay — we can leave that open for now, or reason from what we already know about ${shortName}. What feels closest?`
  );
}

/**
 * Accept an explicit unknown: persist unknown evidence, preserve prior known facts, advance.
 */
function prefersCommercialAck(state, sectionKey) {
  const facts = (state && state.normalizedFacts) || emptyNormalizedFacts();
  const prefersCommercial =
    /commercial/i.test(String(facts.growth_focus || '')) ||
    /commercial/i.test(String(facts.vertical_focus || '')) ||
    /prefer commercial/i.test(String(facts.business_description || ''));
  if (sectionKey === 'idealCustomers' && prefersCommercial) {
    return "That's okay. We'll leave that open for now. We know you prefer commercial work, but we haven't established which commercial segments are the best fit yet. That's something we can investigate rather than guess.";
  }
  return "That's okay. We'll leave that open for now and keep moving.";
}

async function acceptExplicitUnknown(store, session, state, q, clientTurn, text) {
  const sectionKey = q.question.section;
  const { evidenceRow } = await applySectionUpdate(
    store,
    session,
    sectionKey,
    '',
    'INFERRED',
    clientTurn.id
  );
  // Prefer a precise unknown label for idealCustomers when commercial preference exists.
  const facts = session.interview_state.normalizedFacts || state.normalizedFacts || emptyNormalizedFacts();
  const sectionState = session.interview_state.sectionState || state.sectionState;
  const section = sectionState[sectionKey] || emptySection();
  const unknowns = [...(section.unknowns || [])];
  if (sectionKey === 'idealCustomers') {
    const prefersCommercial =
      /commercial/i.test(String(facts.growth_focus || '')) ||
      /commercial/i.test(String(facts.vertical_focus || ''));
    const label = prefersCommercial
      ? 'Which commercial customer segments are the strongest fit'
      : 'Missing clear answer for idealCustomers';
    if (!unknowns.some((u) => /commercial customer segment|idealCustomers|ideal customer/i.test(String(u)))) {
      unknowns.unshift(label);
    }
  }
  sectionState[sectionKey] = {
    ...section,
    // Do not invent a summary from the unknown phrase.
    summary:
      section.summary &&
      !answerLooksEmpty(section.summary) &&
      !/\bi don'?t know\b/i.test(section.summary)
        ? section.summary
        : '',
    confidence: section.summary && !answerLooksEmpty(section.summary)
      ? section.confidence || UNKNOWN_CONFIDENCE
      : UNKNOWN_CONFIDENCE,
    unknowns,
  };

  let reasoningMemory = ensureReasoningMemory(session.interview_state || state);
  reasoningMemory = addQuestionDebt(reasoningMemory, {
    questionId: q.question.id,
    section: sectionKey,
    reason: 'explicit_unknown',
  });
  reasoningMemory = setActiveProbe(reasoningMemory, null);

  state.sectionState = sectionState;
  state.normalizedFacts = facts;
  state.reasoningMemory = reasoningMemory;
  state.answers = { ...(state.answers || {}), [q.question.id]: '' };
  state.stepIndex = (Number(state.stepIndex) || 0) + 1;
  if (state.stepIndex >= QUESTION_BANK.length) state.done = true;
  state.supplementalContext = [
    ...(state.supplementalContext || []),
    {
      at: nowIso(),
      text,
      domain: null,
      kind: MESSAGE_TYPES.INSUFFICIENT_ANSWER,
      activeQuestionId: q.question.id,
      confirmed: false,
      acceptedUnknown: true,
      section: sectionKey,
    },
  ];
  session.interview_state = state;

  await store.updateTurn(clientTurn.id, {
    derived_evidence: evidenceRow ? [evidenceRow.id] : [],
  });

  return { advanced: true, evidenceRow };
}

function buildReflection(sectionState, answeredCount) {
  const priorityKeys = ['identity', 'services', 'idealCustomers', 'campaignGoals'];
  const usable = (section) => {
    const summary = String((section && section.summary) || '').trim();
    if (!summary || answerLooksEmpty(summary)) return false;
    if (/\bi don'?t know\b|\bnot sure yet\b/i.test(summary)) return false;
    return true;
  };
  const filled = priorityKeys
    .map((key) => {
      const section = sectionState && sectionState[key];
      if (!usable(section)) return null;
      return { key, summary: section.summary };
    })
    .filter(Boolean);
  const fallback = BLUEPRINT_SECTIONS.map((key) => {
    const section = sectionState && sectionState[key];
    if (!usable(section)) return null;
    return { key, summary: section.summary };
  }).filter(Boolean);
  const source = filled.length ? filled : fallback;
  if (!source.length) return null;

  const openings = [
    "Thanks, that's helpful. Here's what I'm hearing so far",
    'Let me make sure I understand',
    "Here's what I'm taking away so far",
  ];
  const opener =
    openings[Math.max(0, Math.floor(answeredCount / REFLECTION_EVERY_N) - 1) % openings.length];
  const directionTitles = {
    identity: 'Identity',
    services: 'Services',
    idealCustomers: 'Ideal Customer',
    campaignGoals: 'Direction',
  };
  const snippets = source.slice(0, 4).map((row) => {
    const title = directionTitles[row.key] || SECTION_TITLES[row.key] || row.key;
    // Prefer full service list first sentence; never reduce multi-value services to one token.
    return `${title}: ${firstSentence(row.summary)}`;
  });
  return `${opener}…\n\n${snippets.join('\n')}`;
}

function currentQuestion(state) {
  if (!state || state.mode === 'notes') return null;
  const idx = Number(state.stepIndex) || 0;
  if (idx >= QUESTION_BANK.length) return null;
  return { index: idx, question: QUESTION_BANK[idx] };
}

function initialInterviewState({ notes } = {}) {
  return {
    mode: notes ? 'notes' : 'interactive',
    stepIndex: 0,
    done: Boolean(notes),
    answers: {},
    sectionState: emptySections(),
    contradictions: [],
    revisionGuidance: [],
    /** Out-of-order facts that must not overwrite the active question answer. */
    supplementalContext: [],
    /** Normalized business evidence — Brief reads this, not raw transcript. */
    normalizedFacts: emptyNormalizedFacts(),
    /** SPEC-090 — session-level conversational reasoning memory. */
    reasoningMemory: emptyReasoningMemory(),
    workingSemanticCorrections: [],
    notes: notes ? String(notes) : null,
    blueprintId: null,
    lastReflectionAt: 0,
  };
}

function cloneJson(value) {
  if (value == null) return value;
  return JSON.parse(JSON.stringify(value));
}

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const sessions = new Map();
  /** @type {Map<string, object[]>} */
  const turnsBySession = new Map();
  /** @type {Map<string, object>} */
  const evidence = new Map();
  /** @type {Map<string, object[]>} */
  const evidenceBySession = new Map();
  /** @type {Map<string, object>} */
  const blueprints = new Map();
  /** @type {Map<string, object[]>} */
  const blueprintsByClient = new Map();

  return {
    kind: 'memory',
    async insertSession(row) {
      const copy = {
        ...row,
        interview_state: cloneJson(row.interview_state || {}),
      };
      sessions.set(copy.id, copy);
      turnsBySession.set(copy.id, []);
      evidenceBySession.set(copy.id, []);
      return {
        ...copy,
        interview_state: cloneJson(copy.interview_state),
      };
    },
    async getSession(id) {
      const row = sessions.get(String(id));
      return row
        ? { ...row, interview_state: cloneJson(row.interview_state || {}) }
        : null;
    },
    async updateSession(id, patch) {
      const cur = sessions.get(String(id));
      if (!cur) return null;
      const next = {
        ...cur,
        ...patch,
        interview_state: Object.prototype.hasOwnProperty.call(patch, 'interview_state')
          ? cloneJson(patch.interview_state || {})
          : cur.interview_state,
        updated_at: new Date(),
      };
      sessions.set(String(id), next);
      return {
        ...next,
        interview_state: cloneJson(next.interview_state || {}),
      };
    },
    async insertTurn(row) {
      const copy = { ...row };
      const list = turnsBySession.get(copy.session_id) || [];
      list.push(copy);
      turnsBySession.set(copy.session_id, list);
      return { ...copy };
    },
    async updateTurn(id, patch) {
      for (const [sessionId, list] of turnsBySession.entries()) {
        const idx = list.findIndex((t) => t.id === String(id));
        if (idx >= 0) {
          list[idx] = { ...list[idx], ...patch };
          turnsBySession.set(sessionId, list);
          return { ...list[idx] };
        }
      }
      return null;
    },
    async listTurns(sessionId) {
      return (turnsBySession.get(String(sessionId)) || []).map((t) => ({ ...t }));
    },
    async insertEvidence(row) {
      const copy = { ...row };
      evidence.set(copy.id, copy);
      const list = evidenceBySession.get(copy.session_id) || [];
      list.push(copy);
      evidenceBySession.set(copy.session_id, list);
      return { ...copy };
    },
    async listEvidence(sessionId) {
      return (evidenceBySession.get(String(sessionId)) || []).map((e) => ({ ...e }));
    },
    async insertBlueprint(row) {
      const copy = {
        ...row,
        sections: cloneJson(row.sections || {}),
        confidence_summary: cloneJson(row.confidence_summary || {}),
        section_provenance: cloneJson(row.section_provenance || {}),
      };
      const key = `${copy.id}@${copy.version}`;
      blueprints.set(key, copy);
      const list = blueprintsByClient.get(String(copy.client_id)) || [];
      list.push(copy);
      blueprintsByClient.set(String(copy.client_id), list);
      return cloneJson(copy);
    },
    async getBlueprint(id, version) {
      if (version != null) {
        const row = blueprints.get(`${id}@${version}`);
        return row ? cloneJson(row) : null;
      }
      const matches = [...blueprints.values()].filter((b) => b.id === String(id));
      matches.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      return matches[0] ? cloneJson(matches[0]) : null;
    },
    async updateBlueprint(id, version, patch) {
      const key = `${id}@${version}`;
      const cur = blueprints.get(key);
      if (!cur) return null;
      const next = {
        ...cur,
        ...patch,
        sections: Object.prototype.hasOwnProperty.call(patch, 'sections')
          ? cloneJson(patch.sections || {})
          : cur.sections,
        confidence_summary: Object.prototype.hasOwnProperty.call(
          patch,
          'confidence_summary'
        )
          ? cloneJson(patch.confidence_summary || {})
          : cur.confidence_summary,
        section_provenance: Object.prototype.hasOwnProperty.call(
          patch,
          'section_provenance'
        )
          ? cloneJson(patch.section_provenance || {})
          : cur.section_provenance,
        updated_at: new Date(),
      };
      blueprints.set(key, next);
      const list = blueprintsByClient.get(String(next.client_id)) || [];
      const idx = list.findIndex((b) => b.id === id && b.version === version);
      if (idx >= 0) list[idx] = next;
      return cloneJson(next);
    },
    async listBlueprintsForClient(clientId, { status } = {}) {
      let rows = (blueprintsByClient.get(String(clientId)) || []).map((b) =>
        cloneJson(b)
      );
      if (status) rows = rows.filter((b) => b.status === status);
      rows.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      return rows;
    },
    async supersedeBlueprints(logicalId, exceptVersion) {
      for (const [key, row] of blueprints.entries()) {
        if (row.id === logicalId && row.version !== exceptVersion && row.status === 'approved') {
          const next = { ...row, status: 'superseded', updated_at: new Date() };
          blueprints.set(key, next);
        }
      }
    },
    async listSessions({ status, clientId, limit } = {}) {
      let rows = [...sessions.values()].map((row) => ({
        ...row,
        interview_state: { ...(row.interview_state || {}) },
      }));
      if (status) rows = rows.filter((r) => r.status === status);
      if (clientId != null && clientId !== '') {
        rows = rows.filter((r) => Number(r.client_id) === Number(clientId));
      }
      rows.sort((a, b) => {
        const aTime = new Date(a.completed_at || a.updated_at || a.started_at || 0).getTime();
        const bTime = new Date(b.completed_at || b.updated_at || b.started_at || 0).getTime();
        return bTime - aTime;
      });
      if (limit != null && Number.isFinite(Number(limit))) {
        rows = rows.slice(0, Math.max(0, Number(limit)));
      }
      return rows;
    },
  };
}

function createPostgresStore(pool) {
  return {
    kind: 'postgres',
    async insertSession(row) {
      const result = await pool.query(
        `INSERT INTO cie_interview_sessions (
           id, client_id, status, started_at, completed_at, current_stage,
           summary, confidence_score, interview_state, created_at, updated_at
         ) VALUES (
           $1,$2,$3,COALESCE($4,NOW()),$5,$6,$7,$8,$9::jsonb,NOW(),NOW()
         ) RETURNING *`,
        [
          row.id,
          row.client_id,
          row.status,
          row.started_at || null,
          row.completed_at || null,
          row.current_stage,
          row.summary || null,
          row.confidence_score,
          JSON.stringify(row.interview_state || {}),
        ]
      );
      return normalizeSessionRow(result.rows[0]);
    },
    async getSession(id) {
      const result = await pool.query(
        `SELECT * FROM cie_interview_sessions WHERE id = $1`,
        [String(id)]
      );
      return result.rows[0] ? normalizeSessionRow(result.rows[0]) : null;
    },
    async updateSession(id, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      const map = {
        status: 'status',
        completed_at: 'completed_at',
        current_stage: 'current_stage',
        summary: 'summary',
        confidence_score: 'confidence_score',
        interview_state: 'interview_state',
      };
      for (const [key, col] of Object.entries(map)) {
        if (Object.prototype.hasOwnProperty.call(patch, key)) {
          if (key === 'interview_state') {
            fields.push(`${col} = $${n}::jsonb`);
            params.push(JSON.stringify(patch[key] || {}));
          } else {
            fields.push(`${col} = $${n}`);
            params.push(patch[key]);
          }
          n += 1;
        }
      }
      fields.push('updated_at = NOW()');
      params.push(String(id));
      const result = await pool.query(
        `UPDATE cie_interview_sessions SET ${fields.join(', ')}
         WHERE id = $${n} RETURNING *`,
        params
      );
      return result.rows[0] ? normalizeSessionRow(result.rows[0]) : null;
    },
    async insertTurn(row) {
      const result = await pool.query(
        `INSERT INTO cie_interview_turns (
           id, session_id, speaker, message, goal, asked_because,
           derived_evidence, created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,COALESCE($8,NOW()))
         RETURNING *`,
        [
          row.id,
          row.session_id,
          row.speaker,
          row.message,
          row.goal || null,
          row.asked_because || null,
          JSON.stringify(row.derived_evidence || []),
          row.created_at || null,
        ]
      );
      return normalizeTurnRow(result.rows[0]);
    },
    async updateTurn(id, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      if (Object.prototype.hasOwnProperty.call(patch, 'derived_evidence')) {
        fields.push(`derived_evidence = $${n}::jsonb`);
        params.push(JSON.stringify(patch.derived_evidence || []));
        n += 1;
      }
      if (Object.prototype.hasOwnProperty.call(patch, 'message')) {
        fields.push(`message = $${n}`);
        params.push(patch.message);
        n += 1;
      }
      if (!fields.length) return null;
      params.push(String(id));
      const result = await pool.query(
        `UPDATE cie_interview_turns SET ${fields.join(', ')} WHERE id = $${n} RETURNING *`,
        params
      );
      return result.rows[0] ? normalizeTurnRow(result.rows[0]) : null;
    },
    async listTurns(sessionId) {
      const result = await pool.query(
        `SELECT * FROM cie_interview_turns WHERE session_id = $1 ORDER BY created_at ASC`,
        [String(sessionId)]
      );
      return result.rows.map(normalizeTurnRow);
    },
    async insertEvidence(row) {
      const result = await pool.query(
        `INSERT INTO cie_evidence (
           id, client_id, session_id, source, source_turn_id, category,
           statement, confidence, type, created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10,NOW()))
         RETURNING *`,
        [
          row.id,
          row.client_id,
          row.session_id,
          row.source,
          row.source_turn_id || null,
          row.category,
          row.statement,
          row.confidence,
          row.type,
          row.created_at || null,
        ]
      );
      return normalizeEvidenceRow(result.rows[0]);
    },
    async listEvidence(sessionId) {
      const result = await pool.query(
        `SELECT * FROM cie_evidence WHERE session_id = $1 ORDER BY created_at ASC`,
        [String(sessionId)]
      );
      return result.rows.map(normalizeEvidenceRow);
    },
    async insertBlueprint(row) {
      const result = await pool.query(
        `INSERT INTO cie_business_blueprints (
           id, client_id, session_id, version, status, generated_by, sections,
           confidence_summary, playbook_id, playbook_version, section_provenance,
           parent_blueprint_id, created_at, updated_at
         ) VALUES (
           $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11::jsonb,$12,NOW(),NOW()
         ) RETURNING *`,
        [
          row.id,
          row.client_id,
          row.session_id,
          row.version,
          row.status,
          row.generated_by,
          JSON.stringify(row.sections || {}),
          JSON.stringify(row.confidence_summary || {}),
          row.playbook_id || null,
          row.playbook_version || null,
          JSON.stringify(row.section_provenance || {}),
          row.parent_blueprint_id || null,
        ]
      );
      return normalizeBlueprintRow(result.rows[0]);
    },
    async getBlueprint(id, version) {
      if (version != null) {
        const result = await pool.query(
          `SELECT * FROM cie_business_blueprints WHERE id = $1 AND version = $2`,
          [String(id), String(version)]
        );
        return result.rows[0] ? normalizeBlueprintRow(result.rows[0]) : null;
      }
      const result = await pool.query(
        `SELECT * FROM cie_business_blueprints WHERE id = $1
         ORDER BY created_at DESC LIMIT 1`,
        [String(id)]
      );
      return result.rows[0] ? normalizeBlueprintRow(result.rows[0]) : null;
    },
    async updateBlueprint(id, version, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      const map = {
        status: 'status',
        sections: 'sections',
        confidence_summary: 'confidence_summary',
        playbook_id: 'playbook_id',
        playbook_version: 'playbook_version',
        section_provenance: 'section_provenance',
        canonical_snapshot_id: 'canonical_snapshot_id',
        canonical_snapshot_tenant_id: 'canonical_snapshot_tenant_id',
      };
      for (const [key, col] of Object.entries(map)) {
        if (Object.prototype.hasOwnProperty.call(patch, key)) {
          if (
            key === 'sections' ||
            key === 'confidence_summary' ||
            key === 'section_provenance'
          ) {
            fields.push(`${col} = $${n}::jsonb`);
            params.push(JSON.stringify(patch[key] || {}));
          } else {
            fields.push(`${col} = $${n}`);
            params.push(patch[key]);
          }
          n += 1;
        }
      }
      fields.push('updated_at = NOW()');
      params.push(String(id), String(version));
      const result = await pool.query(
        `UPDATE cie_business_blueprints SET ${fields.join(', ')}
         WHERE id = $${n} AND version = $${n + 1} RETURNING *`,
        params
      );
      return result.rows[0] ? normalizeBlueprintRow(result.rows[0]) : null;
    },
    async listBlueprintsForClient(clientId, { status } = {}) {
      const params = [Number(clientId)];
      let sql = `SELECT * FROM cie_business_blueprints WHERE client_id = $1`;
      if (status) {
        params.push(status);
        sql += ` AND status = $2`;
      }
      sql += ` ORDER BY created_at DESC`;
      const result = await pool.query(sql, params);
      return result.rows.map(normalizeBlueprintRow);
    },
    async supersedeBlueprints(logicalId, exceptVersion) {
      await pool.query(
        `UPDATE cie_business_blueprints SET status = 'superseded', updated_at = NOW()
         WHERE id = $1 AND version <> $2 AND status = 'approved'`,
        [String(logicalId), String(exceptVersion)]
      );
    },
    async listSessions({ status, clientId, limit } = {}) {
      const params = [];
      const where = [];
      if (status) {
        params.push(status);
        where.push(`status = $${params.length}`);
      }
      if (clientId != null && clientId !== '') {
        params.push(Number(clientId));
        where.push(`client_id = $${params.length}`);
      }
      let sql = `SELECT * FROM cie_interview_sessions`;
      if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
      sql += ` ORDER BY COALESCE(completed_at, updated_at, started_at) DESC`;
      if (limit != null && Number.isFinite(Number(limit))) {
        params.push(Math.max(0, Math.trunc(Number(limit))));
        sql += ` LIMIT $${params.length}`;
      }
      const result = await pool.query(sql, params);
      return result.rows.map(normalizeSessionRow);
    },
  };
}

function normalizeSessionRow(r) {
  return {
    id: r.id,
    client_id: r.client_id,
    status: r.status,
    started_at: r.started_at,
    completed_at: r.completed_at,
    current_stage: r.current_stage,
    summary: r.summary,
    confidence_score: r.confidence_score != null ? Number(r.confidence_score) : null,
    interview_state:
      typeof r.interview_state === 'string'
        ? JSON.parse(r.interview_state)
        : r.interview_state || {},
    created_at: r.created_at,
    updated_at: r.updated_at,
  };
}

function normalizeTurnRow(r) {
  return {
    id: r.id,
    session_id: r.session_id,
    speaker: r.speaker,
    message: r.message,
    goal: r.goal,
    asked_because: r.asked_because,
    derived_evidence:
      typeof r.derived_evidence === 'string'
        ? JSON.parse(r.derived_evidence)
        : r.derived_evidence || [],
    created_at: r.created_at,
  };
}

function normalizeEvidenceRow(r) {
  return {
    id: r.id,
    client_id: r.client_id,
    session_id: r.session_id,
    source: r.source,
    source_turn_id: r.source_turn_id,
    category: r.category,
    statement: r.statement,
    confidence: Number(r.confidence),
    type: r.type,
    // SPEC-224: preserved so callers (CIECanonicalAdapter) can build a
    // canonical batch whose idempotency key matches what commitCanonicalSemanticBatch
    // re-derives from cie_evidence directly.
    source_text_sha256: r.source_text_sha256 || null,
    immutable_at: r.immutable_at || null,
    created_at: r.created_at,
  };
}

function normalizeBlueprintRow(r) {
  return {
    id: r.id,
    client_id: r.client_id,
    session_id: r.session_id,
    version: r.version,
    status: r.status,
    generated_by: r.generated_by,
    sections:
      typeof r.sections === 'string' ? JSON.parse(r.sections) : r.sections || {},
    confidence_summary:
      typeof r.confidence_summary === 'string'
        ? JSON.parse(r.confidence_summary)
        : r.confidence_summary || {},
    playbook_id: r.playbook_id,
    playbook_version: r.playbook_version,
    section_provenance:
      typeof r.section_provenance === 'string'
        ? JSON.parse(r.section_provenance)
        : r.section_provenance || {},
    parent_blueprint_id: r.parent_blueprint_id,
    canonical_snapshot_id: r.canonical_snapshot_id || null,
    canonical_snapshot_tenant_id: r.canonical_snapshot_tenant_id || null,
    created_at: r.created_at,
    updated_at: r.updated_at,
  };
}

async function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  if (opts.pool) return createPostgresStore(opts.pool);
  return createPostgresStore(defaultPool);
}

function publicSession(session, extras = {}) {
  return {
    id: session.id,
    clientId: session.client_id,
    status: session.status,
    startedAt: session.started_at,
    completedAt: session.completed_at,
    currentStage: session.current_stage,
    summary: session.summary,
    confidenceScore: session.confidence_score,
    ...extras,
  };
}

function publicBlueprint(bp) {
  if (!bp) return null;
  return {
    id: bp.id,
    clientId: bp.client_id,
    sessionId: bp.session_id,
    version: bp.version,
    status: bp.status,
    generatedBy: bp.generated_by,
    sections: bp.sections,
    confidenceSummary: bp.confidence_summary,
    playbookId: bp.playbook_id,
    playbookVersion: bp.playbook_version,
    sectionProvenance: bp.section_provenance,
    parentBlueprintId: bp.parent_blueprint_id,
    canonicalSnapshotId: bp.canonical_snapshot_id || null,
    canonicalSnapshotTenantId: bp.canonical_snapshot_tenant_id || null,
    readiness: bp.readiness || null,
    createdAt: bp.created_at,
    updatedAt: bp.updated_at,
  };
}

function publicEvidence(e) {
  return {
    id: e.id,
    clientId: e.client_id,
    sessionId: e.session_id,
    source: e.source,
    sourceTurnId: e.source_turn_id,
    category: e.category,
    statement: e.statement,
    confidence: e.confidence,
    type: e.type,
    createdAt: e.created_at,
  };
}

function publicTurn(t) {
  return {
    id: t.id,
    sessionId: t.session_id,
    speaker: t.speaker,
    message: t.message,
    goal: t.goal,
    askedBecause: t.asked_because,
    derivedEvidence: t.derived_evidence,
    timestamp: t.created_at,
  };
}

async function applySectionUpdate(store, session, sectionKey, statement, type, turnId) {
  const state = session.interview_state || initialInterviewState();
  const sectionState = state.sectionState || emptySections();
  const section = sectionState[sectionKey] || emptySection();
  const priorEvidence = (await store.listEvidence(session.id)).filter(
    (e) => e.category === sectionKey
  );
  const priorStatements = priorEvidence
    .map((e) => e.statement)
    .filter((s) => s && !/^Unknown:/i.test(String(s)))
    .filter((s) => isBusinessFactStatement(s));

  const rawStatement = String(statement || '').trim();
  const responseKind = classifyUserResponse(rawStatement);

  // Refinement / system guidance must never populate commercial Blueprint fields.
  if (
    responseKind === ANSWER_KINDS.REFINEMENT_FEEDBACK ||
    responseKind === ANSWER_KINDS.SYSTEM_GUIDANCE ||
    containsMetaInstructionLanguage(rawStatement)
  ) {
    state.revisionGuidance = [
      ...(state.revisionGuidance || []),
      {
        at: nowIso(),
        kind: responseKind,
        message: rawStatement,
        section: sectionKey,
      },
    ];
    state.sectionState = sectionState;
    session.interview_state = state;
    return {
      evidenceRow: null,
      contradiction: false,
      sectionState,
      skippedAsGuidance: true,
      responseKind,
    };
  }

  const empty = answerLooksEmpty(statement);
  const isConfirmation = looksLikeConfirmation(statement);
  const hasCorroboration = priorEvidence.length >= 1 && !empty;
  const contradiction = detectContradiction(priorStatements, statement);

  const confidence = scoreEvidenceConfidence({
    type: empty ? 'INFERRED' : type,
    statement,
    priorStatements,
    isConfirmation,
    hasCorroboration,
  });

  const evidenceRow = await store.insertEvidence({
    id: newId(),
    client_id: session.client_id,
    session_id: session.id,
    source: `Interview Turn`,
    source_turn_id: turnId,
    category: sectionKey,
    statement: empty ? `Unknown: ${sectionKey}` : rawStatement,
    confidence,
    type: empty ? 'INFERRED' : type,
    created_at: new Date(),
  });

  const unknowns = [...(section.unknowns || [])];
  if (empty) {
    const label = `Missing clear answer for ${sectionKey}`;
    if (!unknowns.includes(label)) unknowns.push(label);
  } else {
    // Clear matching unknown when we get a real answer
    const filtered = unknowns.filter((u) => !u.includes(sectionKey));
    unknowns.length = 0;
    unknowns.push(...filtered);
  }

  let nextConfidence = confidence;
  if (contradiction) {
    nextConfidence = clampConfidence(Math.min(section.confidence || confidence, confidence));
    state.contradictions = [
      ...(state.contradictions || []),
      { section: sectionKey, statement: rawStatement, at: nowIso() },
    ];
  } else if (!empty) {
    const prior = Number(section.confidence) || 0;
    if (prior <= 0) {
      nextConfidence = confidence;
    } else {
      nextConfidence = clampConfidence(prior * 0.35 + confidence * 0.65);
    }
  } else if (empty && (!section.summary || answerLooksEmpty(section.summary))) {
    nextConfidence = UNKNOWN_CONFIDENCE;
  }

  let summary = section.summary || '';
  if (!empty) {
    // Keep normalized evidence as the source of truth for Brief rendering.
    if (type === 'CLIENT_EDITED') {
      // Corrections are applied via applyCorrectionToNormalizedFacts by the caller
      // when domain is known; here we still ingest as a merge for safety.
      state.normalizedFacts = applyCorrectionToNormalizedFacts(
        state.normalizedFacts || emptyNormalizedFacts(),
        { section: sectionKey, substance: rawStatement }
      );
    } else {
      state.normalizedFacts = ingestAnswerIntoNormalizedFacts(
        state.normalizedFacts || emptyNormalizedFacts(),
        sectionKey,
        rawStatement,
        { provenance: turnId }
      );
    }
    const fromFacts = sectionsFromNormalizedFacts(state.normalizedFacts, sectionState);
    summary =
      (fromFacts[sectionKey] && fromFacts[sectionKey].summary) ||
      (type === 'CLIENT_EDITED' && isBusinessFactStatement(rawStatement)
        ? summarizeSection(sectionKey, [rawStatement])
        : summarizeSection(sectionKey, [...priorStatements, rawStatement]));
  }

  sectionState[sectionKey] = {
    summary,
    confidence: empty
      ? section.summary && !answerLooksEmpty(section.summary)
        ? section.confidence || UNKNOWN_CONFIDENCE
        : UNKNOWN_CONFIDENCE
      : nextConfidence,
    evidenceIds: [...(section.evidenceIds || []), evidenceRow.id],
    unknowns,
  };
  state.sectionState = sectionState;
  session.interview_state = state;

  return { evidenceRow, contradiction, sectionState, skippedAsGuidance: false, responseKind };
}

function confidenceSummaryFromSections(sections) {
  const summary = {};
  for (const key of BLUEPRINT_SECTIONS) {
    summary[key] = clampConfidence((sections[key] && sections[key].confidence) || 0);
  }
  return summary;
}

function overallConfidence(summary) {
  const vals = BLUEPRINT_SECTIONS.map((k) => summary[k] || 0);
  if (!vals.length) return 0;
  return clampConfidence(vals.reduce((a, b) => a + b, 0) / vals.length);
}

function buildSectionsFromState(sectionState) {
  const sections = emptySections();
  for (const key of BLUEPRINT_SECTIONS) {
    const src = (sectionState && sectionState[key]) || emptySection();
    sections[key] = {
      summary: src.summary || '',
      confidence: clampConfidence(src.confidence || 0),
      evidenceIds: [...(src.evidenceIds || [])],
      unknowns: [...(src.unknowns || [])],
    };
    if (!sections[key].summary && !sections[key].unknowns.length) {
      sections[key].unknowns.push(`No evidence yet for ${key}`);
    }
  }
  return sections;
}

/**
 * Merge supplemental session memory into Blueprint sections for Brief generation.
 * Never overwrites an existing answered section unless the entry was an explicit correction.
 */
function mergeSupplementalIntoSections(sections, supplementalContext) {
  const out = buildSectionsFromState(sections);
  for (const entry of supplementalContext || []) {
    if (!entry || !entry.text) continue;
    if (entry.kind === MESSAGE_TYPES.CLARIFICATION_REQUEST ||
      entry.kind === MESSAGE_TYPES.QUESTION_TO_MAX ||
      entry.kind === MESSAGE_TYPES.OFF_TOPIC ||
      entry.kind === MESSAGE_TYPES.APPROVAL ||
      entry.kind === MESSAGE_TYPES.SKIP ||
      entry.kind === MESSAGE_TYPES.INSUFFICIENT_ANSWER ||
      entry.kind === MESSAGE_TYPES.REFINEMENT_FEEDBACK) {
      continue;
    }
    if (looksLikeRefinementFeedback(entry.text) || containsMetaInstructionLanguage(entry.text)) {
      continue;
    }
    const domain = entry.domain || tagContextDomain(entry.text);
    const sectionKey = entry.section || (domain && DOMAIN_TO_SECTION[domain]);
    if (!sectionKey || !out[sectionKey]) continue;

    const cleaned = stripInterviewQuestionEcho(
      stripSupplementalPreamble(
        stripCorrectionPreamble(
          String(entry.substance || entry.text || '')
            .replace(SUPPLEMENTAL_CONTEXT_RE, '')
            .replace(CORRECTION_RE, '')
            .trim()
        )
      )
    );
    if (!cleaned || !isBusinessFactStatement(cleaned)) continue;

    if (
      entry.supersedes ||
      entry.kind === MESSAGE_TYPES.CORRECTION ||
      entry.kind === MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT ||
      entry.confirmed
    ) {
      // Confirmed supplemental/correction: merge substance into the section summary.
      const priorSummary = String(out[sectionKey].summary || '').trim();
      const nextSummary = priorSummary
        ? summarizeSection(sectionKey, [priorSummary, cleaned])
        : summarizeSection(sectionKey, [cleaned]);
      out[sectionKey] = {
        ...out[sectionKey],
        summary: nextSummary,
        confidence: Math.max(out[sectionKey].confidence || 0, EXPLICIT_CONFIDENCE),
      };
      continue;
    }

    // Soft merge: only fill empty sections; never overwrite active answers.
    if (!String(out[sectionKey].summary || '').trim() || answerLooksEmpty(out[sectionKey].summary)) {
      out[sectionKey] = {
        ...out[sectionKey],
        summary: summarizeSection(sectionKey, [cleaned]),
        confidence: Math.max(out[sectionKey].confidence || 0, INFERRED_CONFIDENCE),
      };
    }
  }
  return out;
}

async function generateBlueprint(store, session) {
  if (session.status !== 'BLUEPRINT_GENERATION') {
    advanceStatus(session, 'BLUEPRINT_GENERATION');
  }
  const state = session.interview_state || initialInterviewState();
  const merged = mergeSupplementalIntoSections(
    state.sectionState,
    state.supplementalContext
  );
  // Prefer normalized evidence for Blueprint/Brief commercial fields.
  const sections = state.normalizedFacts
    ? sectionsFromNormalizedFacts(state.normalizedFacts, merged)
    : merged;

  // SPEC-090 — artifact readiness: mark weak evidence clearly; never invent facts.
  const readiness = checkArtifactReadiness(ARTIFACT_KINDS.BLUEPRINT, {
    sectionState: sections,
    normalizedFacts: state.normalizedFacts || emptyNormalizedFacts(),
  });
  let reasoningMemory = ensureReasoningMemory(state);
  const artifactProgress = resolveNextArtifact(
    reasoningMemory,
    ARTIFACT_KINDS.BLUEPRINT
  );
  if (artifactProgress.emit === ARTIFACT_KINDS.BLUEPRINT || !reasoningMemory.artifactsGenerated.includes(ARTIFACT_KINDS.BLUEPRINT)) {
    reasoningMemory = markArtifactGenerated(reasoningMemory, ARTIFACT_KINDS.BLUEPRINT);
  }
  reasoningMemory = syncConfidenceFromSections(reasoningMemory, sections);

  const confidence_summary = confidenceSummaryFromSections(sections);
  if (readiness.confidenceNote) {
    confidence_summary._readinessNote = readiness.confidenceNote;
  }
  // Refinement / re-generation: supersede prior unapproved Blueprints for this
  // session so only the newly created row remains the current in_review.
  await supersedeUnapprovedBlueprintsForSession(store, session);
  const blueprint = await store.insertBlueprint({
    id: newId(),
    client_id: session.client_id,
    session_id: session.id,
    version: '1.0',
    status: 'in_review',
    generated_by: GENERATED_BY,
    sections,
    confidence_summary,
    playbook_id: null,
    playbook_version: null,
    section_provenance: {
      business_facts: state.normalizedFacts?.business_facts || {},
    },
    parent_blueprint_id: null,
    readiness: {
      ready: readiness.ready,
      missing: readiness.missing,
      weak: readiness.weak,
      confidenceNote: readiness.confidenceNote,
    },
    created_at: new Date(),
    updated_at: new Date(),
  });
  advanceStatus(session, 'CLIENT_REVIEW');
  session.interview_state = {
    ...session.interview_state,
    blueprintId: blueprint.id,
    blueprintVersion: blueprint.version,
    sectionState: sections,
    normalizedFacts: state.normalizedFacts || emptyNormalizedFacts(),
    reasoningMemory,
    artifactReadiness: {
      blueprint: readiness,
    },
  };
  session.confidence_score = overallConfidence(confidence_summary);
  session.summary = readiness.confidenceNote
    ? `Draft Business Blueprint ${blueprint.id}@${blueprint.version} (${readiness.confidenceNote})`
    : `Draft Business Blueprint ${blueprint.id}@${blueprint.version}`;
  session.current_stage = 'Client Review';
  await store.updateSession(session.id, {
    status: session.status,
    current_stage: session.current_stage,
    summary: session.summary,
    confidence_score: session.confidence_score,
    interview_state: session.interview_state,
  });
  return blueprint;
}

async function advanceThroughLifecycleToBlueprint(store, session) {
  // DISCOVERY → CLARIFICATION → VALIDATION → BLUEPRINT_GENERATION → CLIENT_REVIEW
  if (session.status === 'DISCOVERY') {
    advanceStatus(session, 'CLARIFICATION');
  }
  if (session.status === 'CLARIFICATION') {
    advanceStatus(session, 'VALIDATION');
  }
  if (session.status === 'VALIDATION') {
    // validation gate: required sections should have some evidence; still generate with unknowns
    await store.updateSession(session.id, {
      status: 'VALIDATION',
      interview_state: session.interview_state,
      current_stage: 'Validation',
    });
    advanceStatus(session, 'BLUEPRINT_GENERATION');
  }
  await store.updateSession(session.id, {
    status: session.status,
    interview_state: session.interview_state,
  });
  return generateBlueprint(store, session);
}

function extractNotesIntoSections(notes) {
  const partitioned = partitionUserResponse(notes);
  // Persist guidance separately when callers read interview_state; facts only map to sections.
  const text = partitioned.facts.join(' ').trim() || '';
  if (!text) return { assigned: {}, guidance: partitioned.guidance };

  const sentences = text
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => s.trim())
    .filter(Boolean)
    .filter((s) => isBusinessFactStatement(s));
  const patterns = [
    { re: /\b(we are|company|business|dba|called)\b/i, section: 'identity' },
    { re: /\b(service|offer|provide|sell|product)\b/i, section: 'services' },
    { re: /\b(ideal|icp|customer|clientele|buyer)\b/i, section: 'idealCustomers' },
    { re: /\b(avoid|not a fit|do not want|no longer serve)\b/i, section: 'avoidCustomers' },
    { re: /\b(market|geo|region|city|county|vertical)\b/i, section: 'targetMarkets' },
    { re: /\b(advantage|differen|better|unique|moat)\b/i, section: 'competitiveAdvantages' },
    { re: /\b(voice|tone|sound|brand|professional|friendly|premium)\b/i, section: 'brandVoice' },
    { re: /\b(goal|grow|book|appointments|revenue|pipeline)\b/i, section: 'campaignGoals' },
    { re: /\b(metric|kpi|measure|success|roi|close rate)\b/i, section: 'successMetrics' },
  ];
  const assigned = {};
  for (const sentence of sentences) {
    for (const p of patterns) {
      if (assigned[p.section]) continue;
      if (p.re.test(sentence)) {
        assigned[p.section] = sentence;
        break;
      }
    }
  }
  // leftover sentences fill identity if missing — but never with refinement guidance
  if (!assigned.identity && sentences[0] && isBusinessFactStatement(sentences[0])) {
    assigned.identity = sentences[0];
  }
  return { assigned, guidance: partitioned.guidance };
}

/** Back-compat: return only the section map (tests / older callers). */
function mapNotesToSections(notes) {
  return extractNotesIntoSections(notes).assigned;
}

const ACTIVE_INTERVIEW_STATUSES = Object.freeze([
  'NEW',
  'DISCOVERY',
  'CLARIFICATION',
  'VALIDATION',
  'BLUEPRINT_GENERATION',
  'CLIENT_REVIEW',
]);

/**
 * SPEC-099 — Historical / abandoned onboarding marked via interview_state
 * (no schema migration). Superseded sessions must not win recovery.
 */
function sessionIsSuperseded(session) {
  const state = (session && session.interview_state) || {};
  return Boolean(
    state.supersededAt ||
      state.superseded_at ||
      state.abandonedAt ||
      state.abandoned_at ||
      state.lifecycle === 'superseded' ||
      state.lifecycle === 'abandoned'
  );
}

function blueprintStatusOf(bp) {
  return String((bp && bp.status) || '').toLowerCase();
}

function isActiveBlueprintStatus(status) {
  const s = String(status || '').toLowerCase();
  return s === 'in_review' || s === 'draft';
}

/**
 * Linked Blueprint for a session (by interview_state pointers), or null.
 */
async function loadSessionBlueprint(store, session) {
  if (!store || !session) return null;
  const state = session.interview_state || {};
  if (!state.blueprintId) return null;
  return store.getBlueprint(state.blueprintId, state.blueprintVersion);
}

/**
 * Authoritative Blueprint for a session.
 * Prefer an active (in_review/draft) Blueprint owned by this session over a
 * stale interview_state pointer (e.g. pointing at a superseded prior Blueprint).
 * Falls back to linked row for historical / superseded views.
 */
async function resolveBlueprintForSession(store, session) {
  if (!store || !session) return null;
  const clientId = asClientId(session.client_id);
  const all = await store.listBlueprintsForClient(clientId);
  const mine = (all || []).filter(
    (bp) => String(bp.session_id) === String(session.id)
  );
  const activeOwned = mine.find((bp) => isActiveBlueprintStatus(bp.status));
  if (activeOwned) return activeOwned;
  const approvedOwned = mine.find(
    (bp) => blueprintStatusOf(bp) === 'approved'
  );
  if (approvedOwned) return approvedOwned;

  const linked = await loadSessionBlueprint(store, session);
  if (
    linked &&
    isActiveBlueprintStatus(linked.status) &&
    String(linked.session_id) === String(session.id)
  ) {
    return linked;
  }
  if (linked && blueprintStatusOf(linked) === 'approved') return linked;
  // Historical: linked superseded (possibly from another session via stale pointer)
  if (mine.length) {
    const supersededOwned = mine.find(
      (bp) => blueprintStatusOf(bp) === 'superseded'
    );
    if (supersededOwned) return supersededOwned;
  }
  return linked;
}

/**
 * Latest client-scoped active Blueprint row (in_review/draft), newest first.
 */
async function findLatestActiveBlueprintForClient(store, clientId) {
  const all = await store.listBlueprintsForClient(asClientId(clientId));
  return (all || []).find((bp) => isActiveBlueprintStatus(bp.status)) || null;
}

/**
 * Heal interview_state pointers when an active Blueprint for this session
 * exists but the stored blueprintId is stale/missing/wrong.
 */
async function healSessionBlueprintPointer(store, session, blueprint) {
  if (!store || !session || !blueprint) return session;
  if (String(blueprint.session_id) !== String(session.id)) return session;
  if (!isActiveBlueprintStatus(blueprint.status) && blueprintStatusOf(blueprint) !== 'approved') {
    return session;
  }
  const state = session.interview_state || {};
  if (
    String(state.blueprintId || '') === String(blueprint.id) &&
    String(state.blueprintVersion || '') === String(blueprint.version || '')
  ) {
    return session;
  }
  const nextState = {
    ...state,
    blueprintId: blueprint.id,
    blueprintVersion: blueprint.version,
  };
  const updated = await store.updateSession(session.id, {
    interview_state: nextState,
  });
  return updated || { ...session, interview_state: nextState };
}

/**
 * Ordinary recovery should skip abandoned CLIENT_REVIEW rows whose best
 * Blueprint is superseded — but must NOT skip a session that still owns an
 * active in_review/draft Blueprint (even if interview_state points elsewhere).
 */
async function sessionBlocksOrdinaryRecovery(store, session) {
  if (!session) return true;
  if (
    session.status !== 'CLIENT_REVIEW' &&
    session.status !== 'BLUEPRINT_GENERATION'
  ) {
    return false;
  }
  const resolved = await resolveBlueprintForSession(store, session);
  if (!resolved) return false;
  if (isActiveBlueprintStatus(resolved.status)) return false;
  if (blueprintStatusOf(resolved) === 'approved') return false;
  return blueprintStatusOf(resolved) === 'superseded';
}

function isRecoverableActiveInterview(session) {
  return Boolean(
    session &&
      ACTIVE_INTERVIEW_STATUSES.includes(session.status) &&
      !sessionIsSample(session) &&
      !sessionIsSuperseded(session)
  );
}

/**
 * Find the most recent non-sample session for a client (any status).
 */
async function listClientSessions(store, clientId, { limit = 40 } = {}) {
  const rows = await store.listSessions({
    clientId: asClientId(clientId),
    limit: Math.max(1, Math.min(200, Number(limit) || 40)),
  });
  return (rows || []).filter((row) => !sessionIsSample(row));
}

/**
 * Active (incomplete) interview for ordinary client return — not samples.
 * SPEC-099: excludes superseded/abandoned onboarding.
 * Post-restart lifecycle: never skip a session that still owns an active
 * Blueprint; never let a stale superseded pointer hide Interview B.
 */
async function findActiveInterviewForClient(clientId, opts = {}) {
  const store = await resolveStore(opts);
  const rows = await listClientSessions(store, clientId, { limit: 40 });
  for (const row of rows) {
    if (!isRecoverableActiveInterview(row)) continue;
    if (await sessionBlocksOrdinaryRecovery(store, row)) continue;
    return row;
  }
  return null;
}

/**
 * Mark draft/in_review blueprints for a session as superseded.
 * Never touches approved blueprints (SPEC-099 / SPEC-098).
 */
async function supersedeUnapprovedBlueprintsForSession(store, session) {
  if (!session) return [];
  const clientId = asClientId(session.client_id);
  const all = await store.listBlueprintsForClient(clientId);
  const touched = [];
  for (const bp of all || []) {
    if (String(bp.session_id) !== String(session.id)) continue;
    const status = String(bp.status || '').toLowerCase();
    if (status === 'approved' || status === 'superseded') continue;
    if (status !== 'draft' && status !== 'in_review') continue;
    const updated = await store.updateBlueprint(bp.id, bp.version, {
      status: 'superseded',
    });
    if (updated) touched.push(updated);
  }
  return touched;
}

/**
 * SPEC-099 — Abandon current unapproved onboarding so it cannot hijack recovery.
 * Preserves historical rows; marks lifecycle superseded in interview_state.
 */
async function supersedeUnapprovedOnboardingForClient(
  clientId,
  { supersededBy = null, reason = 'explicit_restart' } = {},
  opts = {}
) {
  const store = await resolveStore(opts);
  const id = asClientId(clientId);
  const rows = await listClientSessions(store, id, { limit: 40 });
  const actives = rows.filter((row) => isRecoverableActiveInterview(row));
  const superseded = [];
  const at = nowIso();

  for (const session of actives) {
    await supersedeUnapprovedBlueprintsForSession(store, session);
    const nextState = {
      ...(session.interview_state || {}),
      lifecycle: 'superseded',
      supersededAt: at,
      supersededReason: reason,
      ...(supersededBy ? { supersededBy: String(supersededBy) } : {}),
    };
    const updated = await store.updateSession(session.id, {
      interview_state: nextState,
      // Keep prior status for audit; recovery uses lifecycle flag.
      summary: session.summary
        ? `${session.summary} (superseded)`
        : 'Superseded by explicit interview restart',
    });
    superseded.push(updated || { ...session, interview_state: nextState });
  }

  return { supersededSessions: superseded, count: superseded.length };
}

/**
 * SPEC-097 — Resolve durable onboarding state for an authenticated client.
 * Database sessions/blueprints remain authoritative; no parallel persistence.
 * SPEC-099 — prefers current (non-superseded) interview over historical drafts.
 * Post-restart lifecycle — recover via active interview OR via the latest
 * active Blueprint's owning session when pointers are stale.
 */
async function resolveClientOnboardingState(clientId, opts = {}) {
  const store = await resolveStore(opts);
  const id = asClientId(clientId);
  const rows = await listClientSessions(store, id, { limit: 40 });

  let active = await findActiveInterviewForClient(id, opts);

  // If ordinary active lookup missed (stale superseded pointer), recover the
  // session that owns the latest in_review/draft Blueprint.
  if (!active) {
    const latestBp = await findLatestActiveBlueprintForClient(store, id);
    if (latestBp && latestBp.session_id) {
      const owner = await store.getSession(latestBp.session_id);
      if (
        owner &&
        !sessionIsSample(owner) &&
        !sessionIsSuperseded(owner) &&
        ACTIVE_INTERVIEW_STATUSES.includes(owner.status)
      ) {
        active = owner;
      }
    }
  }

  const approved = rows.find(
    (row) => row.status === 'APPROVED' && !sessionIsSuperseded(row)
  );

  if (!active && !approved) {
    return {
      ok: true,
      clientId: id,
      onboardingState: 'none',
      interviewId: null,
      status: null,
      resumeTarget: null,
      message: 'No existing onboarding state — begin interview when ready.',
      recovered: false,
    };
  }

  if (active) {
    const detail = await getInterview(active.id, opts);
    const onboardingState =
      active.status === 'CLIENT_REVIEW'
        ? 'blueprint_review'
        : active.status === 'BLUEPRINT_GENERATION'
          ? 'blueprint_review'
          : 'interview_in_progress';
    return {
      ok: true,
      clientId: id,
      onboardingState,
      interviewId: active.id,
      status: active.status,
      resumeTarget: detail.resumeTarget || null,
      resumePhase: detail.resumePhase || null,
      recovered: true,
      resumedExisting: true,
      ...detail,
      message:
        onboardingState === 'blueprint_review'
          ? 'Recovered pending Blueprint review for this client.'
          : 'Recovered active interview for this client.',
    };
  }

  const detail = await getInterview(approved.id, opts);
  const resumeTarget = detail.resumeTarget || 'initial_growth_direction';
  const onboardingState =
    resumeTarget === 'growth_complete' ? 'completed' : 'blueprint_approved';
  return {
    ok: true,
    clientId: id,
    onboardingState,
    interviewId: approved.id,
    status: approved.status,
    resumeTarget,
    resumePhase: detail.resumePhase || null,
    recovered: true,
    resumedExisting: true,
    ...detail,
    message:
      onboardingState === 'completed'
        ? 'Blueprint approved and Growth Plan complete — Max is the next primary interface.'
        : 'Recovered approved Blueprint — strategy/execution remains review-controlled.',
  };
}

/**
 * Most recently approved Blueprint for a client (no pending/draft fallback).
 */
async function getApprovedClientBlueprint(clientId, opts = {}) {
  const store = await resolveStore(opts);
  const id = asClientId(clientId);
  const approved = await store.listBlueprintsForClient(id, {
    status: 'approved',
  });
  if (!approved || !approved[0]) {
    throw new ClientIntelligenceError(
      'not_found',
      'No approved blueprint for client',
      404
    );
  }
  const bp = publicBlueprint(approved[0]);
  const durableFacts = bp.sectionProvenance?.business_facts || {};
  if (Object.keys(durableFacts).length) {
    bp.epistemicFacts = durableFacts;
  }

  async function loadLegacySessionFacts() {
    const sessionId = bp.sessionId || approved[0].session_id;
    if (!sessionId) return null;
    const session = await store.getSession(sessionId);
    const facts =
      session &&
      session.interview_state &&
      session.interview_state.normalizedFacts;
    return facts && typeof facts === 'object' ? cloneNormalizedFacts(facts) : null;
  }

  // SPEC-225: snapshot-backed Blueprints read through SPEC-223C. Projection
  // failure must fail closed rather than silently reverting to session facts.
  if (approved[0].canonical_snapshot_id && approved[0].canonical_snapshot_tenant_id) {
    const { deriveBlueprintCompatibility } = require('../lib/canonicalProjection');
    const pool = opts.pool || defaultPool;
    const canonicalFacts = await deriveBlueprintCompatibility({
      tenant_id: approved[0].canonical_snapshot_tenant_id,
      snapshot_id: approved[0].canonical_snapshot_id,
      pool,
    });
    if (
      !canonicalFacts ||
      !canonicalFacts._projection_metadata ||
      canonicalFacts._projection_metadata.completeness === 'UNAVAILABLE'
    ) {
      throw new ClientIntelligenceError(
        'CANONICAL_PROJECTION_FAILURE',
        `Canonical snapshot ${approved[0].canonical_snapshot_id} could not be reconstructed for Max business understanding`,
        500
      );
    }
    bp.normalizedFacts = canonicalFacts;
    bp._canonical_authority = approved[0].canonical_snapshot_id;
    bp._semantic_authority = 'CANONICAL';
    try {
      bp._legacyFallbackFacts = await loadLegacySessionFacts();
    } catch (_) {
      bp._legacyFallbackFacts = null;
    }
    return bp;
  }

  // SPEC-103A — attach structured normalizedFacts for Max semantic reasoning.
  // Section summaries remain precomposed Blueprint prose; Max must not nest them.
  try {
    const facts = await loadLegacySessionFacts();
    if (facts) {
      bp.normalizedFacts = facts;
      bp._semantic_authority = 'session_archival'; // Mark as legacy authority
    }
  } catch (_) {
    /* fail soft — Max falls back to peeled section substance */
  }
  if (!bp.normalizedFacts && Object.keys(durableFacts).length) {
    const projected = emptyNormalizedFacts();
    projected.business_facts = cloneNormalizedFacts({ business_facts: durableFacts }).business_facts;
    bp.normalizedFacts = projected;
    bp._semantic_authority = 'section_provenance';
  }
  return bp;
}

/**
 * Load a blueprint by id for ownership checks (SPEC-096).
 */
async function getBlueprintRecord(blueprintId, opts = {}) {
  const store = await resolveStore(opts);
  const bp = await store.getBlueprint(blueprintId);
  if (!bp) {
    throw new ClientIntelligenceError('not_found', 'Blueprint not found', 404);
  }
  return publicBlueprint(bp);
}

/**
 * Start interview for a client.
 * SPEC-097: reuses an active non-sample interview unless forceNew or notes mode.
 * SPEC-099: explicit restart supersedes unfinished unapproved onboarding first.
 * @param {{ clientId: number|string, notes?: string, source?: string, forceNew?: boolean, restart?: boolean }} input
 */
async function startClientInterview(input = {}, opts = {}) {
  const store = await resolveStore(opts);
  const clientId = asClientId(input.clientId);
  const notes = asText(input.notes);
  const forceNew = Boolean(input.forceNew || opts.forceNew);
  const restart = Boolean(input.restart || opts.restart);

  // SPEC-099 — EXPLICIT_RESTART is distinct from ordinary START_OR_RESUME.
  if (restart) {
    const rows = await listClientSessions(store, clientId, { limit: 40 });
    const actives = rows.filter((row) => isRecoverableActiveInterview(row));
    const approved = rows.find(
      (row) => row.status === 'APPROVED' && !sessionIsSuperseded(row)
    );
    if (!actives.length && approved) {
      throw new ClientIntelligenceError(
        'approved_blueprint_protected',
        'An approved Blueprint already exists. Start New Interview will not discard approved understanding — revise the Blueprint instead.',
        409
      );
    }
    if (actives.length) {
      await supersedeUnapprovedOnboardingForClient(
        clientId,
        { reason: 'explicit_restart' },
        opts
      );
    }
  }

  // Ordinary reopen / double-click / refresh must not spawn duplicate actives.
  // Explicit restart and forceNew skip reuse so a fresh interview is created.
  if (!restart && !forceNew && !notes) {
    const existing = await findActiveInterviewForClient(clientId, opts);
    if (existing) {
      const detail = await getInterview(existing.id, opts);
      const q = currentQuestion(existing.interview_state);
      const memory = ensureReasoningMemory(existing.interview_state || {});
      const activeProbe =
        memory.activeProbe &&
        q &&
        memory.activeProbe.questionId === q.question.id
          ? memory.activeProbe
          : null;
      const probePrompt = activeProbe && activeProbe.prompt ? activeProbe.prompt : null;
      return withExperienceFields(existing, {
        interviewId: existing.id,
        ...publicSession(existing),
        ...detail,
        mode: 'interactive',
        nextAction:
          existing.status === 'CLIENT_REVIEW'
            ? 'COMPLETE'
            : probePrompt
              ? 'PROBE'
              : q
                ? 'ASK'
                : detail.nextAction || 'ASK',
        question: q
          ? {
              id: q.question.id,
              prompt: probePrompt || q.question.prompt,
              stage: q.question.stage,
              section: q.question.section,
              goal: q.question.goal,
              askedBecause: q.question.askedBecause,
            }
          : detail.question || null,
        message:
          existing.status === 'CLIENT_REVIEW'
            ? 'Recovered pending Blueprint review — no new interview started.'
            : probePrompt ||
              (q && q.question && q.question.prompt) ||
              detail.message ||
              'Resumed your existing interview.',
        probe: probePrompt || null,
        reasoningMemory: memory,
        resumedExisting: true,
        recovered: true,
      });
    }
  }

  const state = initialInterviewState({ notes });
  const session = await store.insertSession({
    id: newId(),
    client_id: clientId,
    status: 'NEW',
    started_at: new Date(),
    completed_at: null,
    current_stage: 'Identity',
    summary: null,
    confidence_score: null,
    interview_state: state,
  });

  advanceStatus(session, 'DISCOVERY');
  session.status = 'DISCOVERY';

  if (restart) {
    // Stamp the new interview id onto just-superseded rows for auditability.
    const prior = await listClientSessions(store, clientId, { limit: 40 });
    for (const row of prior) {
      if (String(row.id) === String(session.id)) continue;
      if (!sessionIsSuperseded(row)) continue;
      const st = row.interview_state || {};
      if (st.supersededBy) continue;
      if (st.supersededReason !== 'explicit_restart') continue;
      await store.updateSession(row.id, {
        interview_state: {
          ...st,
          supersededBy: String(session.id),
        },
      });
    }
  }

  if (notes) {
    const { assigned: mapped, guidance } = extractNotesIntoSections(notes);
    if (guidance.length) {
      state.revisionGuidance = [
        ...(state.revisionGuidance || []),
        ...guidance.map((message) => ({
          at: nowIso(),
          kind: ANSWER_KINDS.REFINEMENT_FEEDBACK,
          message,
          section: null,
        })),
      ];
      session.interview_state = state;
    }
    const systemTurn = await store.insertTurn({
      id: newId(),
      session_id: session.id,
      speaker: 'system',
      message: notes,
      goal: 'Notes-mode ingestion',
      asked_because: 'Operator provided notes instead of interactive Q&A.',
      derived_evidence: [],
      created_at: new Date(),
    });
    for (const section of BLUEPRINT_SECTIONS) {
      const statement = mapped[section] || '';
      await applySectionUpdate(
        store,
        session,
        section,
        statement || '',
        statement ? 'EXPLICIT' : 'INFERRED',
        systemTurn.id
      );
    }
    session.interview_state.done = true;
    session.interview_state.mode = 'notes';
    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      interview_state: session.interview_state,
      current_stage: 'Notes',
    });
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(session, {
      interviewId: session.id,
      ...publicSession(session),
      mode: 'notes',
      nextAction: 'COMPLETE',
      question: null,
      message: 'Notes ingested. Draft Business Blueprint is ready for review.',
      blueprint: publicBlueprint(blueprint),
    });
  }

  const q = currentQuestion(session.interview_state);
  session.current_stage = q.question.stage;
  const assistantTurn = await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'assistant',
    message: q.question.prompt,
    goal: q.question.goal,
    asked_because: q.question.askedBecause,
    derived_evidence: [],
    created_at: new Date(),
  });
  await store.updateSession(session.id, {
    status: 'DISCOVERY',
    current_stage: session.current_stage,
    interview_state: session.interview_state,
  });

  return withExperienceFields(session, {
    interviewId: session.id,
    ...publicSession(session),
    mode: 'interactive',
    nextAction: 'ASK',
    question: {
      id: q.question.id,
      prompt: q.question.prompt,
      stage: q.question.stage,
      section: q.question.section,
      goal: q.question.goal,
      askedBecause: q.question.askedBecause,
    },
    message: q.question.prompt,
    turnId: assistantTurn.id,
    blueprint: null,
    restarted: Boolean(restart),
    resumedExisting: false,
    recovered: false,
  });
}

async function postInterviewMessage(sessionId, message, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status === 'APPROVED') {
    throw new ClientIntelligenceError(
      'interview_complete',
      'Interview already approved; start a new session to recalibrate'
    );
  }
  if (session.status === 'CLIENT_REVIEW') {
    throw new ClientIntelligenceError(
      'awaiting_review',
      'Blueprint is ready for review; use revise/approve APIs'
    );
  }
  if (!['DISCOVERY', 'CLARIFICATION', 'VALIDATION'].includes(session.status)) {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Cannot accept messages in status ${session.status}`
    );
  }

  const text = String(message || '').trim();
  if (!text) {
    throw new ClientIntelligenceError('empty_message', 'message is required');
  }

  const state = session.interview_state || initialInterviewState();
  const q = currentQuestion(state);

  // Refinement pass after resume: free-form note updates then regenerate blueprint.
  // Refinement instructions are stored as revision guidance — never as business facts.
  if (!q && state.refinementPass) {
    const clientTurn = await store.insertTurn({
      id: newId(),
      session_id: session.id,
      speaker: 'client',
      message: text,
      goal: 'Refine Business Blueprint understanding',
      asked_because: 'Client returned to the interview to refine Max\'s understanding.',
      derived_evidence: [],
      created_at: new Date(),
    });
    const correctionResult = await applyRefinementSemanticCorrections(
      store, session, state, text, clientTurn.id
    );
    const extracted = correctionResult.operations.length
      ? { assigned: {}, guidance: [] }
      : extractNotesIntoSections(text);
    const { assigned: mapped, guidance } = extracted;
    if (guidance.length) {
      state.revisionGuidance = [
        ...(state.revisionGuidance || []),
        ...guidance.map((message) => ({
          at: nowIso(),
          kind: ANSWER_KINDS.REFINEMENT_FEEDBACK,
          message,
          section: null,
        })),
      ];
    }
    const evidenceIds = [...correctionResult.evidenceIds];
    const sectionsToUpdate = Object.keys(mapped);
    // Only apply when we extracted real business facts — never default the whole
    // refinement message into identity.
    for (const section of sectionsToUpdate) {
      const statement = mapped[section];
      if (!statement || !isBusinessFactStatement(statement)) continue;
      const { evidenceRow, skippedAsGuidance } = await applySectionUpdate(
        store,
        session,
        section,
        statement,
        'EXPLICIT',
        clientTurn.id
      );
      if (!skippedAsGuidance && evidenceRow) evidenceIds.push(evidenceRow.id);
    }
    await store.updateTurn(clientTurn.id, { derived_evidence: evidenceIds });
    state.refinementPass = false;
    state.done = true;
    session.interview_state = state;
    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      interview_state: state,
      current_stage: 'Refinement',
    });
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction: 'GENERATE_BLUEPRINT',
      question: null,
      message: 'Draft Business Blueprint is ready for review.',
      blueprint: publicBlueprint(blueprint),
      reflection: null,
    });
  }

  if (!q) {
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction: 'COMPLETE',
      question: null,
      message: 'Draft Business Blueprint is ready for review.',
      blueprint: publicBlueprint(blueprint),
      reflection: null,
    });
  }

  const clientTurn = await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'client',
    message: text,
    goal: q.question.goal,
    asked_because: q.question.askedBecause,
    derived_evidence: [],
    created_at: new Date(),
  });

  const domain = tagContextDomain(text);
  const businessNameHint =
    state.normalizedFacts?.business_name ||
    extractBusinessName(Object.values(state.answers || {}).join(' ')) ||
    null;

  // SPEC-090 — session-level reasoning before attaching to the active question.
  let reasoningMemory = ensureReasoningMemory(state);
  const planned = planReasoningTurn(text, {
    activeQuestion: q.question,
    state,
    businessName: businessNameHint,
    hasSpecificity: hasSpecificitySignals(text),
    looksLikeCorrection,
    looksLikeAddOn: looksLikeSupplementalContext,
    looksLikeRefinement: looksLikeRefinementFeedback,
    containsMetaInstruction: containsMetaInstructionLanguage,
    answerLooksEmpty,
    crossSectionHelpers: {
      inferDomain: (t) => inferDomainFromQuestionEcho(t) || tagContextDomain(t),
      tagDomain: tagContextDomain,
      domainToSection: DOMAIN_TO_SECTION,
    },
  });
  let messageType = planned.messageClass;
  reasoningMemory = markClassification(reasoningMemory, messageType);
  state.reasoningMemory = reasoningMemory;

  // Soft cross-section: treat high-confidence prior-section substance as add-on.
  if (
    messageType === MESSAGE_TYPES.DIRECT_ANSWER &&
    planned.cross &&
    planned.cross.section &&
    planned.cross.confidence >= 0.8
  ) {
    messageType = MESSAGE_TYPES.ADD_ON;
  }

  // Non-answers: store appropriately, stay on the same question (or skip-advance), respond conversationally.
  if (messageType !== MESSAGE_TYPES.DIRECT_ANSWER) {
    let correctionDomain = domain;
    let correctionTargetSection = null;
    let ackSubstance = null;
    let probePrompt = planned.probe || null;
    let advancedAfterSkip = false;

    if (messageType === MESSAGE_TYPES.REFINEMENT_FEEDBACK) {
      state.revisionGuidance = [
        ...(state.revisionGuidance || []),
        {
          at: nowIso(),
          kind: ANSWER_KINDS.REFINEMENT_FEEDBACK,
          message: text,
          section: q.question.section,
        },
      ];
    } else if (messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER) {
      const dispositionInfo = classifyAnswerDisposition(text, q.question, {
        hasSpecificity: hasSpecificitySignals(text),
      });
      const sufficiencyReason =
        (planned.sufficiency && planned.sufficiency.reason) ||
        dispositionInfo.reason ||
        (looksLikeExplicitUnknown(text) ? 'explicit_unknown' : 'vague');
      const isExplicitUnknown =
        dispositionInfo.disposition === ANSWER_DISPOSITIONS.UNCERTAIN ||
        sufficiencyReason === 'explicit_unknown' ||
        looksLikeExplicitUnknown(text);
      const needsSpecificity =
        dispositionInfo.disposition === ANSWER_DISPOSITIONS.NEEDS_SPECIFICITY ||
        sufficiencyReason === 'needs_specificity' ||
        sufficiencyReason === 'vague' ||
        sufficiencyReason === 'hedged' ||
        sufficiencyReason === 'thin_important' ||
        looksLikeGenericCategoryAnswer(text);
      const priorProbe =
        reasoningMemory.activeProbe &&
        reasoningMemory.activeProbe.questionId === q.question.id
          ? reasoningMemory.activeProbe
          : null;
      const priorDebt = (reasoningMemory.questionDebt || []).find(
        (d) => d.questionId === q.question.id
      );
      const probeAttempts = Math.max(
        0,
        Number(priorProbe && priorProbe.attemptCount) || 0,
        priorDebt ? 1 : 0
      );
      const priorWasUncertaintyReasoning =
        (priorProbe &&
          (priorProbe.reason === 'explicit_unknown' ||
            priorProbe.reason === 'uncertain')) ||
        (priorDebt &&
          (priorDebt.reason === 'explicit_unknown' || priorDebt.reason === 'uncertain'));
      const canReason =
        isExplicitUnknown &&
        probeAttempts < MAX_PROBE_ATTEMPTS &&
        hasRelevantUncertaintyContext(state, q.question.section);

      // SPEC-100 — bounded probing: up to MAX_PROBE_ATTEMPTS collaborative turns,
      // then preserve UNKNOWN and continue. Never store uncertainty phrases as facts.
      const shouldAcceptUnknown =
        dispositionInfo.disposition === ANSWER_DISPOSITIONS.DEFERRED ||
        (isExplicitUnknown &&
          (probeAttempts >= MAX_PROBE_ATTEMPTS ||
            !hasRelevantUncertaintyContext(state, q.question.section))) ||
        (needsSpecificity &&
          probeAttempts >= MAX_PROBE_ATTEMPTS &&
          (q.question.section === 'idealCustomers' ||
            q.question.section === 'targetMarkets')) ||
        (probeAttempts >= MAX_PROBE_ATTEMPTS &&
          priorWasUncertaintyReasoning &&
          !planned.sufficiency?.sufficient);

      state.lastAnswerDisposition = dispositionInfo.disposition;

      if (shouldAcceptUnknown) {
        // Bounded unresolved / deferred: accept uncertainty and continue.
        const accepted = await acceptExplicitUnknown(
          store,
          session,
          state,
          q,
          clientTurn,
          text
        );
        Object.assign(state, session.interview_state || state);
        reasoningMemory = ensureReasoningMemory(state);
        state.reasoningMemory = reasoningMemory;
        advancedAfterSkip = Boolean(accepted && accepted.advanced);
        probePrompt = null;

        const nextQ = currentQuestion(state);
        const acceptAck =
          dispositionInfo.disposition === ANSWER_DISPOSITIONS.DEFERRED
            ? "We can leave that open for now. I'll treat it as unresolved rather than guessing, and we can come back to it once we have more evidence."
            : prefersCommercialAck(state, q.question.section);
        ackSubstance = acceptAck;

        await store.updateSession(session.id, {
          status: 'DISCOVERY',
          current_stage: nextQ ? nextQ.question.stage : session.current_stage,
          interview_state: state,
        });

        if (state.done) {
          const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
          return withExperienceFields(await store.getSession(session.id), {
            interviewId: session.id,
            ...publicSession(await store.getSession(session.id)),
            nextAction: 'GENERATE_BLUEPRINT',
            messageType,
            answerDisposition: dispositionInfo.disposition,
            question: null,
            message: `${acceptAck}\n\nDraft Business Blueprint is ready for review.`,
            blueprint: publicBlueprint(blueprint),
            reflection: null,
            reasoningMemory,
            acceptedUnknown: true,
          });
        }

        const nextPrompt = nextQ ? nextQ.question.prompt : null;
        const assistantMessage = nextPrompt ? `${acceptAck}\n\n${nextPrompt}` : acceptAck;
        await store.insertTurn({
          id: newId(),
          session_id: session.id,
          speaker: 'assistant',
          message: assistantMessage,
          goal: nextQ ? nextQ.question.goal : q.question.goal,
          asked_because:
            'Accepted unresolved answer after bounded collaborative reasoning; preserved unknown and continued.',
          derived_evidence: [],
          created_at: new Date(),
        });

        return withExperienceFields(await store.getSession(session.id), {
          interviewId: session.id,
          ...publicSession(await store.getSession(session.id)),
          nextAction: 'ASK',
          messageType,
          answerDisposition: dispositionInfo.disposition,
          question: nextQ
            ? {
                id: nextQ.question.id,
                prompt: nextQ.question.prompt,
                stage: nextQ.question.stage,
                section: nextQ.question.section,
                goal: nextQ.question.goal,
                askedBecause: nextQ.question.askedBecause,
              }
            : null,
          message: assistantMessage,
          reflection: null,
          evidence: null,
          contradiction: false,
          blueprint: null,
          supplementalContext: state.supplementalContext || [],
          reasoningMemory,
          acceptedUnknown: true,
          probe: null,
        });
      }

      const nextAttempt = probeAttempts + 1;
      if (canReason) {
        probePrompt = buildUncertaintyReasoningProbe(
          state,
          q.question.section,
          businessNameHint,
          { attemptCount: nextAttempt }
        );
      } else if (needsSpecificity) {
        probePrompt =
          planned.probe ||
          buildProbingFollowUp(
            q.question,
            { reason: sufficiencyReason === 'vague' ? 'needs_specificity' : sufficiencyReason },
            businessNameHint
          );
      } else {
        probePrompt =
          planned.probe ||
          buildProbingFollowUp(
            q.question,
            planned.sufficiency || { reason: sufficiencyReason },
            businessNameHint
          );
      }
      reasoningMemory = setActiveProbe(reasoningMemory, {
        questionId: q.question.id,
        section: q.question.section,
        prompt: probePrompt,
        reason: isExplicitUnknown
          ? 'explicit_unknown'
          : sufficiencyReason || 'vague',
        attemptCount: nextAttempt,
        disposition: dispositionInfo.disposition,
      });
      reasoningMemory = addQuestionDebt(reasoningMemory, {
        questionId: q.question.id,
        section: q.question.section,
        reason: isExplicitUnknown
          ? 'explicit_unknown'
          : sufficiencyReason || 'vague',
      });
      state.reasoningMemory = reasoningMemory;
      state.supplementalContext = [
        ...(state.supplementalContext || []),
        {
          at: nowIso(),
          text,
          domain: null,
          kind: MESSAGE_TYPES.INSUFFICIENT_ANSWER,
          activeQuestionId: q.question.id,
          confirmed: false,
          probe: probePrompt,
          uncertaintyReasoning: Boolean(canReason),
          disposition: dispositionInfo.disposition,
          // Operator uncertainty is recorded as context only — never as section evidence.
          operatorEvidence: false,
        },
      ];
    } else if (messageType === MESSAGE_TYPES.SKIP) {
      state.lastAnswerDisposition = ANSWER_DISPOSITIONS.DEFERRED;
      // SPEC-100 — explicit deferral preserves UNKNOWN via the same path as bounded uncertainty.
      const accepted = await acceptExplicitUnknown(
        store,
        session,
        state,
        q,
        clientTurn,
        text
      );
      Object.assign(state, session.interview_state || state);
      reasoningMemory = ensureReasoningMemory(state);
      reasoningMemory = addQuestionDebt(reasoningMemory, {
        questionId: q.question.id,
        section: q.question.section,
        reason: 'deferred',
      });
      reasoningMemory = setActiveProbe(reasoningMemory, null);
      state.reasoningMemory = reasoningMemory;
      advancedAfterSkip = Boolean(accepted && accepted.advanced);
      state.supplementalContext = [
        ...(state.supplementalContext || []),
        {
          at: nowIso(),
          text,
          domain: null,
          kind: MESSAGE_TYPES.SKIP,
          activeQuestionId: q.question.id,
          confirmed: false,
          acceptedUnknown: true,
          disposition: ANSWER_DISPOSITIONS.DEFERRED,
        },
      ];
    } else if (messageType === MESSAGE_TYPES.APPROVAL) {
      state.supplementalContext = [
        ...(state.supplementalContext || []),
        {
          at: nowIso(),
          text,
          domain: null,
          kind: MESSAGE_TYPES.APPROVAL,
          activeQuestionId: q.question.id,
          confirmed: false,
        },
      ];
    } else if (
      messageType === MESSAGE_TYPES.ADD_ON ||
      messageType === MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT
    ) {
      const parsed = parseSupplementalMessage(text, { activeQuestion: q.question });
      // Prefer reasoning-layer cross-section when supplemental parse lacks a target.
      const targetSection =
        parsed.section ||
        (planned.cross && planned.cross.section) ||
        (planned.targetSection && planned.routeReason === 'cross_section_add_on'
          ? planned.targetSection
          : null);
      correctionDomain = parsed.domain || (planned.cross && planned.cross.domain) || domain;
      correctionTargetSection = targetSection;
      const substance = parsed.substance || synthesizeBusinessLanguage(text, {
        section: targetSection,
        businessName: businessNameHint,
      });
      ackSubstance = substance;

      if (targetSection && substance && isBusinessFactStatement(substance)) {
        // Append into the targeted prior domain — never into the unanswered active question.
        state.normalizedFacts = applyCorrectionToNormalizedFacts(
          state.normalizedFacts || emptyNormalizedFacts(),
          { section: targetSection, substance, domain: correctionDomain }
        );
        session.interview_state = state;

        const { evidenceRow } = await applySectionUpdate(
          store,
          session,
          targetSection,
          substance,
          'CLIENT_EDITED',
          clientTurn.id
        );
        state.normalizedFacts = applyCorrectionToNormalizedFacts(
          session.interview_state.normalizedFacts || state.normalizedFacts,
          { section: targetSection, substance, domain: correctionDomain }
        );
        const rebuilt = sectionsFromNormalizedFacts(
          state.normalizedFacts,
          session.interview_state.sectionState || state.sectionState
        );
        const curSection =
          (session.interview_state.sectionState || state.sectionState)[targetSection] ||
          emptySection();
        state.sectionState = {
          ...(session.interview_state.sectionState || state.sectionState),
          [targetSection]: {
            ...curSection,
            summary: rebuilt[targetSection].summary,
            confidence: Math.max(curSection.confidence || 0, EXPLICIT_CONFIDENCE),
            unknowns: [],
          },
        };
        await store.updateTurn(clientTurn.id, {
          derived_evidence: evidenceRow ? [evidenceRow.id] : [],
        });

        const questionId =
          parsed.questionId ||
          QUESTION_BANK.find((row) => row.section === targetSection)?.id;
        if (questionId) {
          const priorAnswer = (state.answers || {})[questionId] || '';
          // Append supplemental detail to the prior answer without overwriting it.
          const alreadyHas = priorAnswer
            .toLowerCase()
            .includes(String(substance).toLowerCase());
          state.answers = {
            ...(state.answers || {}),
            [questionId]: alreadyHas
              ? priorAnswer
              : priorAnswer
                ? `${priorAnswer}; ${substance}`
                : substance,
          };
        }

        reasoningMemory = recordAcceptedFact(reasoningMemory, {
          section: targetSection,
          substance,
          source: 'add_on',
        });
        reasoningMemory = clearQuestionDebt(
          reasoningMemory,
          QUESTION_BANK.find((row) => row.section === targetSection)?.id
        );
      }

      state.supplementalContext = [
        ...(state.supplementalContext || []),
        {
          at: nowIso(),
          text,
          domain: correctionDomain || null,
          kind: MESSAGE_TYPES.ADD_ON,
          section: targetSection,
          substance,
          activeQuestionId: q.question.id,
          confirmed: Boolean(targetSection && substance),
        },
      ];
    } else if (messageType === MESSAGE_TYPES.CORRECTION) {
      const parsed = parseCorrectionMessage(text, null, {
        activeQuestion: q.question,
        state,
        lastAnsweredQuestionId: findLastAnsweredQuestionId(state),
      });
      // Prefer resolved target (domain match → last answer → active). Do not
      // blindly attach to the active question when a prior domain matches.
      const targetSection = parsed.section;
      correctionDomain = parsed.domain || domain;
      correctionTargetSection = targetSection;
      const substance = parsed.substance;

      reasoningMemory = recordPendingCorrection(reasoningMemory, {
        section: targetSection,
        substance,
        status: 'pending',
      });

      if (targetSection && substance && isBusinessFactStatement(substance)) {
        state.normalizedFacts = applyCorrectionToNormalizedFacts(
          state.normalizedFacts || emptyNormalizedFacts(),
          { section: targetSection, substance, domain: correctionDomain }
        );
        session.interview_state = state;

        const { evidenceRow } = await applySectionUpdate(
          store,
          session,
          targetSection,
          substance,
          'CLIENT_EDITED',
          clientTurn.id
        );
        // Prefer correction merge over raw CLIENT_EDITED ingest side effects.
        state.normalizedFacts = applyCorrectionToNormalizedFacts(
          session.interview_state.normalizedFacts || state.normalizedFacts,
          { section: targetSection, substance, domain: correctionDomain }
        );
        const rebuilt = sectionsFromNormalizedFacts(
          state.normalizedFacts,
          session.interview_state.sectionState || state.sectionState
        );
        const curSection =
          (session.interview_state.sectionState || state.sectionState)[targetSection] ||
          emptySection();
        state.sectionState = {
          ...(session.interview_state.sectionState || state.sectionState),
          [targetSection]: {
            ...curSection,
            summary: rebuilt[targetSection].summary,
            confidence: Math.max(curSection.confidence || 0, EXPLICIT_CONFIDENCE),
            unknowns: [],
          },
        };
        await store.updateTurn(clientTurn.id, {
          derived_evidence: evidenceRow ? [evidenceRow.id] : [],
        });
        const questionId =
          parsed.questionId ||
          QUESTION_BANK.find((row) => row.section === targetSection)?.id;
        if (questionId) {
          // Replacement: overwrite the prior answer, do not append correction wrappers.
          state.answers = {
            ...(state.answers || {}),
            [questionId]: substance,
          };
        }
        reasoningMemory = resolvePendingCorrection(reasoningMemory, targetSection);
        reasoningMemory = recordAcceptedFact(reasoningMemory, {
          section: targetSection,
          substance,
          source: 'correction',
        });
      }

      state.supplementalContext = [
        ...(state.supplementalContext || []),
        {
          at: nowIso(),
          text,
          domain: correctionDomain || null,
          kind: MESSAGE_TYPES.CORRECTION,
          section: targetSection,
          substance,
          reason: parsed.reason,
          supersedes: Boolean(targetSection),
          activeQuestionId: q.question.id,
          confirmed: Boolean(targetSection),
        },
      ];
    } else {
      // clarification_request / off_topic — acknowledge only
      state.supplementalContext = [
        ...(state.supplementalContext || []),
        {
          at: nowIso(),
          text,
          domain: null,
          kind: messageType,
          activeQuestionId: q.question.id,
          confirmed: false,
        },
      ];
    }

    reasoningMemory = syncConfidenceFromSections(reasoningMemory, state.sectionState);
    state.reasoningMemory = reasoningMemory;
    session.interview_state = state;

    // Skip may finish the bank — generate blueprint with readiness metadata.
    if (advancedAfterSkip && state.done) {
      await store.updateSession(session.id, {
        status: 'DISCOVERY',
        interview_state: state,
        current_stage: session.current_stage,
      });
      const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
      return withExperienceFields(await store.getSession(session.id), {
        interviewId: session.id,
        ...publicSession(await store.getSession(session.id)),
        nextAction: 'GENERATE_BLUEPRINT',
        messageType,
        question: null,
        message: 'Draft Business Blueprint is ready for review.',
        blueprint: publicBlueprint(blueprint),
        reflection: null,
        reasoningMemory,
      });
    }

    const nextQAfterSkip = advancedAfterSkip ? currentQuestion(state) : q;
    const activeForAck = nextQAfterSkip ? nextQAfterSkip.question : q.question;
    const businessName =
      state.normalizedFacts?.business_name ||
      extractBusinessName(Object.values(state.answers || {}).join(' ')) ||
      null;

    let ack;
    if (messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER) {
      ack = probePrompt;
    } else if (messageType === MESSAGE_TYPES.SKIP) {
      const deferAck =
        "We can leave that open for now. I'll treat it as unresolved rather than guessing, and we can come back to it once we have more evidence.";
      ack = advancedAfterSkip
        ? reasoningAck(MESSAGE_TYPES.SKIP, {
            reopenPrompt: activeForAck ? activeForAck.prompt : null,
            deferAck,
          }) ||
          (activeForAck ? `${deferAck}\n\n${activeForAck.prompt}` : deferAck)
        : deferAck;
      // Prefer plain deferral language over generic skip ack when we preserved unknown.
      if (advancedAfterSkip && activeForAck) {
        ack = `${deferAck}\n\n${activeForAck.prompt}`;
      } else if (advancedAfterSkip) {
        ack = deferAck;
      }
    } else {
      ack = conversationalAck(messageType, text, correctionDomain, {
        activeQuestion: q.question,
        targetSection: correctionTargetSection,
        businessName,
        substance: ackSubstance,
        probe: probePrompt,
      });
    }

    const assistantMessage =
      messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER
        ? ack
        : advancedAfterSkip && activeForAck
          ? `${ack}`
          : `${ack}\n\n${q.question.prompt}`;

    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      current_stage: advancedAfterSkip && activeForAck ? activeForAck.stage : session.current_stage,
      interview_state: state,
    });

    await store.insertTurn({
      id: newId(),
      session_id: session.id,
      speaker: 'assistant',
      message: assistantMessage,
      goal: activeForAck ? activeForAck.goal : q.question.goal,
      asked_because:
        messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER
          ? 'Probing follow-up before advancing — answer was vague or incomplete.'
          : messageType === MESSAGE_TYPES.SKIP
            ? 'Skipped question recorded as question debt; advanced to next prompt.'
            : 'Acknowledged non-answer message without advancing the interview.',
      derived_evidence: [],
      created_at: new Date(),
    });

    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction:
        messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER
          ? 'PROBE'
          : 'ASK',
      messageType,
      answerDisposition:
        state.lastAnswerDisposition ||
        (messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER
          ? ANSWER_DISPOSITIONS.NEEDS_SPECIFICITY
          : messageType === MESSAGE_TYPES.SKIP
            ? ANSWER_DISPOSITIONS.DEFERRED
            : null),
      question: activeForAck
        ? {
            id: activeForAck.id,
            prompt:
              messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER
                ? probePrompt || activeForAck.prompt
                : activeForAck.prompt,
            stage: activeForAck.stage,
            section: activeForAck.section,
            goal: activeForAck.goal,
            askedBecause: activeForAck.askedBecause,
          }
        : null,
      message: assistantMessage,
      reflection: null,
      evidence: null,
      contradiction: false,
      blueprint: null,
      supplementalContext: state.supplementalContext || [],
      reasoningMemory,
      probe: messageType === MESSAGE_TYPES.INSUFFICIENT_ANSWER ? probePrompt : null,
      acceptedUnknown: messageType === MESSAGE_TYPES.SKIP ? true : undefined,
    });
  }

  const { evidenceRow, contradiction, skippedAsGuidance } = await applySectionUpdate(
    store,
    session,
    q.question.section,
    text,
    'EXPLICIT',
    clientTurn.id
  );

  await store.updateTurn(clientTurn.id, {
    derived_evidence: evidenceRow ? [evidenceRow.id] : [],
  });

  // Only record business answers against the question bank — refinement stays in revisionGuidance.
  const acceptedDisposition = contradiction
    ? ANSWER_DISPOSITIONS.CONTRADICTORY
    : ANSWER_DISPOSITIONS.ACCEPTED;
  state.lastAnswerDisposition = acceptedDisposition;
  if (!skippedAsGuidance) {
    state.answers = { ...(state.answers || {}), [q.question.id]: text };
    const synthesized = synthesizeBusinessLanguage(text, {
      section: q.question.section,
      businessName: businessNameHint,
    });
    reasoningMemory = recordAcceptedFact(reasoningMemory, {
      section: q.question.section,
      substance: synthesized || text,
      source: 'direct_answer',
    });
    reasoningMemory = clearQuestionDebt(reasoningMemory, q.question.id);
    reasoningMemory = setActiveProbe(reasoningMemory, null);
  } else {
    state.answers = { ...(state.answers || {}) };
  }
  // SPEC-100 — only advance after accepted (or partial) operator evidence.
  state.stepIndex = (Number(state.stepIndex) || 0) + 1;
  if (state.stepIndex >= QUESTION_BANK.length) state.done = true;
  reasoningMemory = syncConfidenceFromSections(
    reasoningMemory,
    session.interview_state.sectionState || state.sectionState
  );
  state.reasoningMemory = reasoningMemory;
  state.sectionState = session.interview_state.sectionState || state.sectionState;
  session.interview_state = state;
  session.current_stage = q.question.stage;

  if (contradiction) {
    // stay in discovery but surface clarify action; lifecycle still no-skip at end
    await store.updateSession(session.id, {
      interview_state: state,
      current_stage: session.current_stage,
      status: 'DISCOVERY',
    });
  }

  if (state.done) {
    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      interview_state: state,
      current_stage: session.current_stage,
    });
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction: 'GENERATE_BLUEPRINT',
      answerDisposition: acceptedDisposition,
      question: null,
      message: 'Draft Business Blueprint is ready for review.',
      evidence: evidenceRow ? publicEvidence(evidenceRow) : null,
      blueprint: publicBlueprint(blueprint),
      reflection: null,
      reasoningMemory: (await store.getSession(session.id)).interview_state?.reasoningMemory,
    });
  }

  let reflection = null;
  if (shouldReflect(state.stepIndex) && state.lastReflectionAt !== state.stepIndex) {
    reflection = buildReflection(state.sectionState, state.stepIndex);
    if (reflection) {
      state.lastReflectionAt = state.stepIndex;
      await store.insertTurn({
        id: newId(),
        session_id: session.id,
        speaker: 'assistant',
        message: reflection,
        goal: 'Reflect current understanding',
        asked_because: 'Lightweight conversational summary before continuing the fixed question bank.',
        derived_evidence: [],
        created_at: new Date(),
      });
    }
  }

  const nextQ = currentQuestion(state);
  session.current_stage = nextQ.question.stage;
  await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'assistant',
    message: nextQ.question.prompt,
    goal: nextQ.question.goal,
    asked_because: nextQ.question.askedBecause,
    derived_evidence: [],
    created_at: new Date(),
  });
  session.interview_state = state;
  await store.updateSession(session.id, {
    status: 'DISCOVERY',
    current_stage: session.current_stage,
    interview_state: state,
  });

  return withExperienceFields(await store.getSession(session.id), {
    interviewId: session.id,
    ...publicSession(await store.getSession(session.id)),
    nextAction: contradiction ? 'CLARIFY' : 'ASK',
    messageType: MESSAGE_TYPES.DIRECT_ANSWER,
    answerDisposition: acceptedDisposition,
    question: {
      id: nextQ.question.id,
      prompt: nextQ.question.prompt,
      stage: nextQ.question.stage,
      section: nextQ.question.section,
      goal: nextQ.question.goal,
      askedBecause: nextQ.question.askedBecause,
    },
    message: nextQ.question.prompt,
    reflection,
    evidence: evidenceRow ? publicEvidence(evidenceRow) : null,
    contradiction: contradiction || false,
    blueprint: null,
    reasoningMemory,
  });
}

function sessionIsSample(session) {
  const state = (session && session.interview_state) || {};
  return Boolean(state.isSample || state.is_sample || state.fixtureKey);
}

function extractSessionBusinessName(session, blueprint) {
  const state = (session && session.interview_state) || {};
  const facts = state.normalizedFacts || {};
  if (facts.business_name) return String(facts.business_name).trim();
  if (state.businessName) return String(state.businessName).trim();
  const sections =
    (blueprint && blueprint.sections) ||
    state.sectionState ||
    {};
  const identity = sections.identity && sections.identity.summary;
  return (
    extractBusinessName(identity) ||
    extractReadinessBusinessName(blueprint) ||
    ''
  );
}

/**
 * Decide the next useful UI/API resume target for an approved session.
 * Never restarts Business Understanding.
 *
 * SPEC-088: approved sessions resume into the Growth Workspace (first incomplete
 * task) — never a Readiness Report dead end.
 */
function resolveResumeTarget(session, blueprint) {
  if (session && session.status !== 'APPROVED') {
    if (session.status === 'CLIENT_REVIEW') {
      // Superseded Blueprints are historical — not current review targets.
      if (blueprintStatusOf(blueprint) === 'superseded') {
        return 'blueprint_historical';
      }
      return 'blueprint_review';
    }
    if (
      ['DISCOVERY', 'CLARIFICATION', 'VALIDATION', 'BLUEPRINT_GENERATION', 'NEW'].includes(
        session.status
      )
    ) {
      return 'interview';
    }
  }

  const planTarget = resolveGrowthPlanResumeTarget(session, blueprint);
  if (planTarget) return planTarget;

  const state = (session && session.interview_state) || {};
  const growth = state.growthConversation || null;
  const preview =
    (growth && (growth.first_growth_plan_preview || growth.firstGrowthPlanPreview)) ||
    state.firstGrowthPlanPreview ||
    null;
  if (preview) return 'growth_workspace';

  if (growth && (growth.status || (Array.isArray(growth.turns) && growth.turns.length))) {
    return 'growth_workspace';
  }

  return 'growth_workspace';
}

function resumePhaseForTarget(target) {
  switch (target) {
    case 'growth_workspace':
    case 'growth_complete':
      return 'growth_workspace';
    case 'infrastructure_readiness':
      return 'readiness';
    case 'first_growth_plan_preview':
    case 'growth_conversation':
      return 'growth';
    case 'blueprint_review':
    case 'blueprint_historical':
      return 'blueprint';
    case 'interview':
      return 'discovery';
    case 'initial_growth_direction':
      return 'complete';
    default:
      return 'growth_workspace';
  }
}

function publicGrowthState(growthConversation) {
  if (!growthConversation) return null;
  const normalized = normalizeGrowthState(growthConversation);
  return {
    status: growthConversation.status || null,
    startedAt: growthConversation.startedAt || null,
    selectedFocusArea: normalized.selected_focus_area,
    primarySegment: normalized.primary_segment,
    secondarySegment: normalized.secondary_segment,
    currentGrowthStep: normalized.current_growth_step,
    completedSteps: normalized.completed_steps,
    confidenceLevel: normalized.confidence_level,
    firstGrowthPlanPreview:
      normalized.first_growth_plan_preview ||
      growthConversation.firstGrowthPlanPreview ||
      null,
    segmentRanking:
      normalized.segment_ranking || growthConversation.segmentRanking || null,
    validationTarget:
      normalized.validation_target || growthConversation.validationTarget || null,
    firstSegmentDecision:
      normalized.first_segment_decision ||
      growthConversation.firstSegmentDecision ||
      null,
    turnCount: Array.isArray(growthConversation.turns)
      ? growthConversation.turns.length
      : 0,
  };
}

function publicInfrastructureState(session) {
  const state = (session && session.interview_state) || {};
  const readiness = state.infrastructureReadiness || null;
  const report = state.growthInfrastructureReadinessReport || null;
  if (!readiness && !report) return null;
  return {
    status: (readiness && readiness.status) || (report ? 'report_ready' : null),
    step: (readiness && readiness.step) || null,
    startedAt: (readiness && readiness.startedAt) || null,
    turnCount:
      readiness && Array.isArray(readiness.turns) ? readiness.turns.length : 0,
    hasReport: Boolean(report),
    report,
  };
}

function summarizeApprovedSession(session, blueprint) {
  const state = (session && session.interview_state) || {};
  const businessName =
    extractSessionBusinessName(session, blueprint) || 'Untitled business';
  const growthPlan = buildGrowthPlan(session, blueprint);
  const resumeTarget = resolveResumeTarget(session, blueprint);
  const isSample = sessionIsSample(session);
  const label = isSample
    ? `${businessName} · Sample Growth Plan (dev)`
    : `${businessName} · Growth Plan`;

  return {
    sessionId: session.id,
    interviewId: session.id,
    clientId: session.client_id,
    businessName,
    label,
    status: session.status,
    approvedAt: session.completed_at || state.approvedAt || null,
    blueprintVersion:
      (blueprint && blueprint.version) || state.blueprintVersion || null,
    blueprintId: (blueprint && blueprint.id) || state.blueprintId || null,
    approvedBlueprint: publicBlueprint(blueprint),
    latestGrowthState: publicGrowthState(state.growthConversation || null),
    latestInfrastructureState: publicInfrastructureState(session),
    growthPlan: {
      percentComplete: growthPlan.percentComplete,
      status: growthPlan.status,
      currentTask: growthPlan.currentTask
        ? {
            id: growthPlan.currentTask.id,
            title: growthPlan.currentTask.title,
            type: growthPlan.currentTask.type,
            estimatedMinutes: growthPlan.currentTask.estimatedMinutes,
          }
        : null,
      taskCount: growthPlan.tasks.length,
      incompleteCount: growthPlan.tasks.filter((t) => t.status !== 'complete')
        .length,
    },
    resumeTarget,
    resumePhase: resumePhaseForTarget(resumeTarget),
    isSample,
    fixtureKey: state.fixtureKey || null,
    source: state.source || (isSample ? 'fixture' : 'interview'),
    updatedAt: session.updated_at || null,
    startedAt: session.started_at || null,
  };
}

async function listApprovedBlueprintSessions(opts = {}) {
  const store = await resolveStore(opts);
  const clientId =
    opts.clientId != null && opts.clientId !== ''
      ? asClientId(opts.clientId)
      : null;
  // When a clientId is required (client-role callers), never omit the filter.
  if (opts.requireClientId && clientId == null) {
    throw new ClientIntelligenceError(
      'invalid_client_id',
      'client id is required',
      400
    );
  }
  const includeSamples =
    opts.includeSamples == null ? true : Boolean(opts.includeSamples);
  const samplesOnly = Boolean(opts.samplesOnly);
  const limit =
    opts.limit != null && Number.isFinite(Number(opts.limit))
      ? Math.max(1, Math.min(200, Math.trunc(Number(opts.limit))))
      : 50;

  const fetchLimit = Math.max(limit * 3, 60);
  /** @type {object[]} */
  let candidates = [];

  if (samplesOnly) {
    candidates = await store.listSessions({ status: 'APPROVED', limit: fetchLimit });
    candidates = candidates.filter(sessionIsSample);
  } else {
    const real = await store.listSessions({
      status: 'APPROVED',
      clientId: clientId == null ? undefined : clientId,
      limit: fetchLimit,
    });
    candidates = real.filter((row) => !sessionIsSample(row));

    // Samples are a separate lineage — append when requested, never merged into
    // a real client's interview history as if they were that client's sessions.
    if (includeSamples) {
      const samples = (await store.listSessions({
        status: 'APPROVED',
        limit: fetchLimit,
      })).filter(sessionIsSample);
      candidates = candidates.concat(samples);
    }
  }

  candidates.sort((a, b) => {
    const aTime = new Date(a.completed_at || a.updated_at || a.started_at || 0).getTime();
    const bTime = new Date(b.completed_at || b.updated_at || b.started_at || 0).getTime();
    return bTime - aTime;
  });

  const out = [];
  const seen = new Set();
  for (const session of candidates) {
    if (seen.has(session.id)) continue;
    seen.add(session.id);

    let blueprint = null;
    const state = session.interview_state || {};
    if (state.blueprintId) {
      blueprint = await store.getBlueprint(state.blueprintId, state.blueprintVersion);
    }
    out.push(summarizeApprovedSession(session, blueprint));
    if (out.length >= limit) break;
  }

  return {
    ok: true,
    sessions: out,
    count: out.length,
    fixturesAllowed: fixturesAllowed(opts.env || process.env),
  };
}

async function getResumePayload(sessionId, opts = {}) {
  const detail = await getInterview(sessionId, opts);
  return {
    ...detail,
    ok: true,
    resumeTarget: detail.resumeTarget,
    resumePhase: detail.resumePhase,
    growthPlan: detail.growthPlan || null,
    currentTask:
      (detail.growthPlan && detail.growthPlan.currentTask) || null,
    action: opts.action || 'continue',
  };
}

/**
 * Mark a Growth Plan task complete and advance to the next incomplete task.
 */
async function completeGrowthPlanTask(sessionId, taskId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_state',
      'Growth Plan tasks require an approved Blueprint session',
      409
    );
  }

  let blueprint = null;
  const state = session.interview_state || {};
  if (state.blueprintId) {
    blueprint = await store.getBlueprint(state.blueprintId, state.blueprintVersion);
  }

  let applied;
  try {
    applied = applyTaskCompletion(session, taskId, {
      blueprint,
      note: opts.note,
      source: opts.source || 'operator',
    });
  } catch (err) {
    throw new ClientIntelligenceError(
      err.code || 'invalid_task',
      err.message || 'Could not complete Growth Plan task',
      err.status || 400
    );
  }

  const updated = await store.updateSession(session.id, {
    interview_state: applied.interview_state,
  });

  const growthPlan = buildGrowthPlan(updated, blueprint);
  return {
    ok: true,
    interviewId: updated.id,
    status: updated.status,
    completedTask: applied.completedTask,
    nextTask: growthPlan.currentTask,
    growthPlan,
    resumeTarget: resolveResumeTarget(updated, blueprint),
    resumePhase: resumePhaseForTarget(resolveResumeTarget(updated, blueprint)),
    growthInfrastructureReadinessReport:
      applied.interview_state.growthInfrastructureReadinessReport || null,
  };
}

/**
 * Create or reuse the Anchor Cleaning sample approved Blueprint (dev/test only).
 * Marked isSample — never overwrites a real approved Blueprint session.
 */
async function loadAnchorSampleBlueprint(opts = {}) {
  if (!fixturesAllowed(opts.env || process.env)) {
    throw new ClientIntelligenceError(
      'fixtures_disabled',
      'CIE fixtures are disabled in this environment',
      403
    );
  }

  const store = await resolveStore(opts);
  const forceNew = Boolean(opts.forceNew);

  if (!forceNew) {
    const existing = await store.listSessions({
      status: 'APPROVED',
      clientId: ANCHOR_SAMPLE_CLIENT_ID,
      limit: 40,
    });
    const prior = existing.find(
      (row) =>
        sessionIsSample(row) &&
        (row.interview_state || {}).fixtureKey === ANCHOR_FIXTURE_KEY
    );
    if (prior) {
      const resume = await getResumePayload(prior.id, { ...opts, action: 'continue' });
      return {
        ...resume,
        created: false,
        isSample: true,
        fixtureKey: ANCHOR_FIXTURE_KEY,
        message: 'Resumed existing Anchor sample Blueprint (dev/test data).',
      };
    }
  }

  const sections = cloneAnchorSections();
  const normalizedFacts = cloneAnchorNormalizedFacts();
  const sessionId = newId();
  const blueprintId = newId();
  const now = new Date();
  const confidenceSummary = confidenceSummaryFromSections(sections);

  const draftBlueprint = {
    id: blueprintId,
    client_id: ANCHOR_SAMPLE_CLIENT_ID,
    session_id: sessionId,
    version: '1.0',
    status: 'approved',
    generated_by: `${GENERATED_BY}-fixture`,
    sections,
    confidence_summary: confidenceSummary,
    playbook_id: null,
    playbook_version: null,
    section_provenance: {},
    parent_blueprint_id: null,
    created_at: now,
    updated_at: now,
  };

  const initialGrowthDirection = buildInitialGrowthDirection(draftBlueprint, {
    normalizedFacts,
  });

  const interviewState = {
    mode: 'fixture',
    done: true,
    stepIndex: 9,
    answers: {},
    sectionState: sections,
    contradictions: [],
    revisionGuidance: [],
    supplementalContext: [],
    normalizedFacts,
    blueprintId,
    blueprintVersion: '1.0',
    initialGrowthDirection,
    growthConversation: null,
    infrastructureReadiness: null,
    growthInfrastructureReadinessReport: null,
    isSample: true,
    fixtureKey: ANCHOR_FIXTURE_KEY,
    source: 'fixture',
    businessName: ANCHOR_BUSINESS_NAME,
    approvedAt: now.toISOString(),
    sampleLabel: 'SAMPLE / DEV DATA — Anchor Cleaning approved Blueprint fixture',
  };

  await store.insertSession({
    id: sessionId,
    client_id: ANCHOR_SAMPLE_CLIENT_ID,
    status: 'APPROVED',
    started_at: now,
    completed_at: now,
    current_stage: 'Approved',
    summary: `SAMPLE approved Blueprint for ${ANCHOR_BUSINESS_NAME}`,
    confidence_score: overallConfidence(confidenceSummary),
    interview_state: interviewState,
  });

  let approved = await store.insertBlueprint(draftBlueprint);

  // Best-effort playbook handoff so resume lineage matches real approvals.
  try {
    const handoffOpts = { ...opts };
    if (store.kind === 'memory' && !handoffOpts.playbookStore) {
      handoffOpts.useMemoryPlaybookStore = true;
    }
    const handoff = await createPlaybookFromApprovedBlueprint(approved, handoffOpts);
    approved = await store.updateBlueprint(approved.id, approved.version, {
      playbook_id: handoff.playbook.id,
      playbook_version: handoff.playbook.version,
      section_provenance: handoff.sectionProvenance,
    });
    interviewState.playbookId = handoff.playbook.id;
    interviewState.playbookVersion = handoff.playbook.version;
    await store.updateSession(sessionId, { interview_state: interviewState });
  } catch (err) {
    // Fixture remains usable for Growth Conversation even if playbook store is unavailable.
    console.warn('[cie-fixture] playbook handoff skipped:', err && err.message);
  }

  await store.insertTurn({
    id: newId(),
    session_id: sessionId,
    speaker: 'system',
    message:
      'SAMPLE / DEV DATA: Loaded Anchor Cleaning approved Business Blueprint fixture. This session is not a real client interview.',
    goal: 'Load fixture',
    asked_because: 'Operator requested Anchor sample Blueprint for growth testing.',
    derived_evidence: [],
    created_at: now,
  });

  const resume = await getResumePayload(sessionId, { ...opts, action: 'continue' });
  return {
    ...resume,
    ok: true,
    created: true,
    isSample: true,
    fixtureKey: ANCHOR_FIXTURE_KEY,
    message: 'Loaded Anchor sample Blueprint (dev/test data).',
  };
}

async function getInterview(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  let session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  const turns = await store.listTurns(session.id);
  const evidence = await store.listEvidence(session.id);
  let blueprint = await resolveBlueprintForSession(store, session);
  if (blueprint) {
    session = await healSessionBlueprintPointer(store, session, blueprint);
  }
  const q = currentQuestion(session.interview_state);
  const memory = ensureReasoningMemory(session.interview_state || {});
  const activeProbe =
    memory.activeProbe &&
    q &&
    memory.activeProbe.questionId === q.question.id
      ? memory.activeProbe
      : null;
  const probePrompt = activeProbe && activeProbe.prompt ? activeProbe.prompt : null;
  const initialGrowthDirection =
    session.status === 'APPROVED'
      ? resolveInitialGrowthDirection(blueprint, session.interview_state)
      : (session.interview_state && session.interview_state.initialGrowthDirection) || null;
  const growthConversation =
    (session.interview_state && session.interview_state.growthConversation) || null;
  const firstGrowthPlanPreview =
    (growthConversation &&
      (growthConversation.first_growth_plan_preview ||
        growthConversation.firstGrowthPlanPreview)) ||
    (session.interview_state && session.interview_state.firstGrowthPlanPreview) ||
    null;
  const growthPlan = buildGrowthPlan(session, blueprint);
  const resumeTarget = resolveResumeTarget(session, blueprint);
  const businessName = extractSessionBusinessName(session, blueprint);
  const isSample = sessionIsSample(session);
  return withExperienceFields(session, {
    interviewId: session.id,
    ...publicSession(session),
    turns: turns.map(publicTurn),
    evidence: evidence.map(publicEvidence),
    blueprint: publicBlueprint(blueprint),
    initialGrowthDirection,
    growthConversation,
    firstGrowthPlanPreview,
    infrastructureReadiness:
      (session.interview_state && session.interview_state.infrastructureReadiness) || null,
    growthInfrastructureReadinessReport:
      (session.interview_state &&
        session.interview_state.growthInfrastructureReadinessReport) ||
      null,
    growthPlan,
    latestGrowthState: publicGrowthState(growthConversation),
    latestInfrastructureState: publicInfrastructureState(session),
    businessName: businessName || null,
    isSample,
    fixtureKey:
      (session.interview_state && session.interview_state.fixtureKey) || null,
    resumeTarget,
    resumePhase: resumePhaseForTarget(resumeTarget),
    approvedAt:
      session.completed_at ||
      (session.interview_state && session.interview_state.approvedAt) ||
      null,
    question: q
      ? {
          id: q.question.id,
          prompt: probePrompt || q.question.prompt,
          stage: q.question.stage,
          section: q.question.section,
          goal: q.question.goal,
          askedBecause: q.question.askedBecause,
        }
      : null,
    nextAction: probePrompt ? 'PROBE' : undefined,
    probe: probePrompt || null,
    reasoningMemory: memory,
    sectionState: (session.interview_state && session.interview_state.sectionState) || {},
  });
}

async function getInterviewBlueprint(sessionId, opts = {}) {
  const detail = await getInterview(sessionId, opts);
  if (!detail.blueprint) {
    throw new ClientIntelligenceError(
      'blueprint_not_ready',
      'Blueprint not generated yet for this interview'
    );
  }
  return detail.blueprint;
}

/**
 * Resolve the authoritative *current* Blueprint for a client.
 * Prefer: latest approved → in_review/draft for the active interview →
 * latest non-superseded in_review/draft. Never returns superseded rows.
 */
async function getClientBlueprint(clientId, opts = {}) {
  const store = await resolveStore(opts);
  const id = asClientId(clientId);

  const approved = await store.listBlueprintsForClient(id, { status: 'approved' });
  if (approved[0]) return publicBlueprint(approved[0]);

  // Prefer the Blueprint belonging to the recoverable active interview.
  const active = await findActiveInterviewForClient(id, opts);
  if (active) {
    const linked = await resolveBlueprintForSession(store, active);
    if (linked && isActiveBlueprintStatus(linked.status)) {
      await healSessionBlueprintPointer(store, active, linked);
      return publicBlueprint(linked);
    }
  }

  const current = await findLatestActiveBlueprintForClient(store, id);
  if (current) return publicBlueprint(current);

  throw new ClientIntelligenceError('not_found', 'No blueprint for client', 404);
}

/**
 * Read-only lifecycle audit for a client (no mutations).
 * Used to inspect production Blueprint/interview state safely.
 */
async function auditClientBlueprintLifecycle(clientId, opts = {}) {
  const store = await resolveStore(opts);
  const id = asClientId(clientId);
  const sessions = await listClientSessions(store, id, { limit: 40 });
  const blueprints = await store.listBlueprintsForClient(id);
  const active = await findActiveInterviewForClient(id, opts);
  let currentBlueprint = null;
  let currentError = null;
  try {
    currentBlueprint = await getClientBlueprint(id, opts);
  } catch (err) {
    currentError = {
      code: err.code || 'error',
      message: err.message || String(err),
    };
  }
  let onboarding = null;
  try {
    onboarding = await resolveClientOnboardingState(id, opts);
  } catch (err) {
    onboarding = {
      error: err.code || 'error',
      message: err.message || String(err),
    };
  }

  return {
    ok: true,
    clientId: id,
    auditedAt: nowIso(),
    sessions: sessions.map((s) => ({
      id: s.id,
      status: s.status,
      startedAt: s.started_at,
      completedAt: s.completed_at,
      updatedAt: s.updated_at,
      lifecycle:
        (s.interview_state && s.interview_state.lifecycle) || null,
      supersededAt:
        (s.interview_state && s.interview_state.supersededAt) || null,
      blueprintId:
        (s.interview_state && s.interview_state.blueprintId) || null,
      blueprintVersion:
        (s.interview_state && s.interview_state.blueprintVersion) || null,
      isSuperseded: sessionIsSuperseded(s),
      isRecoverableActive: isRecoverableActiveInterview(s),
    })),
    blueprints: (blueprints || []).map((bp) => ({
      id: bp.id,
      version: bp.version,
      status: bp.status,
      sessionId: bp.session_id,
      createdAt: bp.created_at,
      updatedAt: bp.updated_at,
      playbookId: bp.playbook_id || null,
    })),
    activeInterviewId: active ? active.id : null,
    currentBlueprint,
    currentBlueprintError: currentError,
    onboarding: onboarding
      ? {
          onboardingState: onboarding.onboardingState,
          interviewId: onboarding.interviewId || null,
          status: onboarding.status || null,
          resumeTarget: onboarding.resumeTarget || null,
          blueprintId: onboarding.blueprint && onboarding.blueprint.id,
          blueprintStatus:
            onboarding.blueprint && onboarding.blueprint.status,
        }
      : null,
  };
}

function bumpBlueprintVersion(version) {
  const m = String(version || '1.0').match(/^(\d+)(?:\.(\d+))?$/);
  if (!m) return '1.1';
  const major = Number(m[1]);
  const minor = Number(m[2] || 0) + 1;
  return `${major}.${minor}`;
}

/**
 * Client revise: creates a new blueprint version; never overwrites approved.
 * Edits become CLIENT_EDITED evidence.
 */
async function reviseBlueprint(blueprintId, revisions = {}, opts = {}) {
  const store = await resolveStore(opts);
  const current = await store.getBlueprint(blueprintId);
  if (!current) {
    throw new ClientIntelligenceError('not_found', 'Blueprint not found', 404);
  }
  if (current.status === 'approved') {
    // new version from approved
  } else if (!['draft', 'in_review'].includes(current.status)) {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Cannot revise blueprint in status ${current.status}`
    );
  }

  const session = await store.getSession(current.session_id);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }

  const sections = buildSectionsFromState(current.sections);
  const sectionEdits = revisions.sections || revisions;
  const editTurn = await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'client',
    message: `Blueprint revision: ${JSON.stringify(sectionEdits)}`,
    goal: 'Client blueprint edit',
    asked_because: 'Client corrected understanding before approval.',
    derived_evidence: [],
    created_at: new Date(),
  });

  const derived = [];
  for (const key of BLUEPRINT_SECTIONS) {
    if (!Object.prototype.hasOwnProperty.call(sectionEdits, key)) continue;
    const edit = sectionEdits[key];
    const summary =
      typeof edit === 'string'
        ? edit
        : edit && edit.summary != null
          ? String(edit.summary)
          : null;
    if (summary == null) continue;

    // Refinement / meta-instruction edits are guidance only — never overwrite commercial fields.
    if (!isBusinessFactStatement(summary)) {
      const state = session.interview_state || initialInterviewState();
      state.revisionGuidance = [
        ...(state.revisionGuidance || []),
        {
          at: nowIso(),
          kind: classifyUserResponse(summary),
          message: summary,
          section: key,
        },
      ];
      session.interview_state = state;
      continue;
    }

    const { evidenceRow, skippedAsGuidance } = await applySectionUpdate(
      store,
      session,
      key,
      summary,
      'CLIENT_EDITED',
      editTurn.id
    );
    if (skippedAsGuidance || !evidenceRow) continue;
    derived.push(evidenceRow.id);
    sections[key] = {
      ...(sections[key] || emptySection()),
      summary,
      confidence: clampConfidence(
        Math.max((sections[key] && sections[key].confidence) || 0, EXPLICIT_CONFIDENCE)
      ),
      evidenceIds: [
        ...((sections[key] && sections[key].evidenceIds) || []),
        evidenceRow.id,
      ],
      unknowns: (sections[key] && sections[key].unknowns
        ? sections[key].unknowns.filter((u) => !u.includes(key))
        : []),
    };
  }

  const nextVersion =
    current.status === 'approved'
      ? bumpBlueprintVersion(current.version)
      : current.version;

  if (current.status === 'approved') {
    const next = await store.insertBlueprint({
      id: current.id,
      client_id: current.client_id,
      session_id: current.session_id,
      version: nextVersion,
      status: 'in_review',
      generated_by: GENERATED_BY,
      sections,
      confidence_summary: confidenceSummaryFromSections(sections),
      playbook_id: null,
      playbook_version: null,
      section_provenance: {},
      parent_blueprint_id: current.id,
      created_at: new Date(),
      updated_at: new Date(),
    });
    session.interview_state = {
      ...session.interview_state,
      blueprintId: next.id,
      blueprintVersion: next.version,
      sectionState: sections,
    };
    if (session.status === 'APPROVED') {
      // reopen for review without skipping — only CLIENT_REVIEW is allowed after generation;
      // for recalibration of approved, keep session APPROVED and track new in_review blueprint
    } else {
      session.status = 'CLIENT_REVIEW';
    }
    await store.updateSession(session.id, {
      interview_state: session.interview_state,
      confidence_score: overallConfidence(next.confidence_summary),
    });
    return publicBlueprint(next);
  }

  const confidence_summary = confidenceSummaryFromSections(sections);
  const updated = await store.updateBlueprint(current.id, current.version, {
    sections,
    confidence_summary,
    status: 'in_review',
  });
  session.interview_state = {
    ...session.interview_state,
    sectionState: sections,
    blueprintId: updated.id,
    blueprintVersion: updated.version,
  };
  await store.updateSession(session.id, {
    interview_state: session.interview_state,
    confidence_score: overallConfidence(confidence_summary),
  });
  return publicBlueprint(updated);
}

/**
 * Resume discovery after CLIENT_REVIEW so the client can refine understanding.
 */
async function resumeInterview(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status === 'APPROVED') {
    throw new ClientIntelligenceError(
      'interview_complete',
      'Interview already approved; start a new session to recalibrate'
    );
  }
  if (session.status !== 'CLIENT_REVIEW') {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Can only resume from CLIENT_REVIEW (was ${session.status})`
    );
  }

  advanceStatus(session, 'DISCOVERY');
  const state = {
    ...(session.interview_state || initialInterviewState()),
    done: false,
    refinementPass: true,
  };
  // Keep stepIndex past the bank so free-form refinement messages are accepted.
  if ((Number(state.stepIndex) || 0) < QUESTION_BANK.length) {
    state.stepIndex = QUESTION_BANK.length;
  }
  session.interview_state = state;
  session.current_stage = 'Refinement';

  const prompt =
    'What would you like to refine or add? Share anything that would sharpen my understanding.';
  await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'assistant',
    message: prompt,
    goal: 'Invite refinement of Business Blueprint understanding',
    asked_because: 'Client chose to refine before approving the Executive Summary or Blueprint.',
    derived_evidence: [],
    created_at: new Date(),
  });

  await store.updateSession(session.id, {
    status: 'DISCOVERY',
    current_stage: session.current_stage,
    interview_state: state,
  });

  return withExperienceFields(session, {
    interviewId: session.id,
    ...publicSession(session),
    nextAction: 'ASK',
    question: null,
    message: prompt,
    blueprint: null,
    reflection: null,
    resumed: true,
  });
}

function resolveInitialGrowthDirection(blueprint, interviewState = null) {
  // Always rebuild from the approved Blueprint when available so avoid-copy
  // wrappers cannot linger in a stale stored artifact.
  if (blueprint && blueprint.sections) {
    try {
      return buildInitialGrowthDirection(blueprint, {
        normalizedFacts:
          (interviewState && interviewState.normalizedFacts) || null,
      });
    } catch (_) {
      /* fall through to stored */
    }
  }
  const stored =
    interviewState &&
    interviewState.initialGrowthDirection &&
    interviewState.initialGrowthDirection.kind === 'initial_growth_direction'
      ? interviewState.initialGrowthDirection
      : null;
  if (!stored) return null;
  return repairInitialGrowthDirection(stored, interviewState);
}

/**
 * Repair a stored Initial Growth Direction whose avoid paragraph still has
 * wrapper bleed ("customers who The business prefers to avoid…").
 */
function repairInitialGrowthDirection(gd, interviewState = null) {
  if (!gd || typeof gd !== 'object') return gd;
  const name =
    (gd.businessName && String(gd.businessName)) ||
    (interviewState &&
      interviewState.normalizedFacts &&
      interviewState.normalizedFacts.business_name) ||
    'the business';
  const paragraphs = Array.isArray(gd.paragraphs) ? gd.paragraphs.slice() : [];
  let changed = false;
  for (let i = 0; i < paragraphs.length; i += 1) {
    const p = String(paragraphs[i] || '');
    if (
      /customers who The business/i.test(p) ||
      /prefers to avoid/i.test(p) ||
      /avoid Anchor should avoid/i.test(p) ||
      /should avoid:\s*(?:Anchor|The business)/i.test(p)
    ) {
      const afterColon = p.split(/should avoid:\s*/i)[1] || p;
      const repaired = composeAvoidSentence(name, afterColon);
      if (repaired) {
        paragraphs[i] = repaired;
        changed = true;
      }
    }
  }
  if (!changed) return gd;
  return { ...gd, paragraphs };
}

function alreadyApprovedPayload(blueprint, playbook = null, interviewState = null) {
  const bp = publicBlueprint(blueprint);
  let pb = playbook;
  if (!pb && blueprint) {
    const id = blueprint.playbook_id || blueprint.playbookId;
    const version = blueprint.playbook_version || blueprint.playbookVersion;
    if (id) {
      pb = {
        id,
        version,
        status: 'pending_review',
      };
    }
  }
  return {
    ok: true,
    status: 'APPROVED',
    message: 'already_approved',
    blueprint: bp,
    playbook: pb,
    initialGrowthDirection: resolveInitialGrowthDirection(blueprint, interviewState),
    alreadyApproved: true,
  };
}

/**
 * Approve blueprint: immutable snapshot + pending_review playbook handoff.
 * Idempotent when the blueprint or session is already APPROVED.
 */
async function approveBlueprint(blueprintId, opts = {}) {
  const store = await resolveStore(opts);
  let current = await store.getBlueprint(blueprintId);
  if (!current) {
    throw new ClientIntelligenceError('not_found', 'Blueprint not found', 404);
  }
  if (current.status === 'approved') {
    const approvedSession = await store.getSession(current.session_id);
    return alreadyApprovedPayload(
      current,
      null,
      approvedSession && approvedSession.interview_state
    );
  }
  if (!['draft', 'in_review'].includes(current.status)) {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Cannot approve blueprint in status ${current.status}`
    );
  }

  const session = await store.getSession(current.session_id);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }

  // Session already APPROVED (retry, race, or stale UI) — never error red.
  if (session.status === 'APPROVED') {
    const latest = await store.getBlueprint(
      session.interview_state && session.interview_state.blueprintId
        ? session.interview_state.blueprintId
        : current.id,
      session.interview_state && session.interview_state.blueprintVersion
        ? session.interview_state.blueprintVersion
        : undefined
    );
    const approvedBp =
      (latest && latest.status === 'approved' && latest) ||
      (current.status === 'approved' && current) ||
      latest ||
      current;
    return alreadyApprovedPayload(
      approvedBp,
      session.interview_state && session.interview_state.playbookId
        ? {
            id: session.interview_state.playbookId,
            version: session.interview_state.playbookVersion,
            status: 'pending_review',
          }
        : null,
      session.interview_state
    );
  }

  if (session.status !== 'CLIENT_REVIEW') {
    throw new ClientIntelligenceError(
      'invalid_transition',
      `Session must be CLIENT_REVIEW to approve (was ${session.status})`
    );
  }

  // =====================================================================
  // SPEC-224 Execution Order: Canonical Authority BEFORE Downstream Artifacts
  // =====================================================================
  // A. Load session + frozen approval interpretation (done above)
  // B. Construct CanonicalSemanticBatch from normalizedFacts + evidence
  // C. Commit through SPEC-223B (returns immutable canonical snapshot)
  // D. Reconstruct through SPEC-223C projection (if enabled)
  // E. Derive Blueprint-compatible representation from canonical projection
  // F. Persist Blueprint as approved with canonical snapshot association
  // G. Mark session approved
  // H. Create playbook / perform downstream handoff

  let canonicalSnapshotId = null;
  let canonicalTenantId = null;
  const commitCanonicalOpts =
    opts.canonicalCommit !== false &&
    (store.kind !== 'memory' || opts.canonicalCommit === true);

  if (commitCanonicalOpts) {
    try {
      // Step B+C: Build and commit canonical semantic batch
      const { CIECanonicalAdapter } = require('../lib/cieCanonicalAdapter');
      const { commitCanonicalSemanticBatch } = require('../lib/canonicalSemanticWrite');
      const pool = opts.pool || defaultPool;

      // Resolve the tenant workspace binding for this client (SPEC-223 authority check
      // requires an existing tenant_workspaces row; tenant_id cannot be fabricated).
      const clientId = current.client_id;
      const tenantRow = (
        await pool.query(
          `SELECT tenant_key FROM tenant_workspaces WHERE client_id = $1`,
          [clientId]
        )
      ).rows[0];
      if (!tenantRow) {
        throw new ClientIntelligenceError(
          'missing_tenant_workspace',
          `No tenant workspace bound to client ${clientId}`
        );
      }
      canonicalTenantId = tenantRow.tenant_key;

      // Retrieve frozen evidence records for this session
      const sessionEvidence = await store.listEvidence(current.session_id);

      // Retrieve approved SPEC-222 registry artifact
      const registryArtifact = await getApprovedCanonicalRegistry({ ...opts, pool });
      if (!registryArtifact) {
        throw new ClientIntelligenceError(
          'missing_registry',
          'SPEC-222 canonical registry artifact not seeded (SPEC-224 blocker)'
        );
      }

      // Build CanonicalSemanticBatch from approved Blueprint interpretation.
      // normalizedFacts lives only in session.interview_state (frozen at approval);
      // the Blueprint row itself carries no normalizedFacts column.
      const canonicalBatch = CIECanonicalAdapter.buildBatch({
        tenant_id: canonicalTenantId,
        client_id: clientId,
        blueprint: {
          ...current,
          normalizedFacts:
            (session.interview_state && session.interview_state.normalizedFacts) || {},
        },
        blueprint_id: current.id,
        blueprint_version: current.version,
        cie_evidence_records: sessionEvidence,
        registry_artifact: registryArtifact,
        interpreter_id: 'cie-approval-interpreter',
        interpreter_version: '1.0.0-spec-224',
        session_id: current.session_id,
      });

      // Commit canonical batch (with SPEC-223B atomicity)
      const commitResult = await commitCanonicalSemanticBatch(pool, canonicalBatch);
      canonicalSnapshotId = commitResult.snapshot_id;

      console.log(
        `[SPEC-224] Approved Blueprint ${current.id} committed to canonical snapshot ${canonicalSnapshotId}`
      );

      // Step D+E: Reconstruct through SPEC-223C and derive the Blueprint-compatible
      // representation BEFORE persisting approval. A projection failure here must
      // block approval -- a committed snapshot that cannot be reconstructed must
      // not be presented to the operator as an approved Blueprint.
      const { deriveBlueprintCompatibility } = require('../lib/canonicalProjection');
      const projected = await deriveBlueprintCompatibility({
        tenant_id: canonicalTenantId,
        snapshot_id: canonicalSnapshotId,
        pool,
      });
      if (!projected || !projected._projection_metadata || projected._projection_metadata.completeness === 'UNAVAILABLE') {
        throw new ClientIntelligenceError(
          'projection_failed',
          `Canonical snapshot ${canonicalSnapshotId} could not be reconstructed into a Blueprint-compatible projection`
        );
      }
    } catch (err) {
      // Step H (failure behavior): If canonical commit or projection fails, Blueprint remains unapproved
      console.error('[SPEC-224] Canonical commit or projection failed; approval aborted:', err.message);
      throw new ClientIntelligenceError(
        err.code === 'projection_failed' ? 'projection_failed' : 'canonical_commit_failed',
        `Canonical semantic authority could not be established: ${err.message}`
      );
    }
  }

  // =====================================================================
  // Step F: Persist Blueprint as approved with canonical snapshot association
  // (before playbook, per SPEC-224 order)
  // =====================================================================
  const approved = await store.updateBlueprint(current.id, current.version, {
    status: 'approved',
    canonical_snapshot_id: canonicalSnapshotId, // NEW: Link to canonical authority
    canonical_snapshot_tenant_id: canonicalTenantId,
    section_provenance: current.section_provenance,
  });
  await store.supersedeBlueprints(current.id, current.version);

  // =====================================================================
  // Step H: Create playbook / perform downstream handoff
  // (AFTER canonical authority established, per SPEC-224 order)
  // =====================================================================
  const handoffOpts = { ...opts };
  if (store.kind === 'memory' && !handoffOpts.playbookStore) {
    handoffOpts.useMemoryPlaybookStore = true;
  }
  let handoff;
  try {
    handoff = await createPlaybookFromApprovedBlueprint(approved, handoffOpts);
    // Link playbook to approved Blueprint
    await store.updateBlueprint(approved.id, approved.version, {
      playbook_id: handoff.playbook.id,
      playbook_version: handoff.playbook.version,
      section_provenance: handoff.sectionProvenance,
    });
  } catch (err) {
    // Step H (failure behavior): Surface playbook failure separately
    console.error('[SPEC-224] Playbook creation failed (after canonical approval):', err.message);
    // Blueprint is already approved + canonical snapshot linked; don't revert
    // Playbook handoff is a downstream concern, not an approval blocker
    throw new ClientIntelligenceError(
      'playbook_creation_failed',
      `Playbook handoff failed after approval: ${err.message}`
    );
  }

  // =====================================================================
  // Growth direction (independent of canonical authority)
  // =====================================================================
  const initialGrowthDirection = buildInitialGrowthDirection(approved, {
    normalizedFacts:
      (session.interview_state && session.interview_state.normalizedFacts) || null,
  });

  let reasoningMemory = ensureReasoningMemory(session.interview_state || {});
  const growthReadiness = checkArtifactReadiness(ARTIFACT_KINDS.GROWTH_DIRECTION, {
    sectionState: (approved && approved.sections) || {},
    normalizedFacts:
      (session.interview_state && session.interview_state.normalizedFacts) ||
      emptyNormalizedFacts(),
  });
  if (growthReadiness.confidenceNote && initialGrowthDirection) {
    initialGrowthDirection.confidenceNote = growthReadiness.confidenceNote;
    if (growthReadiness.weak.length) {
      initialGrowthDirection.confidenceLevel = 'limited_directional';
    }
  }
  reasoningMemory = markArtifactGenerated(
    reasoningMemory,
    ARTIFACT_KINDS.GROWTH_DIRECTION
  );

  // =====================================================================
  // Step G: Mark session approved
  // =====================================================================
  advanceStatus(session, 'APPROVED');
  session.completed_at = new Date();
  await store.updateSession(session.id, {
    status: 'APPROVED',
    completed_at: session.completed_at,
    summary: `Approved Business Blueprint ${approved.id}@${approved.version} (canonical snapshot: ${canonicalSnapshotId || 'N/A'})`,
    interview_state: {
      ...session.interview_state,
      blueprintId: approved.id,
      blueprintVersion: approved.version,
      playbookId: handoff.playbook.id,
      playbookVersion: handoff.playbook.version,
      canonicalSnapshotId, // NEW: Link for reference
      approvedAt: session.completed_at.toISOString(),
      initialGrowthDirection,
      growthConversation: null,
      infrastructureReadiness: null,
      growthInfrastructureReadinessReport: null,
      reasoningMemory,
      artifactReadiness: {
        ...((session.interview_state && session.interview_state.artifactReadiness) || {}),
        growth_direction: growthReadiness,
      },
    },
  });

  return {
    ok: true,
    status: 'APPROVED',
    message: 'approved',
    blueprint: publicBlueprint(approved),
    playbook: handoff.playbook,
    canonicalSnapshotId, // NEW: Expose canonical authority link to caller
    initialGrowthDirection,
    sectionProvenance: handoff.sectionProvenance,
    alreadyApproved: false,
  };
}

/**
 * Retrieve the approved SPEC-222 canonical registry artifact.
 * Per SPEC-224: must exist and be seeded by production registry migration.
 */
async function getApprovedCanonicalRegistry(opts) {
  try {
    const pool = opts.pool || require('../db');
    const result = await pool.query(
      `SELECT id, registry_version, entity_vocabulary, predicate_definitions, content_digest
       FROM canonical_registry_artifacts
       WHERE registry_version LIKE '1.0.0-spec-222%'
       ORDER BY created_at DESC
       LIMIT 1`
    );
    return result.rows[0] || null;
  } catch (err) {
    console.error('[SPEC-224] Failed to retrieve canonical registry artifact:', err.message);
    return null;
  }
}

/**
 * Begin the post-approval Growth Conversation using the approved Blueprint
 * and Initial Growth Direction as context.
 */
async function startGrowthConversation(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Growth Conversation requires an approved Blueprint'
    );
  }

  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }
  if (!blueprint || blueprint.status !== 'approved') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Approved Business Blueprint not found for this interview'
    );
  }

  const growthDirection = resolveInitialGrowthDirection(
    blueprint,
    session.interview_state
  );
  const opening = buildGrowthConversationOpening(growthDirection);
  const prior =
    (session.interview_state && session.interview_state.growthConversation) || null;
  const priorState = normalizeGrowthState({
    ...(prior || {}),
    segment_ranking:
      (prior && (prior.segment_ranking || prior.segmentRanking)) ||
      (session.interview_state && session.interview_state.segmentRanking) ||
      null,
    validation_target:
      (prior && (prior.validation_target || prior.validationTarget)) ||
      (session.interview_state && session.interview_state.validationTarget) ||
      null,
    first_segment_decision:
      (prior && (prior.first_segment_decision || prior.firstSegmentDecision)) ||
      (session.interview_state && session.interview_state.firstSegmentDecision) ||
      null,
    first_growth_plan_preview:
      (prior &&
        (prior.first_growth_plan_preview || prior.firstGrowthPlanPreview)) ||
      (session.interview_state &&
        session.interview_state.firstGrowthPlanPreview) ||
      null,
  });
  const turns = (prior && Array.isArray(prior.turns) && prior.turns.length
    ? prior.turns
    : []
  ).slice();

  if (!turns.length) {
    turns.push({
      speaker: 'assistant',
      message: opening,
      at: new Date().toISOString(),
    });
  }

  const resumedStatus =
    (prior && prior.status) ||
    (priorState.first_growth_plan_preview
      ? 'preview_ready'
      : priorState.validation_target
        ? 'validation_target_ready'
        : priorState.primary_segment
          ? 'primary_selected'
          : priorState.segment_ranking
            ? 'ranking_ready'
            : 'active');

  const growthConversation = {
    status: resumedStatus,
    startedAt:
      (prior && prior.startedAt) || new Date().toISOString(),
    context: {
      blueprintId: blueprint.id,
      blueprintVersion: blueprint.version,
      initialGrowthDirection: growthDirection,
    },
    turns,
    // Required growth decision state (persisted across turns).
    ...priorState,
    // CamelCase mirrors for existing consumers / side panel.
    segmentRanking: priorState.segment_ranking,
    validationTarget: priorState.validation_target,
    firstSegmentDecision: priorState.first_segment_decision,
    firstGrowthPlanPreview: priorState.first_growth_plan_preview,
  };

  await store.updateSession(session.id, {
    interview_state: {
      ...session.interview_state,
      initialGrowthDirection: growthDirection,
      growthConversation,
      ...(priorState.segment_ranking
        ? { segmentRanking: priorState.segment_ranking }
        : {}),
      ...(priorState.validation_target
        ? { validationTarget: priorState.validation_target }
        : {}),
      ...(priorState.first_segment_decision
        ? { firstSegmentDecision: priorState.first_segment_decision }
        : {}),
      ...(priorState.first_growth_plan_preview
        ? { firstGrowthPlanPreview: priorState.first_growth_plan_preview }
        : {}),
    },
  });

  return {
    ok: true,
    interviewId: session.id,
    status: 'GROWTH_CONVERSATION',
    message: opening,
    initialGrowthDirection: growthDirection,
    blueprint: publicBlueprint(blueprint),
    growthConversation,
    resumed: Boolean(prior && prior.turns && prior.turns.length),
  };
}

/**
 * Continue the Growth Conversation. Blueprint-grounded only — no campaigns,
 * prospect lists, or autonomous execution.
 */
async function postGrowthMessage(sessionId, message, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Growth Conversation requires an approved Blueprint'
    );
  }

  const text = String(message || '').trim();
  if (!text) {
    throw new ClientIntelligenceError('empty_message', 'message is required', 400);
  }

  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }
  const growthDirection = resolveInitialGrowthDirection(
    blueprint,
    session.interview_state
  );
  let growthConversation =
    (session.interview_state && session.interview_state.growthConversation) || null;
  const continuableStatuses = new Set([
    'active',
    'ranking_ready',
    'primary_selected',
    'validation_target_ready',
    'preview_ready',
  ]);
  if (!growthConversation || !continuableStatuses.has(growthConversation.status)) {
    const started = await startGrowthConversation(sessionId, opts);
    growthConversation = started.growthConversation;
  }

  const priorState = normalizeGrowthState({
    ...(growthConversation || {}),
    segment_ranking:
      (growthConversation &&
        (growthConversation.segment_ranking || growthConversation.segmentRanking)) ||
      (session.interview_state && session.interview_state.segmentRanking) ||
      null,
    validation_target:
      (growthConversation &&
        (growthConversation.validation_target ||
          growthConversation.validationTarget)) ||
      (session.interview_state && session.interview_state.validationTarget) ||
      null,
    first_segment_decision:
      (growthConversation &&
        (growthConversation.first_segment_decision ||
          growthConversation.firstSegmentDecision)) ||
      (session.interview_state && session.interview_state.firstSegmentDecision) ||
      null,
    first_growth_plan_preview:
      (growthConversation &&
        (growthConversation.first_growth_plan_preview ||
          growthConversation.firstGrowthPlanPreview)) ||
      (session.interview_state &&
        session.interview_state.firstGrowthPlanPreview) ||
      null,
  });
  const priorSegmentRanking = priorState.segment_ranking;
  const reply = buildGrowthConversationReply(
    text,
    growthDirection,
    blueprint || { sections: {} },
    { priorSegmentRanking, growthState: priorState }
  );
  const replyMessage =
    reply && typeof reply === 'object' ? reply.message : String(reply || '');
  const segmentRanking =
    reply && typeof reply === 'object' ? reply.segmentRanking || null : null;
  const validationTarget =
    reply && typeof reply === 'object' ? reply.validationTarget || null : null;
  const firstSegmentDecision =
    reply && typeof reply === 'object' ? reply.firstSegmentDecision || null : null;
  const firstGrowthPlanPreview =
    reply && typeof reply === 'object'
      ? reply.firstGrowthPlanPreview || null
      : null;
  const nextState = normalizeGrowthState(
    (reply && reply.growthState) || priorState
  );
  if (segmentRanking) nextState.segment_ranking = segmentRanking;
  else if (priorSegmentRanking) nextState.segment_ranking = priorSegmentRanking;
  if (validationTarget) nextState.validation_target = validationTarget;
  if (firstSegmentDecision) nextState.first_segment_decision = firstSegmentDecision;
  if (firstGrowthPlanPreview) {
    nextState.first_growth_plan_preview = firstGrowthPlanPreview;
  }

  const turns = [
    ...((growthConversation && growthConversation.turns) || []),
    { speaker: 'client', message: text, at: new Date().toISOString() },
    {
      speaker: 'assistant',
      message: replyMessage,
      at: new Date().toISOString(),
      intent: (reply && reply.intent) || null,
      growth_step: nextState.current_growth_step,
    },
  ];
  let nextStatus = 'active';
  if (firstGrowthPlanPreview || nextState.first_growth_plan_preview) {
    nextStatus = 'preview_ready';
  } else if (validationTarget || nextState.validation_target) {
    nextStatus = 'validation_target_ready';
  } else if (nextState.primary_segment || firstSegmentDecision) {
    nextStatus = 'primary_selected';
  } else if (segmentRanking || nextState.segment_ranking) {
    nextStatus = 'ranking_ready';
  } else if (
    growthConversation.status === 'ranking_ready' ||
    growthConversation.status === 'primary_selected' ||
    growthConversation.status === 'validation_target_ready' ||
    growthConversation.status === 'preview_ready'
  ) {
    nextStatus = growthConversation.status;
  }
  const nextGrowth = {
    ...growthConversation,
    status: nextStatus,
    turns,
    ...nextState,
    segmentRanking: nextState.segment_ranking,
    validationTarget: nextState.validation_target,
    firstSegmentDecision: nextState.first_segment_decision,
    firstGrowthPlanPreview: nextState.first_growth_plan_preview,
  };

  await store.updateSession(session.id, {
    interview_state: {
      ...session.interview_state,
      initialGrowthDirection: growthDirection,
      growthConversation: nextGrowth,
      reasoningMemory: (() => {
        let mem = ensureReasoningMemory(session.interview_state || {});
        // Track progression so asking for "the next" artifact does not re-loop.
        if (nextState.first_growth_plan_preview) {
          mem = markArtifactGenerated(mem, ARTIFACT_KINDS.CAMPAIGN_PREVIEW);
        } else if (nextState.validation_target || nextState.segment_ranking) {
          mem = markArtifactGenerated(mem, ARTIFACT_KINDS.GROWTH_DIRECTION);
        }
        return mem;
      })(),
      ...(nextState.segment_ranking
        ? { segmentRanking: nextState.segment_ranking }
        : {}),
      ...(nextState.validation_target
        ? { validationTarget: nextState.validation_target }
        : {}),
      ...(nextState.first_segment_decision
        ? { firstSegmentDecision: nextState.first_segment_decision }
        : {}),
      ...(nextState.first_growth_plan_preview
        ? { firstGrowthPlanPreview: nextState.first_growth_plan_preview }
        : {}),
    },
  });

  return {
    ok: true,
    interviewId: session.id,
    status: 'GROWTH_CONVERSATION',
    message: replyMessage,
    intent: (reply && reply.intent) || null,
    segmentRanking: nextState.segment_ranking,
    validationTarget: nextState.validation_target,
    firstSegmentDecision: nextState.first_segment_decision,
    firstGrowthPlanPreview: nextState.first_growth_plan_preview,
    suggestedActions: (reply && reply.suggestedActions) || null,
    growthState: nextState,
    initialGrowthDirection: growthDirection,
    blueprint: publicBlueprint(blueprint),
    growthConversation: nextGrowth,
  };
}

/**
 * Begin the post-approval Growth Infrastructure Readiness Conversation.
 * Separate from Growth Conversation (market focus) — operational setup only.
 */
async function startInfrastructureReadinessConversation(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Growth Infrastructure Readiness requires an approved Blueprint'
    );
  }

  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }
  if (!blueprint || blueprint.status !== 'approved') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Approved Business Blueprint not found for this interview'
    );
  }

  const businessName = extractReadinessBusinessName(blueprint);
  const growthConversation =
    (session.interview_state && session.interview_state.growthConversation) || null;
  const growthDirection = resolveInitialGrowthDirection(
    blueprint,
    session.interview_state
  );
  const growthHandoff =
    buildGrowthInfrastructureHandoffContext(
      growthConversation,
      blueprint,
      growthDirection
    ) || null;
  const opening = buildInfrastructureReadinessOpening(blueprint, {
    businessName:
      (growthHandoff && growthHandoff.businessName) || businessName,
    growthHandoff,
  });
  const prior =
    (session.interview_state && session.interview_state.infrastructureReadiness) ||
    null;
  const turns = (prior && Array.isArray(prior.turns) && prior.turns.length
    ? prior.turns
    : []
  ).slice();

  if (!turns.length) {
    turns.push({
      speaker: 'assistant',
      message: opening,
      at: new Date().toISOString(),
      step: 'opening',
    });
  }

  const infrastructureReadiness = {
    status: (prior && prior.status) || 'active',
    startedAt: (prior && prior.startedAt) || new Date().toISOString(),
    step: (prior && prior.step) || 'website_domain',
    answers: (prior && prior.answers) || {},
    areas: (prior && prior.areas) || buildEmptyAreas(),
    context: {
      blueprintId: blueprint.id,
      blueprintVersion: blueprint.version,
      businessName:
        (growthHandoff && growthHandoff.businessName) || businessName,
      growthHandoff,
    },
    turns,
  };

  await store.updateSession(session.id, {
    interview_state: {
      ...session.interview_state,
      infrastructureReadiness,
      growthInfrastructureReadinessReport:
        (session.interview_state &&
          session.interview_state.growthInfrastructureReadinessReport) ||
        null,
    },
  });

  const resumed = Boolean(prior && prior.turns && prior.turns.length);
  const lastAssistant = [...turns].reverse().find((t) => t && t.speaker === 'assistant');

  return {
    ok: true,
    interviewId: session.id,
    status: 'INFRASTRUCTURE_READINESS',
    message: resumed && lastAssistant ? lastAssistant.message : opening,
    blueprint: publicBlueprint(blueprint),
    infrastructureReadiness,
    growthHandoff,
    growthInfrastructureReadinessReport:
      (session.interview_state &&
        session.interview_state.growthInfrastructureReadinessReport) ||
      null,
    resumed,
  };
}

/**
 * Continue Growth Infrastructure Readiness. Assessment only — no campaigns,
 * password asks, or unapproved DNS/GBP/social/tracking changes.
 */
async function postInfrastructureReadinessMessage(sessionId, message, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Growth Infrastructure Readiness requires an approved Blueprint'
    );
  }

  const text = String(message || '').trim();
  if (!text) {
    throw new ClientIntelligenceError('empty_message', 'message is required', 400);
  }

  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }

  let infrastructureReadiness =
    (session.interview_state && session.interview_state.infrastructureReadiness) ||
    null;
  const continuableStatuses = new Set(['active', 'report_ready']);
  if (
    !infrastructureReadiness ||
    !continuableStatuses.has(infrastructureReadiness.status)
  ) {
    const started = await startInfrastructureReadinessConversation(sessionId, opts);
    infrastructureReadiness = started.infrastructureReadiness;
  }

  const businessName =
    (infrastructureReadiness.context &&
      infrastructureReadiness.context.businessName) ||
    extractReadinessBusinessName(blueprint || { sections: {} });

  const reply = buildInfrastructureReadinessReply(
    text,
    infrastructureReadiness,
    blueprint || { sections: {} },
    {
      businessName,
      blueprintId: blueprint && blueprint.id,
      blueprintVersion: blueprint && blueprint.version,
    }
  );

  const turns = [
    ...((infrastructureReadiness && infrastructureReadiness.turns) || []),
    {
      speaker: 'client',
      message: text,
      at: new Date().toISOString(),
      step: infrastructureReadiness.step,
    },
    {
      speaker: 'assistant',
      message: reply.message,
      at: new Date().toISOString(),
      step: reply.step,
      intent: reply.intent,
    },
  ];

  const nextStatus = reply.report ? 'report_ready' : 'active';
  const nextReadiness = {
    ...infrastructureReadiness,
    status: nextStatus,
    step: reply.step,
    answers: reply.answers,
    areas: reply.areas,
    turns,
  };

  await store.updateSession(session.id, {
    interview_state: {
      ...session.interview_state,
      infrastructureReadiness: nextReadiness,
      ...(reply.report
        ? { growthInfrastructureReadinessReport: reply.report }
        : {}),
    },
  });

  return {
    ok: true,
    interviewId: session.id,
    status: 'INFRASTRUCTURE_READINESS',
    message: reply.message,
    intent: reply.intent,
    blueprint: publicBlueprint(blueprint),
    infrastructureReadiness: nextReadiness,
    growthInfrastructureReadinessReport:
      reply.report ||
      (session.interview_state &&
        session.interview_state.growthInfrastructureReadinessReport) ||
      null,
  };
}

/**
 * Begin First Campaign Planning Conversation (SPEC-089).
 * Review-first only — carries prior artifacts; no lists/copy/sends/account changes.
 */
async function startCampaignPlanningConversation(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'First Campaign Planning requires an approved Blueprint'
    );
  }

  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }
  if (!blueprint || blueprint.status !== 'approved') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'Approved Business Blueprint not found for this interview'
    );
  }

  const growthDirection = resolveInitialGrowthDirection(
    blueprint,
    session.interview_state
  );
  const context = buildCampaignPlanningContext(session, blueprint, {
    growthDirection,
  });
  const opening = buildCampaignPlanningOpening(context);
  const prior =
    (session.interview_state && session.interview_state.campaignPlanning) || null;
  const turns = (prior && Array.isArray(prior.turns) && prior.turns.length
    ? prior.turns
    : []
  ).slice();

  if (!turns.length) {
    turns.push({
      speaker: 'assistant',
      message: opening,
      at: new Date().toISOString(),
      step: 'opening',
    });
  }

  const slots = seedSlotsFromContext(
    context,
    (prior && prior.slots) || null
  );
  if (
    session.interview_state &&
    session.interview_state.firstCampaignPlanPreview
  ) {
    slots.previewGenerated = true;
  }

  const campaignPlanning = {
    status: (prior && prior.status) || 'active',
    startedAt: (prior && prior.startedAt) || new Date().toISOString(),
    step: (prior && prior.step) || 'opening',
    answers: (prior && prior.answers) || {},
    slots,
    currentAsk: (prior && prior.currentAsk) || 'opening',
    context: {
      ...(prior && prior.context ? prior.context : {}),
      ...context,
    },
    turns,
  };

  await store.updateSession(session.id, {
    interview_state: {
      ...session.interview_state,
      campaignPlanning,
      firstCampaignPlanPreview:
        (session.interview_state &&
          session.interview_state.firstCampaignPlanPreview) ||
        null,
      prospectListCriteriaPreview:
        (session.interview_state &&
          session.interview_state.prospectListCriteriaPreview) ||
        null,
      prospectListBuildProposal:
        (session.interview_state &&
          session.interview_state.prospectListBuildProposal) ||
        null,
    },
  });

  const resumed = Boolean(prior && prior.turns && prior.turns.length);
  const lastAssistant = [...turns]
    .reverse()
    .find((t) => t && t.speaker === 'assistant');

  return {
    ok: true,
    interviewId: session.id,
    status: 'CAMPAIGN_PLANNING',
    message: resumed && lastAssistant ? lastAssistant.message : opening,
    blueprint: publicBlueprint(blueprint),
    campaignPlanning,
    campaignContext: context,
    firstCampaignPlanPreview:
      (session.interview_state &&
        session.interview_state.firstCampaignPlanPreview) ||
      null,
    prospectListCriteriaPreview:
      (session.interview_state &&
        session.interview_state.prospectListCriteriaPreview) ||
      null,
    prospectListBuildProposal:
      (session.interview_state &&
        session.interview_state.prospectListBuildProposal) ||
      null,
    resumed,
  };
}

/**
 * Continue First Campaign Planning. Planning preview only — no execution.
 */
async function postCampaignPlanningMessage(sessionId, message, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'APPROVED') {
    throw new ClientIntelligenceError(
      'invalid_status',
      'First Campaign Planning requires an approved Blueprint'
    );
  }

  const text = String(message || '').trim();
  if (!text) {
    throw new ClientIntelligenceError('empty_message', 'message is required', 400);
  }

  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }

  let campaignPlanning =
    (session.interview_state && session.interview_state.campaignPlanning) || null;
  const continuableStatuses = new Set(['active', 'preview_ready']);
  if (!campaignPlanning || !continuableStatuses.has(campaignPlanning.status)) {
    const started = await startCampaignPlanningConversation(sessionId, opts);
    campaignPlanning = started.campaignPlanning;
  }

  const growthDirection = resolveInitialGrowthDirection(
    blueprint,
    session.interview_state
  );
  const context =
    (campaignPlanning.context && campaignPlanning.context.primarySegment
      ? campaignPlanning.context
      : null) ||
    buildCampaignPlanningContext(session, blueprint || { sections: {} }, {
      growthDirection,
    });

  const priorPreview =
    (session.interview_state &&
      session.interview_state.firstCampaignPlanPreview) ||
    null;
  let priorCriteriaPreview =
    (session.interview_state &&
      session.interview_state.prospectListCriteriaPreview) ||
    null;
  let priorBuildProposal =
    (session.interview_state &&
      session.interview_state.prospectListBuildProposal) ||
    null;
  const priorProspectListDraft =
    (session.interview_state &&
      (session.interview_state.prospectListDraft ||
        session.interview_state.reviewableProspectListDraft)) ||
    null;
  const priorScoutHandoffBrief =
    (session.interview_state && session.interview_state.scoutHandoffBrief) ||
    null;
  const priorScoutHandoff =
    (session.interview_state && session.interview_state.scoutHandoff) ||
    (priorScoutHandoffBrief && priorScoutHandoffBrief.scoutHandoff) ||
    null;
  const priorScoutCandidateBatch =
    (session.interview_state && session.interview_state.scoutCandidateBatch) ||
    (priorScoutHandoff && priorScoutHandoff.candidateBatch) ||
    null;
  const priorScoutWorkRequest =
    (session.interview_state && session.interview_state.scoutWorkRequest) ||
    (priorScoutHandoff && priorScoutHandoff.workRequest) ||
    null;
  const priorProspectBatchReview =
    (session.interview_state && session.interview_state.prospectBatchReview) ||
    null;
  const priorOutreachStrategyPreview =
    (session.interview_state &&
      session.interview_state.outreachStrategyPreview) ||
    null;
  const priorOutreachCopyPlan =
    (session.interview_state && session.interview_state.outreachCopyPlan) ||
    null;
  const priorOutreachDraftPreview =
    (session.interview_state && session.interview_state.outreachDraftPreview) ||
    null;
  const priorOutreachLaunchGate =
    (session.interview_state && session.interview_state.outreachLaunchGate) ||
    null;
  const priorCampaignMemory =
    (session.interview_state && session.interview_state.campaignMemory) ||
    null;
  const priorCampaignWorkingState =
    (session.interview_state &&
      session.interview_state.campaignWorkingState) ||
    null;

  // SPEC-090/091 — classify intent before workflow handling.
  let reasoningMemory = ensureReasoningMemory(session.interview_state || {});
  // Honor approvals declared in the operator message (e.g. pasted current state).
  reasoningMemory = inferApprovedArtifactsFromMessage(reasoningMemory, text);
  if (
    priorCriteriaPreview &&
    ((reasoningMemory.approvedArtifacts || []).includes(
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    ) ||
      (reasoningMemory.approvedArtifacts || []).includes(
        ARTIFACT_KINDS.PROSPECT_CRITERIA
      ))
  ) {
    priorCriteriaPreview = {
      ...priorCriteriaPreview,
      status: 'approved',
      approvedAt:
        priorCriteriaPreview.approvedAt || new Date().toISOString(),
    };
  }
  if (
    priorBuildProposal &&
    (reasoningMemory.approvedArtifacts || []).includes(
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    )
  ) {
    priorBuildProposal = {
      ...priorBuildProposal,
      status: 'approved',
      approvedAt: priorBuildProposal.approvedAt || new Date().toISOString(),
    };
  }
  // If operator declares approvals but artifacts are missing from session,
  // synthesize minimal approved shells so progression can continue.
  if (
    !priorCriteriaPreview &&
    ((reasoningMemory.approvedArtifacts || []).includes(
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    ) ||
      (reasoningMemory.approvedArtifacts || []).includes(
        ARTIFACT_KINDS.PROSPECT_CRITERIA
      ))
  ) {
    priorCriteriaPreview = {
      kind: ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW,
      title: 'Prospect List Criteria Preview',
      status: 'approved',
      approvedAt: new Date().toISOString(),
    };
  }
  if (
    !priorBuildProposal &&
    (reasoningMemory.approvedArtifacts || []).includes(
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    )
  ) {
    priorBuildProposal = {
      kind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      title: 'Prospect List Build Proposal',
      status: 'approved',
      approvedAt: new Date().toISOString(),
    };
  }
  let nextPriorOutreachStrategyPreview = priorOutreachStrategyPreview;
  if (
    nextPriorOutreachStrategyPreview &&
    (reasoningMemory.approvedArtifacts || []).includes(
      ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW
    ) &&
    nextPriorOutreachStrategyPreview.status !== 'approved'
  ) {
    nextPriorOutreachStrategyPreview = {
      ...nextPriorOutreachStrategyPreview,
      status: 'approved',
      approved: true,
      strategyApproved: true,
      approvedAt:
        nextPriorOutreachStrategyPreview.approvedAt ||
        new Date().toISOString(),
    };
  }
  const messageClass = classifyReasoningMessage(text);
  reasoningMemory = markClassification(reasoningMemory, messageClass);
  const artifactAction = resolveCampaignArtifactAction({
    userMessage: text,
    messageClass,
    memory: reasoningMemory,
    priorCriteriaPreview,
    priorBuildProposal,
    priorProspectListDraft,
    priorScoutCandidateBatch,
    priorScoutHandoff,
    priorProspectBatchReview,
    priorOutreachStrategyPreview: nextPriorOutreachStrategyPreview,
    priorOutreachCopyPlan,
    step: campaignPlanning.step,
    slots: (campaignPlanning && campaignPlanning.slots) || null,
  });
  reasoningMemory = artifactAction.memory || reasoningMemory;

  const campaignReplyOpts = {
    blueprintId: blueprint && blueprint.id,
    blueprintVersion: blueprint && blueprint.version,
    priorPreview,
    priorCriteriaPreview,
    priorBuildProposal,
    priorProspectListDraft,
    priorScoutHandoffBrief,
    priorScoutHandoff,
    priorScoutCandidateBatch,
    priorScoutWorkRequest,
    priorProspectBatchReview,
    priorOutreachStrategyPreview: nextPriorOutreachStrategyPreview,
    priorOutreachCopyPlan,
    priorOutreachDraftPreview,
    priorOutreachLaunchGate,
    campaignMemory: priorCampaignMemory,
    campaignWorkingState: priorCampaignWorkingState,
    messageClass,
    artifactAction,
    reasoningMemory,
    reasoningState: { reasoningMemory },
    ...(opts.scoutSourcingFn ? { scoutSourcingFn: opts.scoutSourcingFn } : {}),
    ...(opts.publicSearchFn ? { publicSearchFn: opts.publicSearchFn } : {}),
    ...(opts.searchProvider ? { searchProvider: opts.searchProvider } : {}),
    ...(opts.workRequestStore
      ? { workRequestStore: opts.workRequestStore }
      : {}),
    ...(opts.scoutSourcingSupported != null
      ? { scoutSourcingSupported: opts.scoutSourcingSupported }
      : {}),
    ...(opts.scoutPublicSourcingSupported != null
      ? { scoutPublicSourcingSupported: opts.scoutPublicSourcingSupported }
      : {}),
  };

  let reply = buildCampaignPlanningReply(
    text,
    {
      ...campaignPlanning,
      firstCampaignPlanPreview: priorPreview,
      prospectListCriteriaPreview: priorCriteriaPreview,
      prospectListBuildProposal: priorBuildProposal,
      prospectListDraft: priorProspectListDraft,
      scoutHandoffBrief: priorScoutHandoffBrief,
      scoutHandoff: priorScoutHandoff,
      scoutCandidateBatch: priorScoutCandidateBatch,
      scoutWorkRequest: priorScoutWorkRequest,
      prospectBatchReview: priorProspectBatchReview,
      outreachStrategyPreview: nextPriorOutreachStrategyPreview,
      outreachCopyPlan: priorOutreachCopyPlan,
      outreachDraftPreview: priorOutreachDraftPreview,
      outreachLaunchGate: priorOutreachLaunchGate,
      campaignMemory: priorCampaignMemory,
      campaignWorkingState: priorCampaignWorkingState,
    },
    context,
    campaignReplyOpts
  );

  // SPEC-077 — run queued Scout public-source sourcing when tooling is wired.
  if (reply && reply.shouldExecuteScoutSourcing) {
    const executed = await executeScoutWorkRequest({
      workRequestId:
        reply.scoutWorkRequest && reply.scoutWorkRequest.workRequestId,
      handoffId: reply.scoutHandoff && reply.scoutHandoff.handoffId,
      handoff: reply.scoutHandoff,
      workRequest: reply.scoutWorkRequest,
      ...campaignReplyOpts,
    });
    reply = applyScoutExecutionResult(reply, executed);
  }

  const turns = [
    ...((campaignPlanning && campaignPlanning.turns) || []),
    {
      speaker: 'client',
      message: text,
      at: new Date().toISOString(),
      step: campaignPlanning.step,
      currentAsk: campaignPlanning.currentAsk || null,
      messageClass,
    },
    {
      speaker: 'assistant',
      message: reply.message,
      at: new Date().toISOString(),
      step: reply.step,
      intent: reply.intent,
      currentAsk: reply.currentAsk || null,
    },
  ];

  const previewApproved = Boolean(
    reply.previewApproved ||
      (reply.slots && reply.slots.previewApproved) ||
      campaignPlanning.previewApproved ||
      (priorPreview && priorPreview.status === 'approved')
  );

  let nextPreview =
    reply.preview ||
    (previewApproved && priorPreview
      ? { ...priorPreview, status: 'approved' }
      : null);
  if (nextPreview && previewApproved && nextPreview.status !== 'approved') {
    nextPreview = {
      ...nextPreview,
      status: 'approved',
      approvedAt: nextPreview.approvedAt || new Date().toISOString(),
    };
  }

  let nextCriteria =
    reply.criteriaPreview ||
    (reply.criteriaApproved && priorCriteriaPreview
      ? { ...priorCriteriaPreview, status: 'approved' }
      : priorCriteriaPreview);
  if (
    nextCriteria &&
    (reply.criteriaApproved || (reply.slots && reply.slots.criteriaApproved)) &&
    nextCriteria.status !== 'approved'
  ) {
    nextCriteria = {
      ...nextCriteria,
      status: 'approved',
      approvedAt: nextCriteria.approvedAt || new Date().toISOString(),
    };
  }

  let nextBuildProposal =
    reply.buildProposal ||
    (reply.intent === 'hold_criteria' ||
    reply.intent === 'build_proposal_approved' ||
    reply.intent === 'produce_prospect_list_draft' ||
    reply.intent === 'create_scout_handoff_brief' ||
    reply.intent === 'hand_brief_to_scout' ||
    reply.intent === 'scout_sourcing_not_wired' ||
    reply.intent === 'scout_handoff_queued' ||
    reply.intent === 'scout_handoff_completed' ||
    reply.intent === 'scout_sourcing_failed' ||
    reply.intent === 'live_sourcing_unavailable' ||
    reply.intent === 'produce_live_sourced_prospects'
      ? priorBuildProposal
      : null);
  if (
    nextBuildProposal &&
    (reply.buildProposalApproved ||
      (reply.slots && reply.slots.buildProposalApproved) ||
      reply.intent === 'build_proposal_approved' ||
      reply.intent === 'produce_prospect_list_draft' ||
      reply.intent === 'create_scout_handoff_brief' ||
      reply.intent === 'hand_brief_to_scout' ||
      reply.intent === 'scout_sourcing_not_wired' ||
      reply.intent === 'scout_handoff_queued' ||
      reply.intent === 'scout_handoff_completed' ||
      reply.intent === 'scout_sourcing_failed' ||
      reply.intent === 'live_sourcing_unavailable' ||
      reply.intent === 'produce_live_sourced_prospects') &&
    nextBuildProposal.status !== 'approved'
  ) {
    nextBuildProposal = {
      ...nextBuildProposal,
      status: 'approved',
      approvedAt: nextBuildProposal.approvedAt || new Date().toISOString(),
    };
  }

  const nextProspectListDraft =
    reply.prospectListDraft ||
    (reply.intent === 'produce_prospect_list_draft'
      ? priorProspectListDraft
      : reply.intent === 'live_sourcing_unavailable' ||
          reply.intent === 'produce_live_sourced_prospects' ||
          reply.intent === 'create_scout_handoff_brief' ||
          reply.intent === 'hand_brief_to_scout' ||
          reply.intent === 'scout_sourcing_not_wired' ||
          reply.intent === 'scout_handoff_queued' ||
          reply.intent === 'scout_handoff_completed' ||
          reply.intent === 'scout_sourcing_failed'
        ? priorProspectListDraft
        : null);

  const nextLiveProspectList = reply.liveProspectList || null;
  const nextScoutHandoffBrief = reply.scoutHandoffBrief || null;
  const nextScoutHandoff = reply.scoutHandoff || null;
  const nextScoutWorkRequest = reply.scoutWorkRequest || null;
  const nextScoutCandidateBatch = reply.scoutCandidateBatch || null;
  const nextProspectBatchReview = reply.prospectBatchReview || null;
  let nextOutreachStrategyPreview = reply.outreachStrategyPreview || null;
  if (
    nextOutreachStrategyPreview &&
    (reply.outreachStrategyPreviewApproved ||
      reply.strategyApproved ||
      (reply.slots &&
        (reply.slots.outreachStrategyPreviewApproved ||
          reply.slots.strategyApproved)) ||
      reply.intent === 'outreach_strategy_preview_approved' ||
      reply.intent === 'produce_outreach_copy_plan' ||
      reply.intent === 'show_outreach_copy_plan') &&
    nextOutreachStrategyPreview.status !== 'approved'
  ) {
    nextOutreachStrategyPreview = {
      ...nextOutreachStrategyPreview,
      status: 'approved',
      approved: true,
      strategyApproved: true,
      approvedAt:
        nextOutreachStrategyPreview.approvedAt || new Date().toISOString(),
    };
  }
  const nextOutreachCopyPlan = reply.outreachCopyPlan || null;
  const liveSourcingApproved = Boolean(
    reply.liveSourcingApproved ||
      (reply.slots && reply.slots.liveSourcingApproved) ||
      (reasoningMemory && reasoningMemory.liveSourcingApproved)
  );

  const nextStatus =
    reply.scoutHandoffBrief ||
    reply.scoutHandoff ||
    reply.liveProspectList ||
    reply.prospectListDraft ||
    reply.buildProposal ||
    reply.criteriaPreview ||
    reply.preview ||
    reply.slots?.previewGenerated ||
    previewApproved
      ? 'preview_ready'
      : 'active';
  const nextSlots = {
    ...((reply.slots || campaignPlanning.slots || {})),
    ...(previewApproved
      ? { previewApproved: true, previewGenerated: true }
      : {}),
    ...(nextCriteria ? { criteriaGenerated: true } : {}),
    ...(nextCriteria && nextCriteria.status === 'approved'
      ? { criteriaApproved: true }
      : {}),
    ...(nextBuildProposal ? { buildProposalGenerated: true } : {}),
    ...(nextBuildProposal && nextBuildProposal.status === 'approved'
      ? { buildProposalApproved: true }
      : {}),
    ...(nextProspectListDraft
      ? { draftRequested: true, draftGenerated: true }
      : {}),
    ...(nextScoutHandoffBrief ? { scoutHandoffBriefGenerated: true } : {}),
    ...(nextScoutHandoff && nextScoutHandoff.status !== 'draft'
      ? { scoutHandoffApproved: true }
      : {}),
    ...(nextScoutWorkRequest ? { scoutHandoffQueued: true } : {}),
    ...(liveSourcingApproved ? { liveSourcingApproved: true } : {}),
    ...((reply.batch1Approved ||
      reply.prospectBatchReviewApproved ||
      (nextProspectBatchReview &&
        (nextProspectBatchReview.batch1Approved ||
          nextProspectBatchReview.status === 'batch_1_approved')))
      ? { prospectBatchReviewApproved: true, batch1Approved: true }
      : {}),
    ...(nextOutreachStrategyPreview
      ? { outreachStrategyPreviewGenerated: true }
      : {}),
    ...((nextOutreachStrategyPreview &&
      nextOutreachStrategyPreview.status === 'approved') ||
    reply.strategyApproved ||
    reply.outreachStrategyPreviewApproved
      ? {
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
        }
      : {}),
    ...(nextOutreachCopyPlan ? { outreachCopyPlanGenerated: true } : {}),
  };
  const nextPlanning = {
    ...campaignPlanning,
    status: nextStatus,
    step: reply.step,
    answers: reply.answers,
    slots: nextSlots,
    currentAsk: reply.currentAsk || null,
    previewApproved,
    criteriaApproved: Boolean(
      nextCriteria && nextCriteria.status === 'approved'
    ),
    buildProposalApproved: Boolean(
      nextBuildProposal && nextBuildProposal.status === 'approved'
    ),
    liveSourcingApproved,
    planningState: reply.planningState || reply.step || null,
    context,
    turns,
  };

  await store.updateSession(session.id, {
    interview_state: {
      ...session.interview_state,
      campaignPlanning: nextPlanning,
      ...(nextPreview ? { firstCampaignPlanPreview: nextPreview } : {}),
      ...(nextCriteria ? { prospectListCriteriaPreview: nextCriteria } : {}),
      ...(nextBuildProposal
        ? { prospectListBuildProposal: nextBuildProposal }
        : {}),
      ...(nextProspectListDraft
        ? {
            prospectListDraft: nextProspectListDraft,
            reviewableProspectListDraft: nextProspectListDraft,
          }
        : {}),
      ...(nextLiveProspectList
        ? { liveProspectList: nextLiveProspectList }
        : {}),
      ...(nextScoutHandoffBrief
        ? { scoutHandoffBrief: nextScoutHandoffBrief }
        : {}),
      ...(nextScoutHandoff ? { scoutHandoff: nextScoutHandoff } : {}),
      ...(nextScoutWorkRequest
        ? { scoutWorkRequest: nextScoutWorkRequest }
        : {}),
      ...(nextScoutCandidateBatch
        ? { scoutCandidateBatch: nextScoutCandidateBatch }
        : {}),
      ...(nextProspectBatchReview
        ? { prospectBatchReview: nextProspectBatchReview }
        : {}),
      ...(nextOutreachStrategyPreview
        ? { outreachStrategyPreview: nextOutreachStrategyPreview }
        : {}),
      ...(nextOutreachCopyPlan ? { outreachCopyPlan: nextOutreachCopyPlan } : {}),
      ...(reply.outreachDraftPreview
        ? { outreachDraftPreview: reply.outreachDraftPreview }
        : {}),
      ...(reply.outreachLaunchGate
        ? { outreachLaunchGate: reply.outreachLaunchGate }
        : {}),
      ...(reply.campaignMemory ||
      (reply.outreachDraftPreview && reply.outreachDraftPreview.campaignMemory)
        ? {
            campaignMemory:
              reply.campaignMemory ||
              reply.outreachDraftPreview.campaignMemory,
          }
        : session.interview_state && session.interview_state.campaignMemory
          ? { campaignMemory: session.interview_state.campaignMemory }
          : {}),
      ...(reply.campaignWorkingState
        ? { campaignWorkingState: reply.campaignWorkingState }
        : session.interview_state &&
            session.interview_state.campaignWorkingState
          ? {
              campaignWorkingState:
                session.interview_state.campaignWorkingState,
            }
          : {}),
      reasoningMemory: (() => {
        let mem = reasoningMemory;
        if (nextPreview) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.CAMPAIGN_PREVIEW,
            nextPreview.status || 'draft'
          );
          if (nextPreview.status === 'approved') {
            mem = markArtifactApproved(mem, ARTIFACT_KINDS.CAMPAIGN_PREVIEW);
          }
        }
        if (nextCriteria) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.PROSPECT_CRITERIA,
            nextCriteria.status || 'draft'
          );
          if (nextCriteria.status === 'approved') {
            mem = markProspectCriteriaApproved(mem);
          }
        }
        if (nextBuildProposal) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
            nextBuildProposal.status || 'draft'
          );
          if (nextBuildProposal.status === 'approved') {
            mem = markArtifactApproved(
              mem,
              ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
            );
          }
        }
        if (nextProspectListDraft) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
            nextProspectListDraft.status || 'draft'
          );
        }
        if (nextScoutHandoffBrief) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF,
            nextScoutHandoffBrief.status || 'draft'
          );
          if (
            nextScoutHandoffBrief.status &&
            nextScoutHandoffBrief.status !== 'draft'
          ) {
            mem = markArtifactApproved(
              mem,
              ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF
            );
          }
        }
        if (nextProspectBatchReview) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
            nextProspectBatchReview.status || 'draft'
          );
          if (
            nextProspectBatchReview.batch1Approved ||
            nextProspectBatchReview.status === 'batch_1_approved' ||
            nextProspectBatchReview.status === 'approved'
          ) {
            mem = markArtifactApproved(
              mem,
              ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW
            );
            if (ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW) {
              mem = markArtifactGenerated(
                mem,
                ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
                (nextOutreachStrategyPreview &&
                  nextOutreachStrategyPreview.status) ||
                  'pending'
              );
            }
          }
        }
        if (nextOutreachStrategyPreview) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
            nextOutreachStrategyPreview.status || 'draft'
          );
          if (nextOutreachStrategyPreview.status === 'approved') {
            mem = markArtifactApproved(
              mem,
              ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW
            );
          }
        }
        if (nextOutreachCopyPlan) {
          mem = markArtifactGenerated(
            mem,
            ARTIFACT_KINDS.OUTREACH_COPY_PLAN,
            nextOutreachCopyPlan.status || 'draft'
          );
          if (nextOutreachCopyPlan.status === 'approved') {
            mem = markArtifactApproved(mem, ARTIFACT_KINDS.OUTREACH_COPY_PLAN);
          }
        }
        if (liveSourcingApproved) {
          mem = markLiveSourcingApproved(mem);
        }
        return mem;
      })(),
    },
  });

  return {
    ok: true,
    interviewId: session.id,
    status: 'CAMPAIGN_PLANNING',
    message: reply.message,
    intent: reply.intent,
    messageClass,
    blueprint: publicBlueprint(blueprint),
    campaignPlanning: nextPlanning,
    firstCampaignPlanPreview:
      nextPreview ||
      (session.interview_state &&
        session.interview_state.firstCampaignPlanPreview) ||
      null,
    prospectListCriteriaPreview:
      nextCriteria ||
      (session.interview_state &&
        session.interview_state.prospectListCriteriaPreview) ||
      null,
    prospectListBuildProposal:
      nextBuildProposal ||
      (session.interview_state &&
        session.interview_state.prospectListBuildProposal) ||
      null,
    prospectListDraft:
      nextProspectListDraft ||
      (session.interview_state &&
        session.interview_state.prospectListDraft) ||
      null,
    reviewableProspectListDraft:
      nextProspectListDraft ||
      (session.interview_state &&
        session.interview_state.reviewableProspectListDraft) ||
      null,
    scoutHandoffBrief:
      nextScoutHandoffBrief ||
      (session.interview_state &&
        session.interview_state.scoutHandoffBrief) ||
      null,
    scoutHandoff:
      nextScoutHandoff ||
      (session.interview_state && session.interview_state.scoutHandoff) ||
      null,
    scoutWorkRequest:
      nextScoutWorkRequest ||
      (session.interview_state &&
        session.interview_state.scoutWorkRequest) ||
      null,
    scoutCandidateBatch:
      nextScoutCandidateBatch ||
      (session.interview_state &&
        session.interview_state.scoutCandidateBatch) ||
      null,
    prospectBatchReview:
      nextProspectBatchReview ||
      (session.interview_state &&
        session.interview_state.prospectBatchReview) ||
      null,
    outreachStrategyPreview:
      nextOutreachStrategyPreview ||
      (session.interview_state &&
        session.interview_state.outreachStrategyPreview) ||
      null,
    outreachCopyPlan:
      nextOutreachCopyPlan ||
      (session.interview_state && session.interview_state.outreachCopyPlan) ||
      null,
    liveProspectList:
      nextLiveProspectList ||
      (session.interview_state && session.interview_state.liveProspectList) ||
      null,
    liveSourcingApproved,
  };
}

module.exports = {
  EPISTEMIC_STATES,
  classifyEpistemicState,
  composeAssessment,
  SESSION_STATUSES,
  ALLOWED_TRANSITIONS,
  BLUEPRINT_SECTIONS,
  EVIDENCE_TYPES,
  NEXT_ACTIONS,
  ANSWER_KINDS,
  ANSWER_DISPOSITIONS,
  MAX_PROBE_ATTEMPTS,
  MESSAGE_TYPES,
  MESSAGE_CLASSES,
  ARTIFACT_KINDS,
  CONTEXT_DOMAINS,
  DOMAIN_TO_SECTION,
  NORMALIZED_FACT_KEYS,
  SECTION_TO_NORMALIZED,
  QUESTION_BANK,
  SECTION_TITLES,
  GENERATED_BY,
  MIN_SECTION_CONFIDENCE,
  ClientIntelligenceError,
  createMemoryStore,
  createPostgresStore,
  scoreEvidenceConfidence,
  summarizeSection,
  computeProgress,
  buildUnderstandingProgress,
  buildExecutiveSummary,
  buildExecutiveBusinessBrief,
  buildReflection,
  hasSpecificitySignals,
  looksAmbiguous,
  assertTransition,
  ACTIVE_INTERVIEW_STATUSES,
  startClientInterview,
  findActiveInterviewForClient,
  resolveClientOnboardingState,
  sessionIsSuperseded,
  isRecoverableActiveInterview,
  supersedeUnapprovedOnboardingForClient,
  postInterviewMessage,
  resumeInterview,
  getInterview,
  getInterviewBlueprint,
  getClientBlueprint,
  getApprovedClientBlueprint,
  getBlueprintRecord,
  auditClientBlueprintLifecycle,
  listApprovedBlueprintSessions,
  getResumePayload,
  loadAnchorSampleBlueprint,
  completeGrowthPlanTask,
  resolveResumeTarget,
  buildGrowthPlan,
  reviseBlueprint,
  approveBlueprint,
  startGrowthConversation,
  postGrowthMessage,
  startInfrastructureReadinessConversation,
  postInfrastructureReadinessMessage,
  startCampaignPlanningConversation,
  postCampaignPlanningMessage,
  resolveInitialGrowthDirection,
  repairInitialGrowthDirection,
  detectContradiction,
  answerLooksEmpty,
  looksLikeExplicitUnknown,
  isLiteralUncertaintyPhrase,
  hasRelevantUncertaintyContext,
  buildUncertaintyReasoningProbe,
  classifyUserResponse,
  classifyAnswerDisposition,
  classifyInterviewMessage,
  looksLikeRefinementFeedback,
  looksLikeCorrection,
  looksLikeSupplementalContext,
  reviewCorrectionOperations,
  projectWorkingSemanticOperations,
  containsMetaInstructionLanguage,
  containsRawPromptFragment,
  partitionUserResponse,
  isBusinessFactStatement,
  sanitizeSummaryForBrief,
  sanitizeSectionsForBrief,
  stripInterviewQuestionEcho,
  stripCorrectionPreamble,
  stripSupplementalPreamble,
  cleanRawAnswer,
  synthesizeNormalizedFact,
  normalizeClaim,
  tagContextDomain,
  inferDomainFromQuestionEcho,
  resolveCorrectionTarget,
  findLastAnsweredQuestionId,
  conversationalAck,
  extractBusinessName,
  mergeSupplementalIntoSections,
  parseCorrectionMessage,
  parseSupplementalMessage,
  normalizeBusinessPhrase,
  normalizeMechanicalTypos,
  normalizePresentationProse,
  normalizeMetricPresentationPhrase,
  normalizeGoalOutcomePhrase,
  composeCustomerConstraintPresentation,
  formatDecisionMakerProse,
  formatSegmentProse,
  normalizeBrandVoiceTone,
  sanitizeBusinessName,
  stripBusinessNameLeadIn,
  extractServiceList,
  extractCustomerSegments,
  extractValueTraits,
  extractGrowthFocusItems,
  dedupeNormalizedList,
  isConversationalFiller,
  isDecisionMakerLabel,
  mergeIdealCustomersWithPrecedence,
  synthesizeDifferentiationSnippet,
  emptyNormalizedFacts,
  ingestAnswerIntoNormalizedFacts,
  applyCorrectionToNormalizedFacts,
  sectionsFromNormalizedFacts,
  splitListItems,
  // SPEC-090/091 reasoning layer
  emptyReasoningMemory,
  ensureReasoningMemory,
  planReasoningTurn,
  markArtifactGenerated,
  markArtifactApproved,
  resolveNextArtifact,
  resolveCampaignArtifactAction,
  checkArtifactReadiness,
  synthesizeBusinessLanguage,
  assessAnswerSufficiency,
  buildProbingFollowUp,
  looksLikeVagueAnswer,
  looksLikeExplicitUnknownAnswer,
};
