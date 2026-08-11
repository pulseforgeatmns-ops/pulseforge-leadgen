'use strict';

/**
 * SPEC-090 — Max Conversational Reasoning Layer.
 *
 * Session-level classify → memory → probe → readiness → synthesis helpers.
 * Runs before CIE question attachment, extraction, and artifact generation.
 * Does not invent campaigns, lists, outreach, or account changes.
 */

const MESSAGE_CLASSES = Object.freeze({
  DIRECT_ANSWER: 'direct_answer',
  CORRECTION: 'correction',
  ADD_ON: 'add_on',
  APPROVAL: 'approval',
  /** Approval plus an explicit ask for the next planning step / approach. */
  APPROVAL_PLUS_NEXT_REQUEST: 'approval_plus_next_request',
  CLARIFICATION_REQUEST: 'clarification_request',
  ARTIFACT_REQUEST: 'artifact_request',
  INSUFFICIENT_ANSWER: 'insufficient_answer',
  OFF_TOPIC: 'off_topic',
  SKIP: 'skip',
  /** Operator / Max guidance — never commercial evidence. */
  REFINEMENT_FEEDBACK: 'refinement_feedback',
});

/** Artifact kinds gated by readiness checks. */
const ARTIFACT_KINDS = Object.freeze({
  BLUEPRINT: 'blueprint',
  GROWTH_DIRECTION: 'growth_direction',
  CAMPAIGN_PREVIEW: 'campaign_preview',
  PROSPECT_CRITERIA: 'prospect_criteria',
  /** Alias used in approvedArtifacts progression guards (SPEC-091+). */
  PROSPECT_LIST_CRITERIA_PREVIEW: 'prospect_list_criteria_preview',
  PROSPECT_LIST_BUILD_PROPOSAL: 'prospect_list_build_proposal',
  REVIEWABLE_PROSPECT_LIST_DRAFT: 'reviewable_prospect_list_draft',
  /** Planning handoff for Scout — not live sourcing by Max. */
  SCOUT_HANDOFF_BRIEF: 'scout_handoff_brief',
  /** Operator-facing review of a completed Scout candidate batch. */
  PROSPECT_BATCH_REVIEW: 'prospect_batch_review',
  /** Review-first outreach strategy planning after Batch 1 approval. */
  OUTREACH_STRATEGY_PREVIEW: 'outreach_strategy_preview',
});

/**
 * Prospect-acquisition artifact intents.
 * create_scout_handoff_brief = Max writes a planning/handoff artifact.
 * perform_live_sourcing = Scout/browser/tooling gathers real prospects.
 * execute_existing_scout_work_request = run/retry a preserved Scout work request by ID.
 * emit_prospect_batch_review = format completed Scout results for operator review.
 */
const PROSPECT_ACQUISITION_INTENTS = Object.freeze({
  CREATE_SCOUT_HANDOFF_BRIEF: 'create_scout_handoff_brief',
  HAND_BRIEF_TO_SCOUT: 'hand_brief_to_scout',
  EXECUTE_EXISTING_SCOUT_WORK_REQUEST: 'execute_existing_scout_work_request',
  EMIT_PROSPECT_BATCH_REVIEW: 'emit_prospect_batch_review',
  /** Revise an active Prospect Batch Review (relationship overrides / corrections). */
  CORRECT_PROSPECT_BATCH_REVIEW: 'correct_prospect_batch_review',
  /** Approve Batch 1 (accepted cold first-pass) and advance. */
  APPROVE_PROSPECT_BATCH_REVIEW: 'approve_prospect_batch_review',
  PERFORM_LIVE_SOURCING: 'perform_live_sourcing',
});

/** Explicit workRequestId token in operator messages. */
const WORK_REQUEST_ID_RE =
  /\bworkRequestId\s*[:=]\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/i;

const ARTIFACT_ORDER = Object.freeze([
  ARTIFACT_KINDS.BLUEPRINT,
  ARTIFACT_KINDS.GROWTH_DIRECTION,
  ARTIFACT_KINDS.CAMPAIGN_PREVIEW,
  ARTIFACT_KINDS.PROSPECT_CRITERIA,
  ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
  ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
]);

/** Banned reopen of criteria after criteria + build proposal are approved. */
const CRITERIA_REPLAY_QUESTION_RE =
  /Before building a prospect list, define what should qualify or disqualify/i;

const MIN_ARTIFACT_SECTION_CONFIDENCE = 0.45;
const MIN_PROBE_WORD_COUNT = 4;

const APPROVAL_RE =
  /^\s*(?:yes[,.]?\s+)?(?:looks?\s+good|lgtm|approved?|approve(?:\s+it)?|ship\s+it|go\s+ahead|proceed|sounds?\s+good|that\s+works|perfect|confirmed?|i\s+approve)\s*[.!]*$/i;

/** Leading approval token inside a longer turn. */
const APPROVAL_LEAD_RE =
  /^\s*(?:yes[,.]?\s+)?(?:looks?\s+good|lgtm|approved?|approve(?:\s+it)?|ship\s+it|go\s+ahead|proceed|sounds?\s+good|that\s+works|perfect|confirmed?|i\s+approve)\b/i;

const NEXT_REQUEST_RE =
  /\b(?:before\s+we\s+build(?:\s+anything)?|how\s+(?:would|will|do|should)\s+you\s+approach|tell\s+me\s+how\s+you\s+would|what'?s\s+next|what\s+is\s+next|next\s+step|approach\s+building|build(?:ing)?\s+(?:the\s+)?(?:first\s+)?(?:prospect\s+)?list|how\s+to\s+build|propose\s+(?:the\s+)?(?:build|approach)|planning\s+(?:the\s+)?build)\b/i;

/** Explicit ask to generate the first reviewable prospect list batch/draft. */
const PROSPECT_LIST_DRAFT_REQUEST_RE =
  /\b(?:generate|create|produce|build|start|begin|draft|prepare)\b[\s\S]{0,120}\b(?:first\s+)?reviewable\s+(?:prospect\s+)?list(?:\s+batch|\s+draft)?\b|\b(?:generate|create|produce|start|begin|draft|prepare)\b[\s\S]{0,80}\b(?:prospect\s+)?list\s+(?:batch|draft)\b|\bfirst\s+reviewable\s+(?:prospect\s+)?list\b|\breviewable\s+(?:prospect\s+)?list\s+(?:batch|draft)\b/i;

/** Ask for Prospect Batch Review from a completed Scout result (not a new draft). */
const PROSPECT_BATCH_REVIEW_REQUEST_RE =
  /\bprospect\s+batch\s+review\b|\breview\s+(?:the\s+)?(?:scout\s+)?(?:candidate\s+)?batch\b|\bscout\s+(?:candidate\s+)?(?:batch\s+)?review\b|\b(?:show|create|produce|format|emit)\b[\s\S]{0,80}\b(?:prospect\s+)?batch\s+review\b|\b(?:accepted|review\s+required|rejected)\s+candidates\b|\bfrom\s+the\s+latest\s+completed\s+scout\b/i;

/**
 * Operator correction against an active Prospect Batch Review
 * (relationship overrides, remove-from-accepted, nurture reclassification).
 */
const PROSPECT_BATCH_REVIEW_CORRECTION_RE =
  /\b(?:remove|drop|exclude|take\s+out)\b[\s\S]{0,80}\b(?:from\s+(?:the\s+)?(?:accepted|batch|review|candidates|first[- ]pass)|keyrenter)\b|\b(?:keyrenter|[\w&.\-]+)\b[\s\S]{0,100}\b(?:existing\s+relationship|not\s+a\s+cold\s+prospect|nurture(?:\s+account)?)\b|\b(?:keep|treat|mark|reclassify)\b[\s\S]{0,80}\b(?:as\s+(?:an\s+)?(?:existing[- ]relationship|nurture)|existing\s+relationship)\b|\bexisting[- ]relationship\b|\bnurture\s+account\b/i;

/** Approve accepted cold first-pass candidates as Batch 1 (or approve the active review). */
const PROSPECT_BATCH_REVIEW_APPROVAL_RE =
  /\bapprov(?:e|ed|ing)\b[\s\S]{0,160}\b(?:batch\s*1|accepted\s+cold\s+first[- ]pass|accepted\s+first[- ]pass|cold\s+first[- ]pass\s+candidates|(?:the\s+)?\d+\s+accepted(?:\s+cold)?(?:\s+first[- ]pass)?\s+candidates)\b|\bapprov(?:e|ed|ing)\b[\s\S]{0,100}\b(?:prospect\s+)?batch\s+review\b|\bbatch\s*1\b[\s\S]{0,40}\bapprov(?:e|ed|ing)\b/i;

const ARTIFACT_REQUEST_RE =
  /\b(?:show|view|see|open|pull\s+up|regenerate|revise|replay)\s+(?:me\s+)?(?:the\s+)?(?:criteria|preview|blueprint|build\s+proposal|proposal|list\s+draft|prospect\s+list)\b|\b(?:criteria\s+preview|build\s+proposal|campaign\s+preview|list\s+draft)\s+again\b/i;

const EXPLICIT_REPLAY_RE =
  /\b(?:show|view|see|open|pull\s+up|regenerate|revise|replay|again)\b/i;

const SKIP_RE =
  /^\s*(?:skip(?:\s+(?:this|it|for\s+now))?|pass|next(?:\s+question)?|n\/?a|not\s+applicable|come\s+back\s+later|no\s+answer|i'?ll\s+skip)\s*[.!]*$/i;

const VAGUE_ONLY_RE =
  /^\s*(?:stuff|things|various|etc\.?|the\s+usual|same\s+as\s+(?:usual|always)|idk|tbd|whatever|normal|standard|good|fine|ok|okay|sure|yeah|yep|maybe|not\s+sure|unsure|kind\s+of|sort\s+of|something\s+like\s+that)\s*[.!?]*$/i;

const VAGUE_MARKERS_RE =
  /\b(maybe|perhaps|not sure|unsure|kind of|sort of|various|etc\.?|something like|i think|probably|roughly|around|whatever|idk|tbd|the usual|stuff|things)\b/i;

const CLARIFICATION_REQUEST_RE =
  /^(what|why|how|when|where|who|can you|could you|would you|do you|are you|is that|should i|what do you mean|can you clarify|explain)\b/i;

/**
 * Required evidence keys (normalized / section) per artifact.
 * Keys are Blueprint section names used by CIE sectionState.
 */
const ARTIFACT_REQUIRED_SECTIONS = Object.freeze({
  [ARTIFACT_KINDS.BLUEPRINT]: [
    'identity',
    'services',
    'idealCustomers',
    'targetMarkets',
  ],
  [ARTIFACT_KINDS.GROWTH_DIRECTION]: [
    'identity',
    'services',
    'idealCustomers',
    'targetMarkets',
    'campaignGoals',
  ],
  [ARTIFACT_KINDS.CAMPAIGN_PREVIEW]: [
    'idealCustomers',
    'targetMarkets',
    'campaignGoals',
    'avoidCustomers',
  ],
  [ARTIFACT_KINDS.PROSPECT_CRITERIA]: [
    'idealCustomers',
    'avoidCustomers',
    'targetMarkets',
  ],
  [ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL]: [
    'idealCustomers',
    'avoidCustomers',
    'targetMarkets',
  ],
});

function nowIso() {
  return new Date().toISOString();
}

function emptyReasoningMemory() {
  return {
    acceptedFacts: [],
    pendingCorrections: [],
    openQuestions: [],
    confidenceBySection: {},
    evidenceBySection: {},
    questionDebt: [],
    activeProbe: null,
    artifactsGenerated: [],
    lastClassification: null,
    lastArtifactType: null,
    lastArtifactStatus: null,
    approvedArtifacts: [],
    nextRecommendedArtifact: null,
    pendingUserRequest: null,
    /** Explicit operator approval to live-source real public prospects. */
    liveSourcingApproved: false,
  };
}

function ensureReasoningMemory(state = {}) {
  const existing = state && state.reasoningMemory;
  if (existing && typeof existing === 'object') {
    return {
      ...emptyReasoningMemory(),
      ...existing,
      acceptedFacts: [...(existing.acceptedFacts || [])],
      pendingCorrections: [...(existing.pendingCorrections || [])],
      openQuestions: [...(existing.openQuestions || [])],
      confidenceBySection: { ...(existing.confidenceBySection || {}) },
      evidenceBySection: { ...(existing.evidenceBySection || {}) },
      questionDebt: [...(existing.questionDebt || [])],
      artifactsGenerated: [...(existing.artifactsGenerated || [])],
      approvedArtifacts: [...(existing.approvedArtifacts || [])],
    };
  }
  return emptyReasoningMemory();
}

function wordCount(text) {
  return String(text || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function looksLikeApproval(text) {
  return APPROVAL_RE.test(String(text || '').trim());
}

function looksLikeApprovalLead(text) {
  return APPROVAL_LEAD_RE.test(String(text || '').trim());
}

function looksLikeNextPlanningRequest(text) {
  return NEXT_REQUEST_RE.test(String(text || ''));
}

/**
 * Extract a preserved Scout workRequestId from an operator message.
 * @returns {string|null}
 */
function extractWorkRequestIdFromMessage(text) {
  const s = String(text || '');
  if (!s.trim()) return null;
  const m = s.match(WORK_REQUEST_ID_RE);
  return m && m[1] ? m[1] : null;
}

/**
 * Operator asks to execute / retry / run an existing Scout work request by ID.
 * Distinct from creating a brief, handing a brief to Scout, or Max live-sourcing.
 */
function looksLikeExecuteExistingScoutWorkRequest(text) {
  const s = String(text || '');
  if (!s.trim()) return false;
  const workRequestId = extractWorkRequestIdFromMessage(s);
  if (!workRequestId) return false;
  const executeVerb =
    /\b(?:execute|retry|re-?run|run)\b/i.test(s) ||
    /\bexecute\s+the\s+existing\b/i.test(s);
  if (!executeVerb) return false;
  if (
    /\bscout\b/i.test(s) ||
    /\bwork\s*request\b/i.test(s) ||
    /\bworkRequestId\b/i.test(s)
  ) {
    return true;
  }
  return false;
}

/**
 * Operator asks to approve + queue the existing Scout Handoff Brief.
 * Distinct from creating the brief and from Max live-sourcing.
 */
function looksLikeHandBriefToScoutRequest(text) {
  const s = String(text || '');
  if (!s.trim()) return false;
  // Existing work-request execution wins over creating/queuing a new handoff.
  if (looksLikeExecuteExistingScoutWorkRequest(s)) return false;
  if (/\bhand\s+this\s+brief\s+to\s+scout\b/i.test(s)) return true;
  if (
    /\bhand\s+(?:the\s+)?(?:scout\s+)?(?:handoff\s+)?brief\s+to\s+scout\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:send|pass|queue|give|deliver)\s+(?:this\s+|the\s+)?(?:scout\s+)?(?:handoff\s+)?brief\s+to\s+scout\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\bapprove\s+(?:and\s+)?(?:queue|hand\s+off)\s+(?:(?:this|the)\s+)?(?:scout\s+)?(?:handoff\s+)?brief\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\bqueue\s+(?:this\s+|the\s+)?scout\s+handoff\b/i.test(s) ||
    /\bhand\s+off\s+(?:this\s+|the\s+)?(?:brief\s+)?to\s+scout\b/i.test(s)
  ) {
    return true;
  }
  return false;
}

/**
 * Max should create a Scout Handoff Brief (planning artifact), not source
 * prospects itself. Mentions of "public sources" describe Scout's job.
 * Never matches "Hand this brief to Scout" (executable handoff).
 */
function looksLikeScoutHandoffBriefRequest(text) {
  const s = String(text || '');
  if (!s.trim()) return false;
  // Executable handoff / existing work-request execution are different intents.
  if (looksLikeExecuteExistingScoutWorkRequest(s)) return false;
  if (looksLikeHandBriefToScoutRequest(s)) return false;
  if (
    /\b(?:create|generate|produce|draft|write|prepare|build)\b[\s\S]{0,80}\b(?:a\s+)?scout\s+handoff\s+brief\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (/\bscout\s+handoff\s+brief\b/i.test(s) && !/\bhand\b/i.test(s)) {
    return true;
  }
  if (
    /\b(?:create|generate|produce|draft|write|prepare|build)\b[\s\S]{0,80}\b(?:a\s+)?scout\s+handoff\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\bdo\s+not\s+build\b[\s\S]{0,120}\b(?:the\s+)?prospect\s+list\b[\s\S]{0,80}\bas\s+max\b/i.test(
      s
    ) &&
    /\bscout\b/i.test(s)
  ) {
    return true;
  }
  return false;
}

function looksLikeProspectBatchReviewRequest(text) {
  const s = String(text || '');
  if (!s.trim()) return false;
  if (looksLikeExecuteExistingScoutWorkRequest(s)) return false;
  return PROSPECT_BATCH_REVIEW_REQUEST_RE.test(s);
}

function hasActiveProspectBatchReview(opts = {}) {
  const prior =
    opts.priorProspectBatchReview || opts.prospectBatchReview || null;
  if (
    prior &&
    (prior.kind === ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW || prior.title)
  ) {
    return true;
  }
  const step = String(opts.step || '');
  if (
    step === 'prospect_batch_review' ||
    step === ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW
  ) {
    return true;
  }
  const memory = ensureReasoningMemory(
    opts.state || { reasoningMemory: opts.memory || {} }
  );
  const generated = memory.generatedArtifacts || [];
  const approved = memory.approvedArtifacts || [];
  return (
    generated.includes(ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW) ||
    approved.includes(ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW) ||
    memory.nextRecommendedArtifact === ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW
  );
}

/**
 * True when the operator is correcting an active Prospect Batch Review
 * (e.g. remove Keyrenter / mark existing relationship) rather than
 * requesting a new batch or falling back to Build Proposal ack.
 */
function looksLikeProspectBatchReviewCorrection(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (!hasActiveProspectBatchReview(opts)) return false;
  // Explicit Batch 1 / first-pass approval wins over nurture language in the same turn.
  if (looksLikeProspectBatchReviewApproval(s, opts)) return false;
  if (PROSPECT_BATCH_REVIEW_CORRECTION_RE.test(s)) return true;
  if (
    opts.messageClass === MESSAGE_CLASSES.CORRECTION ||
    opts.messageClass === MESSAGE_CLASSES.ADD_ON ||
    opts.messageClass === MESSAGE_CLASSES.REFINEMENT_FEEDBACK
  ) {
    return /\b(?:keyrenter|accepted|candidate|batch\s+review|nurture|relationship|remove|exclude)\b/i.test(
      s
    );
  }
  return false;
}

function isProspectBatchReviewAlreadyApproved(review) {
  if (!review || typeof review !== 'object') return false;
  return Boolean(
    review.batch1Approved ||
      review.status === 'batch_1_approved' ||
      review.status === 'approved' ||
      (review.approvedBatch && review.approvedBatch.status === 'approved')
  );
}

/**
 * True when the operator is approving Batch 1 / accepted cold first-pass
 * candidates on an active Prospect Batch Review.
 */
function looksLikeProspectBatchReviewApproval(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return false;
  const active = hasActiveProspectBatchReview(opts);
  if (!active) return false;

  if (PROSPECT_BATCH_REVIEW_APPROVAL_RE.test(s)) return true;

  // Pure approval ("Approved." / "LGTM") against an unapproved active review.
  if (
    (looksLikeApproval(s) ||
      opts.messageClass === MESSAGE_CLASSES.APPROVAL) &&
    !isProspectBatchReviewAlreadyApproved(
      opts.priorProspectBatchReview || opts.prospectBatchReview
    )
  ) {
    return true;
  }

  // "Approve …" lead plus first-pass / batch framing.
  if (
    looksLikeApprovalLead(s) &&
    /\b(?:batch\s*1|first[- ]pass|accepted\s+cold|cold\s+candidates|prospect\s+batch\s+review)\b/i.test(
      s
    )
  ) {
    return true;
  }

  return false;
}

function hasCompletedScoutCandidateBatch(batch) {
  if (!batch || typeof batch !== 'object') return false;
  const candidates = Array.isArray(batch.candidates) ? batch.candidates : [];
  const rejected = Array.isArray(batch.rejected) ? batch.rejected : [];
  const groups = batch.groups || {};
  const grouped =
    (Array.isArray(groups.accepted) ? groups.accepted.length : 0) +
    (Array.isArray(groups.review_required) ? groups.review_required.length : 0) +
    (Array.isArray(groups.rejected) ? groups.rejected.length : 0);
  return candidates.length > 0 || rejected.length > 0 || grouped > 0;
}

function looksLikeProspectListDraftRequest(text) {
  const s = String(text || '');
  // Handoff brief / live sourcing / existing WR execution are never drafts.
  if (looksLikeExecuteExistingScoutWorkRequest(s)) return false;
  if (looksLikeProspectBatchReviewRequest(s)) return false;
  if (looksLikeHandBriefToScoutRequest(s)) return false;
  if (looksLikeScoutHandoffBriefRequest(s)) return false;
  if (looksLikeLiveSourcingApproval(s)) return false;
  if (/\breviewable\s+prospect\s+list\s+batch\b/i.test(s)) return true;
  if (/\bprospect\s+list\s+draft\b/i.test(s)) return true;
  if (/\breviewable\s+(?:prospect\s+)?list\s+(?:batch|draft)\b/i.test(s)) {
    return true;
  }
  return PROSPECT_LIST_DRAFT_REQUEST_RE.test(s);
}

/**
 * True approval utterance for live sourcing — not "using the approved criteria".
 */
function hasLiveSourcingApprovalSignal(text) {
  const s = String(text || '');
  if (looksLikeApprovalLead(s)) return true;
  // Sentence-initial approval after punctuation ("… Done. Approved. Build…").
  if (
    /(?:^|[.!?]\s+)(?:yes[,.]?\s+)?(?:approved?|approve(?:\s+it)?|go\s+ahead|proceed|ship\s+it|i\s+approve)\b/i.test(
      s
    )
  ) {
    return true;
  }
  return false;
}

/**
 * Explicit approval to live-source real prospects from public sources.
 * Example: "Approved. Use only public sources. Build 15–25 real prospects."
 * Never matches Scout Handoff Brief / planning-handoff requests.
 */
function looksLikeLiveSourcingApproval(text) {
  const s = String(text || '');
  if (!s.trim()) return false;
  // Planning handoff / existing WR execution is never Max live-sourcing.
  if (looksLikeExecuteExistingScoutWorkRequest(s)) return false;
  if (looksLikeHandBriefToScoutRequest(s)) return false;
  if (looksLikeScoutHandoffBriefRequest(s)) return false;

  const hasApproval = hasLiveSourcingApprovalSignal(s);
  const publicSources =
    /\b(?:only\s+)?public\s+sources?\b/i.test(s) ||
    /\buse\s+only\s+public\b/i.test(s) ||
    /\blive\s+(?:public[- ]?)?sourc/i.test(s);
  const forbidsMaxBuild = /\bdo\s+not\s+build\b/i.test(s);
  const realProspects =
    /\breal\s+prospects?\b/i.test(s) ||
    /\b(?:\d+\s*[–-]\s*\d+|\d+)\s+real\s+prospects?\b/i.test(s) ||
    (!forbidsMaxBuild &&
      /\bbuild\b[\s\S]{0,80}\b(?:real\s+)?prospects?\b/i.test(s)) ||
    /\binclude\s+source\s+urls?\b/i.test(s);
  if (hasApproval && publicSources && realProspects) return true;
  if (
    hasApproval &&
    publicSources &&
    !forbidsMaxBuild &&
    /\b(?:build|generate|create|source)\b[\s\S]{0,80}\bprospects?\b/i.test(s)
  ) {
    return true;
  }
  if (hasApproval && /\blive\s+sourc/i.test(s) && /\bprospects?\b/i.test(s)) {
    return true;
  }
  return false;
}

/**
 * Classify whether the operator wants a Scout handoff artifact, to hand that
 * brief to Scout, to execute an existing Scout work request, or Max to perform
 * live sourcing directly.
 * @returns {'execute_existing_scout_work_request'|'hand_brief_to_scout'|'create_scout_handoff_brief'|'perform_live_sourcing'|null}
 */
function classifyProspectAcquisitionIntent(text, opts = {}) {
  const s = String(text || '');
  if (looksLikeExecuteExistingScoutWorkRequest(s)) {
    return PROSPECT_ACQUISITION_INTENTS.EXECUTE_EXISTING_SCOUT_WORK_REQUEST;
  }
  // Batch 1 approval before correction — approval turns may mention nurture/Cedar.
  if (looksLikeProspectBatchReviewApproval(s, opts)) {
    return PROSPECT_ACQUISITION_INTENTS.APPROVE_PROSPECT_BATCH_REVIEW;
  }
  if (looksLikeProspectBatchReviewCorrection(s, opts)) {
    return PROSPECT_ACQUISITION_INTENTS.CORRECT_PROSPECT_BATCH_REVIEW;
  }
  if (looksLikeProspectBatchReviewRequest(s)) {
    return PROSPECT_ACQUISITION_INTENTS.EMIT_PROSPECT_BATCH_REVIEW;
  }
  if (looksLikeHandBriefToScoutRequest(s)) {
    return PROSPECT_ACQUISITION_INTENTS.HAND_BRIEF_TO_SCOUT;
  }
  if (looksLikeScoutHandoffBriefRequest(s)) {
    return PROSPECT_ACQUISITION_INTENTS.CREATE_SCOUT_HANDOFF_BRIEF;
  }
  if (looksLikeLiveSourcingApproval(s)) {
    return PROSPECT_ACQUISITION_INTENTS.PERFORM_LIVE_SOURCING;
  }
  return null;
}

function markLiveSourcingApproved(memory) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.liveSourcingApproved = true;
  return next;
}

function isLiveSourcingApproved(memory, text) {
  const mem = ensureReasoningMemory({ reasoningMemory: memory });
  if (mem.liveSourcingApproved) return true;
  return looksLikeLiveSourcingApproval(text);
}

/**
 * Explicit criteria revision only.
 * Never treat "reviewable" / draft generation as revise-criteria.
 */
function looksLikeReviseCriteriaRequest(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  // Draft / reviewable list requests are never criteria revisions.
  if (looksLikeProspectListDraftRequest(s)) return false;
  if (/\breviewable\b/i.test(s) && !/\brevise\b/i.test(s)) return false;

  if (
    /\b(?:revise|change|update|redefine|redo|rework)\s+(?:the\s+)?(?:prospect[- ]list\s+)?criteria\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:revise|change|update|edit)\s+(?:the\s+)?(?:inclusion|exclusion)(?:\s*(?:\/|and|or)\s*(?:inclusion|exclusion))?(?:\s+criteria)?\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:update|change)\s+inclusion\s*\/\s*exclusion\b/i.test(s) ||
    /\bredefine\s+(?:the\s+)?(?:qualification|disqualification)\b/i.test(s)
  ) {
    return true;
  }
  // Narrow fallback: explicit revise verb + criteria vocabulary, but not
  // when the operator is only mentioning an approved criteria artifact.
  if (
    /\b(revise|change|update|edit|redo|rework)\b/i.test(s) &&
    /\b(inclusion|exclusion|qualify|disqualify)\b/i.test(s)
  ) {
    return true;
  }
  return false;
}

/**
 * Infer criteria/build approvals declared in operator text into memory.
 * Example: "Prospect List Criteria Preview approved".
 */
function inferApprovedArtifactsFromMessage(memory, text) {
  let next = ensureReasoningMemory({ reasoningMemory: memory });
  const s = String(text || '');
  if (
    /\bprospect\s+list\s+criteria\s+preview\b[\s\S]{0,40}\bapproved\b/i.test(s) ||
    /\bcriteria\s+preview\b[\s\S]{0,40}\bapproved\b/i.test(s) ||
    /\bapproved\b[\s\S]{0,40}\bprospect\s+list\s+criteria\s+preview\b/i.test(s)
  ) {
    next = markProspectCriteriaApproved(next);
  }
  if (
    /\bprospect\s+list\s+build\s+proposal\b[\s\S]{0,40}\bapproved\b/i.test(s) ||
    /\bbuild\s+proposal\b[\s\S]{0,40}\bapproved\b/i.test(s) ||
    /\bapproved\b[\s\S]{0,40}\bprospect\s+list\s+build\s+proposal\b/i.test(s)
  ) {
    next = markArtifactApproved(
      next,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
  }
  if (looksLikeLiveSourcingApproval(s)) {
    next = markLiveSourcingApproved(next);
  }
  return next;
}

/**
 * Hard progression guard for reviewable prospect list draft.
 * Draft wording + approved criteria + approved build proposal → draft path.
 */
function shouldForceProspectListDraft(message, memory, opts = {}) {
  if (!looksLikeProspectListDraftRequest(message)) return false;
  let mem = ensureReasoningMemory({
    reasoningMemory: inferApprovedArtifactsFromMessage(memory, message),
  });
  if (opts.priorCriteriaPreview && opts.priorCriteriaPreview.status === 'approved') {
    mem = markProspectCriteriaApproved(mem);
  }
  if (opts.priorBuildProposal && opts.priorBuildProposal.status === 'approved') {
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
  }
  const approved = mem.approvedArtifacts || [];
  const hasCriteria =
    approved.includes(ARTIFACT_KINDS.PROSPECT_CRITERIA) ||
    approved.includes(ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW);
  const hasBuild = approved.includes(
    ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
  );
  return hasCriteria && hasBuild;
}

/**
 * Approval plus an ask for the next planning step / build approach.
 * Example: "Approved. Before we build anything, tell me how you would approach…"
 */
function looksLikeApprovalPlusNextRequest(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (looksLikeApproval(s)) return false; // pure approval handled separately
  return (
    looksLikeApprovalLead(s) &&
    (looksLikeNextPlanningRequest(s) || looksLikeProspectListDraftRequest(s))
  );
}

function looksLikeArtifactRequest(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (looksLikeProspectListDraftRequest(s)) return true;
  if (ARTIFACT_REQUEST_RE.test(s)) return true;
  if (looksLikeNextPlanningRequest(s) && /\b(list|build|approach|proposal|criteria|preview|draft|batch)\b/i.test(s)) {
    return true;
  }
  return false;
}

function approvedArtifactsInclude(memory, kind) {
  const approved = (memory && memory.approvedArtifacts) || [];
  return approved.includes(kind);
}

/**
 * True when criteria + build proposal are both approved — criteria question
 * must not be replayed unless the operator explicitly revises criteria.
 */
function shouldBlockCriteriaQuestionReplay(memory) {
  const approved = (memory && memory.approvedArtifacts) || [];
  const hasCriteria =
    approved.includes(ARTIFACT_KINDS.PROSPECT_CRITERIA) ||
    approved.includes(ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW);
  const hasBuild = approved.includes(
    ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
  );
  return hasCriteria && hasBuild;
}

function isBannedCriteriaReplayQuestion(text) {
  return CRITERIA_REPLAY_QUESTION_RE.test(String(text || ''));
}

function looksLikeExplicitReplayRequest(text) {
  return EXPLICIT_REPLAY_RE.test(String(text || ''));
}

function looksLikeSkip(text) {
  return SKIP_RE.test(String(text || '').trim());
}

function looksLikeClarificationRequest(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (/\?\s*$/.test(s) && CLARIFICATION_REQUEST_RE.test(s)) return true;
  if (/\b(what do you mean|can you (?:clarify|explain)|i'?m not sure what you(?:'re| are) asking)\b/i.test(s)) {
    return true;
  }
  return false;
}

function looksLikeVagueAnswer(text) {
  const s = String(text || '').trim();
  if (!s) return true;
  if (VAGUE_ONLY_RE.test(s)) return true;
  // Hedged short answers ("maybe some stuff") — not concrete nouns like "Homeowners".
  if (wordCount(s) < MIN_PROBE_WORD_COUNT && VAGUE_MARKERS_RE.test(s)) return true;
  if (
    wordCount(s) <= 2 &&
    /^(?:stuff|things|whatever|something|nothing|various|etc\.?|the\s+usual)\b/i.test(s)
  ) {
    return true;
  }
  return false;
}

/**
 * Classify a user message for the conversational reasoning layer.
 * Callers may supply prior detectors so CIE can reuse existing heuristics
 * without duplicating them here.
 *
 * @param {string} text
 * @param {{
 *   speaker?: string,
 *   context?: string,
 *   activeQuestion?: object|null,
 *   looksLikeCorrection?: (t: string) => boolean,
 *   looksLikeAddOn?: (t: string, opts?: object) => boolean,
 *   looksLikeRefinement?: (t: string) => boolean,
 *   containsMetaInstruction?: (t: string) => boolean,
 *   answerLooksEmpty?: (t: string) => boolean,
 * }} [opts]
 * @returns {string} one of MESSAGE_CLASSES
 */
function classifyReasoningMessage(text, opts = {}) {
  const speaker = String(opts.speaker || '').toLowerCase();
  const context = String(opts.context || '').toLowerCase();
  const raw = String(text || '').trim();

  if (context === 'generated_brief' || speaker === 'assistant') {
    return MESSAGE_CLASSES.REFINEMENT_FEEDBACK;
  }
  if (speaker === 'system' || speaker === 'developer' || context === 'system_guidance') {
    return MESSAGE_CLASSES.OFF_TOPIC;
  }
  if (!raw) return MESSAGE_CLASSES.INSUFFICIENT_ANSWER;

  if (typeof opts.looksLikeCorrection === 'function' && opts.looksLikeCorrection(raw)) {
    return MESSAGE_CLASSES.CORRECTION;
  }
  if (typeof opts.looksLikeAddOn === 'function' && opts.looksLikeAddOn(raw, opts)) {
    return MESSAGE_CLASSES.ADD_ON;
  }
  if (
    (typeof opts.looksLikeRefinement === 'function' && opts.looksLikeRefinement(raw)) ||
    (typeof opts.containsMetaInstruction === 'function' && opts.containsMetaInstruction(raw))
  ) {
    return MESSAGE_CLASSES.REFINEMENT_FEEDBACK;
  }
  if (looksLikeSkip(raw)) return MESSAGE_CLASSES.SKIP;
  // Approval+next before pure approval so longer turns are not truncated.
  if (looksLikeApprovalPlusNextRequest(raw)) {
    return MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST;
  }
  if (looksLikeApproval(raw)) return MESSAGE_CLASSES.APPROVAL;
  if (looksLikeArtifactRequest(raw)) return MESSAGE_CLASSES.ARTIFACT_REQUEST;
  if (looksLikeClarificationRequest(raw)) return MESSAGE_CLASSES.CLARIFICATION_REQUEST;
  if (/^(hi|hello|hey|thanks|thank you|ok|okay|cool|great|nice)\s*[.!]?$/i.test(raw)) {
    return MESSAGE_CLASSES.OFF_TOPIC;
  }
  if (typeof opts.answerLooksEmpty === 'function' && opts.answerLooksEmpty(raw)) {
    return MESSAGE_CLASSES.INSUFFICIENT_ANSWER;
  }
  if (looksLikeVagueAnswer(raw)) return MESSAGE_CLASSES.INSUFFICIENT_ANSWER;

  return MESSAGE_CLASSES.DIRECT_ANSWER;
}

/**
 * Assess whether a candidate direct answer is sufficient to advance.
 * @returns {{ sufficient: boolean, reason: string|null, shouldProbe: boolean }}
 */
function assessAnswerSufficiency(text, activeQuestion = null, opts = {}) {
  const raw = String(text || '').trim();
  if (!raw) {
    return { sufficient: false, reason: 'empty', shouldProbe: true };
  }
  if (looksLikeVagueAnswer(raw)) {
    return { sufficient: false, reason: 'vague', shouldProbe: true };
  }
  if (VAGUE_MARKERS_RE.test(raw) && wordCount(raw) < 8) {
    return { sufficient: false, reason: 'hedged', shouldProbe: true };
  }

  // Operationally important sections: probe hedged/thin answers, not concrete nouns.
  const section = activeQuestion && activeQuestion.section;
  const important = new Set([
    'idealCustomers',
    'avoidCustomers',
    'targetMarkets',
    'campaignGoals',
    'services',
  ]);
  if (
    important.has(section) &&
    VAGUE_MARKERS_RE.test(raw) &&
    wordCount(raw) < 8 &&
    !opts.hasSpecificity
  ) {
    return { sufficient: false, reason: 'thin_important', shouldProbe: true };
  }

  if (opts.contradiction) {
    return { sufficient: false, reason: 'contradiction', shouldProbe: true };
  }

  return { sufficient: true, reason: null, shouldProbe: false };
}

/**
 * Build one focused probing follow-up for the active question.
 */
function buildProbingFollowUp(activeQuestion, assessment = {}, businessName = null) {
  const q = activeQuestion || {};
  const section = q.section || 'general';
  const name = String(businessName || 'the business').trim() || 'the business';
  const shortName = name.replace(/\s+Cleaning$/i, '') || name;
  const reason = assessment.reason || 'vague';

  const probes = {
    identity: `Could you give me the business name and a one-sentence description of what ${shortName} does today?`,
    services: `Which specific services does ${shortName} deliver most often — for example recurring office cleans, deep cleans, or something else?`,
    idealCustomers: `Who is the strongest-fit customer for ${shortName} — a role, business type, or segment I can target first?`,
    avoidCustomers: `Which customers or segments should we actively avoid, and what's the main reason?`,
    targetMarkets: `Where should we focus first — a geography, a vertical, or both? Name the top markets.`,
    competitiveAdvantages: `When a great-fit customer chooses ${shortName}, what usually tips the decision?`,
    brandVoice: `How should ${shortName} sound in writing — two or three tone words, plus anything to avoid?`,
    campaignGoals: `Looking at the next 90 days, what one or two outcomes would make this growth work feel successful?`,
    successMetrics: `How will we know it's working — which 2–3 signals should we watch?`,
  };

  const base =
    probes[section] ||
    `Could you get a bit more specific on that so I can capture it cleanly?`;

  if (reason === 'contradiction') {
    return `That seems to conflict with what we captured earlier. ${base}`;
  }
  if (reason === 'thin_important' || reason === 'hedged') {
    return `That's helpful direction — I need one more concrete detail before we move on. ${base}`;
  }
  return base;
}

/**
 * Infer whether substance belongs to a different section than the active question.
 * Used for cross-section add-ons that may lack explicit "also/forgot" wrappers.
 *
 * @returns {{ section: string|null, domain: string|null, confidence: number }}
 */
function inferCrossSectionTarget(text, activeQuestion, helpers = {}) {
  const activeSection = activeQuestion && activeQuestion.section;
  const domain =
    (typeof helpers.inferDomain === 'function' && helpers.inferDomain(text)) ||
    (typeof helpers.tagDomain === 'function' && helpers.tagDomain(text)) ||
    null;
  const section =
    (domain && helpers.domainToSection && helpers.domainToSection[domain]) || null;

  if (!section || !activeSection || section === activeSection) {
    return { section: null, domain: null, confidence: 0 };
  }

  // Strong cues: explicit ICP / forgot / also language, or clear domain mismatch
  // with customer-segment nouns while on avoidCustomers.
  const s = String(text || '');
  let confidence = 0.55;
  if (/\b(forgot|also|add(?:ing)?|icp|ideal\s+customer)\b/i.test(s)) confidence = 0.9;
  if (
    activeSection === 'avoidCustomers' &&
    section === 'idealCustomers' &&
    /\b(property managers?|facility managers?|law firms?|offices?|daycares?)\b/i.test(s)
  ) {
    confidence = Math.max(confidence, 0.85);
  }
  if (
    activeSection === 'idealCustomers' &&
    section === 'avoidCustomers' &&
    /\b(avoid|don'?t want|lowest price|bargain)\b/i.test(s)
  ) {
    confidence = Math.max(confidence, 0.85);
  }

  return { section, domain, confidence };
}

function recordAcceptedFact(memory, { section, substance, source = 'direct_answer' }) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  if (!section || !substance) return next;
  next.acceptedFacts.push({
    section,
    substance: String(substance).trim(),
    at: nowIso(),
    source,
  });
  if (next.acceptedFacts.length > 80) {
    next.acceptedFacts = next.acceptedFacts.slice(-80);
  }
  const list = next.evidenceBySection[section] || [];
  list.push(String(substance).trim());
  next.evidenceBySection[section] = list.slice(-12);
  return next;
}

function recordPendingCorrection(memory, { section, substance, status = 'pending' }) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.pendingCorrections.push({
    section: section || null,
    substance: String(substance || '').trim(),
    at: nowIso(),
    status,
  });
  if (next.pendingCorrections.length > 40) {
    next.pendingCorrections = next.pendingCorrections.slice(-40);
  }
  return next;
}

function resolvePendingCorrection(memory, section) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.pendingCorrections = (next.pendingCorrections || []).map((row) => {
    if (section && row.section === section && row.status === 'pending') {
      return { ...row, status: 'applied' };
    }
    return row;
  });
  return next;
}

function syncConfidenceFromSections(memory, sectionState = {}) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  const conf = { ...next.confidenceBySection };
  for (const [key, section] of Object.entries(sectionState || {})) {
    if (section && typeof section.confidence === 'number') {
      conf[key] = section.confidence;
    }
  }
  next.confidenceBySection = conf;
  return next;
}

function addQuestionDebt(memory, { questionId, section, reason }) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  const already = (next.questionDebt || []).some(
    (d) => d.questionId === questionId && d.reason === reason
  );
  if (!already) {
    next.questionDebt.push({
      questionId: questionId || null,
      section: section || null,
      reason: reason || 'unanswered',
      at: nowIso(),
    });
  }
  // Keep openQuestions aligned with debt.
  if (questionId) {
    const openAlready = (next.openQuestions || []).some((q) => q.questionId === questionId);
    if (!openAlready) {
      next.openQuestions.push({
        questionId,
        section: section || null,
        reason: reason || 'unanswered',
      });
    }
  }
  return next;
}

function clearQuestionDebt(memory, questionId) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.questionDebt = (next.questionDebt || []).filter((d) => d.questionId !== questionId);
  next.openQuestions = (next.openQuestions || []).filter((q) => q.questionId !== questionId);
  if (next.activeProbe && next.activeProbe.questionId === questionId) {
    next.activeProbe = null;
  }
  return next;
}

function setActiveProbe(memory, probe) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.activeProbe = probe
    ? {
        questionId: probe.questionId || null,
        section: probe.section || null,
        prompt: probe.prompt || '',
        reason: probe.reason || 'vague',
        at: nowIso(),
      }
    : null;
  return next;
}

function markClassification(memory, messageClass) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.lastClassification = messageClass || null;
  return next;
}

/**
 * Record that an artifact was generated so the next request advances.
 */
function markArtifactGenerated(memory, kind, status = 'draft') {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  const k = String(kind || '').trim();
  if (!k) return next;
  if (!next.artifactsGenerated.includes(k)) {
    next.artifactsGenerated.push(k);
  }
  next.lastArtifactType = k;
  next.lastArtifactStatus = status || 'draft';
  const idx = ARTIFACT_ORDER.indexOf(k);
  if (idx >= 0 && idx < ARTIFACT_ORDER.length - 1) {
    next.nextRecommendedArtifact = ARTIFACT_ORDER[idx + 1];
  } else if (!next.approvedArtifacts.includes(k)) {
    next.nextRecommendedArtifact = k;
  } else {
    next.nextRecommendedArtifact = ARTIFACT_ORDER[idx + 1] || null;
  }
  return next;
}

/**
 * Mark an artifact approved and recommend the next planning artifact.
 */
function markArtifactApproved(memory, kind) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  const k = String(kind || '').trim();
  if (!k) return next;
  if (!next.artifactsGenerated.includes(k)) {
    next.artifactsGenerated.push(k);
  }
  if (!next.approvedArtifacts.includes(k)) {
    next.approvedArtifacts.push(k);
  }
  next.lastArtifactType = k;
  next.lastArtifactStatus = 'approved';
  const idx = ARTIFACT_ORDER.indexOf(k);
  next.nextRecommendedArtifact =
    idx >= 0 ? ARTIFACT_ORDER[idx + 1] || null : null;
  return next;
}

/**
 * Mark criteria approved under both memory keys used by progression guards.
 */
function markProspectCriteriaApproved(memory) {
  let next = markArtifactApproved(memory, ARTIFACT_KINDS.PROSPECT_CRITERIA);
  const recommended = next.nextRecommendedArtifact;
  next = markArtifactApproved(
    next,
    ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
  );
  // Alias kind is not in ARTIFACT_ORDER — restore progression pointer.
  next.nextRecommendedArtifact =
    recommended || ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL;
  return next;
}

function setPendingUserRequest(memory, request) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  next.pendingUserRequest = request
    ? {
        text: String(request.text || request || '').trim(),
        messageClass: request.messageClass || null,
        at: nowIso(),
      }
    : null;
  return next;
}

/**
 * Prevent re-emitting the same artifact when the user asks for the next one.
 * Honors approvedArtifacts: approved kinds are never replayed unless explicitly requested.
 * @returns {{ emit: string|null, hold: string|null, message: string|null }}
 */
function resolveNextArtifact(memory, requestedKind, opts = {}) {
  const next = ensureReasoningMemory({ reasoningMemory: memory });
  const generated = next.artifactsGenerated || [];
  const approved = next.approvedArtifacts || [];
  const requested = String(requestedKind || '').trim();
  const allowReplay = Boolean(opts.allowReplay);

  if (!requested) {
    // Prefer nextRecommendedArtifact when no explicit request.
    const recommended = next.nextRecommendedArtifact;
    if (recommended && !approved.includes(recommended)) {
      return { emit: recommended, hold: null, message: null };
    }
    return { emit: null, hold: null, message: null };
  }

  if (approved.includes(requested) && !allowReplay) {
    const idx = ARTIFACT_ORDER.indexOf(requested);
    const following = idx >= 0 ? ARTIFACT_ORDER.slice(idx + 1) : [];
    const nextKind =
      following.find((k) => !approved.includes(k)) ||
      next.nextRecommendedArtifact ||
      null;
    if (nextKind) {
      return {
        emit: nextKind,
        hold: requested,
        message: `The ${humanArtifactLabel(requested)} is already approved. Next up is the ${humanArtifactLabel(nextKind)}.`,
      };
    }
    return {
      emit: null,
      hold: requested,
      message: `The ${humanArtifactLabel(requested)} is already approved. Say if you'd like to revise it.`,
    };
  }

  if (generated.includes(requested) && !allowReplay) {
    const idx = ARTIFACT_ORDER.indexOf(requested);
    const following = idx >= 0 ? ARTIFACT_ORDER.slice(idx + 1) : [];
    const nextKind = following.find((k) => !generated.includes(k)) || null;
    if (nextKind) {
      return {
        emit: nextKind,
        hold: requested,
        message: `We've already covered the ${humanArtifactLabel(requested)}. Next up is the ${humanArtifactLabel(nextKind)}.`,
      };
    }
    return {
      emit: null,
      hold: requested,
      message: `We've already generated the ${humanArtifactLabel(requested)}. Say if you'd like to revise it, or approve to continue.`,
    };
  }
  return { emit: requested, hold: null, message: null };
}

/**
 * Campaign-loop artifact progression from session context + classified intent.
 *
 * @returns {{
 *   action: 'emit_criteria'|'emit_build_proposal'|'emit_prospect_list_draft'|'emit_live_sourcing'|'emit_scout_handoff_brief'|'hand_brief_to_scout'|'execute_existing_scout_work_request'|'ack_approval'|'ack_build_approval'|'replay_criteria'|'hold',
 *   approveKind: string|null,
 *   emitKind: string|null,
 *   memory: object,
 *   messageClass: string,
 *   note: string|null,
 *   planningState: string|null,
 *   liveSourcingApproved?: boolean,
 *   workRequestId?: string|null,
 * }}
 */
function resolveCampaignArtifactAction(opts = {}) {
  const text = String(opts.userMessage || '').trim();
  const messageClass =
    opts.messageClass || classifyReasoningMessage(text, opts.classifyOpts || {});
  let memory = ensureReasoningMemory(opts.state || { reasoningMemory: opts.memory });
  memory = markClassification(memory, messageClass);
  memory = setPendingUserRequest(memory, {
    text,
    messageClass,
  });
  // Honor approvals declared in the operator message before progression.
  memory = inferApprovedArtifactsFromMessage(memory, text);

  const priorCriteria = opts.priorCriteriaPreview || null;
  const priorBuild = opts.priorBuildProposal || null;
  const priorDraft =
    opts.priorProspectListDraft || opts.priorReviewableProspectListDraft || null;
  const priorBatchReview =
    opts.priorProspectBatchReview || opts.prospectBatchReview || null;
  const priorScoutBatch =
    opts.priorScoutCandidateBatch ||
    opts.scoutCandidateBatch ||
    (opts.priorScoutHandoff && opts.priorScoutHandoff.candidateBatch) ||
    (priorBatchReview && priorBatchReview.scoutCandidateBatch) ||
    null;
  const scoutBatchReady = hasCompletedScoutCandidateBatch(priorScoutBatch);
  const scoutCompletedStep =
    opts.step === 'scout_handoff_completed' ||
    opts.step === 'scout_handoff_failed' ||
    opts.step === 'prospect_batch_review' ||
    (opts.priorScoutHandoff &&
      (opts.priorScoutHandoff.status === 'completed' ||
        opts.priorScoutHandoff.status === 'failed_quality_gate' ||
        opts.priorScoutHandoff.scoutRan));

  const acquisitionIntent = classifyProspectAcquisitionIntent(text, {
    priorProspectBatchReview: priorBatchReview,
    step: opts.step,
    memory,
    messageClass,
  });
  const executeWorkRequestId = extractWorkRequestIdFromMessage(text);

  // HARD GUARD: approve Batch 1 on an active Prospect Batch Review.
  // Never re-render the same review / closing question.
  const batchReviewApproval =
    acquisitionIntent ===
      PROSPECT_ACQUISITION_INTENTS.APPROVE_PROSPECT_BATCH_REVIEW ||
    looksLikeProspectBatchReviewApproval(text, {
      priorProspectBatchReview: priorBatchReview,
      step: opts.step,
      memory,
      messageClass,
      state: opts.state,
    });

  if (batchReviewApproval) {
    memory = markArtifactApproved(
      memory,
      ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW
    );
    memory.nextRecommendedArtifact =
      ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW;
    return {
      action: 'approve_prospect_batch_review',
      approveKind: ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      emitKind: ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
      memory,
      messageClass:
        messageClass === MESSAGE_CLASSES.APPROVAL ||
        messageClass === MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST
          ? messageClass
          : MESSAGE_CLASSES.APPROVAL,
      note: 'Batch 1 approved. Next step: prepare outreach strategy preview.',
      planningState: 'outreach_strategy_preview',
      batch1Approved: true,
    };
  }

  // HARD GUARD: correction against an active Prospect Batch Review.
  // Never fall back to "Build proposal already approved…" / draft prompt.
  const batchReviewCorrection =
    acquisitionIntent ===
      PROSPECT_ACQUISITION_INTENTS.CORRECT_PROSPECT_BATCH_REVIEW ||
    looksLikeProspectBatchReviewCorrection(text, {
      priorProspectBatchReview: priorBatchReview,
      step: opts.step,
      memory,
      messageClass,
      state: opts.state,
    });

  if (batchReviewCorrection) {
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW;
    memory = markArtifactGenerated(
      memory,
      ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      'draft'
    );
    return {
      action: 'emit_prospect_batch_review',
      approveKind: null,
      emitKind: ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      memory,
      messageClass:
        messageClass === MESSAGE_CLASSES.CORRECTION
          ? MESSAGE_CLASSES.CORRECTION
          : MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note:
        'Revising the active Prospect Batch Review with operator relationship overrides. Max will not repeat the build proposal.',
      planningState: 'prospect_batch_review',
      relationshipCorrection: true,
    };
  }

  // HARD GUARD: completed Scout batch already exists — emit Prospect Batch Review.
  // Never ask to generate the first reviewable batch or re-emit the build proposal.
  const wantsBatchReview =
    acquisitionIntent ===
      PROSPECT_ACQUISITION_INTENTS.EMIT_PROSPECT_BATCH_REVIEW ||
    looksLikeProspectBatchReviewRequest(text) ||
    (scoutBatchReady &&
      scoutCompletedStep &&
      (looksLikeNextPlanningRequest(text) ||
        messageClass === MESSAGE_CLASSES.ARTIFACT_REQUEST ||
        messageClass === MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST ||
        // Stale "generate first reviewable batch" asks must surface the Scout result.
        looksLikeProspectListDraftRequest(text)));

  if (
    wantsBatchReview &&
    acquisitionIntent !==
      PROSPECT_ACQUISITION_INTENTS.EXECUTE_EXISTING_SCOUT_WORK_REQUEST
  ) {
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW;
    memory = markArtifactGenerated(
      memory,
      ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      'draft'
    );
    return {
      action: 'emit_prospect_batch_review',
      approveKind: null,
      emitKind: ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      memory,
      messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note: scoutBatchReady
        ? 'Using the latest completed Scout candidate batch. Max will not regenerate the first reviewable batch or repeat the build proposal.'
        : 'Creating the Prospect Batch Review from the completed Scout result.',
      planningState: 'scout_handoff_completed',
    };
  }

  // HARD GUARD: execute / retry an existing Scout work request by ID.
  // Never fall through to "ask me to generate batch when ready".
  if (
    acquisitionIntent ===
    PROSPECT_ACQUISITION_INTENTS.EXECUTE_EXISTING_SCOUT_WORK_REQUEST
  ) {
    // If the work request already completed and the session still has the batch,
    // prefer Prospect Batch Review over re-sourcing (unless message says retry).
    const wantsRetry = /\bretry|re-?run\b/i.test(text);
    if (scoutBatchReady && !wantsRetry) {
      memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW;
      return {
        action: 'emit_prospect_batch_review',
        approveKind: null,
        emitKind: ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
        memory,
        messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
        note:
          'Scout work request already completed — presenting Prospect Batch Review from the latest result.',
        planningState: 'scout_handoff_completed',
        workRequestId: executeWorkRequestId,
      };
    }
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF;
    return {
      action: 'execute_existing_scout_work_request',
      approveKind: null,
      emitKind: null,
      memory,
      messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note:
        'Executing the existing Scout work request. Max will not create a new handoff or ask for build-proposal approval again.',
      planningState: 'scout_handoff_queued',
      workRequestId: executeWorkRequestId,
    };
  }

  // HARD GUARD: Hand brief to Scout — approve + queue work request (not Max live sourcing).
  if (
    acquisitionIntent === PROSPECT_ACQUISITION_INTENTS.HAND_BRIEF_TO_SCOUT
  ) {
    memory = markProspectCriteriaApproved(memory);
    if (
      priorBuild ||
      approvedArtifactsInclude(memory, ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL)
    ) {
      memory = markArtifactApproved(
        memory,
        ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
      );
    }
    memory = markArtifactGenerated(
      memory,
      ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF,
      'approved'
    );
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF;
    return {
      action: 'hand_brief_to_scout',
      approveKind: ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF,
      emitKind: ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF,
      memory,
      messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note:
        'Approving the Scout Handoff Brief and queuing a Scout work request. Max does not pretend Scout ran unless sourcing execution is wired.',
      planningState: 'scout_handoff_queued',
    };
  }

  // HARD GUARD: Scout Handoff Brief is a planning artifact — never live sourcing.
  if (
    acquisitionIntent ===
    PROSPECT_ACQUISITION_INTENTS.CREATE_SCOUT_HANDOFF_BRIEF
  ) {
    memory = markProspectCriteriaApproved(memory);
    if (
      priorBuild ||
      approvedArtifactsInclude(memory, ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL)
    ) {
      memory = markArtifactApproved(
        memory,
        ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
      );
    }
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF;
    return {
      action: 'emit_scout_handoff_brief',
      approveKind: null,
      emitKind: ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF,
      memory,
      messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note:
        'Creating the Scout Handoff Brief from approved campaign/list criteria — planning only. Say “Hand this brief to Scout” to approve and queue Scout.',
      planningState: 'scout_handoff_brief',
    };
  }

  // HARD GUARD: explicit live-sourcing approval never regenerates placeholders.
  // Only when the operator asks Max to perform live sourcing — not sticky alone.
  const liveApprovalNow = looksLikeLiveSourcingApproval(text);
  const blockPlaceholderAfterLive =
    Boolean(memory.liveSourcingApproved) &&
    looksLikeProspectListDraftRequest(text);
  if (liveApprovalNow || blockPlaceholderAfterLive) {
    memory = markLiveSourcingApproved(memory);
    memory = markProspectCriteriaApproved(memory);
    if (priorBuild || approvedArtifactsInclude(memory, ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL)) {
      memory = markArtifactApproved(
        memory,
        ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
      );
    }
    return {
      action: 'emit_live_sourcing',
      approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      emitKind: null,
      memory,
      messageClass,
      note: null,
      planningState: 'live_sourcing_approved',
      liveSourcingApproved: true,
    };
  }

  // HARD GUARD: draft request + approved criteria + approved build proposal
  // always routes to draft — never criteria replay / revise fallback.
  if (
    shouldForceProspectListDraft(text, memory, {
      priorCriteriaPreview: priorCriteria,
      priorBuildProposal: priorBuild,
    })
  ) {
    memory = markProspectCriteriaApproved(memory);
    memory = markArtifactApproved(
      memory,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    memory.nextRecommendedArtifact =
      ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
    return {
      action: 'emit_prospect_list_draft',
      approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      emitKind: ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
      memory,
      messageClass: looksLikeProspectListDraftRequest(text)
        ? MESSAGE_CLASSES.ARTIFACT_REQUEST
        : messageClass,
      note:
        'Build proposal approved — generating the first reviewable prospect list draft.',
      planningState: 'prospect_list_draft_requested',
    };
  }

  const criteriaShown = Boolean(
    priorCriteria &&
      (priorCriteria.kind === 'prospect_list_criteria_preview' ||
        opts.step === 'prospect_list_criteria_preview' ||
        opts.step === 'prospect_list_criteria_approved' ||
        opts.step === 'prospect_list_build_proposal' ||
        opts.step === 'prospect_list_build_proposal_approved')
  );
  const criteriaApproved =
    Boolean(priorCriteria && priorCriteria.status === 'approved') ||
    approvedArtifactsInclude(memory, ARTIFACT_KINDS.PROSPECT_CRITERIA) ||
    approvedArtifactsInclude(
      memory,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
  const buildShown = Boolean(
    priorBuild &&
      (priorBuild.kind === ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL ||
        priorBuild.kind === 'prospect_list_build_proposal')
  );
  const buildApproved =
    Boolean(priorBuild && priorBuild.status === 'approved') ||
    approvedArtifactsInclude(
      memory,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    ) ||
    opts.step === 'prospect_list_build_proposal_approved' ||
    opts.step === 'prospect_list_draft_requested' ||
    opts.step === 'prospect_list_draft_generated';
  const draftShown = Boolean(
    priorDraft &&
      (priorDraft.kind === ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT ||
        priorDraft.kind === 'reviewable_prospect_list_draft')
  );
  const wantsDraft = looksLikeProspectListDraftRequest(text);
  const allowReplay =
    looksLikeExplicitReplayRequest(text) &&
    /\b(criteria|preview)\b/i.test(text) &&
    !looksLikeNextPlanningRequest(text) &&
    !wantsDraft;

  // First-time criteria emission is handled by the caller when criteria are new.
  if (!criteriaShown) {
    return {
      action: 'emit_criteria',
      approveKind: null,
      emitKind: ARTIFACT_KINDS.PROSPECT_CRITERIA,
      memory,
      messageClass,
      note: null,
      planningState: null,
    };
  }

  if (allowReplay) {
    return {
      action: 'replay_criteria',
      approveKind: null,
      emitKind: ARTIFACT_KINDS.PROSPECT_CRITERIA,
      memory,
      messageClass,
      note: null,
      planningState: 'prospect_list_criteria',
    };
  }

  // Build proposal approved (or shown) + explicit draft request → draft path.
  if (wantsDraft && (buildApproved || buildShown)) {
    memory = markProspectCriteriaApproved(memory);
    memory = markArtifactApproved(
      memory,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    memory.nextRecommendedArtifact =
      ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
    return {
      action: 'emit_prospect_list_draft',
      approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      emitKind: ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
      memory,
      messageClass: looksLikeApprovalPlusNextRequest(text)
        ? MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST
        : MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note:
        'Build proposal approved — generating the first reviewable prospect list draft.',
      planningState: draftShown
        ? 'prospect_list_draft_generated'
        : 'prospect_list_draft_requested',
    };
  }

  if (messageClass === MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST) {
    if (buildShown || buildApproved) {
      memory = markProspectCriteriaApproved(memory);
      memory = markArtifactApproved(
        memory,
        ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
      );
      if (wantsDraft || looksLikeNextPlanningRequest(text)) {
        memory.nextRecommendedArtifact =
          ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
        return {
          action: 'emit_prospect_list_draft',
          approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
          emitKind: ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
          memory,
          messageClass,
          note:
            'Build proposal approved — generating the first reviewable prospect list draft.',
          planningState: 'prospect_list_draft_requested',
        };
      }
      memory.nextRecommendedArtifact =
        ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
      return {
        action: 'ack_build_approval',
        approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
        emitKind: null,
        memory,
        messageClass,
        note:
          'Build proposal approved. Ask me to generate the first reviewable prospect list batch when ready.',
        planningState: 'prospect_list_build_proposal_approved',
      };
    }
    memory = markProspectCriteriaApproved(memory);
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL;
    return {
      action: 'emit_build_proposal',
      approveKind: ARTIFACT_KINDS.PROSPECT_CRITERIA,
      emitKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      memory,
      messageClass,
      note: 'Criteria approved — advancing to Prospect List Build Proposal.',
      planningState: 'prospect_list_criteria_approved',
    };
  }

  if (messageClass === MESSAGE_CLASSES.APPROVAL) {
    if (buildShown || buildApproved) {
      memory = markProspectCriteriaApproved(memory);
      memory = markArtifactApproved(
        memory,
        ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
      );
      memory.nextRecommendedArtifact =
        ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
      return {
        action: 'ack_build_approval',
        approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
        emitKind: null,
        memory,
        messageClass,
        note:
          'Build proposal approved. Ask me to generate the first reviewable prospect list batch when ready.',
        planningState: 'prospect_list_build_proposal_approved',
      };
    }
    memory = markProspectCriteriaApproved(memory);
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL;
    // Pure approval: acknowledge and offer/emit the next artifact (build proposal)
    // rather than replaying criteria.
    return {
      action: 'emit_build_proposal',
      approveKind: ARTIFACT_KINDS.PROSPECT_CRITERIA,
      emitKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      memory,
      messageClass,
      note: 'Criteria approved. Here is the Prospect List Build Proposal — still planning-only.',
      planningState: 'prospect_list_criteria_approved',
    };
  }

  if (
    messageClass === MESSAGE_CLASSES.ARTIFACT_REQUEST ||
    looksLikeNextPlanningRequest(text) ||
    wantsDraft
  ) {
    if (buildShown || buildApproved) {
      memory = markProspectCriteriaApproved(memory);
      if (!buildApproved) {
        memory = markArtifactApproved(
          memory,
          ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
        );
      }
      memory.nextRecommendedArtifact =
        ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
      if (wantsDraft) {
        return {
          action: 'emit_prospect_list_draft',
          approveKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
          emitKind: ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
          memory,
          messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
          note: null,
          planningState: 'prospect_list_draft_requested',
        };
      }
      return {
        action: 'ack_build_approval',
        approveKind: buildApproved
          ? null
          : ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
        emitKind: null,
        memory,
        messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
        note:
          'Build proposal is ready. Ask me to generate the first reviewable prospect list batch when you want the draft.',
        planningState: 'prospect_list_build_proposal_approved',
      };
    }
    if (criteriaShown && !criteriaApproved) {
      memory = markProspectCriteriaApproved(memory);
    }
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL;
    return {
      action: 'emit_build_proposal',
      approveKind: criteriaApproved ? null : ARTIFACT_KINDS.PROSPECT_CRITERIA,
      emitKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      memory,
      messageClass: MESSAGE_CLASSES.ARTIFACT_REQUEST,
      note: null,
      planningState: 'prospect_list_criteria_approved',
    };
  }

  // Criteria already shown — never silently re-emit unless explicit revise.
  // If Prospect Batch Review is already active, do not fall back to Build Proposal.
  if (criteriaApproved) {
    if (priorBatchReview || opts.step === 'prospect_batch_review') {
      if (isProspectBatchReviewAlreadyApproved(priorBatchReview)) {
        memory = markArtifactApproved(
          memory,
          ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW
        );
        memory.nextRecommendedArtifact =
          ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW;
        return {
          action: 'ack_prospect_batch_approval',
          approveKind: ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
          emitKind: ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
          memory,
          messageClass,
          note:
            'Batch 1 approved. Next step: prepare outreach strategy preview.',
          planningState: 'outreach_strategy_preview',
          batch1Approved: true,
        };
      }
      memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW;
      return {
        action: 'hold_prospect_batch_review',
        approveKind: null,
        emitKind: null,
        memory,
        messageClass,
        note:
          'Prospect Batch Review is ready. Approve the accepted cold first-pass candidates as Batch 1 to continue, or provide a relationship correction.',
        planningState: 'prospect_batch_review',
      };
    }
    if (buildShown || buildApproved) {
      memory.nextRecommendedArtifact =
        ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT;
      return {
        action: buildApproved ? 'ack_build_approval' : 'hold',
        approveKind: null,
        emitKind: null,
        memory,
        messageClass,
        note: buildApproved
          ? 'Build proposal already approved. Ask me to generate the first reviewable prospect list batch when ready.'
          : 'Criteria already approved. Approve the build proposal, or ask me to generate the first reviewable prospect list batch.',
        planningState: buildApproved
          ? 'prospect_list_build_proposal_approved'
          : 'prospect_list_criteria_approved',
      };
    }
    memory.nextRecommendedArtifact = ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL;
    return {
      action: 'emit_build_proposal',
      approveKind: null,
      emitKind: ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL,
      memory,
      messageClass,
      note: null,
      planningState: 'prospect_list_criteria_approved',
    };
  }

  return {
    action: 'ack_approval',
    approveKind: null,
    emitKind: null,
    memory,
    messageClass,
    note:
      'The Prospect List Criteria Preview is ready. Approve it, or ask how I would approach building the first list.',
    planningState: null,
  };
}

function humanArtifactLabel(kind) {
  switch (kind) {
    case ARTIFACT_KINDS.BLUEPRINT:
      return 'Business Blueprint';
    case ARTIFACT_KINDS.GROWTH_DIRECTION:
      return 'Growth Direction';
    case ARTIFACT_KINDS.CAMPAIGN_PREVIEW:
      return 'Campaign Preview';
    case ARTIFACT_KINDS.PROSPECT_CRITERIA:
    case ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW:
      return 'Prospect List Criteria Preview';
    case ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL:
      return 'Prospect List Build Proposal';
    case ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF:
      return 'Scout Handoff Brief';
    case ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT:
      return 'Reviewable Prospect List Draft';
    case ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW:
      return 'Prospect Batch Review';
    case ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW:
      return 'Outreach Strategy Preview';
    default:
      return kind || 'artifact';
  }
}

/**
 * Check whether required evidence is present before generating an artifact.
 * @returns {{ ready: boolean, missing: string[], weak: string[], confidenceNote: string|null, followUp: string|null }}
 */
function checkArtifactReadiness(kind, opts = {}) {
  const required = ARTIFACT_REQUIRED_SECTIONS[kind] || [];
  const sectionState = opts.sectionState || {};
  const normalizedFacts = opts.normalizedFacts || {};
  const minConfidence =
    typeof opts.minConfidence === 'number'
      ? opts.minConfidence
      : MIN_ARTIFACT_SECTION_CONFIDENCE;

  const missing = [];
  const weak = [];

  for (const section of required) {
    const row = sectionState[section] || {};
    const summary = String(row.summary || '').trim();
    const confidence = Number(row.confidence) || 0;
    const factPresent = sectionHasNormalizedEvidence(section, normalizedFacts);

    if (!summary && !factPresent) {
      missing.push(section);
    } else if (confidence > 0 && confidence < minConfidence) {
      weak.push(section);
    } else if (!summary && factPresent && confidence < minConfidence) {
      weak.push(section);
    }
  }

  const ready = missing.length === 0;
  let confidenceNote = null;
  let followUp = null;

  if (!ready) {
    followUp = buildReadinessFollowUp(kind, missing);
    confidenceNote = `Need clearer evidence on ${missing.map(humanSectionLabel).join(', ')} before generating a solid ${humanArtifactLabel(kind)}.`;
  } else if (weak.length) {
    confidenceNote = `Directional only — evidence is still thin on ${weak.map(humanSectionLabel).join(', ')}.`;
  }

  return { ready, missing, weak, confidenceNote, followUp };
}

function sectionHasNormalizedEvidence(section, facts) {
  if (!facts || typeof facts !== 'object') return false;
  const map = {
    identity: ['business_name', 'business_description'],
    services: ['services'],
    idealCustomers: ['ideal_customers', 'ideal_customer_traits'],
    avoidCustomers: ['disqualified_customers'],
    targetMarkets: ['geography', 'vertical_focus', 'growth_focus'],
    competitiveAdvantages: ['differentiation'],
    brandVoice: ['brand_voice'],
    campaignGoals: ['ninety_day_outcomes', 'growth_focus'],
    successMetrics: ['success_metrics'],
  };
  const keys = map[section] || [];
  return keys.some((k) => {
    const v = facts[k];
    if (Array.isArray(v)) return v.some((x) => String(x || '').trim());
    return Boolean(String(v || '').trim());
  });
}

function humanSectionLabel(section) {
  const labels = {
    identity: 'business identity',
    services: 'services',
    idealCustomers: 'ideal customers',
    avoidCustomers: 'customers to avoid',
    targetMarkets: 'target markets',
    competitiveAdvantages: 'differentiation',
    brandVoice: 'brand voice',
    campaignGoals: 'near-term goals',
    successMetrics: 'success metrics',
  };
  return labels[section] || section;
}

function buildReadinessFollowUp(kind, missing) {
  const first = missing && missing[0];
  if (!first) return null;
  const probes = {
    identity: 'Before I draft that, what is the business name and what does it do today?',
    services: 'Before I draft that, which services should we treat as primary?',
    idealCustomers: 'Before I draft that, who is the strongest-fit customer segment?',
    avoidCustomers: 'Before I draft that, who should we deliberately avoid?',
    targetMarkets: 'Before I draft that, where should we focus first geographically or by vertical?',
    campaignGoals: 'Before I draft that, what 90-day outcome would make this feel successful?',
  };
  return (
    probes[first] ||
    `Before I generate the ${humanArtifactLabel(kind)}, can you clarify ${humanSectionLabel(first)}?`
  );
}

/**
 * Rewrite raw / stitched answer text into clean business language.
 * Deterministic — no LLM. Strips prompt echoes and conversational wrappers.
 */
function synthesizeBusinessLanguage(text, opts = {}) {
  let s = String(text || '').trim();
  if (!s) return '';

  const businessName = String(opts.businessName || '').trim();

  // Strip operator / meta instruction language entirely.
  if (
    /\b(please\s+refine|regenerate(?:\s+the\s+brief)?|instructions?\s+to\s+max|not\s+facts?\s+about|raw\s+interview\s+answers|clean\s+business\s+language)\b/i.test(
      s
    )
  ) {
    return '';
  }

  s = s
    .replace(/\b(i\s+also\s+forgot(?:\s+to\s+mention)?|forgot\s+to\s+mention|also[,:\s]+|for\s+context[,:\s]*)/gi, ' ')
    .replace(/\b(actually|correction|i meant(?:\s+to\s+say)?|replace that|disregard\s+(?:the\s+)?(?:last|previous)\s+(?:message|answer)|please\s+replace\s+with(?:\s+the\s+following)?)\b[,;:\s-]*/gi, ' ')
    .replace(/\bwhen a great-fit customer chooses\b[^.!?]*/gi, ' ')
    .replace(/\bif i were writing as (?:your|the) brand[^.!?]*/gi, ' ')
    .replace(/\bover the next 90 days(?:,?\s*this growth work)?\b/gi, ' ')
    .replace(/\bwe will know(?: the growth work is working)?(?:\s+by)?\b/gi, ' ')
    .replace(/\bi don't want to work with\b/gi, 'avoid')
    .replace(/\bpaint me a picture of the ideal customer\b/gi, ' ')
    .replace(/\s+for\s+(?:the\s+)?(?:icp|ideal\s+customers?|ideal\s+customer\s+profile)\b/gi, ' ')
    .replace(/\bas\s+part\s+of\s+(?:my|our|the)\s+ideal\s+customer(?:\s+profile)?\b/gi, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim()
    .replace(/^[,;:\-–—]\s*/, '')
    .replace(/\s+[,;]$/, '');

  if (!s) return '';

  // Prefer sentence case without dumping the user's exact preamble.
  if (opts.section === 'idealCustomers' && businessName) {
    if (!/\b(ideal|target|serve|prefer)\b/i.test(s)) {
      s = `${businessName} prioritizes ${s}`;
    }
  } else if (opts.section === 'avoidCustomers' && businessName) {
    if (!/\b(avoid|decline|does not|won't)\b/i.test(s)) {
      s = `${businessName} deliberately avoids ${s}`;
    }
  } else if (opts.section === 'services' && businessName) {
    if (!/\b(provides?|offers?|delivers?|services?)\b/i.test(s)) {
      s = `${businessName} provides ${s}`;
    }
  }

  // Capitalize first letter.
  s = s.charAt(0).toUpperCase() + s.slice(1);
  if (!/[.!?]$/.test(s)) s = `${s}.`;
  return s;
}

/**
 * Conversational ack for reasoning-layer message classes not covered by CIE.
 */
function reasoningAck(messageClass, opts = {}) {
  const activeQuestion = opts.activeQuestion || null;
  const reopen = opts.reopenPrompt || (activeQuestion && activeQuestion.prompt) || null;

  switch (messageClass) {
    case MESSAGE_CLASSES.APPROVAL:
      return reopen
        ? `Noted — I'll take that as approval of what we have so far. For this question: ${reopen}`
        : "Noted — I'll take that as approval of what we have so far.";
    case MESSAGE_CLASSES.SKIP: {
      const debtNote = "I'll mark this as open and we can return to it if it matters later.";
      return reopen ? `${debtNote} Next: ${reopen}` : debtNote;
    }
    case MESSAGE_CLASSES.INSUFFICIENT_ANSWER:
      return opts.probe || "Could you get a bit more specific so I can capture that cleanly?";
    case MESSAGE_CLASSES.CLARIFICATION_REQUEST:
      return reopen
        ? `Happy to clarify. ${opts.clarifyNote || "I'm looking for a concrete business fact here."} ${reopen}`
        : opts.clarifyNote || "I'm looking for a concrete business fact here.";
    case MESSAGE_CLASSES.OFF_TOPIC:
      return reopen
        ? `Noted. Whenever you're ready: ${reopen}`
        : "Noted. Whenever you're ready, we can continue.";
    case MESSAGE_CLASSES.REFINEMENT_FEEDBACK:
      return "Understood — I'll treat that as guidance for how I write, not as business evidence.";
    default:
      return "Thanks — I've got that.";
  }
}

/**
 * High-level turn planner: classify + optional cross-section route + sufficiency.
 * Pure function — does not mutate store.
 */
function planReasoningTurn(text, context = {}) {
  const activeQuestion = context.activeQuestion || null;
  const messageClass = classifyReasoningMessage(text, context);
  const memory = ensureReasoningMemory(context.state || {});

  let targetSection = activeQuestion && activeQuestion.section;
  let routeReason = 'active_question';
  let cross = { section: null, domain: null, confidence: 0 };

  if (messageClass === MESSAGE_CLASSES.ADD_ON || messageClass === MESSAGE_CLASSES.CORRECTION) {
    cross = inferCrossSectionTarget(text, activeQuestion, context.crossSectionHelpers || {});
    if (cross.section && cross.confidence >= 0.55) {
      targetSection = cross.section;
      routeReason = messageClass === MESSAGE_CLASSES.CORRECTION ? 'correction' : 'add_on';
    }
  } else if (messageClass === MESSAGE_CLASSES.DIRECT_ANSWER) {
    // Soft cross-section: "I forgot property managers" on avoid question.
    cross = inferCrossSectionTarget(text, activeQuestion, context.crossSectionHelpers || {});
    if (cross.section && cross.confidence >= 0.8) {
      return {
        messageClass: MESSAGE_CLASSES.ADD_ON,
        targetSection: cross.section,
        routeReason: 'cross_section_add_on',
        sufficiency: { sufficient: true, reason: null, shouldProbe: false },
        probe: null,
        memory,
        cross,
      };
    }
  }

  let sufficiency = { sufficient: true, reason: null, shouldProbe: false };
  let probe = null;

  if (messageClass === MESSAGE_CLASSES.DIRECT_ANSWER) {
    sufficiency = assessAnswerSufficiency(text, activeQuestion, {
      hasSpecificity: Boolean(context.hasSpecificity),
      contradiction: Boolean(context.contradiction),
    });
    if (!sufficiency.sufficient) {
      probe = buildProbingFollowUp(activeQuestion, sufficiency, context.businessName);
      return {
        messageClass: MESSAGE_CLASSES.INSUFFICIENT_ANSWER,
        targetSection,
        routeReason,
        sufficiency,
        probe,
        memory,
        cross,
      };
    }
  }

  if (messageClass === MESSAGE_CLASSES.INSUFFICIENT_ANSWER) {
    sufficiency = assessAnswerSufficiency(text, activeQuestion, context);
    probe = buildProbingFollowUp(activeQuestion, sufficiency, context.businessName);
  }

  return {
    messageClass,
    targetSection,
    routeReason,
    sufficiency,
    probe,
    memory,
    cross,
  };
}

module.exports = {
  MESSAGE_CLASSES,
  ARTIFACT_KINDS,
  ARTIFACT_ORDER,
  ARTIFACT_REQUIRED_SECTIONS,
  PROSPECT_ACQUISITION_INTENTS,
  MIN_ARTIFACT_SECTION_CONFIDENCE,
  emptyReasoningMemory,
  ensureReasoningMemory,
  classifyReasoningMessage,
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
  setPendingUserRequest,
  resolveNextArtifact,
  resolveCampaignArtifactAction,
  shouldBlockCriteriaQuestionReplay,
  isBannedCriteriaReplayQuestion,
  checkArtifactReadiness,
  synthesizeBusinessLanguage,
  reasoningAck,
  planReasoningTurn,
  looksLikeApproval,
  looksLikeApprovalPlusNextRequest,
  looksLikeNextPlanningRequest,
  looksLikeProspectListDraftRequest,
  looksLikeProspectBatchReviewRequest,
  looksLikeProspectBatchReviewCorrection,
  looksLikeProspectBatchReviewApproval,
  hasActiveProspectBatchReview,
  isProspectBatchReviewAlreadyApproved,
  hasCompletedScoutCandidateBatch,
  looksLikeScoutHandoffBriefRequest,
  looksLikeHandBriefToScoutRequest,
  looksLikeExecuteExistingScoutWorkRequest,
  extractWorkRequestIdFromMessage,
  looksLikeLiveSourcingApproval,
  classifyProspectAcquisitionIntent,
  looksLikeReviseCriteriaRequest,
  inferApprovedArtifactsFromMessage,
  shouldForceProspectListDraft,
  markLiveSourcingApproved,
  isLiveSourcingApproved,
  looksLikeArtifactRequest,
  looksLikeExplicitReplayRequest,
  looksLikeSkip,
  looksLikeClarificationRequest,
  looksLikeVagueAnswer,
  humanArtifactLabel,
  humanSectionLabel,
};
