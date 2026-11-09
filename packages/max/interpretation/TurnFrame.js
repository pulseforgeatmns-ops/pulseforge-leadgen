'use strict';

/**
 * P0 Unified Reasoning Architecture — TurnFrame contract (PROTOTYPE, NOT WIRED).
 *
 * A TurnFrame is the single structured interpretation of one operator turn.
 * See docs/P0-MAX-UNIFIED-REASONING-ARCHITECTURE.md §3.1.
 *
 * Invariants enforced here (not by the producer):
 *   1. Enum-only. No free text from a producer survives validation.
 *   2. Referents come ONLY from the candidate set the session supplied. Any
 *      other id is dropped and turns into an ambiguity.
 *   3. The tenant is never chosen by a producer. It is copied from the
 *      session context; a producer can only flag a foreign-tenant reference.
 *   4. A frame never grants authority. `authority.grantsAuthority` is a frozen
 *      `false`. `executionCandidate` only says "the deterministic authority
 *      path may evaluate this turn"; it is computed here from validated
 *      fields, never taken from the producer.
 *   5. Negation is monotonic: a negated or hypothetical action can never
 *      become an execution candidate.
 */

const FRAME_VERSION = 'turn-frame/v1';

const INTENTS = Object.freeze([
  'question', 'investigation', 'instruction', 'correction', 'hypothetical',
  'authorization', 'prohibition', 'ambiguous', 'other',
]);
const SUBJECT_KINDS = Object.freeze([
  'mission', 'agent_activity', 'prospecting', 'outbound', 'crm_pipeline',
  'campaign', 'pending_decision', 'portfolio', 'business_process', 'none',
]);
const RELATIONS = Object.freeze([
  'new_topic', 'follow_up', 'correction', 'refinement', 'clarification_answer', 'none',
]);
const REFERENT_ROLES = Object.freeze(['primary', 'comparison', 'excluded']);
const EVIDENCE_DOMAINS = Object.freeze([
  'mission_lifecycle', 'scout_execution', 'outbound_delivery', 'outbound_authorization',
  'crm_pipeline', 'agent_execution', 'campaign_performance', 'operator_decisions',
]);
const FACETS = Object.freeze([
  'status', 'cause', 'consequence', 'relevance', 'history', 'comparison',
  'next_step', 'definition', 'recommendation', 'feasibility',
]);
const OUTCOMES = Object.freeze([
  'explanation', 'diagnosis', 'recommendation', 'status', 'comparison',
  'action', 'clarification', 'acknowledgement',
]);
const ACTION_VERBS = Object.freeze([
  'none', 'approve', 'reject', 'cancel', 'pause', 'resume', 'restart',
  'send', 'create', 'modify', 'run_agent', 'leave_conversation',
]);
const POLARITIES = Object.freeze(['affirmative', 'negated', 'hypothetical', 'none']);
const SCOPES = Object.freeze(['focused', 'portfolio']);
const AMBIGUITY_KINDS = Object.freeze(['referent', 'intent', 'scope', 'action']);
const PRODUCERS = Object.freeze(['lexical', 'model', 'model_degraded_to_lexical']);

/** Verbs that, affirmatively, change state and therefore need deterministic authority. */
const STATE_CHANGING_VERBS = Object.freeze([
  'approve', 'reject', 'cancel', 'pause', 'resume', 'restart', 'send', 'create', 'modify', 'run_agent',
]);

const EXECUTION_CANDIDATE_MIN_CONFIDENCE = 0.8;

const enumOf = (values) => ({ type: 'string', enum: [...values] });
const idList = { type: 'array', items: { type: 'string' } };

/**
 * JSON Schema handed to a model producer (structured output). Every object has
 * additionalProperties:false and every property is required, so the producer
 * cannot add fields. Candidate ids are strings here; membership is checked by
 * validateTurnFrame (a schema cannot know the per-turn candidate set).
 */
const TURN_FRAME_OUTPUT_SCHEMA = Object.freeze({
  type: 'object',
  additionalProperties: false,
  required: [
    'intent', 'subjectKind', 'relationToPrior', 'referents', 'evidenceNeeds', 'facets',
    'requestedOutcome', 'requestedAction', 'scope', 'foreignTenantReference',
    'ambiguities', 'confidence',
  ],
  properties: {
    intent: enumOf(INTENTS),
    subjectKind: enumOf(SUBJECT_KINDS),
    relationToPrior: enumOf(RELATIONS),
    referents: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['candidateId', 'role'],
        properties: { candidateId: { type: 'string' }, role: enumOf(REFERENT_ROLES) },
      },
    },
    evidenceNeeds: { type: 'array', items: enumOf(EVIDENCE_DOMAINS) },
    facets: { type: 'array', items: enumOf(FACETS) },
    requestedOutcome: enumOf(OUTCOMES),
    requestedAction: {
      type: 'object',
      additionalProperties: false,
      required: ['verb', 'targetCandidateId', 'polarity'],
      properties: {
        verb: enumOf(ACTION_VERBS),
        targetCandidateId: { anyOf: [{ type: 'string' }, { type: 'null' }] },
        polarity: enumOf(POLARITIES),
      },
    },
    scope: enumOf(SCOPES),
    foreignTenantReference: { type: 'boolean' },
    ambiguities: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['kind', 'candidateIds'],
        properties: { kind: enumOf(AMBIGUITY_KINDS), candidateIds: idList },
      },
    },
    confidence: { type: 'number' },
  },
});

class TurnFrameValidationError extends Error {
  constructor(field) {
    // Field names come from our schema only; never echo producer values.
    super(`Invalid turn frame field: ${field}`);
    this.code = 'invalid_turn_frame';
    this.field = field;
  }
}

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

function checkEnum(value, values, field) {
  if (!values.includes(value)) throw new TurnFrameValidationError(field);
  return value;
}

function checkEnumList(value, values, field) {
  if (!Array.isArray(value)) throw new TurnFrameValidationError(field);
  return [...new Set(value.map((v) => checkEnum(v, values, field)))];
}

function checkKeys(obj, allowed, field) {
  if (!isObject(obj)) throw new TurnFrameValidationError(field);
  for (const k of Object.keys(obj)) if (!allowed.includes(k)) throw new TurnFrameValidationError(`${field}.unexpected`);
  for (const k of allowed) if (!(k in obj)) throw new TurnFrameValidationError(`${field}.${k}`);
}

/**
 * @typedef {{ id: string, kind: string, label?: string }} FrameCandidate
 * @typedef {{
 *   tenantId: string|number,
 *   candidates?: FrameCandidate[],
 *   pendingDecisionId?: string|null,
 *   producer?: string,
 * }} FrameContext
 */

/**
 * Validate a producer's raw output against the contract and the per-turn
 * context. Structural errors throw (the caller degrades to the lexical
 * producer). Semantic violations are repaired toward SAFETY: dropped ids,
 * downgraded intents, forced clarification — never toward authority.
 *
 * @param {unknown} raw
 * @param {FrameContext} ctx
 * @returns {Readonly<object>} validated TurnFrame
 */
function validateTurnFrame(raw, ctx = {}) {
  if (ctx.tenantId == null || String(ctx.tenantId).trim() === '') {
    throw new TurnFrameValidationError('context.tenantId');
  }
  checkKeys(raw, TURN_FRAME_OUTPUT_SCHEMA.required, 'frame');
  const candidates = Array.isArray(ctx.candidates) ? ctx.candidates : [];
  const candidateIds = new Set(candidates.map((c) => String(c.id)));
  const repairs = [];

  let intent = checkEnum(raw.intent, INTENTS, 'intent');
  const subjectKind = checkEnum(raw.subjectKind, SUBJECT_KINDS, 'subjectKind');
  const relationToPrior = checkEnum(raw.relationToPrior, RELATIONS, 'relationToPrior');
  const requestedOutcome = checkEnum(raw.requestedOutcome, OUTCOMES, 'requestedOutcome');
  const scope = checkEnum(raw.scope, SCOPES, 'scope');
  const evidenceNeeds = checkEnumList(raw.evidenceNeeds, EVIDENCE_DOMAINS, 'evidenceNeeds');
  const facets = checkEnumList(raw.facets, FACETS, 'facets');
  if (typeof raw.foreignTenantReference !== 'boolean') throw new TurnFrameValidationError('foreignTenantReference');
  if (typeof raw.confidence !== 'number' || !Number.isFinite(raw.confidence) || raw.confidence < 0 || raw.confidence > 1) {
    throw new TurnFrameValidationError('confidence');
  }
  let confidence = raw.confidence;

  // Referents: candidate-constrained.
  if (!Array.isArray(raw.referents)) throw new TurnFrameValidationError('referents');
  const ambiguities = [];
  const referents = [];
  for (const r of raw.referents) {
    checkKeys(r, ['candidateId', 'role'], 'referents[]');
    const role = checkEnum(r.role, REFERENT_ROLES, 'referents[].role');
    const id = String(r.candidateId);
    if (!candidateIds.has(id)) {
      repairs.push('referent_not_in_candidates');
      if (role === 'primary') ambiguities.push({ kind: 'referent', candidateIds: [] });
      continue;
    }
    if (!referents.some((x) => x.candidateId === id)) referents.push({ candidateId: id, role });
  }

  if (!Array.isArray(raw.ambiguities)) throw new TurnFrameValidationError('ambiguities');
  for (const a of raw.ambiguities) {
    checkKeys(a, ['kind', 'candidateIds'], 'ambiguities[]');
    const kind = checkEnum(a.kind, AMBIGUITY_KINDS, 'ambiguities[].kind');
    if (!Array.isArray(a.candidateIds)) throw new TurnFrameValidationError('ambiguities[].candidateIds');
    ambiguities.push({ kind, candidateIds: a.candidateIds.map(String).filter((id) => candidateIds.has(id)) });
  }

  // Requested action.
  checkKeys(raw.requestedAction, ['verb', 'targetCandidateId', 'polarity'], 'requestedAction');
  const verb = checkEnum(raw.requestedAction.verb, ACTION_VERBS, 'requestedAction.verb');
  let polarity = checkEnum(raw.requestedAction.polarity, POLARITIES, 'requestedAction.polarity');
  let target = raw.requestedAction.targetCandidateId;
  if (target !== null && typeof target !== 'string') throw new TurnFrameValidationError('requestedAction.targetCandidateId');
  if (target !== null && !candidateIds.has(target)) {
    repairs.push('action_target_not_in_candidates');
    target = null;
    if (verb !== 'none') ambiguities.push({ kind: 'action', candidateIds: [] });
  }
  if (verb === 'none' && polarity !== 'none') polarity = 'none';

  // Internal consistency, repaired toward safety.
  if (intent === 'prohibition' && polarity === 'affirmative') {
    repairs.push('prohibition_with_affirmative_action');
    intent = 'ambiguous';
    ambiguities.push({ kind: 'intent', candidateIds: [] });
  }
  if (polarity === 'negated' && (intent === 'authorization' || intent === 'instruction')) {
    repairs.push('negated_action_reclassified_as_prohibition');
    intent = 'prohibition';
  }
  if (intent === 'authorization') {
    const pending = ctx.pendingDecisionId ? String(ctx.pendingDecisionId) : null;
    if (!pending || (target !== null && target !== pending)) {
      repairs.push('authorization_without_bound_pending_decision');
      intent = 'ambiguous';
      ambiguities.push({ kind: 'action', candidateIds: pending ? [pending] : [] });
    }
  }
  if (repairs.length) confidence = Math.min(confidence, 0.5);

  const producer = PRODUCERS.includes(ctx.producer) ? ctx.producer : 'lexical';
  const clarificationRequired = intent === 'ambiguous' || ambiguities.length > 0;
  // A degraded frame (model unavailable/invalid) may describe the turn, but it
  // never nominates it for execution: the lexical fallback must not convert an
  // uncertain request into an execution candidate (§8 failure handling).
  const executionCandidate =
    producer !== 'model_degraded_to_lexical' &&
    !clarificationRequired &&
    !raw.foreignTenantReference &&
    (intent === 'instruction' || intent === 'authorization') &&
    polarity === 'affirmative' &&
    STATE_CHANGING_VERBS.includes(verb) &&
    confidence >= EXECUTION_CANDIDATE_MIN_CONFIDENCE;

  return deepFreeze({
    version: FRAME_VERSION,
    producer,
    tenantId: String(ctx.tenantId),
    intent,
    subjectKind,
    relationToPrior,
    referents,
    evidenceNeeds,
    facets,
    requestedOutcome,
    requestedAction: { verb, targetCandidateId: target, polarity },
    scope,
    foreignTenantReference: raw.foreignTenantReference,
    ambiguities,
    clarificationRequired,
    confidence,
    executionCandidate,
    authority: { grantsAuthority: false },
    repairs,
  });
}

function deepFreeze(o) {
  if (o && typeof o === 'object') {
    Object.values(o).forEach(deepFreeze);
    Object.freeze(o);
  }
  return o;
}

/** A safe frame for when no producer is usable: ambiguous, never actionable. */
function unavailableFrame(ctx) {
  return validateTurnFrame({
    intent: 'ambiguous', subjectKind: 'none', relationToPrior: 'none', referents: [],
    evidenceNeeds: [], facets: [], requestedOutcome: 'clarification',
    requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
    scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0,
  }, ctx);
}

module.exports = {
  FRAME_VERSION,
  INTENTS,
  SUBJECT_KINDS,
  RELATIONS,
  REFERENT_ROLES,
  EVIDENCE_DOMAINS,
  FACETS,
  OUTCOMES,
  ACTION_VERBS,
  POLARITIES,
  SCOPES,
  AMBIGUITY_KINDS,
  PRODUCERS,
  STATE_CHANGING_VERBS,
  EXECUTION_CANDIDATE_MIN_CONFIDENCE,
  TURN_FRAME_OUTPUT_SCHEMA,
  TurnFrameValidationError,
  validateTurnFrame,
  unavailableFrame,
};
