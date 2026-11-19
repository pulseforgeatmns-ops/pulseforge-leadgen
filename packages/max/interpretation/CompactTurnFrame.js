'use strict';

/**
 * Compact structured-output wire format for TurnFrame (P0-MAX-REASONING-001,
 * Gemini latency). PROTOTYPE, NOT WIRED into production routing.
 *
 * Why: Gemini generation time scales with output tokens, and the canonical
 * schema spends most of its ~190 output tokens on long key names
 * ("relationToPrior", "foreignTenantReference", "targetCandidateId", …) and on
 * the nested requestedAction object. This format keeps every semantic field
 * and every enum VALUE unchanged (the model still writes "prohibition",
 * "negated", "cancel", "leave_conversation", …), and only:
 *   1. shortens each key to a fixed code, and
 *   2. flattens requestedAction into three top-level keys (av / at / ap).
 *
 * It is a wire format only. `expandCompactTurnFrame` deterministically maps it
 * back to the canonical producer shape, and the caller then runs the unchanged
 * repairUnsafeAuthorization + validateTurnFrame. Nothing here decides intent,
 * authority, tenant, or referent membership; those stay in TurnFrame.js.
 *
 * Expansion is strict: the exact key set is required (no missing keys, no
 * extra keys, no key aliasing, no defaults). Any deviation throws
 * TurnFrameValidationError, which the producer turns into a degraded
 * (lexical, never-executable) frame. Missing data is never filled in.
 */

const {
  INTENTS, SUBJECT_KINDS, RELATIONS, REFERENT_ROLES, EVIDENCE_DOMAINS, FACETS,
  OUTCOMES, ACTION_VERBS, POLARITIES, SCOPES, AMBIGUITY_KINDS,
  TURN_FRAME_OUTPUT_SCHEMA, TurnFrameValidationError,
} = require('./TurnFrame');

const COMPACT_FORMAT = 'turn-frame-compact/v1';

/** Compact key → canonical field, in canonical (and output) order. */
const TOP_LEVEL_KEYS = Object.freeze([
  ['i', 'intent'],
  ['s', 'subjectKind'],
  ['r', 'relationToPrior'],
  ['ref', 'referents'],
  ['ev', 'evidenceNeeds'],
  ['fa', 'facets'],
  ['o', 'requestedOutcome'],
  ['av', 'requestedAction.verb'],
  ['at', 'requestedAction.targetCandidateId'],
  ['ap', 'requestedAction.polarity'],
  ['sc', 'scope'],
  ['ft', 'foreignTenantReference'],
  ['amb', 'ambiguities'],
  ['cf', 'confidence'],
]);
const REFERENT_KEYS = Object.freeze([['id', 'candidateId'], ['ro', 'role']]);
const AMBIGUITY_KEYS = Object.freeze([['k', 'kind'], ['ids', 'candidateIds']]);

const enumOf = (values, description) => ({ type: 'string', enum: [...values], description });

/**
 * Structured-output schema for the compact format. Same enums as
 * TURN_FRAME_OUTPUT_SCHEMA; every object closed and every key required.
 * `description` names the canonical field so the provider can map each code.
 */
const COMPACT_TURN_FRAME_OUTPUT_SCHEMA = Object.freeze({
  type: 'object',
  additionalProperties: false,
  required: TOP_LEVEL_KEYS.map(([k]) => k),
  properties: {
    i: enumOf(INTENTS, 'intent'),
    s: enumOf(SUBJECT_KINDS, 'subjectKind'),
    r: enumOf(RELATIONS, 'relationToPrior'),
    ref: {
      type: 'array',
      description: 'referents',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['id', 'ro'],
        properties: {
          id: { type: 'string', description: 'candidateId' },
          ro: enumOf(REFERENT_ROLES, 'role'),
        },
      },
    },
    ev: { type: 'array', description: 'evidenceNeeds', items: { type: 'string', enum: [...EVIDENCE_DOMAINS] } },
    fa: { type: 'array', description: 'facets', items: { type: 'string', enum: [...FACETS] } },
    o: enumOf(OUTCOMES, 'requestedOutcome'),
    av: enumOf(ACTION_VERBS, 'requestedAction.verb'),
    at: { anyOf: [{ type: 'string' }, { type: 'null' }], description: 'requestedAction.targetCandidateId' },
    ap: enumOf(POLARITIES, 'requestedAction.polarity'),
    sc: enumOf(SCOPES, 'scope'),
    ft: { type: 'boolean', description: 'foreignTenantReference' },
    amb: {
      type: 'array',
      description: 'ambiguities',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['k', 'ids'],
        properties: {
          k: enumOf(AMBIGUITY_KINDS, 'kind'),
          ids: { type: 'array', items: { type: 'string' }, description: 'candidateIds' },
        },
      },
    },
    cf: { type: 'number', description: 'confidence' },
  },
});

/**
 * Appended (never substituted) to the unchanged SYSTEM_PROMPT for providers
 * that use the compact format. The field instructions above it still name the
 * canonical fields; this only says how to spell them on the wire.
 */
const COMPACT_OUTPUT_LEGEND = `Output keys (compact spelling of the fields above; values are unchanged): ${TOP_LEVEL_KEYS.map(([k, f]) => `${k}=${f}`).join(', ')}. Inside referents: ${REFERENT_KEYS.map(([k, f]) => `${k}=${f}`).join(', ')}. Inside ambiguities: ${AMBIGUITY_KEYS.map(([k, f]) => `${k}=${f}`).join(', ')}.`;

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

function exactKeys(obj, pairs, field) {
  if (!isObject(obj)) throw new TurnFrameValidationError(field);
  const allowed = pairs.map(([k]) => k);
  for (const k of Object.keys(obj)) if (!allowed.includes(k)) throw new TurnFrameValidationError(`${field}.unexpected`);
  for (const k of allowed) if (!Object.prototype.hasOwnProperty.call(obj, k)) throw new TurnFrameValidationError(`${field}.${k}`);
}

function expandList(value, pairs, field) {
  if (!Array.isArray(value)) throw new TurnFrameValidationError(field);
  return value.map((item) => {
    exactKeys(item, pairs, `${field}[]`);
    const out = {};
    for (const [k, f] of pairs) out[f] = item[k];
    return out;
  });
}

/**
 * Compact wire object → canonical producer object (the shape
 * TURN_FRAME_OUTPUT_SCHEMA describes). Pure and structural: values are copied
 * as-is (enum/type checks and every safety repair happen in validateTurnFrame).
 *
 * @param {unknown} compact
 * @returns {object} canonical raw frame
 * @throws {TurnFrameValidationError}
 */
function expandCompactTurnFrame(compact) {
  exactKeys(compact, TOP_LEVEL_KEYS, 'compact');
  const amb = compact.amb;
  const out = {
    intent: compact.i,
    subjectKind: compact.s,
    relationToPrior: compact.r,
    referents: expandList(compact.ref, REFERENT_KEYS, 'compact.ref'),
    evidenceNeeds: Array.isArray(compact.ev) ? [...compact.ev] : compact.ev,
    facets: Array.isArray(compact.fa) ? [...compact.fa] : compact.fa,
    requestedOutcome: compact.o,
    requestedAction: { verb: compact.av, targetCandidateId: compact.at, polarity: compact.ap },
    scope: compact.sc,
    foreignTenantReference: compact.ft,
    ambiguities: expandList(amb, AMBIGUITY_KEYS, 'compact.amb').map((a) => ({
      kind: a.kind,
      candidateIds: Array.isArray(a.candidateIds) ? [...a.candidateIds] : a.candidateIds,
    })),
    confidence: compact.cf,
  };
  return out;
}

/** Canonical producer object → compact wire object (inverse; tests and fixtures). */
function compactTurnFrame(raw) {
  const a = raw.requestedAction || {};
  return {
    i: raw.intent,
    s: raw.subjectKind,
    r: raw.relationToPrior,
    ref: (raw.referents || []).map((x) => ({ id: x.candidateId, ro: x.role })),
    ev: [...(raw.evidenceNeeds || [])],
    fa: [...(raw.facets || [])],
    o: raw.requestedOutcome,
    av: a.verb,
    at: a.targetCandidateId === undefined ? null : a.targetCandidateId,
    ap: a.polarity,
    sc: raw.scope,
    ft: raw.foreignTenantReference,
    amb: (raw.ambiguities || []).map((x) => ({ k: x.kind, ids: [...(x.candidateIds || [])] })),
    cf: raw.confidence,
  };
}

// Load-time guard: the compact format must cover exactly the canonical fields.
(function assertCoverage() {
  const covered = new Set(TOP_LEVEL_KEYS.map(([, f]) => f.split('.')[0]));
  const canonical = TURN_FRAME_OUTPUT_SCHEMA.required;
  if (covered.size !== canonical.length || !canonical.every((f) => covered.has(f))) {
    throw new Error('CompactTurnFrame does not cover the canonical TurnFrame fields');
  }
})();

module.exports = {
  COMPACT_FORMAT,
  COMPACT_TURN_FRAME_OUTPUT_SCHEMA,
  COMPACT_OUTPUT_LEGEND,
  TOP_LEVEL_KEYS,
  expandCompactTurnFrame,
  compactTurnFrame,
};
