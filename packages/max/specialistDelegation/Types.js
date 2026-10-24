'use strict';

/**
 * SPEC-098 — canonical SpecialistDelegation / SpecialistResult types.
 * Semantic contract only — transport remains an implementation detail.
 */

const AUTHORITY_LEVELS = Object.freeze([
  'observe',
  'recommend',
  'draft',
  'execute_after_approval',
  'execute',
]);

const AUTHORITY_RANK = Object.freeze({
  observe: 0,
  recommend: 1,
  draft: 2,
  execute_after_approval: 3,
  execute: 4,
});

const DELEGATION_STATUSES = Object.freeze([
  'created',
  'authorized',
  'rejected',
  'running',
  'completed',
  'partial',
  'blocked',
  'failed',
  'declined_policy',
]);

const RESULT_STATUSES = Object.freeze([
  'completed',
  'partial',
  'blocked',
  'failed',
  'declined_policy',
]);

const EVIDENCE_KINDS = Object.freeze([
  'company',
  'market',
  'content_outcome',
  'operator_correction',
  'campaign_result',
  'conversation',
  'blueprint_claim',
  'specialist_result',
  'recommendation',
  'test',
]);

const SOURCE_KINDS = Object.freeze([
  'observed_fact',
  'max_inference',
  'operator_instruction',
]);

const TARGET_ENTITY_KINDS = Object.freeze([
  'company',
  'person',
  'campaign',
  'publication',
  'ad_account',
  'conversation',
  'market',
  'prospect_cohort',
]);

const CONSTRAINT_KEYS = Object.freeze([
  'allowedChannels',
  'excludedChannels',
  'geography',
  'targetSegments',
  'budgetLimit',
  'contactRestrictions',
  'clientPolicyRefs',
  'approvalRequirements',
  'timeWindow',
]);

const POLICY_EVENT_KINDS = Object.freeze([
  'missing_authority',
  'unsupported_authority',
  'tenant_policy_conflict',
  'capability_policy_conflict',
  'platform_safety_conflict',
  'constraint_indeterminate',
  'tenant_mismatch',
  'unknown_capability',
  'adapter_unavailable',
  'recursion_blocked',
]);

class SpecialistDelegationError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   * @param {object} [details]
   */
  constructor(code, message, status = 400, details = null) {
    super(message);
    this.name = 'SpecialistDelegationError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asTenantId(value) {
  return asText(value);
}

function clone(obj) {
  if (obj == null) return obj;
  return JSON.parse(JSON.stringify(obj));
}

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * @param {unknown} value
 * @returns {object[]}
 */
function normalizeEvidenceRefs(value) {
  if (!Array.isArray(value)) return [];
  const out = [];
  for (const raw of value) {
    if (!raw || typeof raw !== 'object') continue;
    const id = asText(raw.id);
    if (!id) continue;
    const kind = asText(raw.kind) || 'test';
    const sourceKind = SOURCE_KINDS.includes(raw.sourceKind)
      ? raw.sourceKind
      : 'observed_fact';
    out.push({
      id,
      kind: EVIDENCE_KINDS.includes(kind) ? kind : 'test',
      sourceKind,
      label: asText(raw.label),
      snapshot: isPlainObject(raw.snapshot) ? clone(raw.snapshot) : null,
    });
  }
  return out;
}

/**
 * @param {unknown} value
 * @returns {object}
 */
function normalizeConstraints(value) {
  const src = isPlainObject(value) ? value : {};
  const out = {};
  for (const key of CONSTRAINT_KEYS) {
    if (src[key] !== undefined) out[key] = clone(src[key]);
  }
  if (Array.isArray(src.requiredDeterminate)) {
    out.requiredDeterminate = src.requiredDeterminate
      .map(asText)
      .filter((k) => k && CONSTRAINT_KEYS.includes(k));
  }
  return out;
}

/**
 * @param {unknown} value
 * @returns {object}
 */
function normalizeExpectedReturn(value) {
  const src = isPlainObject(value) ? value : {};
  return {
    type: asText(src.type) || 'intelligence',
    requireEvidence: src.requireEvidence === true,
    requireConfidence: src.requireConfidence === true,
    requireRecommendation: src.requireRecommendation === true,
  };
}

/**
 * @param {unknown} value
 * @returns {object}
 */
function normalizeBusinessContext(value) {
  const src = isPlainObject(value) ? value : {};
  const allowed = [
    'blueprintRef',
    'blueprintSnapshot',
    'playbookRef',
    'playbookSnapshot',
    'operatorDirection',
    'operatorDirectionRef',
    'businessConstraints',
    'targetMarket',
    'offer',
    'historicalLearning',
    'notes',
    'serviceGeography',
    'commercialCapability',
    'preferredSegments',
    'acquisitionDirection',
    'exclusions',
    'offerContext',
    'approvedUnderstanding',
    'campaignLearnings',
    'maxAvailableContext',
  ];
  const out = {};
  for (const key of allowed) {
    if (src[key] !== undefined) out[key] = clone(src[key]);
  }
  return out;
}

/**
 * @param {unknown} value
 * @returns {object|null}
 */
function normalizeTargetContext(value) {
  if (value == null) return null;
  if (!isPlainObject(value)) return null;
  const entities = Array.isArray(value.entities)
    ? value.entities
        .filter((e) => e && typeof e === 'object' && asText(e.id))
        .map((e) => ({
          id: asText(e.id),
          kind: TARGET_ENTITY_KINDS.includes(e.kind) ? e.kind : 'company',
          snapshot: isPlainObject(e.snapshot) ? clone(e.snapshot) : null,
        }))
    : [];
  return {
    entities,
    notes: asText(value.notes),
    geography: asText(value.geography),
    segments: Array.isArray(value.segments)
      ? value.segments.map(asText).filter(Boolean)
      : [],
    businessType: asText(value.businessType),
    desiredSignals: Array.isArray(value.desiredSignals)
      ? value.desiredSignals.map(asText).filter(Boolean)
      : [],
    priorDelegationId: asText(value.priorDelegationId),
    priorResultId: asText(value.priorResultId),
    seedCompanyId: asText(value.seedCompanyId),
  };
}

/**
 * @param {unknown} value
 * @returns {object[]}
 */
function normalizeStringRecords(value, textKey = 'text') {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (typeof item === 'string') {
        const text = asText(item);
        return text ? { [textKey]: text } : null;
      }
      if (!item || typeof item !== 'object') return null;
      const text = asText(item[textKey] || item.summary || item.message);
      if (!text) return null;
      return { ...clone(item), [textKey]: text };
    })
    .filter(Boolean);
}

module.exports = {
  AUTHORITY_LEVELS,
  AUTHORITY_RANK,
  DELEGATION_STATUSES,
  RESULT_STATUSES,
  EVIDENCE_KINDS,
  SOURCE_KINDS,
  TARGET_ENTITY_KINDS,
  CONSTRAINT_KEYS,
  POLICY_EVENT_KINDS,
  SpecialistDelegationError,
  nowIso,
  asText,
  asTenantId,
  clone,
  isPlainObject,
  normalizeEvidenceRefs,
  normalizeConstraints,
  normalizeExpectedReturn,
  normalizeBusinessContext,
  normalizeTargetContext,
  normalizeStringRecords,
};
