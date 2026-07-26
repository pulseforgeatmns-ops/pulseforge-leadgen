'use strict';

/**
 * Live Intelligence Loop types (SPEC-011 / ADR-006).
 * Events are transitions — not polling theater.
 */

const crypto = require('crypto');
const { deepFreeze } = require('../reasoning/ReasoningTypes');
const { CHANGE_TYPES } = require('../memory/snapshots/MemoryTypes');

/** Lifecycle of an intelligence object. */
const LIFECYCLE = Object.freeze({
  DETECTED: 'detected',
  VERIFIED: 'verified',
  STRENGTHENED: 'strengthened',
  CONTRADICTED: 'contradicted',
  RESOLVED: 'resolved',
  ARCHIVED: 'archived',
});

const LIFECYCLE_ORDER = Object.freeze([
  LIFECYCLE.DETECTED,
  LIFECYCLE.VERIFIED,
  LIFECYCLE.STRENGTHENED,
  LIFECYCLE.CONTRADICTED,
  LIFECYCLE.RESOLVED,
  LIFECYCLE.ARCHIVED,
]);

/** Event severity. */
const SEVERITY = Object.freeze({
  CRITICAL: 'critical',
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
  INFO: 'info',
});

/**
 * Canonical intelligence event types.
 * Memory CHANGE_TYPES map into these where possible.
 */
const EVENT_TYPES = Object.freeze({
  DETECTED: 'detected',
  VERIFIED: 'verified',
  STRENGTHENED: 'strengthened',
  CONTRADICTED: 'contradicted',
  RESOLVED: 'resolved',
  ARCHIVED: 'archived',
  NEW_HIRING_SIGNAL: 'new_hiring_signal',
  NEW_EVIDENCE: 'new_evidence',
  EVIDENCE_CONTRADICTED: 'evidence_contradicted',
  CONFIDENCE_INCREASED: 'confidence_increased',
  CONFIDENCE_DECREASED: 'confidence_decreased',
  CONFIDENCE_THRESHOLD_CROSSED: 'confidence_threshold_crossed',
  SCORE_INCREASED: 'score_increased',
  SCORE_DECREASED: 'score_decreased',
  RECOMMENDATION_CHANGED: 'recommendation_changed',
  RECOMMENDATION_PROMOTED: 'recommendation_promoted',
  RECOMMENDATION_BLOCKED: 'recommendation_blocked',
  HIGHEST_LEVERAGE_REPLACED: 'highest_leverage_replaced',
  WATCH_ALERT_APPEARED: 'watch_alert_appeared',
  WATCH_ALERT_PROMOTED: 'watch_alert_promoted',
  OPPORTUNITY_EXPIRED: 'opportunity_expired',
  POLICY_BLOCKED: 'policy_blocked_execution',
  BRIEFING_EVOLVED: 'briefing_evolved',
  SUPPORTING_EVIDENCE_ADDED: 'supporting_evidence_added',
});

/** Types that may notify the operator. */
const MATERIAL_EVENT_TYPES = Object.freeze(
  new Set([
    EVENT_TYPES.HIGHEST_LEVERAGE_REPLACED,
    EVENT_TYPES.WATCH_ALERT_PROMOTED,
    EVENT_TYPES.CONFIDENCE_THRESHOLD_CROSSED,
    EVENT_TYPES.RECOMMENDATION_BLOCKED,
    EVENT_TYPES.OPPORTUNITY_EXPIRED,
    EVENT_TYPES.POLICY_BLOCKED,
    EVENT_TYPES.EVIDENCE_CONTRADICTED,
    EVENT_TYPES.RECOMMENDATION_PROMOTED,
  ])
);

const ENTITY_KINDS = Object.freeze({
  DECK: 'deck',
  BRIEFING: 'briefing',
  RECOMMENDATION: 'recommendation',
  COMPANY: 'company',
  WATCH: 'watch',
  EVIDENCE: 'evidence',
  POLICY: 'policy',
  OPPORTUNITY: 'opportunity',
});

/** Default confidence threshold that counts as material. */
const DEFAULT_CONFIDENCE_THRESHOLD = 0.75;

/**
 * @typedef {object} IntelligenceEntityRef
 * @property {string} kind
 * @property {string} id
 * @property {string} [label]
 */

/**
 * @typedef {object} IntelligenceEvent
 * @property {string} id
 * @property {string} type
 * @property {IntelligenceEntityRef} entity
 * @property {string} severity
 * @property {string} timestamp
 * @property {string} summary
 * @property {string[]} relatedEvidence
 * @property {boolean} material
 * @property {string|null} lifecycle
 * @property {string} tenantId
 * @property {number} seq
 * @property {object|null} [payload]
 */

/**
 * @param {object} input
 * @returns {IntelligenceEvent}
 */
function buildIntelligenceEvent(input) {
  if (!input || !input.type) {
    throw new Error('IntelligenceEvent requires type');
  }
  if (!input.entity || input.entity.id == null) {
    throw new Error('IntelligenceEvent requires entity.id');
  }
  const type = String(input.type);
  const material =
    input.material != null
      ? Boolean(input.material)
      : MATERIAL_EVENT_TYPES.has(type);
  const id =
    input.id != null
      ? String(input.id)
      : `ievt:${crypto.randomBytes(8).toString('hex')}`;
  const timestamp = input.timestamp || new Date().toISOString();
  const entity = {
    kind: String(input.entity.kind || ENTITY_KINDS.RECOMMENDATION),
    id: String(input.entity.id),
    label:
      input.entity.label != null && String(input.entity.label).trim()
        ? String(input.entity.label).trim()
        : String(input.entity.id),
  };
  const relatedEvidence = Array.isArray(input.relatedEvidence)
    ? input.relatedEvidence.map(String)
    : [];

  return deepFreeze({
    id,
    type,
    entity,
    severity: String(input.severity || SEVERITY.INFO),
    timestamp,
    summary: input.summary != null ? String(input.summary) : type,
    relatedEvidence,
    material,
    lifecycle: input.lifecycle != null ? String(input.lifecycle) : null,
    tenantId: input.tenantId != null ? String(input.tenantId) : '',
    seq: Number.isFinite(Number(input.seq)) ? Number(input.seq) : 0,
    payload:
      input.payload && typeof input.payload === 'object' ? input.payload : null,
  });
}

/**
 * Map SPEC-003 change types → live event types.
 * @param {string} changeType
 */
function mapChangeTypeToEventType(changeType) {
  const map = {
    [CHANGE_TYPES.SCORE_INCREASED]: EVENT_TYPES.SCORE_INCREASED,
    [CHANGE_TYPES.SCORE_DECREASED]: EVENT_TYPES.SCORE_DECREASED,
    [CHANGE_TYPES.CONFIDENCE_INCREASED]: EVENT_TYPES.CONFIDENCE_INCREASED,
    [CHANGE_TYPES.CONFIDENCE_DECREASED]: EVENT_TYPES.CONFIDENCE_DECREASED,
    [CHANGE_TYPES.NEW_EVIDENCE]: EVENT_TYPES.NEW_EVIDENCE,
    [CHANGE_TYPES.NEW_HIRING_SIGNAL]: EVENT_TYPES.NEW_HIRING_SIGNAL,
    [CHANGE_TYPES.NEW_CONTRADICTION]: EVENT_TYPES.EVIDENCE_CONTRADICTED,
    [CHANGE_TYPES.NEW_OPPORTUNITY_SIGNAL]: EVENT_TYPES.STRENGTHENED,
    [CHANGE_TYPES.PRIORITY_CHANGED]: EVENT_TYPES.RECOMMENDATION_CHANGED,
    [CHANGE_TYPES.TYPE_CHANGED]: EVENT_TYPES.RECOMMENDATION_CHANGED,
    [CHANGE_TYPES.ACTION_CHANGED]: EVENT_TYPES.RECOMMENDATION_CHANGED,
  };
  return map[changeType] || changeType;
}

/**
 * Suggest lifecycle transition from an event type.
 * @param {string} eventType
 * @param {string|null} [current]
 */
function lifecycleForEventType(eventType, current = null) {
  switch (eventType) {
    case EVENT_TYPES.DETECTED:
    case EVENT_TYPES.NEW_HIRING_SIGNAL:
      return LIFECYCLE.DETECTED;
    case EVENT_TYPES.VERIFIED:
      return LIFECYCLE.VERIFIED;
    case EVENT_TYPES.STRENGTHENED:
    case EVENT_TYPES.CONFIDENCE_INCREASED:
    case EVENT_TYPES.SCORE_INCREASED:
    case EVENT_TYPES.SUPPORTING_EVIDENCE_ADDED:
    case EVENT_TYPES.NEW_EVIDENCE:
    case EVENT_TYPES.RECOMMENDATION_PROMOTED:
      return LIFECYCLE.STRENGTHENED;
    case EVENT_TYPES.CONTRADICTED:
    case EVENT_TYPES.EVIDENCE_CONTRADICTED:
    case EVENT_TYPES.CONFIDENCE_DECREASED:
    case EVENT_TYPES.SCORE_DECREASED:
      return LIFECYCLE.CONTRADICTED;
    case EVENT_TYPES.RESOLVED:
    case EVENT_TYPES.RECOMMENDATION_BLOCKED:
    case EVENT_TYPES.POLICY_BLOCKED:
      return LIFECYCLE.RESOLVED;
    case EVENT_TYPES.ARCHIVED:
    case EVENT_TYPES.OPPORTUNITY_EXPIRED:
      return LIFECYCLE.ARCHIVED;
    default:
      return current;
  }
}

/**
 * Opaque cursor for soft-poll since queries.
 * @param {number} seq
 */
function encodeCursor(seq) {
  return `c:${Math.max(0, Number(seq) || 0)}`;
}

/**
 * @param {string|null|undefined} cursor
 * @returns {number}
 */
function decodeCursor(cursor) {
  if (cursor == null || cursor === '') return 0;
  const raw = String(cursor);
  if (raw.startsWith('c:')) {
    const n = Number(raw.slice(2));
    return Number.isFinite(n) ? n : 0;
  }
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

module.exports = {
  LIFECYCLE,
  LIFECYCLE_ORDER,
  SEVERITY,
  EVENT_TYPES,
  MATERIAL_EVENT_TYPES,
  ENTITY_KINDS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  buildIntelligenceEvent,
  mapChangeTypeToEventType,
  lifecycleForEventType,
  encodeCursor,
  decodeCursor,
};
