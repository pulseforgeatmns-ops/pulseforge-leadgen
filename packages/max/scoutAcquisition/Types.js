'use strict';

/**
 * SPEC-100 — Scout acquisition intelligence types.
 * Observation ≠ inference ≠ unknown. Signals are reuse-only.
 */

const CLAIM_KINDS = Object.freeze(['observation', 'inference', 'unknown']);

const ACQUISITION_SIGNALS = Object.freeze([
  'expansion',
  'new_location',
  'portfolio_growth',
  'hiring',
  'leadership_change',
  'operational_change',
  'vendor_dissatisfaction',
  'contract_timing',
  'facility_growth',
  'service_gap',
  'decision_maker',
]);

const SCOUT_SPECIALIST = 'scout';
const SCOUT_CAPABILITY = 'acquisition_intelligence';
const SUPPORTED_AUTHORITY = Object.freeze(['observe', 'recommend']);
const FORBIDDEN_AUTHORITY = Object.freeze([
  'draft',
  'execute_after_approval',
  'execute',
]);

const ANCHOR_TENANT_ID = '10';

const DEFAULT_FRESHNESS_MS = 24 * 60 * 60 * 1000;
const TIMELY_SIGNAL_MS = 90 * 24 * 60 * 60 * 1000;

const FORBIDDEN_OUTBOUND = Object.freeze([
  'email',
  'sms',
  'call',
  'linkedin',
  'facebook',
  'direct_mail',
  'sequence',
  'campaign',
  'outreach',
]);

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function clone(obj) {
  if (obj == null) return obj;
  return JSON.parse(JSON.stringify(obj));
}

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeSignal(value) {
  const raw = asText(value);
  if (!raw) return null;
  const key = raw.toLowerCase().replace(/[\s-]+/g, '_');
  return ACQUISITION_SIGNALS.includes(key) ? key : null;
}

function normalizeClaim(raw, fallbackKind = 'observation') {
  if (!raw) return null;
  if (typeof raw === 'string') {
    const text = asText(raw);
    return text
      ? {
          kind: CLAIM_KINDS.includes(fallbackKind) ? fallbackKind : 'observation',
          text,
          entityId: null,
          observedAt: null,
          evidenceId: null,
        }
      : null;
  }
  if (!isPlainObject(raw)) return null;
  const text = asText(raw.text || raw.summary || raw.message);
  if (!text) return null;
  const kind = CLAIM_KINDS.includes(raw.kind) ? raw.kind : fallbackKind;
  return {
    kind,
    text,
    entityId: asText(raw.entityId || raw.companyId || raw.personId),
    observedAt: asText(raw.observedAt || raw.observed_at || raw.timestamp),
    evidenceId: asText(raw.evidenceId || raw.evidence_id),
  };
}

function ageMs(iso, now = Date.now()) {
  if (!iso) return Number.POSITIVE_INFINITY;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return Number.POSITIVE_INFINITY;
  return Math.max(0, now - t);
}

function isTimely(iso, now = Date.now(), windowMs = TIMELY_SIGNAL_MS) {
  return ageMs(iso, now) <= windowMs;
}

module.exports = {
  CLAIM_KINDS,
  ACQUISITION_SIGNALS,
  SCOUT_SPECIALIST,
  SCOUT_CAPABILITY,
  SUPPORTED_AUTHORITY,
  FORBIDDEN_AUTHORITY,
  ANCHOR_TENANT_ID,
  DEFAULT_FRESHNESS_MS,
  TIMELY_SIGNAL_MS,
  FORBIDDEN_OUTBOUND,
  asText,
  clone,
  isPlainObject,
  nowIso,
  normalizeSignal,
  normalizeClaim,
  ageMs,
  isTimely,
};
