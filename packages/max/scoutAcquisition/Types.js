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

const SOURCE_TYPES = Object.freeze({
  EXISTING_PF: 'existing_pf_company_intelligence',
  COMPANY_WEBSITES: 'company_websites',
  PUBLIC_BUSINESS_DATA: 'public_business_data',
  ENRICHMENT_PROVIDER: 'enrichment_provider',
  LINKEDIN: 'linkedin_social_intelligence',
  FACEBOOK: 'facebook_social_intelligence',
  INSTAGRAM: 'instagram_social_intelligence',
});

const CORE_SOURCE_TYPES = Object.freeze([
  SOURCE_TYPES.EXISTING_PF,
  SOURCE_TYPES.COMPANY_WEBSITES,
  SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
  SOURCE_TYPES.ENRICHMENT_PROVIDER,
]);

const SOCIAL_SOURCE_TYPES = Object.freeze([
  SOURCE_TYPES.LINKEDIN,
  SOURCE_TYPES.FACEBOOK,
  SOURCE_TYPES.INSTAGRAM,
]);

const PERCEPTION_CHANNELS = Object.freeze(['linkedin', 'facebook', 'instagram']);

const REJECTION_REASONS = Object.freeze({
  INSUFFICIENT_BUSINESS_FIT: 'insufficient_business_fit',
  STALE_EVIDENCE: 'stale_evidence',
  NO_TIMING_SIGNAL: 'no_timing_signal',
  INSUFFICIENT_SOURCE_SUPPORT: 'insufficient_source_support',
  OUTSIDE_GEOGRAPHY: 'outside_geography',
  EXCLUDED_SEGMENT: 'excluded_segment',
  UNRESOLVED: 'unresolved',
});

const COVERAGE_BANDS = Object.freeze({
  WEAK: 'weak',
  MODERATE: 'moderate',
  STRONG: 'strong',
});

const OPPORTUNITY_CLASSES = Object.freeze({
  SUPPORTED: 'supported',
  FIT: 'fit',
  WATCH: 'watch',
  REJECTED: 'rejected',
});

const FIT_LEVELS = Object.freeze({
  STRONG: 'strong',
  MODERATE: 'moderate',
  WEAK: 'weak',
  REJECTED: 'rejected',
});

const INTENT_STATES = Object.freeze({
  UNKNOWN: 'unknown',
  TIMED: 'timed',
  NONE: 'none',
});

const DEFAULT_COMMERCIAL_CLEANING_SEGMENTS = Object.freeze([
  'property_management',
  'office',
  'daycare',
  'community_facility',
]);

const REFRESH_MS = 30 * 24 * 60 * 60 * 1000;

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
  SOURCE_TYPES,
  CORE_SOURCE_TYPES,
  SOCIAL_SOURCE_TYPES,
  PERCEPTION_CHANNELS,
  REJECTION_REASONS,
  COVERAGE_BANDS,
  OPPORTUNITY_CLASSES,
  FIT_LEVELS,
  INTENT_STATES,
  DEFAULT_COMMERCIAL_CLEANING_SEGMENTS,
  REFRESH_MS,
  asText,
  clone,
  isPlainObject,
  nowIso,
  normalizeSignal,
  normalizeClaim,
  ageMs,
  isTimely,
};
