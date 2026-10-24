'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence types.
 * Max recommends. Operators approve. Drafts are never runtime truth.
 */

const SCORECARD_STATUS = Object.freeze({
  DRAFT: 'draft',
  IN_REVIEW: 'in_review',
  APPROVED: 'approved',
  SUPERSEDED: 'superseded',
});

const METRIC_STATUS = Object.freeze({
  RECOMMENDED: 'recommended',
  ACCEPTED: 'accepted',
  MODIFIED: 'modified',
  REMOVED: 'removed',
  ADDED: 'added',
  UNDER_REVIEW: 'under_review',
});

const REVIEW_ACTIONS = Object.freeze({
  ACCEPT: 'accept',
  MODIFY: 'modify',
  ADD: 'add',
  REMOVE: 'remove',
  REORDER: 'reorder',
});

const INDICATORS = Object.freeze({
  LEADING: 'leading',
  LAGGING: 'lagging',
});

const CATEGORIES = Object.freeze({
  ACQUISITION: 'acquisition',
  MARKET_VALIDATION: 'market_validation',
  SALES: 'sales',
  DELIVERY: 'delivery',
  BUSINESS_OUTCOMES: 'business_outcomes',
  COMMERCIAL: 'commercial',
  OPERATIONS: 'operations',
  TRANSFORMATION: 'transformation',
});

const BUSINESS_STAGES = Object.freeze({
  MARKET_VALIDATION: 'market_validation',
  REPEATABLE_ACQUISITION: 'repeatable_acquisition',
  OPERATIONAL_SCALE: 'operational_scale',
  MATURE_GROWTH: 'mature_growth',
});

const PROFILES = Object.freeze({
  FOUNDER_TRANSFORMATION: 'founder_transformation',
  COMMERCIAL_CLEANING: 'commercial_cleaning',
  HOME_RENOVATION: 'home_renovation',
  DEFAULT: 'default',
});

const METRIC_SOURCE = Object.freeze({
  MAX: 'max',
  OPERATOR: 'operator',
});

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function asList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map(asText).filter(Boolean);
  return asText(value)
    .split(/\s*(?:,|;|\n|\|)\s*/)
    .map((s) => s.replace(/^[-*•]\s*/, '').trim())
    .filter(Boolean);
}

function nowIso(now) {
  if (now instanceof Date) return now.toISOString();
  if (typeof now === 'number') return new Date(now).toISOString();
  return new Date().toISOString();
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

function slugify(text) {
  const slug = asText(text)
    .toLowerCase()
    .replace(/['"]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 72);
  return slug || 'metric';
}

function newId(prefix = 'osi') {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function clampConfidence(value, fallback = 0.8) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  if (n > 1 && n <= 100) return Math.round(n) / 100;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function confidencePercent(value) {
  return Math.round(clampConfidence(value) * 100);
}

function osiError(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  Object.assign(err, extra);
  return err;
}

module.exports = {
  SCORECARD_STATUS,
  METRIC_STATUS,
  REVIEW_ACTIONS,
  INDICATORS,
  CATEGORIES,
  BUSINESS_STAGES,
  PROFILES,
  METRIC_SOURCE,
  asText,
  asList,
  nowIso,
  isPlainObject,
  clone,
  slugify,
  newId,
  clampConfidence,
  confidencePercent,
  osiError,
};
