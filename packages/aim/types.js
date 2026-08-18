'use strict';

/**
 * SPEC-112 — Acquisition Intelligence Model types.
 * AIM is client-expertise intelligence, not operating fact.
 */

const AIM_STATUS = Object.freeze({
  DRAFT: 'draft',
  COMPLETE: 'complete',
  SUPERSEDED: 'superseded',
});

const SCORE_DIMENSIONS = Object.freeze([
  'icpFit',
  'painMatch',
  'evidenceQuality',
  'buyingReadiness',
  'confidence',
]);

const RECOMMENDATIONS = Object.freeze({
  PURSUE: 'pursue',
  NURTURE: 'nurture',
  WATCH: 'watch',
  REJECT: 'reject',
  UNKNOWN: 'unknown',
});

const PAIN_CATEGORIES = Object.freeze({
  PEOPLE_MANAGEMENT: 'people_management',
  CUSTOMER_GROWTH: 'customer_growth',
  FINANCE: 'finance',
});

const PAIN_IDS = Object.freeze({
  FOUNDER_DEPENDENCY: 'founder_dependency',
  DELEGATION: 'delegation',
  HIRING: 'hiring',
  ACCOUNTABILITY: 'accountability',
  INCONSISTENT_PIPELINE: 'inconsistent_pipeline',
  POOR_LEAD_GENERATION: 'poor_lead_generation',
  WEAK_SALES_PROCESS: 'weak_sales_process',
  CASH_FLOW: 'cash_flow',
  PRICING: 'pricing',
  PROFITABILITY: 'profitability',
});

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function asList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map(asText).filter(Boolean);
  const text = asText(value);
  return text ? [text] : [];
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function toPercent(value) {
  return Math.round(clamp01(value) * 100);
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

function haystack(parts) {
  return asList(parts).join(' ').toLowerCase();
}

module.exports = {
  AIM_STATUS,
  SCORE_DIMENSIONS,
  RECOMMENDATIONS,
  PAIN_CATEGORIES,
  PAIN_IDS,
  asText,
  asList,
  clamp01,
  toPercent,
  nowIso,
  isPlainObject,
  clone,
  haystack,
};
