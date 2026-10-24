'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence types.
 */

const crypto = require('crypto');

const GOVERNOR_OUTCOMES = Object.freeze({
  PROCEED: 'proceed',
  SLOW: 'slow',
  PAUSE: 'pause',
  EMERGENCY: 'emergency',
});

const PLAN_STATUS = Object.freeze({
  DRAFT: 'draft',
  APPROVED: 'approved',
  SUPERSEDED: 'superseded',
});

const HEALTH_LABELS = Object.freeze({
  HEALTHY: 'healthy',
  WATCH: 'watch',
  DEGRADED: 'degraded',
  CRITICAL: 'critical',
});

const SPECIALISTS = Object.freeze({
  SCOUT: 'scout',
  MAX: 'max',
  PAIGE: 'paige',
  EMMETT: 'emmett',
  OPERATOR: 'operator',
});

const OUTCOME_TYPES = Object.freeze({
  DELIVERY: 'delivery',
  OPEN: 'open',
  REPLY: 'reply',
  BOUNCE: 'bounce',
  UNSUBSCRIBE: 'unsubscribe',
  SPAM_COMPLAINT: 'spam_complaint',
  MEETING_BOOKED: 'meeting_booked',
  OPPORTUNITY_CREATED: 'opportunity_created',
  REVENUE: 'revenue',
});

const LEARNING_SINKS = Object.freeze({
  PAIGE: 'paige',
  SCOUT: 'scout',
  MAX: 'max',
  EMMETT: 'emmett',
});

const WARMUP_STATUS = Object.freeze({
  HEALTHY: 'healthy',
  WARMING: 'warming',
  STALLED: 'stalled',
  NONE: 'none',
});

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function asText(value) {
  return value == null ? '' : String(value);
}

function nowIso(now) {
  return (now instanceof Date ? now : new Date(now || Date.now())).toISOString();
}

function newId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function eoiError(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function round1(value) {
  return Math.round(Number(value || 0) * 10) / 10;
}

function round2(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function pct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return n > 1 && n <= 100 ? n / 100 : clamp(n, 0, 1);
}

module.exports = {
  GOVERNOR_OUTCOMES,
  PLAN_STATUS,
  HEALTH_LABELS,
  SPECIALISTS,
  OUTCOME_TYPES,
  LEARNING_SINKS,
  WARMUP_STATUS,
  clone,
  asText,
  nowIso,
  newId,
  eoiError,
  clamp,
  round1,
  round2,
  pct,
};
