'use strict';

/**
 * Signal lifecycle transitions (SPEC-031).
 * Detected → Verified → Active → Decays → Archived
 */

const { SIGNAL_LIFECYCLE, SIGNAL_CONFIDENCE } = require('./types');
const { passesVerificationGate } = require('./confidence');
const { ttlForCategory } = require('./categories');

const MS_PER_DAY = 24 * 60 * 60 * 1000;

/**
 * @param {Date|string|number} [asOf]
 * @returns {Date}
 */
function asOfDate(asOf) {
  if (asOf instanceof Date) return asOf;
  if (asOf != null) {
    const d = new Date(asOf);
    if (!Number.isNaN(d.getTime())) return d;
  }
  return new Date();
}

/**
 * @param {string|Date} observedAt
 * @param {string} category
 * @param {string} [explicitExpiresAt]
 * @returns {string} ISO expiresAt
 */
function defaultExpiresAt(observedAt, category, explicitExpiresAt) {
  if (explicitExpiresAt) return String(explicitExpiresAt);
  const observed = new Date(observedAt);
  const ttl = ttlForCategory(category);
  const base = Number.isNaN(observed.getTime()) ? new Date() : observed;
  return new Date(base.getTime() + ttl.hardExpireDays * MS_PER_DAY).toISOString();
}

/**
 * Apply verification gate: Detected → Verified (or stay Detected).
 * @param {object} signal
 * @param {object} [opts]
 * @returns {object}
 */
function verifySignal(signal, opts = {}) {
  if (!signal || !Array.isArray(signal.evidence) || signal.evidence.length === 0) {
    return {
      ...signal,
      lifecycle: SIGNAL_LIFECYCLE.DETECTED,
      confidence: SIGNAL_CONFIDENCE.UNKNOWN,
      confidenceScore: 0,
    };
  }
  if (!passesVerificationGate(signal.confidence, opts)) {
    return { ...signal, lifecycle: SIGNAL_LIFECYCLE.DETECTED };
  }
  const expiresAt = defaultExpiresAt(
    signal.observedAt,
    signal.category,
    signal.expiresAt
  );
  return {
    ...signal,
    lifecycle: SIGNAL_LIFECYCLE.VERIFIED,
    expiresAt,
  };
}

/**
 * Promote Verified → Active when eligible.
 * @param {object} signal
 * @returns {object}
 */
function activateSignal(signal) {
  if (signal.lifecycle !== SIGNAL_LIFECYCLE.VERIFIED) return signal;
  if (signal.confidence === SIGNAL_CONFIDENCE.UNKNOWN) return signal;
  return { ...signal, lifecycle: SIGNAL_LIFECYCLE.ACTIVE };
}

/**
 * Recompute lifecycle from time (Active → Decaying → Archived).
 * @param {object} signal
 * @param {Date|string|number} [asOf]
 * @returns {object}
 */
function advanceLifecycle(signal, asOf) {
  const now = asOfDate(asOf);
  if (
    signal.lifecycle === SIGNAL_LIFECYCLE.DETECTED ||
    signal.lifecycle === SIGNAL_LIFECYCLE.ARCHIVED
  ) {
    return signal;
  }

  const expiresAt = signal.expiresAt
    ? new Date(signal.expiresAt)
    : new Date(
        defaultExpiresAt(signal.observedAt, signal.category, signal.expiresAt)
      );

  if (!Number.isNaN(expiresAt.getTime()) && now.getTime() >= expiresAt.getTime()) {
    return {
      ...signal,
      lifecycle: SIGNAL_LIFECYCLE.ARCHIVED,
      influenceWeight: 0,
      expiresAt: expiresAt.toISOString(),
    };
  }

  const observed = new Date(signal.observedAt);
  const ttl = ttlForCategory(signal.category);
  const softAt = Number.isNaN(observed.getTime())
    ? null
    : new Date(observed.getTime() + ttl.softDecayDays * MS_PER_DAY);

  if (
    softAt &&
    now.getTime() >= softAt.getTime() &&
    (signal.lifecycle === SIGNAL_LIFECYCLE.ACTIVE ||
      signal.lifecycle === SIGNAL_LIFECYCLE.VERIFIED ||
      signal.lifecycle === SIGNAL_LIFECYCLE.DECAYING)
  ) {
    return {
      ...signal,
      lifecycle: SIGNAL_LIFECYCLE.DECAYING,
      expiresAt: expiresAt.toISOString(),
    };
  }

  if (signal.lifecycle === SIGNAL_LIFECYCLE.VERIFIED) {
    return activateSignal({ ...signal, expiresAt: expiresAt.toISOString() });
  }

  return {
    ...signal,
    expiresAt: expiresAt.toISOString(),
  };
}

/**
 * Full path: verify → activate → advance by time.
 * @param {object} signal
 * @param {object} [opts]
 * @returns {object}
 */
function resolveLifecycle(signal, opts = {}) {
  let next = verifySignal(signal, opts);
  next = activateSignal(next);
  next = advanceLifecycle(next, opts.asOf);
  return next;
}

module.exports = {
  MS_PER_DAY,
  asOfDate,
  defaultExpiresAt,
  verifySignal,
  activateSignal,
  advanceLifecycle,
  resolveLifecycle,
};
