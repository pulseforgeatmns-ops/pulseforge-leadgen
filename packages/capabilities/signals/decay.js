'use strict';

/**
 * Influence decay (SPEC-031 / ADR-018).
 * influenceWeight = confidenceScore * max(0, 1 - age/TTL) while Active/Decaying.
 */

const { SIGNAL_LIFECYCLE, SIGNAL_CONFIDENCE, clamp01 } = require('./types');
const { ttlForCategory } = require('./categories');
const { asOfDate, MS_PER_DAY, defaultExpiresAt } = require('./lifecycle');

/**
 * @param {object} signal
 * @param {Date|string|number} [asOf]
 * @returns {number} 0–1
 */
function computeInfluenceWeight(signal, asOf) {
  if (!signal) return 0;
  if (signal.lifecycle === SIGNAL_LIFECYCLE.ARCHIVED) return 0;
  if (signal.lifecycle === SIGNAL_LIFECYCLE.DETECTED) return 0;
  if (signal.confidence === SIGNAL_CONFIDENCE.UNKNOWN) return 0;

  const confidenceScore = Number(signal.confidenceScore) || 0;
  if (confidenceScore <= 0) return 0;

  const now = asOfDate(asOf);
  const observed = new Date(signal.observedAt);
  if (Number.isNaN(observed.getTime())) {
    return clamp01(confidenceScore);
  }

  const expiresAt = signal.expiresAt
    ? new Date(signal.expiresAt)
    : new Date(defaultExpiresAt(signal.observedAt, signal.category));

  if (!Number.isNaN(expiresAt.getTime()) && now.getTime() >= expiresAt.getTime()) {
    return 0;
  }

  const ttl = ttlForCategory(signal.category);
  const ageDays = Math.max(
    0,
    (now.getTime() - observed.getTime()) / MS_PER_DAY
  );
  const ttlDays = ttl.hardExpireDays || 90;
  const freshness = Math.max(0, 1 - ageDays / ttlDays);
  return clamp01(confidenceScore * freshness);
}

/**
 * Attach influenceWeight to signal.
 * @param {object} signal
 * @param {Date|string|number} [asOf]
 * @returns {object}
 */
function withInfluence(signal, asOf) {
  const influenceWeight = computeInfluenceWeight(signal, asOf);
  let lifecycle = signal.lifecycle;
  if (
    influenceWeight === 0 &&
    (lifecycle === SIGNAL_LIFECYCLE.ACTIVE ||
      lifecycle === SIGNAL_LIFECYCLE.DECAYING ||
      lifecycle === SIGNAL_LIFECYCLE.VERIFIED)
  ) {
    const expiresAt = signal.expiresAt
      ? new Date(signal.expiresAt)
      : new Date(defaultExpiresAt(signal.observedAt, signal.category));
    if (!Number.isNaN(expiresAt.getTime()) && asOfDate(asOf) >= expiresAt) {
      lifecycle = SIGNAL_LIFECYCLE.ARCHIVED;
    }
  }
  return { ...signal, influenceWeight, lifecycle };
}

module.exports = {
  computeInfluenceWeight,
  withInfluence,
};
