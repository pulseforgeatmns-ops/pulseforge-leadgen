'use strict';

/**
 * SPEC-144 — Evidence freshness.
 * Old evidence expires; freshness is part of credibility.
 */

const FRESHNESS_BANDS = Object.freeze({
  EXCELLENT: 'excellent',
  GOOD: 'good',
  LOW: 'low_confidence',
  NEEDS_VERIFICATION: 'needs_verification',
});

const FRESHNESS_LABELS = Object.freeze({
  [FRESHNESS_BANDS.EXCELLENT]: 'Excellent',
  [FRESHNESS_BANDS.GOOD]: 'Good',
  [FRESHNESS_BANDS.LOW]: 'Low confidence',
  [FRESHNESS_BANDS.NEEDS_VERIFICATION]: 'Needs verification',
});

/**
 * @param {string|Date|null} observedAt
 * @param {string|Date} [now]
 * @returns {number|null}
 */
function evidenceAgeDays(observedAt, now = new Date()) {
  if (!observedAt) return null;
  const observed = observedAt instanceof Date ? observedAt : new Date(observedAt);
  if (Number.isNaN(observed.getTime())) return null;
  const ref = now instanceof Date ? now : new Date(now);
  return Math.max(0, Math.floor((ref.getTime() - observed.getTime()) / (1000 * 60 * 60 * 24)));
}

/**
 * @param {string|Date|null} observedAt
 * @param {string|Date} [now]
 * @returns {{ band: string, label: string, ageDays: number|null, multiplier: number }}
 */
function classifyFreshness(observedAt, now = new Date()) {
  const ageDays = evidenceAgeDays(observedAt, now);

  if (ageDays == null) {
    return {
      band: FRESHNESS_BANDS.NEEDS_VERIFICATION,
      label: FRESHNESS_LABELS[FRESHNESS_BANDS.NEEDS_VERIFICATION],
      ageDays: null,
      multiplier: 0.75,
    };
  }

  if (ageDays <= 7) {
    return {
      band: FRESHNESS_BANDS.EXCELLENT,
      label: FRESHNESS_LABELS[FRESHNESS_BANDS.EXCELLENT],
      ageDays,
      multiplier: 1,
    };
  }

  if (ageDays <= 90) {
    return {
      band: FRESHNESS_BANDS.GOOD,
      label: FRESHNESS_LABELS[FRESHNESS_BANDS.GOOD],
      ageDays,
      multiplier: 0.92,
    };
  }

  if (ageDays <= 365) {
    return {
      band: FRESHNESS_BANDS.LOW,
      label: FRESHNESS_LABELS[FRESHNESS_BANDS.LOW],
      ageDays,
      multiplier: 0.8,
    };
  }

  return {
    band: FRESHNESS_BANDS.NEEDS_VERIFICATION,
    label: FRESHNESS_LABELS[FRESHNESS_BANDS.NEEDS_VERIFICATION],
    ageDays,
    multiplier: 0.65,
  };
}

/**
 * Apply freshness decay to a base weight.
 * @param {number} weight
 * @param {string|Date|null} observedAt
 * @param {string|Date} [now]
 * @returns {number}
 */
function freshnessAdjustedWeight(weight, observedAt, now = new Date()) {
  const freshness = classifyFreshness(observedAt, now);
  return Number((weight * freshness.multiplier).toFixed(3));
}

/**
 * @param {object} item
 * @param {string|Date} [now]
 * @returns {object}
 */
function enrichEvidenceWithFreshness(item, now = new Date()) {
  const observedAt = item.observedAt || item.observed_at || null;
  const freshness = classifyFreshness(observedAt, now);
  return {
    ...item,
    observedAt,
    freshness: freshness.band,
    freshnessLabel: freshness.label,
    ageDays: freshness.ageDays,
    effectiveWeight:
      item.weight != null
        ? freshnessAdjustedWeight(item.weight, observedAt, now)
        : freshnessAdjustedWeight(item.weight || 0.65, observedAt, now),
  };
}

module.exports = {
  FRESHNESS_BANDS,
  FRESHNESS_LABELS,
  evidenceAgeDays,
  classifyFreshness,
  freshnessAdjustedWeight,
  enrichEvidenceWithFreshness,
};
