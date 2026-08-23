'use strict';

/**
 * SPEC-143 — Memory confidence and freshness decay.
 * Old knowledge naturally decays until refreshed.
 */

const {
  DEFAULT_FRESHNESS_HALF_LIFE_DAYS,
  DEFAULT_STALE_THRESHOLD_DAYS,
  DEFAULT_MIN_CONFIDENCE,
} = require('./types');

function ageDays(iso, nowMs = Date.now()) {
  if (!iso) return Infinity;
  const ts = new Date(iso).getTime();
  if (Number.isNaN(ts)) return Infinity;
  return Math.max(0, (nowMs - ts) / (1000 * 60 * 60 * 24));
}

function computeFreshnessDays(verifiedAt, nowMs = Date.now()) {
  const days = ageDays(verifiedAt, nowMs);
  return days === Infinity ? null : Math.round(days);
}

/**
 * Exponential half-life decay on base confidence.
 * More verification sources slow decay.
 */
function computeEffectiveConfidence(memory, opts = {}) {
  const base = memory.confidence != null ? Number(memory.confidence) : 0;
  if (base <= 0) return 0;

  const halfLifeDays =
    opts.halfLifeDays != null ? Number(opts.halfLifeDays) : DEFAULT_FRESHNESS_HALF_LIFE_DAYS;
  const freshnessDays = computeFreshnessDays(memory.verifiedAt || memory.updatedAt, opts.nowMs);
  if (freshnessDays == null) return base;

  const sourceCount = Math.max(1, Number(memory.sourceCount || 1));
  const sourceBonus = Math.min(0.15, (sourceCount - 1) * 0.04);
  const adjustedBase = Math.min(0.99, base + sourceBonus);

  const decayFactor = Math.pow(0.5, freshnessDays / halfLifeDays);
  const effective = adjustedBase * decayFactor;

  return Number(Math.max(DEFAULT_MIN_CONFIDENCE * 0.5, Math.min(0.99, effective)).toFixed(3));
}

function isMemoryStale(memory, opts = {}) {
  const threshold =
    opts.staleThresholdDays != null
      ? Number(opts.staleThresholdDays)
      : DEFAULT_STALE_THRESHOLD_DAYS;
  const freshnessDays = computeFreshnessDays(memory.verifiedAt || memory.updatedAt, opts.nowMs);
  if (freshnessDays == null) return true;
  const effective = computeEffectiveConfidence(memory, opts);
  return freshnessDays > threshold || effective < DEFAULT_MIN_CONFIDENCE;
}

function meetsConfidenceThreshold(memory, threshold, opts = {}) {
  return computeEffectiveConfidence(memory, opts) >= Number(threshold || DEFAULT_MIN_CONFIDENCE);
}

function mergeVerificationSources(existing = [], incoming = []) {
  const set = new Set(
    [...existing, ...incoming]
      .map((s) => String(s || '').trim().toLowerCase())
      .filter(Boolean)
  );
  return [...set];
}

function refreshMemoryConfidence(existing, incoming, opts = {}) {
  const mergedSources = mergeVerificationSources(
    existing.verificationSources,
    incoming.verificationSources || incoming.supportedBy || incoming.evidence
  );
  const sourceCount = mergedSources.length || Math.max(existing.sourceCount || 1, 1);
  const incomingConf = incoming.confidence != null ? Number(incoming.confidence) : existing.confidence;
  const existingEffective = computeEffectiveConfidence(existing, opts);
  const refreshedConfidence = Number(
    Math.min(0.99, Math.max(existingEffective, incomingConf, existing.confidence || 0)).toFixed(3)
  );

  return {
    confidence: refreshedConfidence,
    verifiedAt: incoming.verifiedAt || opts.nowIso || new Date().toISOString(),
    sourceCount,
    verificationSources: mergedSources,
  };
}

module.exports = {
  ageDays,
  computeFreshnessDays,
  computeEffectiveConfidence,
  isMemoryStale,
  meetsConfidenceThreshold,
  mergeVerificationSources,
  refreshMemoryConfidence,
};
