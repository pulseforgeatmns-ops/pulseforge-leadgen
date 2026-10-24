'use strict';

/**
 * Business Signals types (SPEC-031 / ADR-018).
 * Observations only — never speculative purchase intent.
 */

const SIGNAL_CONFIDENCE = Object.freeze({
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
  UNKNOWN: 'unknown',
});

const SIGNAL_CATEGORY = Object.freeze({
  GROWTH: 'growth',
  OPERATIONAL: 'operational',
  MARKETING: 'marketing',
  ORGANIZATIONAL: 'organizational',
  BUYING: 'buying',
});

const SIGNAL_LIFECYCLE = Object.freeze({
  DETECTED: 'detected',
  VERIFIED: 'verified',
  ACTIVE: 'active',
  DECAYING: 'decaying',
  ARCHIVED: 'archived',
});

/** Confidence → numeric score (Unknown = 0; never influences ranking). */
const CONFIDENCE_SCORE = Object.freeze({
  [SIGNAL_CONFIDENCE.HIGH]: 0.9,
  [SIGNAL_CONFIDENCE.MEDIUM]: 0.7,
  [SIGNAL_CONFIDENCE.LOW]: 0.4,
  [SIGNAL_CONFIDENCE.UNKNOWN]: 0,
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildSignalEvidence(partial = {}) {
  return {
    kind: String(partial.kind || 'observation'),
    summary: String(partial.summary || ''),
    url: partial.url != null ? String(partial.url) : undefined,
    observedAt: partial.observedAt != null ? String(partial.observedAt) : undefined,
    rawRef: partial.rawRef != null ? String(partial.rawRef) : undefined,
  };
}

/**
 * @param {object} [partial]
 * @returns {object} BusinessSignal
 */
function buildBusinessSignal(partial = {}) {
  const confidence = normalizeConfidence(partial.confidence);
  const evidence = Array.isArray(partial.evidence)
    ? partial.evidence.map(buildSignalEvidence)
    : [];
  const evidenceRefs = Array.isArray(partial.evidenceRefs)
    ? partial.evidenceRefs.map(String)
    : evidence
        .map((e) => e.rawRef || e.url || e.summary)
        .filter(Boolean)
        .map(String);

  return {
    id: String(partial.id || ''),
    type: String(partial.type || ''),
    category: normalizeCategory(partial.category),
    title: String(partial.title || ''),
    description: String(partial.description || ''),
    confidence,
    confidenceScore:
      Number.isFinite(Number(partial.confidenceScore))
        ? clamp01(partial.confidenceScore)
        : CONFIDENCE_SCORE[confidence],
    lifecycle: normalizeLifecycle(partial.lifecycle),
    observedAt: String(partial.observedAt || ''),
    source: String(partial.source || ''),
    evidence,
    evidenceRefs,
    expiresAt: partial.expiresAt != null ? String(partial.expiresAt) : undefined,
    influenceWeight: Number.isFinite(Number(partial.influenceWeight))
      ? clamp01(partial.influenceWeight)
      : 0,
    companyId: partial.companyId != null ? String(partial.companyId) : undefined,
    prospectId: partial.prospectId != null ? String(partial.prospectId) : undefined,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildSignalsPackage(partial = {}) {
  const signals = Array.isArray(partial.signals)
    ? partial.signals.map(buildBusinessSignal)
    : [];
  const activeSignals = Array.isArray(partial.activeSignals)
    ? partial.activeSignals.map(buildBusinessSignal)
    : signals.filter(
        (s) =>
          (s.lifecycle === SIGNAL_LIFECYCLE.ACTIVE ||
            s.lifecycle === SIGNAL_LIFECYCLE.DECAYING) &&
          s.influenceWeight > 0 &&
          s.confidence !== SIGNAL_CONFIDENCE.UNKNOWN
      );
  const buyingSignals = Array.isArray(partial.buyingSignals)
    ? partial.buyingSignals.map(buildBusinessSignal)
    : activeSignals.filter(
        (s) =>
          s.category === SIGNAL_CATEGORY.BUYING ||
          s.category === SIGNAL_CATEGORY.GROWTH
      );

  return {
    signals,
    activeSignals,
    buyingSignals,
    archivedCount: Number(partial.archivedCount) || 0,
    knowledgeWrites: Array.isArray(partial.knowledgeWrites)
      ? partial.knowledgeWrites
      : [],
  };
}

function normalizeConfidence(value) {
  const v = String(value || SIGNAL_CONFIDENCE.UNKNOWN).toLowerCase();
  if (Object.values(SIGNAL_CONFIDENCE).includes(v)) return v;
  return SIGNAL_CONFIDENCE.UNKNOWN;
}

function normalizeCategory(value) {
  const v = String(value || SIGNAL_CATEGORY.OPERATIONAL).toLowerCase();
  if (Object.values(SIGNAL_CATEGORY).includes(v)) return v;
  return SIGNAL_CATEGORY.OPERATIONAL;
}

function normalizeLifecycle(value) {
  const v = String(value || SIGNAL_LIFECYCLE.DETECTED).toLowerCase();
  if (Object.values(SIGNAL_LIFECYCLE).includes(v)) return v;
  return SIGNAL_LIFECYCLE.DETECTED;
}

function clamp01(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, Number(v.toFixed(4))));
}

module.exports = {
  SIGNAL_CONFIDENCE,
  SIGNAL_CATEGORY,
  SIGNAL_LIFECYCLE,
  CONFIDENCE_SCORE,
  buildSignalEvidence,
  buildBusinessSignal,
  buildSignalsPackage,
  normalizeConfidence,
  normalizeCategory,
  normalizeLifecycle,
  clamp01,
};
