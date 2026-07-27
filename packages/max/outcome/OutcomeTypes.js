'use strict';

/**
 * Outcome Intelligence types (SPEC-013 / ADR-008).
 * Evaluates whether intelligence was right — never changes reasoning.
 */

const crypto = require('crypto');
const { deepFreeze } = require('../reasoning/ReasoningTypes');

/** Recommendation lifecycle for outcome measurement. */
const LIFECYCLE = Object.freeze({
  GENERATED: 'generated',
  REVIEWED: 'reviewed',
  APPROVED: 'approved',
  EXECUTED: 'executed',
  OBSERVED: 'observed',
  SUCCESSFUL: 'successful',
  UNSUCCESSFUL: 'unsuccessful',
  INCONCLUSIVE: 'inconclusive',
});

/** Terminal business results after observation. */
const OUTCOME_RESULTS = Object.freeze({
  SUCCESSFUL: 'successful',
  UNSUCCESSFUL: 'unsuccessful',
  INCONCLUSIVE: 'inconclusive',
});

/** Valid forward transitions. Terminal results have no further transitions. */
const LIFECYCLE_TRANSITIONS = Object.freeze({
  [LIFECYCLE.GENERATED]: [LIFECYCLE.REVIEWED],
  [LIFECYCLE.REVIEWED]: [LIFECYCLE.APPROVED],
  [LIFECYCLE.APPROVED]: [LIFECYCLE.EXECUTED],
  [LIFECYCLE.EXECUTED]: [LIFECYCLE.OBSERVED],
  [LIFECYCLE.OBSERVED]: [
    LIFECYCLE.SUCCESSFUL,
    LIFECYCLE.UNSUCCESSFUL,
    LIFECYCLE.INCONCLUSIVE,
  ],
  [LIFECYCLE.SUCCESSFUL]: [],
  [LIFECYCLE.UNSUCCESSFUL]: [],
  [LIFECYCLE.INCONCLUSIVE]: [],
});

/** Confidence bands for calibration (engine confidence is 0–100). */
const CONFIDENCE_BANDS = Object.freeze([
  Object.freeze({ id: '90+', min: 90, max: 100 }),
  Object.freeze({ id: '80-89', min: 80, max: 89.999 }),
  Object.freeze({ id: '70-79', min: 70, max: 79.999 }),
  Object.freeze({ id: '60-69', min: 60, max: 69.999 }),
  Object.freeze({ id: '<60', min: 0, max: 59.999 }),
]);

/** Known strategy ids (SPEC-002) — metrics are keyed by these when present. */
const STRATEGY_IDS = Object.freeze([
  'opportunity',
  'engagement',
  'relationship',
  'decision_maker',
  'overflow',
  'technology',
  'risk',
]);

/**
 * @param {string} from
 * @param {string} to
 */
function canTransitionLifecycle(from, to) {
  if (from === to) return true;
  const allowed = LIFECYCLE_TRANSITIONS[from];
  if (!allowed) return false;
  return allowed.includes(to);
}

/**
 * @param {number} confidence
 * @returns {string}
 */
function bandForConfidence(confidence) {
  const c = Number(confidence);
  const value = Number.isFinite(c) ? c : 0;
  for (const band of CONFIDENCE_BANDS) {
    if (value >= band.min && value <= band.max) return band.id;
  }
  return CONFIDENCE_BANDS[CONFIDENCE_BANDS.length - 1].id;
}

/**
 * @param {string} lifecycle
 */
function isTerminalOutcome(lifecycle) {
  return (
    lifecycle === LIFECYCLE.SUCCESSFUL ||
    lifecycle === LIFECYCLE.UNSUCCESSFUL ||
    lifecycle === LIFECYCLE.INCONCLUSIVE
  );
}

/**
 * @param {string} lifecycle
 */
function outcomeFromLifecycle(lifecycle) {
  if (isTerminalOutcome(lifecycle)) return lifecycle;
  return null;
}

/**
 * Build a frozen RecommendationOutcome record.
 * @param {object} input
 */
function buildRecommendationOutcome(input) {
  if (!input || input.recommendationId == null) {
    throw new Error('RecommendationOutcome requires recommendationId');
  }
  if (input.tenantId == null || String(input.tenantId).trim() === '') {
    throw new Error('RecommendationOutcome requires tenantId');
  }

  const lifecycle = String(input.lifecycle || LIFECYCLE.GENERATED).toLowerCase();
  if (!Object.values(LIFECYCLE).includes(lifecycle)) {
    throw new Error(`Unknown lifecycle: ${input.lifecycle}`);
  }

  const confidenceAtRecommendation = clampConfidence(
    input.confidenceAtRecommendation
  );
  const confidenceAtOutcome =
    input.confidenceAtOutcome != null
      ? clampConfidence(input.confidenceAtOutcome)
      : null;

  const outcome =
    input.outcome != null
      ? String(input.outcome).toLowerCase()
      : outcomeFromLifecycle(lifecycle);
  if (
    outcome != null &&
    !Object.values(OUTCOME_RESULTS).includes(outcome)
  ) {
    throw new Error(`Unknown outcome: ${input.outcome}`);
  }

  const id =
    input.id != null
      ? String(input.id)
      : `outc:${crypto.randomBytes(8).toString('hex')}`;

  const generatedAt = input.generatedAt || new Date().toISOString();
  const observedAt =
    input.observedAt ||
    (lifecycle === LIFECYCLE.OBSERVED || isTerminalOutcome(lifecycle)
      ? new Date().toISOString()
      : null);

  return deepFreeze({
    id,
    recommendationId: String(input.recommendationId),
    tenantId: String(input.tenantId),
    strategyId:
      input.strategyId != null && String(input.strategyId).trim()
        ? String(input.strategyId)
        : null,
    executed:
      input.executed === true ||
      lifecycle === LIFECYCLE.EXECUTED ||
      lifecycle === LIFECYCLE.OBSERVED ||
      isTerminalOutcome(lifecycle),
    outcome,
    lifecycle,
    observedAt,
    confidenceAtRecommendation,
    confidenceAtOutcome,
    confidenceBand: bandForConfidence(confidenceAtRecommendation),
    notes: input.notes != null ? String(input.notes) : null,
    generatedAt,
    reviewedAt: input.reviewedAt || null,
    approvedAt: input.approvedAt || null,
    executedAt: input.executedAt || null,
    evidenceSourceIds: Array.isArray(input.evidenceSourceIds)
      ? input.evidenceSourceIds.map(String)
      : [],
    promotedFromWatch: input.promotedFromWatch === true,
    watchAlertEarly: input.watchAlertEarly === true,
    meta:
      input.meta && typeof input.meta === 'object' ? { ...input.meta } : null,
  });
}

function clampConfidence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, n));
}

module.exports = {
  LIFECYCLE,
  OUTCOME_RESULTS,
  LIFECYCLE_TRANSITIONS,
  CONFIDENCE_BANDS,
  STRATEGY_IDS,
  canTransitionLifecycle,
  bandForConfidence,
  isTerminalOutcome,
  outcomeFromLifecycle,
  buildRecommendationOutcome,
  clampConfidence,
};
