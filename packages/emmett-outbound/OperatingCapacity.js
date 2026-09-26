'use strict';

/**
 * Governed outbound operating capacity.
 *
 * Emmett owns operational send capacity. Authorization may still bind
 * effective volume, but that bound is never silent: recommended and
 * effective are always distinct when they differ.
 */

const { GOVERNOR_OUTCOMES } = require('./types');

const LIMITING_FACTORS = Object.freeze({
  GOVERNOR_HALT: 'governor_halt',
  DELIVERABILITY: 'deliverability',
  GOVERNOR_SLOW: 'governor_slow',
  MAILBOX_PROVIDER: 'mailbox_provider',
  WARMUP: 'warmup',
  AUTHORIZATION_DAILY_CAP: 'authorization_daily_cap',
  AUTHORIZATION_REMAINING_TOTAL: 'authorization_remaining_total',
  SPACING_WINDOW: 'spacing_window',
  NONE: 'none',
});

function asNonNegInt(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.trunc(n));
}

function asOptionalCap(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.trunc(n);
}

function governorOutcomeOf(assessed = {}) {
  const raw = String(assessed.governor?.outcome || assessed.governor || '').toLowerCase();
  if (Object.values(GOVERNOR_OUTCOMES).includes(raw)) return raw;
  if (assessed.governor?.halt === true) return GOVERNOR_OUTCOMES.PAUSE;
  return GOVERNOR_OUTCOMES.PROCEED;
}

function emmettRecommended(assessed = {}, fallback = 0) {
  const capacity = assessed.capacity || {};
  if (capacity.recommended != null) return asNonNegInt(capacity.recommended);
  if (assessed.recommended != null) return asNonNegInt(assessed.recommended);
  return asNonNegInt(fallback);
}

function classifyEmmettLimiter(assessed = {}, recommended) {
  const outcome = governorOutcomeOf(assessed);
  const halt = assessed.governor?.halt === true
    || outcome === GOVERNOR_OUTCOMES.PAUSE
    || outcome === GOVERNOR_OUTCOMES.EMERGENCY;
  if (halt || recommended <= 0) {
    return {
      recommendedSafeDailyCapacity: 0,
      factor: LIMITING_FACTORS.GOVERNOR_HALT,
      reason: assessed.governor?.reason || 'Governor halted sending.',
    };
  }

  const snapshot = assessed.snapshot || {};
  const capacity = assessed.capacity || {};
  const slowCap = asOptionalCap(assessed.governor?.slowCap);
  const providerCeiling = asOptionalCap(snapshot.providerCeiling || capacity.ceiling);
  const warmupCap = asOptionalCap(snapshot.warmup?.dailyCap);

  let safe = recommended;
  let factor = LIMITING_FACTORS.DELIVERABILITY;
  let reason = capacity.statement
    || `Emmett recommends ${recommended} based on deliverability health.`;

  if (slowCap != null && slowCap < safe) {
    safe = slowCap;
    factor = LIMITING_FACTORS.GOVERNOR_SLOW;
    reason = assessed.governor?.reason || `Governor slowed today's volume to ${slowCap}.`;
  }
  if (warmupCap != null && warmupCap < safe) {
    safe = warmupCap;
    factor = LIMITING_FACTORS.WARMUP;
    reason = `Mailbox warmup ceiling is ${warmupCap}.`;
  }
  if (providerCeiling != null && providerCeiling < safe) {
    safe = providerCeiling;
    factor = LIMITING_FACTORS.MAILBOX_PROVIDER;
    reason = `Mailbox/provider hard limit is ${providerCeiling}.`;
  }

  return { recommendedSafeDailyCapacity: safe, factor, reason };
}

/**
 * @param {object} input
 * @param {object} [input.assessed] Emmett engine.assess() result
 * @param {object} [input.policy] active grant/policy
 * @param {number} [input.sentToday]
 * @param {number} [input.totalAttempted]
 * @returns {object}
 */
function assessOperatingCapacity(input = {}) {
  const assessed = input.assessed || {};
  const policy = input.policy || {};
  const recommended = emmettRecommended(assessed, input.emmettCapacity);
  const emmett = classifyEmmettLimiter(assessed, recommended);
  const healthScore = Number(
    assessed.health?.score
    ?? assessed.governor?.healthScore
    ?? assessed.capacity?.healthScore
    ?? 0
  );
  const governor = governorOutcomeOf(assessed);

  const authorizationDailyCap = asOptionalCap(policy.dailyCap);
  const authorizationTotalCap = asOptionalCap(policy.totalCap);
  const totalAttempted = asNonNegInt(input.totalAttempted);
  const remainingTotalAuthorization = authorizationTotalCap == null
    ? null
    : Math.max(0, authorizationTotalCap - totalAttempted);

  let effective = emmett.recommendedSafeDailyCapacity;
  let limitingFactor = emmett.factor;
  let capacityReason = emmett.reason;

  if (authorizationDailyCap != null && authorizationDailyCap < effective) {
    effective = authorizationDailyCap;
    limitingFactor = LIMITING_FACTORS.AUTHORIZATION_DAILY_CAP;
    capacityReason = `Operator authorization limits daily sends to ${authorizationDailyCap}; Emmett recommends ${emmett.recommendedSafeDailyCapacity}.`;
  }

  if (remainingTotalAuthorization != null && remainingTotalAuthorization < effective) {
    effective = remainingTotalAuthorization;
    limitingFactor = LIMITING_FACTORS.AUTHORIZATION_REMAINING_TOTAL;
    capacityReason = `Remaining authorization budget is ${remainingTotalAuthorization}; Emmett recommends ${emmett.recommendedSafeDailyCapacity}.`;
  }

  if (effective <= 0 && emmett.recommendedSafeDailyCapacity <= 0) {
    limitingFactor = emmett.factor;
    capacityReason = emmett.reason;
  }

  return {
    recommendedSafeDailyCapacity: emmett.recommendedSafeDailyCapacity,
    effectiveDailyCapacity: effective,
    limitingFactor,
    healthScore,
    governor,
    capacityReason,
    authorizationDailyCap,
    remainingTotalAuthorization,
    emmettRecommended: recommended,
    silentCap: false,
  };
}

module.exports = {
  LIMITING_FACTORS,
  assessOperatingCapacity,
  governorOutcomeOf,
  emmettRecommended,
};
