'use strict';

/**
 * Operator-delegated outbound daily maximum (outer envelope).
 * Emmett recommends safe capacity within this ceiling; it is never a floor.
 */

function asOptionalCap(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.trunc(n);
}

/**
 * Canonical operator outer envelope for daily sends.
 * When `operatorDelegatedMaximumDailyCapacity` is set (tenant-mailbox grants),
 * legacy `dailyCap` may remain as a historical artifact and must not bind capacity.
 */
function resolveOperatorDelegatedMaximumDailyCapacity(policy = {}) {
  if (!policy || typeof policy !== 'object') return null;
  const delegated = asOptionalCap(policy.operatorDelegatedMaximumDailyCapacity);
  if (delegated != null) return delegated;
  return asOptionalCap(policy.dailyCap);
}

function capacityLimitingAuthorityFromFactor(limitingFactor) {
  switch (limitingFactor) {
    case 'governor_halt':
    case 'governor_slow':
      return 'governor_pause';
    case 'authorization_daily_cap':
    case 'authorization_remaining_total':
      return 'operator_ceiling';
    case 'schedule_window_spacing':
      return 'schedule_window';
    case 'spacing_window':
      return 'spacing';
    case 'deliverability':
    case 'warmup':
    case 'mailbox_provider':
    case 'none':
    default:
      return 'emmett';
  }
}

module.exports = {
  resolveOperatorDelegatedMaximumDailyCapacity,
  capacityLimitingAuthorityFromFactor,
};
