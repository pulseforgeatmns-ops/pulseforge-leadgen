'use strict';

/**
 * Operator-delegated outbound daily maximum (outer envelope).
 * Emmett recommends safe capacity within this ceiling; it is never a floor.
 */

const { clock } = require('../acquisition-mission/DailyOutboundPolicy');

const PROGRAM_TOTAL_CAP_MAX = 100;

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

/**
 * Weekday dispatch slots authorized between grant start and expiry (inclusive start days).
 * Matches grant calendar gates used at send time (America/New_York weekday index).
 */
function countGrantWeekdaySlots(policy = {}) {
  const startMs = Date.parse(policy.startsAt);
  const endMs = Date.parse(policy.expiresAt);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return 0;
  const weekdays = policy.weekdays || [1, 2, 3, 4, 5];
  let count = 0;
  for (let offset = 0; offset < 370; offset++) {
    const candidate = new Date(startMs + offset * 86400000);
    if (+candidate >= endMs) break;
    if (+candidate < startMs) continue;
    const { weekday } = clock(candidate);
    if (weekdays.includes(weekday)) count += 1;
  }
  return count;
}

/**
 * Program totalCap is the cumulative first-touch budget for the standing grant window
 * (see anchor governed outbound docs). When the operator delegates ramp authority,
 * totalCap must cover delegated daily max across remaining grant weekdays and stay
 * within the global 100-attempt envelope. Activation canaries (dailyCap=totalCap=1)
 * are replaced — they are safety bounds, not the delegated program ceiling.
 */
function resolveOperatorProgramTotalCapForDelegation(currentPolicy = {}, operatorDelegatedMaximumDailyCapacity) {
  const delegated = asOptionalCap(operatorDelegatedMaximumDailyCapacity);
  if (delegated == null) return asOptionalCap(currentPolicy.totalCap);

  const weekdaySlots = countGrantWeekdaySlots(currentPolicy);
  const grantPeriodTotal = Math.min(
    PROGRAM_TOTAL_CAP_MAX,
    Math.max(delegated, delegated * Math.max(weekdaySlots, 1)),
  );

  const currentTotal = asOptionalCap(currentPolicy.totalCap);
  const legacyDaily = asOptionalCap(currentPolicy.dailyCap) ?? 1;
  const hadDelegated = asOptionalCap(currentPolicy.operatorDelegatedMaximumDailyCapacity) != null;
  const activationSafetyGrant = !hadDelegated && currentTotal != null && currentTotal <= legacyDaily;
  if (activationSafetyGrant) return grantPeriodTotal;
  if (currentTotal == null) return grantPeriodTotal;
  return Math.min(PROGRAM_TOTAL_CAP_MAX, Math.max(currentTotal, grantPeriodTotal));
}

function describeOperatorAuthorityEnvelope(policy = {}) {
  const operatorDelegatedMaximumDailyCapacity = asOptionalCap(policy.operatorDelegatedMaximumDailyCapacity);
  const dailyCap = asOptionalCap(policy.dailyCap);
  const totalCap = asOptionalCap(policy.totalCap);
  const effectiveOperatorDailyCeiling = resolveOperatorDelegatedMaximumDailyCapacity(policy);
  return {
    dailyCap,
    totalCap,
    operatorDelegatedMaximumDailyCapacity,
    effectiveOperatorDailyCeiling,
    effectiveOperatorProgramCeiling: totalCap,
    limitingOperatorAuthority: totalCap != null && effectiveOperatorDailyCeiling != null
      && totalCap < effectiveOperatorDailyCeiling
      ? 'program_total_cap'
      : 'operator_delegated_daily_max',
  };
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
  resolveOperatorProgramTotalCapForDelegation,
  countGrantWeekdaySlots,
  describeOperatorAuthorityEnvelope,
  capacityLimitingAuthorityFromFactor,
  PROGRAM_TOTAL_CAP_MAX,
};
