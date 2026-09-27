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
  SCHEDULE_WINDOW_SPACING: 'schedule_window_spacing',
  NONE: 'none',
});

const DEFAULT_SEND_WINDOW = Object.freeze({ startHour: 9, endHour: 17, timezone: 'America/New_York' });
const DEFAULT_GRANT_WEEKDAYS = Object.freeze([1, 2, 3, 4, 5]);

/**
 * Count first-touch send slots that fit inside the allowed window with minimum spacing.
 * Window bounds are whole-hour [startHour, endHour) in the configured timezone.
 */
function computeScheduleLimitedCapacity({
  allowedSendWindow = DEFAULT_SEND_WINDOW,
  minSpacingMinutes = 60,
  dispatchDayAllowed = true,
} = {}) {
  if (dispatchDayAllowed === false) return 0;
  const start = Number(allowedSendWindow?.startHour);
  const end = Number(allowedSendWindow?.endHour);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return 0;
  const spacing = Number(minSpacingMinutes);
  const windowMinutes = (end - start) * 60;
  if (!Number.isFinite(spacing) || spacing <= 0) {
    return Math.max(0, Math.trunc(windowMinutes / 60));
  }
  return Math.max(0, Math.floor(windowMinutes / spacing));
}

function grantWeekdayIndex(now = new Date(), timeZone = 'America/New_York') {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone,
    weekday: 'short',
  }).formatToParts(now).map(p => [p.type, p.value]));
  return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(parts.weekday);
}

function isGrantDispatchDay(policy = {}, now = new Date()) {
  const weekdays = policy.weekdays;
  if (!Array.isArray(weekdays) || !weekdays.length) return true;
  const tz = policy.timeZone || policy.timezone || DEFAULT_SEND_WINDOW.timezone;
  const weekday = grantWeekdayIndex(now, tz);
  if (weekday < 0) return false;
  return weekdays.includes(weekday);
}

function resolveGrantSendWindow(policy = {}, schedule = {}, snapshot = {}) {
  const fromSchedule = schedule.allowedSendWindow;
  const fromPolicy = policy.allowedSendWindow || (
    policy.startHour != null || policy.endHour != null
      ? {
        startHour: policy.startHour,
        endHour: policy.endHour,
        timezone: policy.timeZone || policy.timezone,
      }
      : null
  );
  const allowedSendWindow = fromSchedule || fromPolicy || snapshot.allowedSendWindow || DEFAULT_SEND_WINDOW;
  const start = Number(allowedSendWindow?.startHour);
  const end = Number(allowedSendWindow?.endHour);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return null;
  }
  return {
    startHour: start,
    endHour: end,
    timezone: allowedSendWindow.timezone || policy.timeZone || policy.timezone || DEFAULT_SEND_WINDOW.timezone,
  };
}

function resolveGrantMinSpacingMinutes(policy = {}, schedule = {}) {
  if (schedule.minSpacingMinutes != null) return Number(schedule.minSpacingMinutes);
  if (policy.minSpacingMinutes != null) return Number(policy.minSpacingMinutes);
  if (policy.spacingMinutes != null) return Number(policy.spacingMinutes);
  return 60;
}

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
function resolveScheduleInput(input = {}) {
  const schedule = input.schedule || {};
  const policy = input.policy || {};
  const allowedSendWindow = resolveGrantSendWindow(policy, schedule, input.assessed?.snapshot || {});
  const minSpacingMinutes = resolveGrantMinSpacingMinutes(policy, schedule);
  const dispatchDayAllowed = isGrantDispatchDay(policy, input.now || new Date());
  return { allowedSendWindow, minSpacingMinutes, dispatchDayAllowed };
}

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

  let authorizationLimitedCapacity = emmett.recommendedSafeDailyCapacity;
  let limitingFactor = emmett.factor;
  let capacityReason = emmett.reason;

  if (authorizationDailyCap != null && authorizationDailyCap < authorizationLimitedCapacity) {
    authorizationLimitedCapacity = authorizationDailyCap;
    limitingFactor = LIMITING_FACTORS.AUTHORIZATION_DAILY_CAP;
    capacityReason = `Operator authorization limits daily sends to ${authorizationDailyCap}; Emmett recommends ${emmett.recommendedSafeDailyCapacity}.`;
  }

  if (remainingTotalAuthorization != null && remainingTotalAuthorization < authorizationLimitedCapacity) {
    authorizationLimitedCapacity = remainingTotalAuthorization;
    limitingFactor = LIMITING_FACTORS.AUTHORIZATION_REMAINING_TOTAL;
    capacityReason = `Remaining authorization budget is ${remainingTotalAuthorization}; Emmett recommends ${emmett.recommendedSafeDailyCapacity}.`;
  }

  const { allowedSendWindow, minSpacingMinutes, dispatchDayAllowed } = resolveScheduleInput(input);
  const scheduleLimitedCapacity = allowedSendWindow
    ? computeScheduleLimitedCapacity({
      allowedSendWindow,
      minSpacingMinutes,
      dispatchDayAllowed,
    })
    : 0;

  let dispatchableDailyCapacity = authorizationLimitedCapacity;
  if (!allowedSendWindow || !dispatchDayAllowed) {
    dispatchableDailyCapacity = 0;
    limitingFactor = LIMITING_FACTORS.SCHEDULE_WINDOW_SPACING;
    capacityReason = !allowedSendWindow
      ? 'Operator send grant has no valid dispatch window; schedule-limited capacity is unavailable.'
      : 'Operator send grant does not permit dispatch on this weekday; schedule-limited capacity is zero.';
  } else if (scheduleLimitedCapacity < dispatchableDailyCapacity) {
    dispatchableDailyCapacity = scheduleLimitedCapacity;
    limitingFactor = LIMITING_FACTORS.SCHEDULE_WINDOW_SPACING;
    capacityReason = `Send window ${allowedSendWindow.startHour}:00–${allowedSendWindow.endHour}:00 with ${minSpacingMinutes}-minute spacing allows ${scheduleLimitedCapacity} dispatchable first-touch slots; Emmett recommends ${emmett.recommendedSafeDailyCapacity} and authorization permits ${authorizationLimitedCapacity}.`;
  }

  if (dispatchableDailyCapacity <= 0 && emmett.recommendedSafeDailyCapacity <= 0) {
    limitingFactor = emmett.factor;
    capacityReason = emmett.reason;
  }

  return {
    recommendedSafeDailyCapacity: emmett.recommendedSafeDailyCapacity,
    authorizationLimitedCapacity,
    scheduleLimitedCapacity,
    dispatchableDailyCapacity,
    /** @deprecated use authorizationLimitedCapacity for auth-bound volume; dispatchableDailyCapacity for Max/send planning */
    effectiveDailyCapacity: authorizationLimitedCapacity,
    limitingFactor,
    healthScore,
    governor,
    capacityReason,
    authorizationDailyCap,
    remainingTotalAuthorization,
    emmettRecommended: recommended,
    allowedSendWindow: allowedSendWindow || null,
    minSpacingMinutes,
    dispatchDayAllowed,
    scheduleUnavailable: !allowedSendWindow,
    silentCap: false,
  };
}

module.exports = {
  LIMITING_FACTORS,
  computeScheduleLimitedCapacity,
  assessOperatingCapacity,
  resolveScheduleInput,
  resolveGrantSendWindow,
  resolveGrantMinSpacingMinutes,
  isGrantDispatchDay,
  governorOutcomeOf,
  emmettRecommended,
};
