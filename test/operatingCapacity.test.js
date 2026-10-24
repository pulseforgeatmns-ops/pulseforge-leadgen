'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  assessOperatingCapacity,
  computeScheduleLimitedCapacity,
  LIMITING_FACTORS,
} = require('../packages/emmett-outbound/OperatingCapacity');
const { buildControlPlan } = require('../services/maxOutboundControlLoop');

const PRODUCTION_GRANT_POLICY = {
  dailyCap: 15,
  totalCap: 100,
  spacingMinutes: 60,
  startHour: 9,
  endHour: 17,
  timeZone: 'America/New_York',
  weekdays: [1, 2, 3, 4, 5],
  startsAt: '2026-01-01T00:00:00.000Z',
  expiresAt: '2026-12-31T23:59:59.000Z',
};

const PRODUCTION_ASSESSED = {
  capacity: { recommended: 16, statement: 'Based on today\'s reputation, I recommend 16.' },
  governor: { outcome: 'proceed', halt: false },
  health: { score: 82 },
};
const { evaluateColdOutboundEligibility } = require('../services/outboundInventory');

test('weekday 9–17 window with 60-minute spacing yields eight dispatchable slots', () => {
  assert.equal(computeScheduleLimitedCapacity({
    allowedSendWindow: { startHour: 9, endHour: 17 },
    minSpacingMinutes: 60,
  }), 8);
});

test('schedule slot regressions honor end-exclusive windows and spacing', () => {
  const window = { startHour: 9, endHour: 17 };
  assert.equal(computeScheduleLimitedCapacity({ allowedSendWindow: window, minSpacingMinutes: 120 }), 4);
  assert.equal(computeScheduleLimitedCapacity({ allowedSendWindow: window, minSpacingMinutes: 30 }), 16);
  assert.equal(computeScheduleLimitedCapacity({ allowedSendWindow: { startHour: 17, endHour: 9 }, minSpacingMinutes: 60 }), 0);
});

test('grant spacingMinutes binds schedule capacity independently of mailbox spacing defaults', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 16 },
      governor: { outcome: 'proceed', halt: false },
      snapshot: { minimumSpacingMinutes: 30 },
    },
    policy: {
      dailyCap: 15,
      spacingMinutes: 60,
      startHour: 9,
      endHour: 17,
      timeZone: 'America/New_York',
      weekdays: [1, 2, 3, 4, 5],
      startsAt: '2026-01-01T00:00:00.000Z',
      expiresAt: '2026-12-31T23:59:59.000Z',
    },
    now: new Date('2026-09-28T15:00:00.000Z'),
  });
  assert.equal(operating.scheduleLimitedCapacity, 8);
  assert.equal(operating.dispatchCapacityNow, 8);
  assert.equal(operating.planningDailyCapacity, 8);
  assert.equal(operating.dispatchableDailyCapacity, 8);
});

test('Sunday decouples dispatch-now zero from planning capacity on the next eligible weekday', () => {
  const sunday = new Date('2026-09-27T15:00:00.000Z');
  const operating = assessOperatingCapacity({
    assessed: PRODUCTION_ASSESSED,
    policy: PRODUCTION_GRANT_POLICY,
    now: sunday,
  });
  assert.equal(operating.scheduleLimitedCapacity, 0);
  assert.equal(operating.nextEligibleScheduleCapacity, 8);
  assert.equal(operating.dispatchCapacityNow, 0);
  assert.equal(operating.planningDailyCapacity, 8);
  const plan = buildControlPlan({
    operatingCapacity: operating,
    cleanInventory: 3,
    targetDays: 3,
  });
  assert.equal(plan.targetInventory, 24);
  assert.equal(plan.deficit, 21);
  assert.equal(plan.shouldReplenish, true);
  assert.equal(plan.dispatchUnavailableNow, true);
});

test('weekday outside send hours keeps planning capacity while dispatch-now stays zero', () => {
  const evening = new Date('2026-09-28T23:30:00.000Z');
  const operating = assessOperatingCapacity({
    assessed: PRODUCTION_ASSESSED,
    policy: PRODUCTION_GRANT_POLICY,
    now: evening,
  });
  assert.equal(operating.dispatchCapacityNow, 0);
  assert.equal(operating.planningDailyCapacity, 8);
  const plan = buildControlPlan({ operatingCapacity: operating, cleanInventory: 3, targetDays: 3 });
  assert.equal(plan.shouldReplenish, true);
});

test('legacy operating snapshots without planningDailyCapacity still plan from schedule headroom', () => {
  const preWindow = new Date('2026-09-28T12:51:00.000Z');
  const plan = buildControlPlan({
    dailyCap: 15,
    emmettCapacity: 16,
    operatingCapacity: {
      recommendedSafeDailyCapacity: 16,
      authorizationLimitedCapacity: 15,
      scheduleLimitedCapacity: 8,
      nextEligibleScheduleCapacity: 8,
      dispatchCapacityNow: 0,
      dispatchableDailyCapacity: 0,
      effectiveDailyCapacity: 15,
      limitingFactor: LIMITING_FACTORS.SCHEDULE_WINDOW_SPACING,
      governor: 'proceed',
      healthScore: 82,
    },
    cleanInventory: 3,
    targetDays: 3,
    now: preWindow,
  });
  assert.equal(plan.planningDailyCapacity, 8);
  assert.equal(plan.targetInventory, 24);
  assert.equal(plan.deficit, 21);
  assert.equal(plan.shouldReplenish, true);
  assert.equal(plan.dispatchCapacityNow, 0);
});

test('governor halt zeroes planning and dispatch-now capacity', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 12 },
      governor: { outcome: 'pause', halt: true, reason: 'Reputation risk too high.' },
      health: { score: 30 },
    },
    policy: PRODUCTION_GRANT_POLICY,
    now: new Date('2026-09-28T15:00:00.000Z'),
  });
  assert.equal(operating.planningDailyCapacity, 0);
  assert.equal(operating.dispatchCapacityNow, 0);
});

test('production policy binds Max demand on schedule before authorization headroom', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 16, statement: 'Based on today\'s reputation, I recommend 16.' },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 82 },
    },
    policy: { dailyCap: 15, totalCap: 100 },
    now: new Date('2026-09-28T15:00:00.000Z'),
    schedule: {
      allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
      minSpacingMinutes: 60,
    },
  });
  assert.equal(operating.recommendedSafeDailyCapacity, 16);
  assert.equal(operating.authorizationLimitedCapacity, 15);
  assert.equal(operating.scheduleLimitedCapacity, 8);
  assert.equal(operating.dispatchCapacityNow, 8);
  assert.equal(operating.planningDailyCapacity, 8);
  assert.equal(operating.dispatchableDailyCapacity, 8);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.SCHEDULE_WINDOW_SPACING);
});

test('Emmett recommended capacity is independent of authorization daily cap', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 12, statement: 'Based on today\'s reputation, I recommend 12.' },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 82 },
      snapshot: { providerCeiling: 50, warmup: { dailyCap: 35, status: 'healthy' } },
    },
    policy: { dailyCap: 5, totalCap: 100 },
    sentToday: 0,
    totalAttempted: 10,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });

  assert.equal(operating.recommendedSafeDailyCapacity, 12);
  assert.equal(operating.authorizationLimitedCapacity, 5);
  assert.equal(operating.dispatchableDailyCapacity, 5);
  assert.equal(operating.effectiveDailyCapacity, 5);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.AUTHORIZATION_DAILY_CAP);
  assert.equal(operating.governor, 'proceed');
  assert.equal(operating.healthScore, 82);
  assert.equal(operating.silentCap, false);
  assert.match(operating.capacityReason, /authorization limits daily sends to 5/i);
});

test('when authorization is higher, deliverability is the visible limiter', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 12 },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 88 },
      snapshot: { providerCeiling: 50 },
    },
    policy: { dailyCap: 20, totalCap: 100 },
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.recommendedSafeDailyCapacity, 12);
  assert.equal(operating.dispatchableDailyCapacity, 12);
  assert.equal(operating.effectiveDailyCapacity, 12);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.DELIVERABILITY);
});

test('remaining total authorization can bind effective capacity', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 12 },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 80 },
    },
    policy: { dailyCap: 20, totalCap: 100 },
    totalAttempted: 97,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.dispatchableDailyCapacity, 3);
  assert.equal(operating.effectiveDailyCapacity, 3);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.AUTHORIZATION_REMAINING_TOTAL);
});

test('governor halt zeroes both recommended and effective capacity', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 12 },
      governor: { outcome: 'pause', halt: true, reason: 'Reputation risk too high.' },
      health: { score: 30 },
    },
    policy: { dailyCap: 20, totalCap: 100 },
  });
  assert.equal(operating.recommendedSafeDailyCapacity, 0);
  assert.equal(operating.effectiveDailyCapacity, 0);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.GOVERNOR_HALT);
});

test('buyer-readiness UNKNOWN does not block cold outbound eligibility', () => {
  const eligible = evaluateColdOutboundEligibility({
    businessFit: 'qualified',
    geography: 'in_scope',
    contactVerified: true,
    dnc: false,
    suppression: null,
    ownership: 'clear',
    buyerReadiness: 'unknown',
  });
  assert.equal(eligible.eligible, true);
  assert.equal(eligible.buyerReadiness, 'unknown');
  assert.equal(eligible.prioritizationOnly, true);
});

test('DNC, suppression, and unverified contact remain fail-closed', () => {
  assert.equal(evaluateColdOutboundEligibility({
    businessFit: 'qualified',
    geography: 'in_scope',
    contactVerified: true,
    dnc: true,
    ownership: 'clear',
  }).eligible, false);
  assert.equal(evaluateColdOutboundEligibility({
    businessFit: 'qualified',
    geography: 'in_scope',
    contactVerified: false,
    ownership: 'clear',
  }).eligible, false);
  assert.equal(evaluateColdOutboundEligibility({
    businessFit: 'qualified',
    geography: 'in_scope',
    contactVerified: true,
    suppression: 'already_attempted',
    ownership: 'clear',
  }).reason, 'already_attempted');
});
