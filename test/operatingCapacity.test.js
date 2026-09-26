'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  assessOperatingCapacity,
  computeScheduleLimitedCapacity,
  LIMITING_FACTORS,
} = require('../packages/emmett-outbound/OperatingCapacity');
const { evaluateColdOutboundEligibility } = require('../services/outboundInventory');

test('weekday 9–17 window with 60-minute spacing yields eight dispatchable slots', () => {
  assert.equal(computeScheduleLimitedCapacity({
    allowedSendWindow: { startHour: 9, endHour: 17 },
    minSpacingMinutes: 60,
  }), 8);
});

test('production policy binds Max demand on schedule before authorization headroom', () => {
  const operating = assessOperatingCapacity({
    assessed: {
      capacity: { recommended: 16, statement: 'Based on today\'s reputation, I recommend 16.' },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 82 },
    },
    policy: { dailyCap: 15, totalCap: 100 },
    schedule: {
      allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
      minSpacingMinutes: 60,
    },
  });
  assert.equal(operating.recommendedSafeDailyCapacity, 16);
  assert.equal(operating.authorizationLimitedCapacity, 15);
  assert.equal(operating.scheduleLimitedCapacity, 8);
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
