'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  assessOperatingCapacity,
  LIMITING_FACTORS,
} = require('../packages/emmett-outbound/OperatingCapacity');
const {
  resolveOperatorDelegatedMaximumDailyCapacity,
} = require('../packages/emmett-outbound/OperatorDelegatedCapacity');
const { buildControlPlan } = require('../services/maxOutboundControlLoop');
const { remainingDispatchCapacity } = require('../services/governedOutboundRefill');
const { policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');

const GRANT = {
  operatorDelegatedMaximumDailyCapacity: 20,
  totalCap: 100,
  spacingMinutes: 60,
  startHour: 9,
  endHour: 17,
  timeZone: 'America/New_York',
  weekdays: [1, 2, 3, 4, 5],
  startsAt: '2026-01-01T00:00:00.000Z',
  expiresAt: '2026-12-31T23:59:59.000Z',
};

function assessedWithRecommended(recommended, governor = { outcome: 'proceed', halt: false }) {
  return {
    capacity: { recommended, statement: `Emmett recommends ${recommended}.` },
    governor,
    health: { score: 80 },
  };
}

function effectiveCapacity(emmettRecommended) {
  return assessOperatingCapacity({
    assessed: assessedWithRecommended(emmettRecommended),
    policy: GRANT,
    now: new Date('2026-09-28T15:00:00.000Z'),
    schedule: {
      allowedSendWindow: { startHour: 0, endHour: 24 },
      minSpacingMinutes: 30,
    },
  });
}

test('operator max 20 + Emmett scenarios bind effective authorization capacity', () => {
  assert.equal(effectiveCapacity(1).authorizationLimitedCapacity, 1);
  assert.equal(effectiveCapacity(4).authorizationLimitedCapacity, 4);
  assert.equal(effectiveCapacity(8).authorizationLimitedCapacity, 8);
  assert.equal(effectiveCapacity(25).authorizationLimitedCapacity, 20);
  assert.equal(effectiveCapacity(0).authorizationLimitedCapacity, 0);
});

test('sent=1 + effective=4 yields remaining dispatch capacity 3', () => {
  const operating = effectiveCapacity(4);
  assert.equal(
    remainingDispatchCapacity({ dispatchCapacityNow: operating.dispatchCapacityNow, sentToday: 1 }),
    3,
  );
});

test('Emmett PAUSE and EMERGENCY zero capacity regardless of operator ceiling', () => {
  for (const outcome of ['pause', 'emergency']) {
    const operating = assessOperatingCapacity({
      assessed: assessedWithRecommended(12, { outcome, halt: true, reason: 'halt' }),
      policy: GRANT,
      schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
    });
    assert.equal(operating.authorizationLimitedCapacity, 0);
    assert.equal(operating.capacityLimitingAuthority, 'governor_pause');
  }
});

test('missing Emmett authority fails closed when required', () => {
  const operating = assessOperatingCapacity({
    assessed: {},
    policy: GRANT,
    requireEmmettAuthority: true,
  });
  assert.equal(operating.emmettAuthorityMissing, true);
  assert.equal(operating.authorizationLimitedCapacity, 0);
});

test('spacing still blocks dispatch-now even with unused daily authorization', () => {
  const evening = new Date('2026-09-28T23:30:00.000Z');
  const operating = assessOperatingCapacity({
    assessed: assessedWithRecommended(20),
    policy: { ...GRANT, startHour: 9, endHour: 17 },
    now: evening,
  });
  assert.equal(operating.dispatchCapacityNow, 0);
  assert.equal(operating.withinSendWindowNow, false);
});

test('Max buffer follows effective Emmett-bound capacity, not operator ceiling alone', () => {
  const operating = effectiveCapacity(4);
  const plan = buildControlPlan({
    operatingCapacity: operating,
    cleanInventory: 0,
    targetDays: 3,
    policy: GRANT,
  });
  assert.equal(plan.planningDailyCapacity, 4);
  assert.equal(plan.targetInventory, 12);
  assert.notEqual(plan.targetInventory, 60);
});

test('legacy dailyCap remains operator ceiling when delegated field is absent (Anchor)', () => {
  const operating = assessOperatingCapacity({
    assessed: assessedWithRecommended(16),
    policy: { dailyCap: 15, totalCap: 100 },
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.authorizationLimitedCapacity, 15);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.AUTHORIZATION_DAILY_CAP);
  assert.equal(operating.capacityLimitingAuthority, 'operator_ceiling');
});

test('tenant 13 grant accepts operatorDelegatedMaximumDailyCapacity while preserving legacy dailyCap', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const p = policy({
    tenantId: '13',
    sourceMissionId: 'mission',
    senderEmail: 'hello@babrun.com',
    inboxIntegrationId: 'mb',
    sendingIdentityId: 'identity',
    dailyCap: 1,
    totalCap: 100,
    operatorDelegatedMaximumDailyCapacity: 20,
    spacingMinutes: 240,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
  }, now);
  assert.equal(p.dailyCap, 1);
  assert.equal(p.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(resolveOperatorDelegatedMaximumDailyCapacity(p), 20);
});
