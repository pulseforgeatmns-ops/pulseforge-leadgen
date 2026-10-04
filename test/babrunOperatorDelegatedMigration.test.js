'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { policy, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const {
  resolveOperatorProgramTotalCapForDelegation,
  describeOperatorAuthorityEnvelope,
  countGrantWeekdaySlots,
} = require('../packages/emmett-outbound/OperatorDelegatedCapacity');
const { assessOperatingCapacity, LIMITING_FACTORS } = require('../packages/emmett-outbound/OperatingCapacity');

const BABRUN_BASE = {
  tenantId: '13',
  sourceMissionId: 'mission',
  senderEmail: 'hello@babrun.com',
  inboxIntegrationId: 'mailbox',
  sendingIdentityId: 'identity',
  dailyCap: 1,
  totalCap: 1,
  spacingMinutes: 240,
  startHour: 9,
  endHour: 16,
};

function babrunPolicy(extra = {}, now = new Date('2026-09-28T15:00:00Z')) {
  return policy({
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
    ...extra,
  }, now);
}

test('delegated daily max above legacy totalCap requires migrated program totalCap', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const current = {
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
  };
  assert.throws(
    () => babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 1 }, now),
    (err) => err.code === 'invalid_bounds',
  );
  const migratedTotal = resolveOperatorProgramTotalCapForDelegation(current, 20);
  assert.ok(migratedTotal >= 20);
  const envelope = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: migratedTotal }, now);
  assert.equal(envelope.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(envelope.dailyCap, 1);
  assert.ok(envelope.totalCap >= 20);
});

test('activation safety grant totalCap is replaced for delegated ramp envelope', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const current = {
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 7 * 86400000).toISOString(),
  };
  const slots = countGrantWeekdaySlots(current);
  const migratedTotal = resolveOperatorProgramTotalCapForDelegation(current, 20);
  assert.equal(migratedTotal, Math.min(100, Math.max(20, 20 * Math.max(slots, 1))));
});

test('Anchor legacy policy without delegated field is unchanged by totalCap helper', () => {
  const now = new Date('2026-09-18T14:00:00Z');
  const anchor = {
    tenantId: '10',
    sourceMissionId: 'source',
    senderEmail: 'sender@anchor.example',
    inboxIntegrationId: 'inbox',
    aoOwnerIds: [1],
    startsAt: now.toISOString(),
    expiresAt: '2026-10-01T00:00:00Z',
    dailyCap: 5,
    totalCap: 100,
  };
  const p = policy(anchor, now);
  assert.equal(resolveOperatorProgramTotalCapForDelegation(p, 20), 100);
  assert.equal(p.operatorDelegatedMaximumDailyCapacity, undefined);
});

test('Emmett 4 with operator daily 20 yields authorization-limited capacity 4', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 4 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    now: new Date('2026-09-28T15:00:00.000Z'),
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.authorizationLimitedCapacity, 4);
  assert.equal(operating.operatorDelegatedMaximumDailyCapacity, 20);
});

test('program totalCap bounds cumulative sends after prior attempts', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 25 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 20 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    totalAttempted: 24,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.remainingTotalAuthorization, 1);
  assert.equal(operating.authorizationLimitedCapacity, 1);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.AUTHORIZATION_REMAINING_TOTAL);
});

test('operator pause and expiry still fail closed', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const paused = assessOperatingCapacity({
    assessed: { capacity: { recommended: 12 }, governor: { outcome: 'pause', halt: true } },
    policy: grant,
  });
  assert.equal(paused.authorizationLimitedCapacity, 0);
  assert.equal(paused.capacityLimitingAuthority, 'governor_pause');
  assert.equal(windowReason(grant, new Date('2026-10-15T15:00:00Z')), 'authorization_expired');
});

test('migration authority envelope reports effective operator ceilings', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const before = describeOperatorAuthorityEnvelope({
    dailyCap: 1,
    totalCap: 1,
  });
  assert.equal(before.operatorDelegatedMaximumDailyCapacity, null);
  assert.equal(before.effectiveOperatorDailyCeiling, 1);

  const migratedTotal = resolveOperatorProgramTotalCapForDelegation({
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
  }, 20);
  const after = describeOperatorAuthorityEnvelope({
    dailyCap: 1,
    totalCap: migratedTotal,
    operatorDelegatedMaximumDailyCapacity: 20,
  });
  assert.equal(after.effectiveOperatorDailyCeiling, 20);
  assert.equal(after.effectiveOperatorProgramCeiling, migratedTotal);
});
