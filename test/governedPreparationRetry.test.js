'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { preparationRetryReason } = require('../services/governedPreparationRetry');
const { windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { evaluatePreparationRefill } = require('../services/governedOutboundRefill');

test('new eligible inventory resumes after three attempts without hot-looping', () => {
  const progress = { attempts: 3, last_error: 'verified_inventory_shortfall', last_attempt_at: '2026-10-02T14:00:00Z' };
  assert.equal(preparationRetryReason(progress, 'old', 'new', new Date('2026-10-02T14:04:59Z')), 'preparation_backoff');
  assert.equal(preparationRetryReason(progress, 'old', 'new', new Date('2026-10-02T14:05:00Z')), null);
  assert.equal(preparationRetryReason(progress, 'old', 'old', new Date('2026-10-02T14:59:59Z')), 'preparation_backoff');
  assert.equal(preparationRetryReason({ ...progress, attempts: 100 }, 'old', 'old', new Date('2026-10-02T15:00:00Z')), null);
  assert.equal(preparationRetryReason({ ...progress, last_error: null }, 'old', 'old', new Date('2026-10-02T14:05:00Z')), null);
});
test('weekend preparation leaves dispatch window and expiry fail-closed', () => {
  const p = { startsAt: '2026-10-01T00:00:00Z', expiresAt: '2026-10-06T00:00:00Z', weekdays: [1,2,3,4,5], startHour:9,endHour:17 };
  const saturday = new Date('2026-10-03T16:00:00Z');
  assert.equal(windowReason(p, saturday, false), null);
  assert.equal(windowReason(p, saturday, true), 'weekend');
  assert.equal(windowReason(p, new Date('2026-10-05T22:00:00Z'), true), 'outside_business_hours');
  assert.equal(windowReason(p, new Date('2026-10-06T01:00:00Z'), false), 'authorization_expired');
});
test('held preparation uses planning capacity and preserves authorization/governor limits', () => {
  const input = { cleanInventory: 9, planningDailyCapacity: 8, remainingDispatchCapacity:0,
    remainingScheduleSlots:0, dailyAuthorizationRemaining:15,totalAuthorizationRemaining:86 };
  assert.equal(evaluatePreparationRefill(input).prepareRequested, 5);
  assert.equal(evaluatePreparationRefill({ ...input, totalAuthorizationRemaining:2 }).prepareRequested,2);
  assert.equal(evaluatePreparationRefill({ ...input, governor:'halt' }).shouldPrepare,false);
  assert.equal(evaluatePreparationRefill({ ...input, grantActive:false }).shouldPrepare,false);
});
