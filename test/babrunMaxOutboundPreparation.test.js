'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  runMaxOutboundControlLoop,
  applyPreparationRefill,
  capturePreparationObservability,
} = require('../services/maxOutboundControlLoop');

const businessWindow = new Date('2026-09-29T15:00:00.000Z'); // 11:00 ET

test('tenant-13-like control cycle executes preparation refill when prepareRequested=1', async (t) => {
  const saved = process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
  process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'true';
  t.after(() => {
    if (saved === undefined) delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = saved;
  });
  const events = [];
  let refillCalled = false;
  const program = {
    id: 'outbound_babrun_test',
    mode: 'active',
    policy_hash: 'policy_hash',
    source_mission_id: 'mission_validation_test',
    policy: {
      dailyCap: 1,
      operatorDelegatedMaximumDailyCapacity: 20,
      totalCap: 2,
      startHour: 9,
      endHour: 17,
      timeZone: 'America/New_York',
      spacingMinutes: 60,
    },
  };
  const infrastructure = {
    cap: 4,
    snapshot: { sentToday: 0 },
    assessed: { governor: { outcome: 'proceed' }, health: { score: 80 } },
    operating: {
      recommendedSafeDailyCapacity: 4,
      authorizationLimitedCapacity: 4,
      scheduleLimitedCapacity: 8,
      dispatchCapacityNow: 4,
      planningDailyCapacity: 4,
      dispatchableDailyCapacity: 4,
      effectiveDailyCapacity: 4,
      governor: 'proceed',
      allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
      minSpacingMinutes: 60,
      dispatchDayAllowed: true,
    },
  };

  const result = await runMaxOutboundControlLoop({
    pool: {},
    tenantId: '13',
    now: businessWindow,
    program,
    source: { id: 'mission_validation_test', payload: {} },
    store: {
      tenantId: '13',
      clientId: 13,
      event: async (type, key, payload) => events.push({ type, payload }),
      envelope: async () => null,
      items: async () => [],
    },
    infrastructure,
    inventory: {
      clean: [{
        candidateId: 'prospect-founder-1',
        prospectId: 'prospect-founder-1',
        companyId: 'co-1',
        email: 'founder@example.com',
        contactClassification: 'VERIFIED_FOUNDER_EMAIL',
      }],
      excluded: [],
      scope: {},
      exclusionCounts: {},
    },
    inventoryAfter: {
      clean: [{
        candidateId: 'prospect-founder-1',
        prospectId: 'prospect-founder-1',
        companyId: 'co-1',
        email: 'founder@example.com',
        contactClassification: 'VERIFIED_FOUNDER_EMAIL',
      }],
      excluded: [],
      scope: {},
      exclusionCounts: {},
    },
    scoutRamp: async () => null,
    runPreparationRefill: async () => {
      refillCalled = true;
      return {
        prepareRequested: 1,
        preparedAdded: 1,
        pendingPrepared: 1,
        prepareSkippedReason: null,
      };
    },
  });

  assert.equal(refillCalled, true);
  assert.equal(result.prepareRequested, 1);
  assert.equal(result.preparedAdded, 1);
  assert.equal(result.prepareSkippedReason, null);
  assert.equal(events[0].payload.preparedAdded, 1);
});

test('execute=false exposes control_execute_disabled instead of silent zero prepare', async (t) => {
  const saved = process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
  process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'true';
  t.after(() => {
    if (saved === undefined) delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = saved;
  });
  const observed = await capturePreparationObservability({
    store: { tenantId: '13', envelope: async () => null, items: async () => [] },
    program: { mode: 'active', policy: { dailyCap: 1, startHour: 9, endHour: 17, timeZone: 'America/New_York', spacingMinutes: 60 } },
    operating: {
      dispatchCapacityNow: 4,
      governor: 'proceed',
      allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
      minSpacingMinutes: 60,
      dispatchDayAllowed: true,
      authorizationLimitedCapacity: 4,
      planningDailyCapacity: 4,
    },
    sentToday: 0,
    cleanInventory: 1,
    now: businessWindow,
  });
  assert.equal(observed.prepareRequested, 1);

  const finalized = await applyPreparationRefill({
    pool: {},
    tenantId: '13',
    preparation: observed,
    execute: false,
  });
  assert.equal(finalized.preparedAdded, 0);
  assert.equal(finalized.prepareSkippedReason, 'control_execute_disabled');
});

test('tenant-10 grant inactive skips preparation demand in observability', async (t) => {
  const saved = process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  t.after(() => {
    if (saved === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
    else process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = saved;
  });
  const observed = await capturePreparationObservability({
    store: { tenantId: '10', envelope: async () => null, items: async () => [] },
    program: { mode: 'active', policy: { dailyCap: 1, startHour: 9, endHour: 17, timeZone: 'America/New_York', spacingMinutes: 60 } },
    operating: {
      dispatchCapacityNow: 1,
      governor: 'proceed',
      allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
      minSpacingMinutes: 60,
      dispatchDayAllowed: true,
      authorizationLimitedCapacity: 1,
    },
    sentToday: 0,
    cleanInventory: 1,
    now: businessWindow,
  });
  assert.equal(observed.prepareRequested, 0);
  assert.equal(observed.prepareSkippedReason, 'grant_inactive');
});
