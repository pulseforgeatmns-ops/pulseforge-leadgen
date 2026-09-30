'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  evaluatePreparationRefill,
  remainingDispatchCapacity,
  remainingScheduleSlots,
  selectRefillEntries,
  selectInventoryRefillEntries,
  finalizePreparationObservability,
  PREPARATION_BATCH_LIMIT,
} = require('../services/governedOutboundRefill');

test('finalizePreparationObservability requires a terminal reason when prepare was requested but nothing added', () => {
  assert.deepEqual(
    finalizePreparationObservability({ prepareRequested: 1, preparedAdded: 0, prepareSkippedReason: null }),
    { prepareRequested: 1, preparedAdded: 0, prepareSkippedReason: 'preparation_not_executed' },
  );
  assert.deepEqual(
    finalizePreparationObservability({ prepareRequested: 1, preparedAdded: 1, prepareSkippedReason: null }),
    { prepareRequested: 1, preparedAdded: 1, prepareSkippedReason: null },
  );
  assert.deepEqual(
    finalizePreparationObservability({ prepareRequested: 1, preparedAdded: 0, prepareSkippedReason: 'no_clean_inventory' }),
    { prepareRequested: 1, preparedAdded: 0, prepareSkippedReason: 'no_clean_inventory' },
  );
});

test('remaining dispatch capacity is sent-today subtracted from dispatchCapacityNow', () => {
  assert.equal(remainingDispatchCapacity({ dispatchCapacityNow: 8, sentToday: 5 }), 3);
  assert.equal(remainingDispatchCapacity({ dispatchCapacityNow: 8, sentToday: 8 }), 0);
});

test('remaining schedule slots count future window slots after last send + spacing', () => {
  const slots = remainingScheduleSlots({
    now: new Date('2026-09-28T18:30:00.000Z'), // 14:30 ET
    lastSendAt: new Date('2026-09-28T17:20:58.000Z'), // 13:20 ET
    allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
    minSpacingMinutes: 60,
  });
  assert.equal(slots, 3);
  const afterClose = remainingScheduleSlots({
    now: new Date('2026-09-28T21:05:00.000Z'), // 17:05 ET
    lastSendAt: new Date('2026-09-28T17:20:58.000Z'),
    allowedSendWindow: { startHour: 9, endHour: 17, timezone: 'America/New_York' },
    minSpacingMinutes: 60,
  });
  assert.equal(afterClose, 0);
});

test('refill trigger keeps bounded batches and skips when inventory or slots are gone', () => {
  const exhaustedBatch = evaluatePreparationRefill({
    pendingPreparedCount: 0,
    remainingDispatchCapacity: 3,
    remainingScheduleSlots: 3,
    cleanInventory: 3,
    governor: 'proceed',
    grantActive: true,
    dailyAuthorizationRemaining: 10,
    totalAuthorizationRemaining: 90,
  });
  assert.equal(exhaustedBatch.shouldPrepare, true);
  assert.equal(exhaustedBatch.prepareRequested, 3);

  const morningBatchFull = evaluatePreparationRefill({
    pendingPreparedCount: 5,
    remainingDispatchCapacity: 8,
    remainingScheduleSlots: 8,
    cleanInventory: 20,
    governor: 'proceed',
    grantActive: true,
    dailyAuthorizationRemaining: 15,
    totalAuthorizationRemaining: 90,
    batchLimit: PREPARATION_BATCH_LIMIT,
  });
  assert.equal(morningBatchFull.shouldPrepare, false);
  assert.equal(morningBatchFull.prepareSkippedReason, 'batch_limit_reached');

  const noInventory = evaluatePreparationRefill({
    pendingPreparedCount: 0,
    remainingDispatchCapacity: 3,
    remainingScheduleSlots: 3,
    cleanInventory: 0,
    governor: 'proceed',
    grantActive: true,
    dailyAuthorizationRemaining: 10,
    totalAuthorizationRemaining: 90,
  });
  assert.equal(noInventory.shouldPrepare, false);
  assert.equal(noInventory.prepareSkippedReason, 'no_clean_inventory');

  const noSlots = evaluatePreparationRefill({
    pendingPreparedCount: 0,
    remainingDispatchCapacity: 3,
    remainingScheduleSlots: 0,
    cleanInventory: 3,
    governor: 'proceed',
    grantActive: true,
    dailyAuthorizationRemaining: 10,
    totalAuthorizationRemaining: 90,
  });
  assert.equal(noSlots.shouldPrepare, false);
  assert.equal(noSlots.prepareSkippedReason, 'no_remaining_slots');

  const halted = evaluatePreparationRefill({
    pendingPreparedCount: 0,
    remainingDispatchCapacity: 3,
    remainingScheduleSlots: 3,
    cleanInventory: 3,
    governor: 'halt',
    grantActive: true,
    dailyAuthorizationRemaining: 10,
    totalAuthorizationRemaining: 90,
  });
  assert.equal(halted.prepareSkippedReason, 'governor_halt');
});

test('selectRefillEntries takes leftover prepared candidates not already in the envelope', async () => {
  const selected = await selectRefillEntries({
    prepared: {
      revision: 'rev-2',
      sender: { senderEmail: 'sender@anchor.example' },
      candidates: [
        { candidateId: 'c0', item: { email: 'ops0@customer.example', sendable: true, paige: { candidateId: 'c0' } },
          message: { subject: 'Cleaning 0', body: 'Would a written quote help?', candidateId: 'c0' } },
        { candidateId: 'c5', item: { email: 'ops5@customer.example', sendable: true, paige: { candidateId: 'c5' } },
          message: { subject: 'Cleaning 5', body: 'Would a written quote help?', candidateId: 'c5' } },
      ],
    },
    program: { policy: { dailyCap: 15 } },
    store: { suppression: async () => null },
    adapters: {
      contact: async (id) => ({
        prospect_id: id,
        id,
        company_id: `co-${id}`,
        email: `ops${id.slice(1)}@customer.example`,
        email_verified: true,
        email_status: 'valid',
        enrichment_provenance: { email: { source: 'website_email' } },
        do_not_contact: false,
      }),
    },
    existingItems: [
      { candidate_id: 'c0', prospect_id: 'c0', company_id: 'co-c0', email: 'ops0@customer.example', status: 'sent' },
    ],
    limit: 3,
  });
  assert.equal(selected.length, 1);
  assert.equal(selected[0].candidateId, 'c5');
  assert.equal(selected[0].email, 'ops5@customer.example');
});

test('inventory refill prepares remaining clean first-touch records not in the original batch', async () => {
  const selected = await selectInventoryRefillEntries({
    cleanRows: [
      {
        candidateId: 'c0',
        prospectId: 'c0',
        companyId: 'co-c0',
        company: 'Harbor Law',
        email: 'ops0@customer.example',
      },
      {
        candidateId: 'c6',
        prospectId: 'c6',
        companyId: 'co-c6',
        company: 'Granite PM',
        email: 'ops6@customer.example',
      },
    ],
    store: { suppression: async () => null },
    adapters: {
      contact: async (id) => ({
        prospect_id: id,
        id,
        company_id: `co-${id}`,
        company_name: id === 'c6' ? 'Granite PM' : 'Harbor Law',
        email: `ops${id.slice(1)}@customer.example`,
        email_verified: true,
        email_status: 'valid',
        enrichment_provenance: { email: { source: 'website_email' } },
        do_not_contact: false,
        first_name: 'Alex',
      }),
    },
    prepared: {
      revision: 'rev-1',
      sender: { senderEmail: 'sender@anchor.example', senderName: 'Jacob Maynard' },
    },
    existingItems: [
      { candidate_id: 'c0', prospect_id: 'c0', company_id: 'co-c0', email: 'ops0@customer.example', status: 'sent' },
    ],
    limit: 3,
  });
  assert.equal(selected.length, 1);
  assert.equal(selected[0].candidateId, 'c6');
  assert.equal(selected[0].refill, true);
  assert.ok(selected[0].message?.subject);
  assert.ok(selected[0].message?.body);
  assert.equal(selected[0].message.candidateId, 'c6');
});
