'use strict';

/**
 * MAX-P0-003 — the real canonical outbound adapter (executeOutboundBundle)
 * against the real PostgreSQL external-action ledger.
 *
 * Only the bundle builder is replaced (a frozen, approved bundle), so the test
 * exercises the adapter's own loop: claim before provider, dispatch marker,
 * replay, duplicate rejection, outcome-unknown blocking and legacy
 * attempted-without-outcome blocking. The provider is a counting stub.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');

const outboundPath = require.resolve('../packages/acquisition-mission/OutboundExecution');
const realOutbound = require(outboundPath);
let currentBundle = null;
require.cache[outboundPath].exports = {
  ...realOutbound,
  buildExecutionBundle: () => ({ ok: true, bundle: JSON.parse(JSON.stringify(currentBundle)) }),
};
const { executeOutboundBundle } = require('../packages/max/workspace/OutboundExecutionAdapter');
const { FIXTURE_CANONICAL_SENDER } = require('../packages/max/workspace/EmmettCapacityExecution');
const { createExternalActionLedger, STATES } = require('../services/maxReliability/externalActionLedger');

const { EXECUTION_RECORD_STATUS } = realOutbound;
const MIGRATION = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');

function bundleFor(missionId, revision = 'rev-1', bodySuffix = '') {
  return {
    missionId,
    tenantId: '10',
    provider: { provider: 'brevo', senderIdentity: 'hello@example.com', senderName: 'Example Sender', sendingDomain: 'example.com' },
    executionApproval: { preparedArtifactRevision: revision, contributionId: 'contrib-approval-1' },
    sends: [
      { prospectId: 'p-1', email: 'Owner@Example.test', toName: 'Owner', queuePosition: 1, status: 'queued', message: { subject: 'Hello', body: 'Body one' + bodySuffix } },
      { prospectId: 'p-2', email: 'ops@example.test', toName: 'Ops', queuePosition: 2, status: 'queued', message: { subject: 'Hello', body: 'Body two' + bodySuffix } },
    ],
  };
}

function run(ledger, sendEmail, extra = {}) {
  return executeOutboundBundle({
    mission: { id: currentBundle.missionId, tenantId: '10' },
    approval: { payload: {} },
    tenantId: '10',
    transactionId: 'tme_test',
    canonicalSender: FIXTURE_CANONICAL_SENDER,
    senderReadiness: { ready: true },
    externalActionLedger: ledger,
    sendEmail,
    ...extra,
  });
}

let fixture;
test.before(async () => {
  const instance = await startDisposablePostgres('max-p0-003-adapter-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 6 });
  await pool.query(fs.readFileSync(MIGRATION, 'utf8'));
  fixture = { instance, pool };
});
test.after(() => closePostgresFixture(fixture.pool, fixture.instance));

test('concurrent executions of one approved bundle send each message exactly once', { timeout: 60000 }, async () => {
  currentBundle = bundleFor('m-adapter-concurrent');
  const ledger = createExternalActionLedger(fixture.pool);
  const calls = [];
  const sendEmail = async (command) => {
    calls.push(command.toEmail);
    await new Promise((r) => setTimeout(r, 50));
    return { success: true, providerMessageId: `mid-${calls.length}` };
  };
  const results = await Promise.all(Array.from({ length: 5 }, () => run(ledger, sendEmail)));
  assert.deepEqual(calls.sort(), ['Owner@Example.test', 'ops@example.test']);
  const sent = results.flatMap((r) => r.records).filter((r) => r.status === EXECUTION_RECORD_STATUS.SENT && !r.deduplicated);
  assert.equal(sent.length, 2);
  for (const r of results.flatMap((x) => x.records).filter((x) => x.status === EXECUTION_RECORD_STATUS.BLOCKED)) {
    assert.ok(['external_action_in_progress', 'external_action_outcome_unknown'].includes(r.providerErrorCode), r.providerErrorCode);
  }

  // A later replay of the same bundle returns the original receipts, no provider call.
  const replay = await run(ledger, sendEmail);
  assert.equal(calls.length, 2);
  assert.ok(replay.records.every((r) => r.status === EXECUTION_RECORD_STATUS.SENT && r.deduplicated === true));
  assert.ok(replay.records.every((r) => /^mid-/.test(r.providerMessageId)), 'original provider ids restored');
});

test('provider exception leaves outcome unknown; the next run does not resend it', { timeout: 60000 }, async () => {
  currentBundle = bundleFor('m-adapter-unknown');
  const ledger = createExternalActionLedger(fixture.pool);
  const persisted = [];
  let calls = 0;
  await assert.rejects(run(ledger, async () => { calls += 1; throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }); },
    { persistExecutionRecord: async (r) => persisted.push(r) }), { code: 'ECONNRESET' });
  assert.equal(calls, 1);
  assert.equal(persisted[0].status, EXECUTION_RECORD_STATUS.ATTEMPTED, 'attempted record persisted before the provider call');

  const second = await run(ledger, async () => { calls += 1; return { success: true, providerMessageId: 'x' }; });
  const first = second.records.find((r) => r.prospectId === 'p-1');
  assert.equal(first.status, EXECUTION_RECORD_STATUS.BLOCKED);
  assert.equal(first.providerErrorCode, 'external_action_outcome_unknown');
  assert.equal(second.records.find((r) => r.prospectId === 'p-2').status, EXECUTION_RECORD_STATUS.SENT, 'other sends unaffected');
  assert.equal(calls, 2, 'p-1 was never re-sent');
  const receipt = await ledger.get('10', `amo-outbound:${first.executionIdentity}`);
  assert.equal(receipt.state, STATES.OUTCOME_UNKNOWN);
});

test('missing ledger schema blocks every send (fail closed)', { timeout: 60000 }, async () => {
  currentBundle = bundleFor('m-adapter-noschema');
  await fixture.pool.query('CREATE SCHEMA IF NOT EXISTS adapter_no_migration');
  const bare = createTrackedPostgresPool({ connectionString: fixture.instance.connectionString, max: 2, options: '-c search_path=adapter_no_migration' });
  try {
    let calls = 0;
    const result = await run(createExternalActionLedger(bare), async () => { calls += 1; return { success: true }; });
    assert.equal(calls, 0);
    assert.ok(result.records.every((r) => r.status === EXECUTION_RECORD_STATUS.BLOCKED && r.providerErrorCode === 'reliability_schema_missing'));
  } finally {
    await bare.end();
  }
});

test('pre-ledger attempted record without an outcome is never re-sent', { timeout: 60000 }, async () => {
  currentBundle = bundleFor('m-adapter-legacy');
  const identity = realOutbound.ensureExecutionIdentityFields({ missionId: 'm-adapter-legacy', prospectId: 'p-1', preparedArtifactRevision: 'rev-1' });
  const legacyAttempt = realOutbound.buildExecutionRecord({
    missionId: 'm-adapter-legacy', tenantId: '10', prospectId: 'p-1', preparedArtifactRevision: 'rev-1',
    status: EXECUTION_RECORD_STATUS.ATTEMPTED, executionIdentity: identity.executionIdentity, idempotencyKey: identity.idempotencyKey,
  });
  let calls = 0;
  const result = await run(null, async () => { calls += 1; return { success: true, providerMessageId: 'y' }; }, { existingRecords: [legacyAttempt] });
  assert.equal(result.records.find((r) => r.prospectId === 'p-1').providerErrorCode, 'external_action_outcome_unknown');
  assert.equal(calls, 1, 'only p-2 was sent');
});

test('R7: a revised message cannot bypass a prior dispatch to the same prospect in the same mission', { timeout: 60000 }, async () => {
  const ledger = createExternalActionLedger(fixture.pool);
  let calls = 0;
  const ok = async () => { calls += 1; return { success: true, providerMessageId: `r7-${calls}` }; };
  currentBundle = bundleFor('m-r7-sent', 'rev-1');
  await run(ledger, ok);
  assert.equal(calls, 2);
  // Paige revises the copy: new revision → new execution identity → new ledger key.
  currentBundle = bundleFor('m-r7-sent', 'rev-2', ' (revised)');
  const revised = await run(ledger, ok);
  assert.equal(calls, 2, 'no second send to either prospect');
  assert.ok(revised.records.every((r) => r.status === EXECUTION_RECORD_STATUS.BLOCKED
    && r.providerErrorCode === 'outreach_intent_already_dispatched'), JSON.stringify(revised.records.map((r) => r.providerErrorCode)));
});

test('R7: an unresolved (unknown) prior send also blocks a revision', { timeout: 60000 }, async () => {
  const ledger = createExternalActionLedger(fixture.pool);
  currentBundle = bundleFor('m-r7-unknown', 'rev-1');
  await assert.rejects(run(ledger, async () => { throw Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' }); }), { code: 'ETIMEDOUT' });
  currentBundle = bundleFor('m-r7-unknown', 'rev-2', ' (revised)');
  let calls = 0;
  const revised = await run(ledger, async () => { calls += 1; return { success: true, providerMessageId: 'x' }; });
  const p1 = revised.records.find((r) => r.prospectId === 'p-1');
  assert.equal(p1.providerErrorCode, 'outreach_intent_already_dispatched');
  assert.equal(calls, 1, 'only p-2 (never attempted) was sent');
});

test('R7: a revision after a definitive failure (nothing delivered) may be sent', { timeout: 60000 }, async () => {
  const ledger = createExternalActionLedger(fixture.pool);
  currentBundle = bundleFor('m-r7-definitive', 'rev-1');
  let calls = 0;
  await run(ledger, async () => { calls += 1; return { success: false, providerErrorCode: 'brevo_http_400', providerBoundaryCrossed: true }; });
  assert.equal(calls, 2);
  currentBundle = bundleFor('m-r7-definitive', 'rev-2', ' (fixed)');
  const fixed = await run(ledger, async () => { calls += 1; return { success: true, providerMessageId: `ok-${calls}` }; });
  assert.equal(calls, 4);
  assert.ok(fixed.records.every((r) => r.status === EXECUTION_RECORD_STATUS.SENT));
});

test('R7: pre-ledger records — a SENT record under another revision blocks the revision', { timeout: 60000 }, async () => {
  currentBundle = bundleFor('m-r7-legacy', 'rev-2', ' (revised)');
  const prior = realOutbound.buildExecutionRecord({ missionId: 'm-r7-legacy', tenantId: '10', prospectId: 'p-1',
    preparedArtifactRevision: 'rev-1', status: EXECUTION_RECORD_STATUS.SENT, providerMessageId: 'old' });
  let calls = 0;
  const result = await run(null, async () => { calls += 1; return { success: true, providerMessageId: 'y' }; }, { existingRecords: [prior] });
  assert.equal(result.records.find((r) => r.prospectId === 'p-1').providerErrorCode, 'outreach_intent_already_dispatched');
  assert.equal(calls, 1);
});

test('R7: an abandoned claim (marker write failed) leaves an ATTEMPTED record that does not block a revision', { timeout: 60000 }, async () => {
  const real = createExternalActionLedger(fixture.pool);
  let failMarker = true;
  const ledger = { ...real, markDispatchAttempted: async (...a) => {
    if (failMarker) { failMarker = false; throw Object.assign(new Error('marker write failed'), { code: 'ECONNRESET' }); }
    return real.markDispatchAttempted(...a);
  } };
  currentBundle = bundleFor('m-r7-abandoned', 'rev-1');
  const persisted = [];
  let calls = 0;
  await assert.rejects(run(ledger, async () => { calls += 1; return { success: true }; },
    { persistExecutionRecord: async (r) => persisted.push(r) }), /marker write failed/);
  assert.equal(calls, 0, 'nothing was sent');
  assert.ok(persisted.some((r) => r.status === EXECUTION_RECORD_STATUS.ATTEMPTED && r.payload.externalActionLedger === true));
  currentBundle = bundleFor('m-r7-abandoned', 'rev-2', ' (revised)');
  const revised = await run(real, async () => { calls += 1; return { success: true, providerMessageId: `ab-${calls}` }; },
    { existingRecords: persisted });
  assert.equal(revised.records.find((r) => r.prospectId === 'p-1').status, EXECUTION_RECORD_STATUS.SENT,
    'abandoned (never dispatched) does not block the revision');
});

test('R7: a SENT record with no execution identity blocks (treated as another revision)', { timeout: 60000 }, async () => {
  currentBundle = bundleFor('m-r7-noid', 'rev-1');
  const legacy = { prospectId: 'p-1', status: EXECUTION_RECORD_STATUS.SENT, executionIdentity: null, payload: {} };
  let calls = 0;
  const result = await run(null, async () => { calls += 1; return { success: true, providerMessageId: 'z' }; }, { existingRecords: [legacy] });
  assert.equal(result.records.find((r) => r.prospectId === 'p-1').providerErrorCode, 'outreach_intent_already_dispatched');
  assert.equal(calls, 1);
});
