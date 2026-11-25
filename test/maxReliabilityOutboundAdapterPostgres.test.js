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

function bundleFor(missionId) {
  return {
    missionId,
    tenantId: '10',
    provider: { provider: 'brevo', senderIdentity: 'hello@example.com', senderName: 'Example Sender', sendingDomain: 'example.com' },
    executionApproval: { preparedArtifactRevision: 'rev-1', contributionId: 'contrib-approval-1' },
    sends: [
      { prospectId: 'p-1', email: 'Owner@Example.test', toName: 'Owner', queuePosition: 1, status: 'queued', message: { subject: 'Hello', body: 'Body one' } },
      { prospectId: 'p-2', email: 'ops@example.test', toName: 'Ops', queuePosition: 2, status: 'queued', message: { subject: 'Hello', body: 'Body two' } },
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
