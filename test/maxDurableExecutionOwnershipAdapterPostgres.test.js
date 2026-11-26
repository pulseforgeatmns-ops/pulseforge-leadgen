'use strict';

/**
 * MAX-P0-004 — the real canonical outbound adapter (executeOutboundBundle)
 * with a durable task ownership binding, against real PostgreSQL with both
 * the P0-003 and P0-004 migrations. Only the bundle builder is replaced; the
 * provider is a counting stub.
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
const { createExternalActionLedger } = require('../services/maxReliability/externalActionLedger');
const { createDurableExecutionOwnership, STATES } = require('../services/maxReliability/durableExecutionOwnership');

const { EXECUTION_RECORD_STATUS } = realOutbound;
const P003 = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const P004 = path.join(__dirname, '../migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql');
const TENANT = '10';

function bundleFor(missionId) {
  return {
    missionId,
    tenantId: TENANT,
    provider: { provider: 'brevo', senderIdentity: 'hello@example.com', senderName: 'Example Sender', sendingDomain: 'example.com' },
    executionApproval: { preparedArtifactRevision: 'rev-1', contributionId: 'contrib-approval-1' },
    sends: [
      { prospectId: 'p-1', email: 'owner@example.test', toName: 'Owner', queuePosition: 1, status: 'queued', message: { subject: 'Hello', body: 'Body one' } },
      { prospectId: 'p-2', email: 'ops@example.test', toName: 'Ops', queuePosition: 2, status: 'queued', message: { subject: 'Hello', body: 'Body two' } },
    ],
  };
}
function run(extra) {
  return executeOutboundBundle({
    mission: { id: currentBundle.missionId, tenantId: TENANT },
    approval: { payload: {} },
    tenantId: TENANT,
    transactionId: 'tme_test',
    canonicalSender: FIXTURE_CANONICAL_SENDER,
    senderReadiness: { ready: true },
    ...extra,
  });
}

let fixture; let n = 0;
test.before(async () => {
  const instance = await startDisposablePostgres('max-p0-004-adapter-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 6 });
  await pool.query(fs.readFileSync(P003, 'utf8'));
  await pool.query(fs.readFileSync(P004, 'utf8'));
  fixture = { instance, pool };
});
test.after(() => closePostgresFixture(fixture.pool, fixture.instance));

async function ownedTask(store) {
  n += 1;
  const accepted = await store.accept({ tenantId: TENANT, logicalKey: `adapter-${process.pid}-${n}`, specialist: 'emmett', missionId: `m-p004-${n}`, immutableInput: { n } });
  const claimed = await store.claim({ tenantId: TENANT, ownerToken: `worker-${n}`, leaseMs: 30000 });
  assert.equal(claimed.id, accepted.task.id);
  await store.start({ tenantId: TENANT, taskId: claimed.id, ownershipToken: `worker-${n}`, fenceEpoch: claimed.fenceEpoch });
  return { task: claimed, binding: { store, tenantId: TENANT, taskId: claimed.id, ownershipToken: `worker-${n}`, fenceEpoch: claimed.fenceEpoch } };
}

test('a current task owner sends once; claims are task-linked and completion needs evidence', { timeout: 60000 }, async () => {
  const store = createDurableExecutionOwnership(fixture.pool); const ledger = createExternalActionLedger(fixture.pool);
  const { task, binding } = await ownedTask(store); currentBundle = bundleFor(task.missionId);
  let calls = 0;
  const sendEmail = async () => { calls += 1; return { success: true, providerMessageId: `mid-${calls}` }; };
  const result = await run({ externalActionLedger: ledger, sendEmail, durableTaskOwnership: binding });
  assert.equal(calls, 2);
  assert.ok(result.records.every(r => r.status === EXECUTION_RECORD_STATUS.SENT));
  const linked = await fixture.pool.query('SELECT state FROM max_external_action_claims WHERE tenant_id=$1 AND task_id=$2 ORDER BY state', [TENANT, task.id]);
  assert.deepEqual(linked.rows.map(r => r.state), ['provider_acknowledged', 'provider_acknowledged']);

  // Specialist completion is not completion: verification needs evidence and no unresolved claim.
  await store.reportCompletion({ ...binding, resultRef: { sends: 2 }, evidenceRefs: [] });
  const verified = await store.verify({ tenantId: TENANT, taskId: task.id, evidenceRefs: result.records.map(r => ({ providerMessageId: r.providerMessageId })) });
  assert.equal(verified.state, STATES.COMPLETED);
});

test('a superseded task owner reaches no provider and leaves no dispatched claim', { timeout: 60000 }, async () => {
  const store = createDurableExecutionOwnership(fixture.pool); const ledger = createExternalActionLedger(fixture.pool);
  const { task, binding } = await ownedTask(store); currentBundle = bundleFor(task.missionId);
  await store.cancel({ tenantId: TENANT, taskId: task.id, reason: 'operator stop' });
  let calls = 0;
  await assert.rejects(run({ externalActionLedger: ledger, sendEmail: async () => { calls += 1; return { success: true, providerMessageId: 'x' }; }, durableTaskOwnership: binding }), { code: 'durable_task_fenced' });
  assert.equal(calls, 0);
  const states = await fixture.pool.query('SELECT state FROM max_external_action_claims WHERE tenant_id=$1 AND task_id=$2', [TENANT, task.id]);
  assert.ok(states.rows.every(r => r.state === 'abandoned_not_dispatched'), JSON.stringify(states.rows));

  // A binding whose fence is valid for the store check but forged for the marker
  // (wrong epoch) is refused by the marker itself.
  const second = await ownedTask(store); currentBundle = bundleFor(second.task.missionId);
  const forged = { ...second.binding, store: { requireOwner: async () => second.task }, fenceEpoch: second.binding.fenceEpoch + 7 };
  await assert.rejects(run({ externalActionLedger: ledger, sendEmail: async () => { calls += 1; return { success: true, providerMessageId: 'y' }; }, durableTaskOwnership: forged }), { code: 'durable_task_fenced' });
  assert.equal(calls, 0);
});

test('durable task execution fails closed without the external-action ledger', { timeout: 60000 }, async () => {
  const store = createDurableExecutionOwnership(fixture.pool);
  const { task, binding } = await ownedTask(store); currentBundle = bundleFor(task.missionId);
  let calls = 0;
  const result = await run({ sendEmail: async () => { calls += 1; return { success: true, providerMessageId: 'z' }; }, durableTaskOwnership: binding });
  assert.equal(calls, 0);
  assert.ok(result.records.every(r => r.status === EXECUTION_RECORD_STATUS.BLOCKED && r.providerErrorCode === 'external_action_ledger_unavailable'));
});

test('inside a task-owned execution the adapter task-links and fences sends from context alone (no binding passed)', { timeout: 60000 }, async () => {
  const { runWithTaskFence } = require('../services/maxReliability/taskExecutionContext');
  const store = createDurableExecutionOwnership(fixture.pool); const ledger = createExternalActionLedger(fixture.pool);
  const { task, binding } = await ownedTask(store); currentBundle = bundleFor(task.missionId);
  const fence = { tenantId: TENANT, taskId: binding.taskId, ownershipToken: binding.ownershipToken, fenceEpoch: binding.fenceEpoch };
  let calls = 0;
  const result = await runWithTaskFence(fence, () => run({ externalActionLedger: ledger, sendEmail: async () => { calls += 1; return { success: true, providerMessageId: `ctx-${calls}` }; } }));
  assert.equal(calls, 2);
  assert.ok(result.records.every((r) => r.status === EXECUTION_RECORD_STATUS.SENT));
  const linked = await fixture.pool.query('SELECT count(*)::int AS n FROM max_external_action_claims WHERE tenant_id=$1 AND task_id=$2', [TENANT, task.id]);
  assert.equal(linked.rows[0].n, 2, 'claims linked to the context task');

  // A stale context fence (cancelled task) reaches no provider.
  const second = await ownedTask(store); currentBundle = bundleFor(second.task.missionId);
  await store.cancel({ tenantId: TENANT, taskId: second.task.id, reason: 'stop' });
  const staleFence = { tenantId: TENANT, taskId: second.binding.taskId, ownershipToken: second.binding.ownershipToken, fenceEpoch: second.binding.fenceEpoch };
  await assert.rejects(runWithTaskFence(staleFence, () => run({ externalActionLedger: ledger, sendEmail: async () => { calls += 1; return { success: true, providerMessageId: 'never' }; } })),
    { code: 'durable_task_fenced' });
  assert.equal(calls, 2, 'no provider call under a stale fence');
  // A fence of another tenant is refused before anything is claimed.
  await assert.rejects(runWithTaskFence({ ...fence, tenantId: '99' }, () => run({ externalActionLedger: ledger, sendEmail: async () => { calls += 1; } })),
    { code: 'durable_task_fenced' });
  assert.equal(calls, 2);
});
