'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createDurableExecutionOwnership, STATES } = require('../services/maxReliability/durableExecutionOwnership');
const { createExternalActionLedger, requestFingerprint } = require('../services/maxReliability/externalActionLedger');
const { guardedDispatch } = require('../services/maxReliability/guardedDispatch');

const P003 = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const P004 = path.join(__dirname, '../migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql');
let fixture; let sequence = 0;
async function db() {
  if (fixture) return fixture;
  const instance = await startDisposablePostgres('max-p0-004-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 8 });
  const pool2 = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 4 });
  await pool.query(fs.readFileSync(P003, 'utf8'));
  await pool.query(fs.readFileSync(P004, 'utf8'));
  fixture = { instance, pool, pool2 }; return fixture;
}
test.after(async () => { if (fixture) { await fixture.pool2.end(); await closePostgresFixture(fixture.pool, fixture.instance); } });
function taskInput(overrides = {}) { sequence += 1; return { tenantId: 'tenant-a', logicalKey: `task-${process.pid}-${sequence}`, specialist: 'research', immutableInput: { query: 'Manchester', n: sequence }, dependencyRefs: [{ id: 'mission', version: '1' }], retryBudget: 2, ...overrides }; }
function owner(task, token) { return { tenantId: task.tenantId, taskId: task.id, ownershipToken: token, fenceEpoch: task.fenceEpoch }; }

test('accepted delegation is durable, idempotent, and survives an originating request boundary', async () => {
  const { pool, pool2 } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput();
  const first = await store.accept(input); const replay = await store.accept(input);
  assert.equal(first.accepted, true); assert.equal(replay.replay, true); assert.equal(replay.task.id, first.task.id);
  await assert.rejects(store.accept({ ...input, immutableInput: { query: 'different' } }), { code: 'durable_task_key_reused' });
  const restartedStore = createDurableExecutionOwnership(pool2);
  assert.equal((await restartedStore.get({ tenantId: input.tenantId, taskId: first.task.id })).state, STATES.QUEUED);
});

test('concurrent workers receive one fenced owner and stale owner cannot commit after takeover', async () => {
  const { pool, pool2 } = await db(); const input = taskInput(); const a = createDurableExecutionOwnership(pool); const b = createDurableExecutionOwnership(pool2);
  await a.accept(input);
  const [one, two] = await Promise.all([a.claim({ tenantId: input.tenantId, ownerToken: 'worker-a', leaseMs: 1000 }), b.claim({ tenantId: input.tenantId, ownerToken: 'worker-b', leaseMs: 1000 })]);
  const claimed = [one, two].filter(Boolean); assert.equal(claimed.length, 1); const old = claimed[0];
  await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [old.id]);
  const recovered = await a.recoverExpired({ tenantId: input.tenantId }); assert.equal(recovered.length, 1);
  const next = await b.claim({ tenantId: input.tenantId, ownerToken: 'worker-new', leaseMs: 1000 }); assert.ok(next); assert.ok(next.fenceEpoch > old.fenceEpoch);
  await assert.rejects(a.checkpoint({ ...owner(old, 'worker-a'), checkpoint: { unsafe: true } }), { code: 'durable_task_fenced' });
  await b.start(owner(next, 'worker-new')); await b.checkpoint({ ...owner(next, 'worker-new'), checkpoint: { cursor: 4 } });
});

test('expired leases enter bounded recovery, preserve checkpoints, and escalate at budget/deadline', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput({ retryBudget: 1 }); const accepted = await store.accept(input);
  const claimed = await store.claim({ tenantId: input.tenantId, ownerToken: 'owner', leaseMs: 1000 }); await store.start(owner(claimed, 'owner'));
  await store.checkpoint({ ...owner(claimed, 'owner'), checkpoint: { completed: ['one'] } });
  await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [claimed.id]);
  await store.recoverExpired({ tenantId: input.tenantId }); const recovering = await store.get({ tenantId: input.tenantId, taskId: accepted.task.id });
  assert.equal(recovering.state, STATES.RECOVERY_REQUIRED); assert.deepEqual(recovering.progress_checkpoint, { completed: ['one'] });
  await store.revalidateDependencies({ tenantId: input.tenantId, taskId: claimed.id, dependencyRefs: input.dependencyRefs });
  const reclaimed = await store.claim({ tenantId: input.tenantId, ownerToken: 'owner-2', leaseMs: 1000 });
  await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [reclaimed.id]);
  await store.recoverExpired({ tenantId: input.tenantId });
  assert.equal((await store.get({ tenantId: input.tenantId, taskId: reclaimed.id })).state, STATES.ESCALATED);
});

test('dependency drift blocks recovery; cancellation fences further work; completion awaits evidence', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput(); const accepted = await store.accept(input);
  const claim = await store.claim({ tenantId: input.tenantId, ownerToken: 'owner-c', leaseMs: 1000 }); await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [claim.id]);
  await store.recoverExpired({ tenantId: input.tenantId });
  const blocked = await store.revalidateDependencies({ tenantId: input.tenantId, taskId: accepted.task.id, dependencyRefs: [{ id: 'mission', version: '2' }] }); assert.equal(blocked.state, STATES.BLOCKED_DEPENDENCY);
  await store.cancel({ tenantId: input.tenantId, taskId: accepted.task.id, reason: 'operator stop' });
  assert.equal((await store.get({ tenantId: input.tenantId, taskId: accepted.task.id })).state, STATES.CANCELLED);
  assert.equal(await store.claim({ tenantId: input.tenantId, ownerToken: 'late' }), null);

  const secondInput = taskInput(); const second = await store.accept(secondInput); const active = await store.claim({ tenantId: secondInput.tenantId, ownerToken: 'owner-v', leaseMs: 1000 }); await store.start(owner(active, 'owner-v'));
  const awaiting = await store.reportCompletion({ ...owner(active, 'owner-v'), resultRef: { report: 'r1' }, evidenceRefs: [] }); assert.equal(awaiting.state, STATES.AWAITING_VERIFICATION);
  await assert.rejects(store.verify({ tenantId: secondInput.tenantId, taskId: second.task.id, evidenceRefs: [] }), { code: 'durable_task_invalid' });
  assert.equal((await store.verify({ tenantId: secondInput.tenantId, taskId: second.task.id, evidenceRefs: [{ uri: 'evidence://r1' }] })).state, STATES.COMPLETED);
});

test('tenant scope and the P0-003 ledger prevent a stale task owner from reaching a provider', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput(); const accepted = await store.accept(input); const claim = await store.claim({ tenantId: input.tenantId, ownerToken: 'owner-send', leaseMs: 1000 });
  assert.equal(await store.get({ tenantId: 'tenant-b', taskId: accepted.task.id }), null);
  await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [claim.id]);
  let providerCalls = 0; const ledger = createExternalActionLedger(pool);
  await assert.rejects(guardedDispatch({ ledger, claim: { tenantId: input.tenantId, idempotencyKey: `task-send-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ accepted: accepted.task.id }) }, beforeDispatch: () => store.requireOwner(owner(claim, 'owner-send')), dispatch: async () => { providerCalls += 1; return { success: true }; } }), { code: 'durable_task_fenced' });
  assert.equal(providerCalls, 0);
});
