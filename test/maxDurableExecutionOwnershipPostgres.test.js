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
function taskInput(overrides = {}) { sequence += 1; return { tenantId: `tenant-a-${sequence}`, logicalKey: `task-${process.pid}-${sequence}`, specialist: 'research', immutableInput: { query: 'Manchester', n: sequence }, dependencyRefs: [{ id: 'mission', version: '1' }], retryBudget: 2, ...overrides }; }
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
  assert.equal(await b.claim({ tenantId: input.tenantId, ownerToken: 'too-early', leaseMs: 1000 }), null, 'recovery_required needs explicit revalidation');
  await a.revalidateDependencies({ tenantId: input.tenantId, taskId: old.id, dependencyRefs: input.dependencyRefs });
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

  const thirdInput = taskInput(); const third = await store.accept(thirdInput); const thirdOwner = await store.claim({ tenantId: thirdInput.tenantId, ownerToken: 'owner-claim', leaseMs: 1000 }); await store.start(owner(thirdOwner, 'owner-claim'));
  await store.reportCompletion({ ...owner(thirdOwner, 'owner-claim'), resultRef: { report: 'r2' }, evidenceRefs: [{ uri: 'evidence://r2' }] });
  // A direct ledger caller attaches an action after completion: verification still refuses.
  const ledger = createExternalActionLedger(pool); const action = await ledger.claim({ tenantId: thirdInput.tenantId, taskId: third.task.id, idempotencyKey: `unresolved-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ task: third.task.id }) });
  await assert.rejects(store.verify({ tenantId: thirdInput.tenantId, taskId: third.task.id, evidenceRefs: [{ uri: 'evidence://r2' }] }), { code: 'durable_task_external_action_unresolved' });
  await ledger.releaseUndispatched({ tenantId: thirdInput.tenantId, idempotencyKey: `unresolved-${sequence}`, ownerToken: action.ownerToken });
});

test('dependency admission is tenant-scoped, cycle-free, and terminal failure blocks descendants', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const tenantId = `tenant-deps-${++sequence}`;
  const base = { tenantId, specialist: 'research', dependencyRefs: [], retryBudget: 0 };
  const first = (await store.accept({ ...base, logicalKey: `dep-first-${sequence}` })).task;
  const second = (await store.accept({ ...base, logicalKey: `dep-second-${sequence}`, dependencyTaskIds: [first.id] })).task;
  assert.equal(second.state, STATES.BLOCKED_DEPENDENCY, 'incomplete prerequisite is not claimable');
  assert.equal(await store.claimTask({ tenantId, taskId: second.id, ownerToken: 'blocked', leaseMs: 1000 }), null);
  const firstOwner = await store.claimTask({ tenantId, taskId: first.id, ownerToken: 'first', leaseMs: 1000 });
  await store.start(owner(firstOwner, 'first'));
  await store.fail({ ...owner(firstOwner, 'first'), reason: 'definitive prerequisite failure' });
  const blocked = await store.get({ tenantId, taskId: second.id });
  assert.equal(blocked.state, STATES.BLOCKED_DEPENDENCY);
  assert.equal(blocked.recoveryReason, 'dependency_failed');
  await assert.rejects(store.accept({ ...base, logicalKey: `wrong-tenant-${sequence}`, dependencyTaskIds: [first.id], tenantId: `${tenantId}-other` }), { code: 'durable_task_dependency_unavailable' });
  await assert.rejects(pool.query('INSERT INTO max_durable_execution_task_dependencies(tenant_id,task_id,depends_on_task_id) VALUES($1,$2,$3)', [tenantId, first.id, second.id]), { code: '23514' });
  await assert.rejects(pool.query('TRUNCATE max_durable_execution_task_dependencies'), { code: '42501' });
});

test('tenant scope and the P0-003 ledger prevent a stale task owner from reaching a provider', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput(); const accepted = await store.accept(input); const claim = await store.claim({ tenantId: input.tenantId, ownerToken: 'owner-send', leaseMs: 1000 });
  assert.equal(await store.get({ tenantId: 'tenant-b', taskId: accepted.task.id }), null);
  await assert.rejects(store.accept(taskInput({ tenantId: 'tenant-b', parentTaskId: accepted.task.id })), { code: '23503' });
  const crossTenantLedger = createExternalActionLedger(pool);
  await assert.rejects(crossTenantLedger.claim({ tenantId: 'tenant-b', taskId: accepted.task.id, idempotencyKey: `cross-tenant-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ task: accepted.task.id }) }), { code: '23503' });
  await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [claim.id]);
  let providerCalls = 0; const ledger = createExternalActionLedger(pool);
  await assert.rejects(guardedDispatch({ ledger, claim: { tenantId: input.tenantId, idempotencyKey: `task-send-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ accepted: accepted.task.id }) }, beforeDispatch: () => store.requireOwner(owner(claim, 'owner-send')), dispatch: async () => { providerCalls += 1; return { success: true }; } }), { code: 'durable_task_fenced' });
  assert.equal(providerCalls, 0);
});

// ---------------------------------------------------------------------------
// Direct-database tenant isolation and invariants (no application store).
// ---------------------------------------------------------------------------
async function expire(pool, id) { await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [id]); }
function rawClaimSql(taskColumn = true) {
  return `INSERT INTO max_external_action_claims (tenant_id, idempotency_key, action_kind, request_fingerprint, attempt, state, ${taskColumn ? 'task_id, ' : ''}owner_token, claimed_at, updated_at)
          VALUES ($1, $2, 'test', $3, 1, 'claimed', ${taskColumn ? '$4, ' : ''}'raw-owner', now(), now())`;
}

test('PostgreSQL itself rejects every cross-tenant task association, even from direct callers', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput();
  const a = (await store.accept(input)).task; const fp = requestFingerprint({ raw: a.id });
  // Ledger claim linked to another tenant's task: INSERT.
  await assert.rejects(pool.query(rawClaimSql(), ['tenant-b', `raw-x-${sequence}`, fp, a.id]), { code: '23503', constraint: 'max_external_action_claims_tenant_task_fkey' });
  // Ledger claim re-pointed at another tenant's task: UPDATE.
  await pool.query(rawClaimSql(false), ['tenant-b', `raw-y-${sequence}`, fp]);
  await assert.rejects(pool.query('UPDATE max_external_action_claims SET task_id=$1 WHERE tenant_id=$2 AND idempotency_key=$3', [a.id, 'tenant-b', `raw-y-${sequence}`]), { code: '23503' });
  // Same-tenant link succeeds; once linked it cannot be detached or moved.
  await pool.query(rawClaimSql(), [input.tenantId, `raw-z-${sequence}`, fp, a.id]);
  await assert.rejects(pool.query('UPDATE max_external_action_claims SET task_id=NULL WHERE tenant_id=$1 AND idempotency_key=$2', [input.tenantId, `raw-z-${sequence}`]), { code: '23514' });
  await pool.query("UPDATE max_external_action_claims SET state='abandoned_not_dispatched' WHERE tenant_id=$1 AND idempotency_key=$2", [input.tenantId, `raw-z-${sequence}`]);
  // Moving a claim's tenant away from its task's tenant is also refused.
  await assert.rejects(pool.query("UPDATE max_external_action_claims SET tenant_id='tenant-b' WHERE tenant_id=$1 AND idempotency_key=$2", [input.tenantId, `raw-z-${sequence}`]), { code: '23503' });
  // Event rows for another tenant's task.
  await assert.rejects(pool.query("INSERT INTO max_durable_execution_task_events(task_id,tenant_id,fence_epoch,event) VALUES($1,'tenant-b',0,'forged')", [a.id]), { code: '23503', constraint: 'max_durable_execution_task_events_tenant_task_fkey' });
  // Parent task from another tenant; a task as its own parent.
  await assert.rejects(pool.query(`INSERT INTO max_durable_execution_tasks(id,tenant_id,parent_task_id,logical_key,input_fingerprint,specialist,state) VALUES(gen_random_uuid(),'tenant-b',$1,$2,$3,'x','queued')`, [a.id, `raw-parent-${sequence}`, 'a'.repeat(64)]), { code: '23503' });
  const self = '00000000-0000-4000-8000-0000000000aa';
  await assert.rejects(pool.query(`INSERT INTO max_durable_execution_tasks(id,tenant_id,parent_task_id,logical_key,input_fingerprint,specialist,state) VALUES($1,$2,$1,$3,$4,'x','queued')`, [self, input.tenantId, `raw-self-${sequence}`, 'a'.repeat(64)]), { code: '23514' });
  // A tenant can never read another tenant's task or events through the store.
  assert.equal(await store.get({ tenantId: 'tenant-b', taskId: a.id }), null);
  assert.deepEqual(await store.listEvents({ tenantId: 'tenant-b', taskId: a.id }), []);
});

test('database guards: immutable accepted input, monotonic fence, final terminal states, append-only evidence', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput();
  const task = (await store.accept(input)).task; const claimed = await store.claim({ tenantId: input.tenantId, ownerToken: 'guard', leaseMs: 5000 });
  await assert.rejects(pool.query("UPDATE max_durable_execution_tasks SET immutable_input='{\"query\":\"swapped\"}' WHERE id=$1", [task.id]), { code: '23514' });
  await assert.rejects(pool.query("UPDATE max_durable_execution_tasks SET tenant_id='tenant-b' WHERE id=$1", [task.id]), { code: '23514' });
  await assert.rejects(pool.query('UPDATE max_durable_execution_tasks SET fence_epoch=0 WHERE id=$1', [task.id]), { code: '23514' });
  await assert.rejects(pool.query('UPDATE max_durable_execution_tasks SET ownership_token=NULL WHERE id=$1', [task.id]), { code: '23514', constraint: 'max_durable_execution_tasks_owner_matches_state' });
  await assert.rejects(pool.query("UPDATE max_durable_execution_task_events SET event='rewritten' WHERE task_id=$1", [task.id]), { code: '23514' });
  await assert.rejects(pool.query('DELETE FROM max_durable_execution_task_events WHERE task_id=$1', [task.id]));
  await assert.rejects(pool.query('DELETE FROM max_durable_execution_tasks WHERE id=$1', [task.id]));
  await store.cancel({ tenantId: input.tenantId, taskId: claimed.id, reason: 'guard test' });
  await assert.rejects(pool.query("UPDATE max_durable_execution_tasks SET state='queued' WHERE id=$1", [task.id]), { code: '23514' });
  const events = await store.listEvents({ tenantId: input.tenantId, taskId: task.id });
  assert.deepEqual(events.map(e => [e.event, e.from_state, e.to_state]), [['accepted', null, 'queued'], ['claimed', 'queued', 'claimed'], ['cancelled', 'claimed', 'cancelled']]);
});

test('acceptance replay is bound to deadline and retry budget as well as input', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput({ deadlineAt: new Date(Date.now() + 3600e3).toISOString() });
  await store.accept(input);
  await assert.rejects(store.accept({ ...input, retryBudget: 5 }), { code: 'durable_task_key_reused' });
  await assert.rejects(store.accept({ ...input, deadlineAt: new Date(Date.now() + 7200e3).toISOString() }), { code: 'durable_task_key_reused' });
  await assert.rejects(store.accept(taskInput({ retryBudget: 1000 })), { code: 'durable_task_invalid' });
  await assert.rejects(store.accept(taskInput({ taskId: '00000000-0000-4000-8000-0000000000ab', parentTaskId: '00000000-0000-4000-8000-0000000000ab' })), { code: 'durable_task_invalid' });
});

test('deadlines fence a live owner and escalate waiting work without replay', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const input = taskInput({ deadlineAt: new Date(Date.now() + 1500).toISOString(), retryBudget: 5 }); const task = (await store.accept(input)).task;
  const c = await store.claim({ tenantId: input.tenantId, ownerToken: 'deadline', leaseMs: 60000 }); await store.start(owner(c, 'deadline'));
  await new Promise(r => setTimeout(r, 1700));
  await assert.rejects(store.heartbeat({ ...owner(c, 'deadline'), leaseMs: 60000 }), { code: 'durable_task_fenced' });
  await assert.rejects(store.requireOwner(owner(c, 'deadline')), { code: 'durable_task_fenced' });
  await assert.rejects(store.reportCompletion({ ...owner(c, 'deadline'), resultRef: {}, evidenceRefs: [] }), { code: 'durable_task_fenced' });
  await expire(pool, task.id);
  const [escalated] = await store.recoverExpired({ tenantId: input.tenantId });
  assert.equal(escalated.state, STATES.ESCALATED); assert.equal(escalated.escalationReason, 'deadline_exceeded');

  const waiting = taskInput({ deadlineAt: new Date(Date.now() + 500).toISOString() }); const w = (await store.accept(waiting)).task;
  await new Promise(r => setTimeout(r, 700));
  assert.equal(await store.claim({ tenantId: waiting.tenantId, ownerToken: 'late' }), null, 'expired queued work is not claimable');
  const [swept] = await store.recoverExpired({ tenantId: waiting.tenantId });
  assert.equal(swept.id, w.id); assert.equal(swept.state, STATES.ESCALATED); assert.equal(swept.escalationReason, 'deadline_exceeded');
});

test('retry budget counts every lease-expiry recovery, including after dependency revalidation', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const input = taskInput({ retryBudget: 2 }); const task = (await store.accept(input)).task;
  for (let i = 1; i <= 2; i += 1) {
    const c = await store.claim({ tenantId: input.tenantId, ownerToken: `budget-${i}`, leaseMs: 1000 }); assert.equal(c.fenceEpoch, i);
    await expire(pool, task.id);
    const [r] = await store.recoverExpired({ tenantId: input.tenantId }); assert.equal(r.state, STATES.RECOVERY_REQUIRED); assert.equal(r.recoveryAttempts, i);
    await store.revalidateDependencies({ tenantId: input.tenantId, taskId: task.id, dependencyRefs: input.dependencyRefs });
  }
  await store.claim({ tenantId: input.tenantId, ownerToken: 'budget-3', leaseMs: 1000 }); await expire(pool, task.id);
  const [last] = await store.recoverExpired({ tenantId: input.tenantId });
  assert.equal(last.state, STATES.ESCALATED); assert.equal(last.escalationReason, 'retry_budget_exhausted');
  const claimEvents = (await store.listEvents({ tenantId: input.tenantId, taskId: task.id })).filter(e => e.event === 'claimed');
  assert.deepEqual(claimEvents.map(e => e.from_state), ['queued', 'queued', 'queued'], 'event history records the real prior state');
});

test('recovery escalates instead of replaying a task whose external action is unresolved', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const ledger = createExternalActionLedger(pool);
  const input = taskInput({ retryBudget: 5 }); const task = (await store.accept(input)).task;
  const c = await store.claim({ tenantId: input.tenantId, ownerToken: 'w1', leaseMs: 5000 }); await store.start(owner(c, 'w1'));
  const key = `unresolved-recovery-${sequence}`;
  const action = await ledger.claim({ tenantId: input.tenantId, taskId: task.id, idempotencyKey: key, actionKind: 'test', requestFingerprint: requestFingerprint({ key }) });
  await ledger.markDispatchAttempted({ tenantId: input.tenantId, idempotencyKey: key, ownerToken: action.ownerToken, taskFence: { taskId: task.id, ownershipToken: 'w1', fenceEpoch: c.fenceEpoch } });
  await expire(pool, task.id);
  const [r] = await store.recoverExpired({ tenantId: input.tenantId });
  assert.equal(r.state, STATES.ESCALATED); assert.equal(r.escalationReason, 'external_action_unresolved');
  assert.equal(await store.claim({ tenantId: input.tenantId, ownerToken: 'w2' }), null, 'never reclaimed');
  assert.equal((await ledger.get(input.tenantId, key)).state, 'dispatch_attempted', 'ledger state preserved for reconciliation');
});

test('cancellation before dispatch fences the marker; cancellation after dispatch escalates with claim states', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const ledger = createExternalActionLedger(pool);
  // Before dispatch: the owner has claimed the action but the task is cancelled.
  const a = taskInput(); const ta = (await store.accept(a)).task; const ca = await store.claim({ tenantId: a.tenantId, ownerToken: 'a1', leaseMs: 5000 }); await store.start(owner(ca, 'a1'));
  let providerCalls = 0;
  const claimA = { tenantId: a.tenantId, taskId: ta.id, idempotencyKey: `cancel-before-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ a: ta.id }) };
  const cancelled = await store.cancel({ tenantId: a.tenantId, taskId: ta.id, reason: 'operator stop' });
  assert.equal(cancelled.state, STATES.CANCELLED);
  await assert.rejects(guardedDispatch({ ledger, claim: claimA, taskFence: { taskId: ta.id, ownershipToken: 'a1', fenceEpoch: ca.fenceEpoch }, dispatch: async () => { providerCalls += 1; } }), { code: 'durable_task_fenced' });
  assert.equal(providerCalls, 0); assert.equal((await ledger.get(a.tenantId, claimA.idempotencyKey)).state, 'abandoned_not_dispatched');

  // After dispatch: the effect cannot be recalled, so the task is escalated.
  const b = taskInput(); const tb = (await store.accept(b)).task; const cb = await store.claim({ tenantId: b.tenantId, ownerToken: 'b1', leaseMs: 5000 }); await store.start(owner(cb, 'b1'));
  const sent = await guardedDispatch({ ledger, claim: { tenantId: b.tenantId, taskId: tb.id, idempotencyKey: `cancel-after-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ b: tb.id }) },
    taskFence: { taskId: tb.id, ownershipToken: 'b1', fenceEpoch: cb.fenceEpoch }, dispatch: async () => { providerCalls += 1; return { success: true, providerMessageId: 'pm-1' }; } });
  assert.equal(sent.outcome, 'dispatched'); assert.equal(providerCalls, 1);
  const escalated = await store.cancel({ tenantId: b.tenantId, taskId: tb.id, reason: 'operator stop' });
  assert.equal(escalated.state, STATES.ESCALATED); assert.equal(escalated.escalationReason, 'cancelled_after_dispatch'); assert.equal(escalated.cancellationReason, 'operator stop');
  const last = (await store.listEvents({ tenantId: b.tenantId, taskId: tb.id })).at(-1);
  assert.equal(last.event, 'cancel_escalated'); assert.deepEqual(last.detail.actionStates, { provider_acknowledged: 1 });
});

test('the dispatch marker is atomic with the fence: a stale owner is refused even without beforeDispatch, and a concurrent cancel serializes', { timeout: 30000 }, async () => {
  const { pool, pool2 } = await db(); const store = createDurableExecutionOwnership(pool); const ledger = createExternalActionLedger(pool);
  // A task-linked action without the fence, or with a fence for another task, never claims.
  const x = taskInput(); const tx = (await store.accept(x)).task;
  await assert.rejects(guardedDispatch({ ledger, claim: { tenantId: x.tenantId, taskId: tx.id, idempotencyKey: `nofence-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ f: sequence }) }, dispatch: async () => {} }), { code: 'durable_task_fenced' });
  assert.equal(await ledger.get(x.tenantId, `nofence-${sequence}`), null);

  // Stale owner after takeover; no beforeDispatch check at all.
  const s = taskInput({ retryBudget: 3 }); const ts = (await store.accept(s)).task;
  const old = await store.claim({ tenantId: s.tenantId, ownerToken: 'old', leaseMs: 1000 }); await expire(pool, ts.id); await store.recoverExpired({ tenantId: s.tenantId });
  await store.revalidateDependencies({ tenantId: s.tenantId, taskId: ts.id, dependencyRefs: s.dependencyRefs });
  const fresh = await store.claim({ tenantId: s.tenantId, ownerToken: 'new', leaseMs: 5000 }); assert.ok(fresh.fenceEpoch > old.fenceEpoch);
  let calls = 0;
  await assert.rejects(guardedDispatch({ ledger, claim: { tenantId: s.tenantId, taskId: ts.id, idempotencyKey: `stale-${sequence}`, actionKind: 'test', requestFingerprint: requestFingerprint({ f: sequence }) },
    taskFence: { taskId: ts.id, ownershipToken: 'old', fenceEpoch: old.fenceEpoch }, dispatch: async () => { calls += 1; } }), { code: 'durable_task_fenced' });
  assert.equal(calls, 0);
  // A fence naming a different tenant's view of the task is also refused.
  await assert.rejects(ledger.markDispatchAttempted({ tenantId: 'tenant-b', idempotencyKey: `stale-${sequence}`, ownerToken: 'x', taskFence: { taskId: ts.id, ownershipToken: 'new', fenceEpoch: fresh.fenceEpoch } }), { code: 'durable_task_fenced' });

  // Concurrent cancellation holds the row lock; the marker waits, then is fenced.
  const k = `race-${sequence}`; const action = await ledger.claim({ tenantId: s.tenantId, taskId: ts.id, idempotencyKey: k, actionKind: 'test', requestFingerprint: requestFingerprint({ f: sequence }) });
  const canceller = await pool2.connect();
  try {
    await canceller.query('BEGIN');
    await canceller.query('SELECT 1 FROM max_durable_execution_tasks WHERE id=$1 FOR UPDATE', [ts.id]);
    let settled = false;
    const marker = ledger.markDispatchAttempted({ tenantId: s.tenantId, idempotencyKey: k, ownerToken: action.ownerToken, taskFence: { taskId: ts.id, ownershipToken: 'new', fenceEpoch: fresh.fenceEpoch } })
      .then(v => { settled = true; return v; }, e => { settled = true; throw e; });
    await new Promise(r => setTimeout(r, 300));
    assert.equal(settled, false, 'marker blocks behind the cancellation lock');
    await canceller.query("UPDATE max_durable_execution_tasks SET state='cancelled', ownership_token=NULL, lease_expires_at=NULL WHERE id=$1", [ts.id]);
    await canceller.query('COMMIT');
    await assert.rejects(marker, { code: 'durable_task_fenced' });
  } finally { canceller.release(); }
  assert.equal((await ledger.get(s.tenantId, k)).state, 'claimed', 'never marked dispatched');
});

test('at most one active task per mission per tenant, under concurrent claimers and direct writes', async () => {
  const { pool, pool2 } = await db(); const a = createDurableExecutionOwnership(pool); const b = createDurableExecutionOwnership(pool2);
  const tenantId = `tenant-mission-${sequence}`; const missionId = `mission-${sequence}`;
  const t1 = (await a.accept(taskInput({ tenantId, missionId }))).task; const t2 = (await a.accept(taskInput({ tenantId, missionId }))).task;
  const other = (await a.accept(taskInput({ tenantId, missionId: `${missionId}-other` }))).task;
  const results = await Promise.all([a.claim({ tenantId, ownerToken: 'c1' }), b.claim({ tenantId, ownerToken: 'c2' }), a.claim({ tenantId, ownerToken: 'c3' })]);
  const won = results.filter(Boolean);
  const activeForMission = won.filter(t => t.missionId === missionId);
  assert.equal(activeForMission.length, 1, 'one active task for the mission');
  assert.ok(won.length <= 2 && won.every(t => [t1.id, t2.id, other.id].includes(t.id)));
  const idle = [t1, t2].find(t => t.id !== activeForMission[0].id);
  await assert.rejects(pool.query("UPDATE max_durable_execution_tasks SET state='claimed', ownership_token='raw', lease_expires_at=clock_timestamp()+interval '1 minute', fence_epoch=fence_epoch+1 WHERE id=$1", [idle.id]),
    { code: '23505', constraint: 'max_durable_execution_tasks_one_active_per_mission' });
  // Once the active task leaves the active states, the sibling becomes claimable.
  await a.cancel({ tenantId, taskId: activeForMission[0].id, reason: 'done with it' });
  const next = await a.claim({ tenantId, ownerToken: 'c4' });
  assert.ok(next && [idle.id, other.id].includes(next.id));
});

test('the operator migration is additive and re-runnable over existing P0-003 rows', async () => {
  const { pool } = await db();
  await pool.query(rawClaimSql(false), ['tenant-legacy', `legacy-${sequence}`, requestFingerprint({ legacy: sequence })]);
  await pool.query(fs.readFileSync(P004, 'utf8'));
  const r = await pool.query('SELECT task_id, state FROM max_external_action_claims WHERE tenant_id=$1 AND idempotency_key=$2', ['tenant-legacy', `legacy-${sequence}`]);
  assert.deepEqual(r.rows, [{ task_id: null, state: 'claimed' }], 'existing P0-003 claims are untouched');
});

test('review regressions: reclaim re-attributes never-delivered claims; completion with an unresolved action escalates; id collisions stay opaque', async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool); const ledger = createExternalActionLedger(pool);
  const tenantId = `tenant-reclaim-${sequence}`; const key = `shared-key-${sequence}`; const fp = requestFingerprint({ key });
  // Task A's guard refuses: the claim is abandoned while linked to A; A is cancelled.
  const a = (await store.accept(taskInput({ tenantId }))).task; const ca = await store.claim({ tenantId, ownerToken: 'ra', leaseMs: 5000 }); await store.start(owner(ca, 'ra'));
  await assert.rejects(guardedDispatch({ ledger, claim: { tenantId, taskId: a.id, idempotencyKey: key, actionKind: 'test', requestFingerprint: fp }, taskFence: { taskId: a.id, ownershipToken: 'ra', fenceEpoch: ca.fenceEpoch },
    beforeDispatch: async () => { throw Object.assign(new Error('guard'), { code: 'guard_refused' }); }, dispatch: async () => {} }), { code: 'guard_refused' });
  await store.cancel({ tenantId, taskId: a.id });
  // Task B for the same logical action can reclaim and send exactly once.
  const b = (await store.accept(taskInput({ tenantId }))).task; const cb = await store.claim({ tenantId, ownerToken: 'rb', leaseMs: 5000 }); await store.start(owner(cb, 'rb'));
  let calls = 0;
  const sent = await guardedDispatch({ ledger, claim: { tenantId, taskId: b.id, idempotencyKey: key, actionKind: 'test', requestFingerprint: fp }, taskFence: { taskId: b.id, ownershipToken: 'rb', fenceEpoch: cb.fenceEpoch },
    dispatch: async () => { calls += 1; return { success: true, providerMessageId: 'pm-b' }; } });
  assert.equal(sent.outcome, 'dispatched'); assert.equal(calls, 1);
  assert.equal((await pool.query('SELECT task_id FROM max_external_action_claims WHERE tenant_id=$1 AND idempotency_key=$2', [tenantId, key])).rows[0].task_id, b.id);
  // An untasked caller cannot reclaim a task-linked, never-delivered claim.
  const k2 = `linked-${sequence}`;
  await assert.rejects(guardedDispatch({ ledger, claim: { tenantId, taskId: b.id, idempotencyKey: k2, actionKind: 'test', requestFingerprint: fp }, taskFence: { taskId: b.id, ownershipToken: 'rb', fenceEpoch: cb.fenceEpoch },
    beforeDispatch: async () => { throw Object.assign(new Error('guard'), { code: 'guard_refused' }); }, dispatch: async () => {} }), { code: 'guard_refused' });
  const untasked = await guardedDispatch({ ledger, claim: { tenantId, idempotencyKey: k2, actionKind: 'test', requestFingerprint: fp }, dispatch: async () => { calls += 1; } });
  assert.equal(untasked.outcome, 'blocked'); assert.equal(untasked.code, 'external_action_task_linked'); assert.equal(calls, 1);
  // A delivered claim can never be re-attributed, even by direct SQL.
  await assert.rejects(pool.query('UPDATE max_external_action_claims SET task_id=$1 WHERE tenant_id=$2 AND idempotency_key=$3', [a.id, tenantId, key]), { code: '23514' });

  // Completion while an action is unresolved escalates rather than waiting forever.
  const c = (await store.accept(taskInput())).task; const cc = await store.claim({ tenantId: c.tenantId, ownerToken: 'rc', leaseMs: 5000 }); await store.start(owner(cc, 'rc'));
  await ledger.claim({ tenantId: c.tenantId, taskId: c.id, idempotencyKey: `pending-${sequence}`, actionKind: 'test', requestFingerprint: fp });
  const done = await store.reportCompletion({ ...owner(cc, 'rc'), resultRef: {}, evidenceRefs: [] });
  assert.equal(done.state, STATES.ESCALATED); assert.equal(done.escalationReason, 'external_action_unresolved');

  // A caller-supplied id that exists in another tenant is refused without revealing that.
  await assert.rejects(store.accept(taskInput({ tenantId: 'tenant-other', taskId: c.id })), { code: 'durable_task_conflict' });
  // A new owner without a new fence epoch is refused by the database; terminal rows are final.
  const d = (await store.accept(taskInput())).task; await store.claim({ tenantId: d.tenantId, ownerToken: 'rd', leaseMs: 5000 });
  await assert.rejects(pool.query("UPDATE max_durable_execution_tasks SET ownership_token='hijack' WHERE id=$1", [d.id]), { code: '23514' });
  await assert.rejects(pool.query("UPDATE max_durable_execution_tasks SET result_ref='{}'::jsonb WHERE id=$1", [c.id]), { code: '23514' });
});
