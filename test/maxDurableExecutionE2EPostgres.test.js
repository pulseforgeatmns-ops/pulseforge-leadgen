'use strict';

/**
 * MAX-P0-004 P2–P6 end to end on real, disposable PostgreSQL:
 * the Canonical Execution Router + real TME + real mission persistence + the
 * durable task store + request receipts + operator recovery.
 *
 * Proves: task acceptance before specialist code; completion only with
 * evidence; replay never re-dispatches; a superseded, cancelled or crashed
 * owner can never make mission state durable (fence checked in the lease
 * transaction); task-owned writes never take an unlocked path; one active task
 * per mission; receipts linked to tasks with live state on replay/conflict;
 * plan-by-default, operator-attributed recovery; tenant scoping. May not skip.
 */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const amo = require('../packages/acquisition-mission');
const TP = require('../packages/acquisition-mission/TransactionalPersistence');
const { executeMissionStage, bumpMissionVersion } = require('../packages/acquisition-mission/TransactionalExecution');
const persistence = require('../services/acquisitionMissionPersistence');
const { ensureOutboundExecutionSchema } = require('../services/acquisitionMissionOutboundPersistence');
const { ensureAcquisitionKnowledgeSchema } = require('../services/acquisitionKnowledgePersistence');
const { createDurableExecutionOwnership } = require('../services/maxReliability/durableExecutionOwnership');
const { runWithTaskFence } = require('../services/maxReliability/taskExecutionContext');
const { runKeyedRequest } = require('../services/maxReliability/keyedRequest');
const { runTaskRecovery } = require('../services/maxReliability/taskRecovery');
const { resolveDurableTasks, summarizeTask } = require('../packages/acquisition-mission/DurableTaskRouting');

const P003 = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const P004 = path.join(__dirname, '../migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql');
const WORKER = path.join(__dirname, 'helpers/maxDurableTaskWorker.cjs');

let fixture;
async function db() {
  if (fixture) return fixture;
  const instance = await startDisposablePostgres('max-p0-004-e2e-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 10 });
  await persistence.ensureAcquisitionMissionSchema(pool);
  await persistence.ensureExecutionAuditSchema(pool);
  await ensureOutboundExecutionSchema(pool);
  await ensureAcquisitionKnowledgeSchema(pool);
  await pool.query(fs.readFileSync(P003, 'utf8'));
  await pool.query(fs.readFileSync(P004, 'utf8'));
  fixture = { instance, pool, connectionString: instance.connectionString };
  return fixture;
}
test.after(async () => { if (fixture) await closePostgresFixture(fixture.pool, fixture.instance); });

let seq = 0;
async function missionFixture(pool) {
  TP.resetMissionDurableLocksForTests();
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: `Acquire facility buyers ${process.pid}-${++seq}.`, targetSegment: 'Facilities' });
  await persistence.persistStageCommit({ mission: engine.get(mission.id, '10') }, pool, { skipEnsure: true });
  engine.durableBaseline = new Map([[mission.id, persistence.missionFingerprint(engine.get(mission.id, '10'))]]);
  return { engine, mission: engine.get(mission.id, '10') };
}
async function durable(pool, missionId) {
  return (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0].payload;
}
function cer(mission, overrides = {}) {
  return amo.createExecutionRequestFromApi({
    intent: amo.EXECUTION_INTENTS.EDIT_PLAN, missionId: mission.id, mission, operatorId: 'operator-e2e',
    stage: mission.stage, objective: mission.objective, runtimeOwner: amo.resolveMissionRuntimeOwner(mission),
    permissions: { canExecute: true }, payload: { question: 'durable e2e' }, ...overrides,
  });
}
/** A router handler that runs one real TME stage; `during` runs inside execute. */
function stageHandler(engine, mission, pool, { during, onExecute } = {}) {
  return async () => executeMissionStage({
    engine, missionId: mission.id, tenantId: '10', pool, specialist: 'operator', stage: mission.stage,
    validatePreconditions: () => ({}),
    execute: async () => { if (onExecute) onExecute(); if (during) await during(); return { ok: true }; },
    commit: async ({ transactionId }) => {
      const current = engine.get(mission.id, '10');
      const next = { ...current, title: `committed-${transactionId}` };
      bumpMissionVersion(next, transactionId);
      engine.store.putMission(next);
      return {};
    },
    persistDurable: persistence.bindStagePersistDurable({ pool, preinitializedSchema: true }, engine, '10'),
  });
}
async function route(engine, mission, pool, store, extra = {}) {
  const request = extra.request || cer(mission);
  return amo.routeExecutionRequest(request, {
    engine, tenantId: '10', pool, durableTasks: store,
    handlers: { [request.intent]: extra.handler || stageHandler(engine, mission, pool, extra) },
    ...extra.context,
  });
}

test('a task-owned request commits the stage and completes only with execution evidence; replay never re-dispatches', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { engine, mission } = await missionFixture(pool);
  let executions = 0;
  const key = `e2e-ok-${seq}`;
  const routed = await route(engine, mission, pool, store, { onExecute: () => { executions += 1; }, context: { durableTaskKey: key } });
  assert.equal(executions, 1);
  assert.equal(routed.execution.state, 'committed_internal');
  assert.equal(routed.durableTask.state, 'completed');
  const after = await durable(pool, mission.id);
  assert.equal(Number(after.version), Number(mission.version || 0) + 1, 'stage durably committed');
  const task = await store.get({ tenantId: '10', taskId: routed.durableTask.id });
  assert.equal(task.evidence_refs[0].transactionId, routed.executionResult.transactionId, 'evidence names the committed transaction');
  const events = (await store.listEvents({ tenantId: '10', taskId: task.id })).map((e) => e.event);
  assert.deepEqual(events, ['accepted', 'claimed', 'started', 'specialist_completed', 'verified']);

  // Same logical key again: the recorded task is returned, nothing runs.
  const replay = await route(engine, engine.get(mission.id, '10'), pool, store, { onExecute: () => { executions += 1; }, context: { durableTaskKey: key } });
  assert.equal(executions, 1, 'no second dispatch');
  assert.equal(replay.replay, true);
  assert.equal(replay.durableTask.id, task.id);
  assert.equal(replay.execution.state, 'already_executed');
});

test('a superseded owner cannot persist mission state: takeover during execute rolls the stage back', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { engine, mission } = await missionFixture(pool);
  const before = await durable(pool, mission.id);
  let successor = null;
  const routed = await route(engine, mission, pool, store, {
    context: { durableTaskLeaseMs: 1000, durableTaskKey: `e2e-takeover-${seq}` },
    during: async () => {
      const fixtureTask = await store.getByLogicalKey({ tenantId: '10', logicalKey: `e2e-takeover-${seq}` });
      // Simulate a stalled owner whose lease expired and a successor that took over.
      await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1", [fixtureTask.id]);
      await runTaskRecovery(pool, { tenantId: '10', mode: 'apply', confirmTenant: '10', operator: 'e2e', limit: 100 });
      // retryBudget is 0 for router tasks: an expired lease escalates, never re-queues.
      successor = await store.get({ tenantId: '10', taskId: fixtureTask.id });
    },
  });
  assert.equal(routed.executionResult.rolledBack, true, 'stage rolled back');
  assert.equal(routed.execution.state, 'failed');
  assert.deepEqual(await durable(pool, mission.id), before, 'stale owner wrote nothing durable');
  assert.equal(successor.state, 'escalated');
  assert.equal(routed.durableTask.state, 'escalated', 'the reported state is the durable one');
});

test('cancellation during execute fences the commit; the task stays cancelled', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { engine, mission } = await missionFixture(pool);
  const before = await durable(pool, mission.id);
  const key = `e2e-cancel-${seq}`;
  const routed = await route(engine, mission, pool, store, {
    context: { durableTaskKey: key },
    during: async () => {
      const t = await store.getByLogicalKey({ tenantId: '10', logicalKey: key });
      await store.cancel({ tenantId: '10', taskId: t.id, reason: 'operator stop', actor: 'operator:e2e' });
    },
  });
  assert.equal(routed.executionResult.rolledBack, true);
  assert.deepEqual(await durable(pool, mission.id), before);
  assert.equal(routed.durableTask.state, 'cancelled');
});

test('task-owned writes never take an unlocked path, and a stage without the lease never runs', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { engine, mission } = await missionFixture(pool);
  const accepted = await store.accept({ tenantId: '10', logicalKey: `e2e-unlocked-${seq}`, specialist: 'operator', missionId: mission.id });
  const owned = await store.claimTask({ tenantId: '10', taskId: accepted.task.id, ownerToken: 'o', leaseMs: 30000 });
  const fence = { tenantId: '10', taskId: owned.id, ownershipToken: 'o', fenceEpoch: owned.fenceEpoch };
  await assert.rejects(runWithTaskFence(fence, () => persistence.persistStageCommit({ mission: engine.get(mission.id, '10') }, pool,
    { skipEnsure: true, skipGlobalLock: true })), { code: 'durable_task_fenced' });
  let executed = false;
  await assert.rejects(runWithTaskFence(fence, () => executeMissionStage({
    engine, missionId: mission.id, tenantId: '10', specialist: 'operator', stage: mission.stage,
    validatePreconditions: () => ({}), execute: async () => { executed = true; return {}; }, commit: async () => ({}),
  })), (err) => err.code === 'durable_task_fenced' || (err.cause && err.cause.code === 'durable_task_fenced'));
  assert.equal(executed, false, 'the specialist never ran');
  // A fence of another tenant is refused by the leased write itself.
  const foreign = { ...fence, tenantId: '99' };
  const res = await runWithTaskFence(foreign, () => executeMissionStage({
    engine, missionId: mission.id, tenantId: '10', pool, specialist: 'operator', stage: mission.stage,
    validatePreconditions: () => ({}), execute: async () => ({}), commit: async ({ transactionId }) => {
      const next = { ...engine.get(mission.id, '10') }; bumpMissionVersion(next, transactionId); engine.store.putMission(next); return {};
    },
    persistDurable: persistence.bindStagePersistDurable({ pool, preinitializedSchema: true }, engine, '10'),
  })).catch((err) => err);
  assert.ok(res instanceof Error && (res.rollback === true || /fenced|tenant/.test(String(res.message))), 'foreign-tenant fence refused');
});

test('one active task per mission: a concurrent request is refused before any specialist runs', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { engine, mission } = await missionFixture(pool);
  const holder = await store.accept({ tenantId: '10', logicalKey: `e2e-holder-${seq}`, specialist: 'operator', missionId: mission.id });
  await store.claimTask({ tenantId: '10', taskId: holder.task.id, ownerToken: 'holder', leaseMs: 30000 });
  let executed = false;
  const key = `e2e-busy-${seq}`;
  await assert.rejects(route(engine, mission, pool, store, { onExecute: () => { executed = true; }, context: { durableTaskKey: key } }),
    { code: 'durable_task_mission_busy' });
  assert.equal(executed, false);
  const refused = await store.getByLogicalKey({ tenantId: '10', logicalKey: key });
  assert.equal(refused.state, 'cancelled');
  assert.equal(refused.cancellationReason, 'mission_has_active_task');
});

test('a crashed owner process leaves an explicit, recoverable state and no mission write (restart + operator recovery)', { timeout: 90000 }, async () => {
  const { pool, connectionString } = await db(); const store = createDurableExecutionOwnership(pool);
  const { mission } = await missionFixture(pool);
  const before = await durable(pool, mission.id);
  const taskKey = `e2e-crash-${seq}`;
  const child = spawn(process.execPath, [WORKER, JSON.stringify({ connectionString, missionId: mission.id, taskKey, leaseMs: 1000 })],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', (d) => { stderr += d; });
  const started = await new Promise((resolve, reject) => {
    let out = '';
    child.stdout.on('data', (d) => { out += d; const m = out.match(/STARTED (\S+) (\d+)/); if (m) resolve({ taskId: m[1], fenceEpoch: Number(m[2]) }); });
    child.on('exit', (code) => reject(new Error(`worker exited early (${code}): ${stderr}`)));
  });
  assert.equal((await store.get({ tenantId: '10', taskId: started.taskId })).state, 'running');
  child.kill('SIGKILL');
  await new Promise((r) => child.once('exit', r));
  await new Promise((r) => setTimeout(r, 1500)); // lease (1s) expires; no heartbeat survives the process

  // Restart: a fresh store. Plan mode reads only.
  const restarted = createDurableExecutionOwnership(pool);
  const eventsBefore = (await restarted.listEvents({ tenantId: '10', taskId: started.taskId })).length;
  const plan = await runTaskRecovery(pool, { tenantId: '10', mode: 'plan' });
  const entry = plan.plan.find((p) => p.task.id === started.taskId);
  assert.ok(entry, 'expired task is in the recovery plan');
  assert.equal((await restarted.listEvents({ tenantId: '10', taskId: started.taskId })).length, eventsBefore, 'plan wrote nothing');
  await assert.rejects(runTaskRecovery(pool, { tenantId: '10', mode: 'apply', confirmTenant: '11', operator: 'ops' }), { code: 'recovery_tenant_unconfirmed' });
  await assert.rejects(runTaskRecovery(pool, { tenantId: '10', mode: 'apply', confirmTenant: '10' }), { code: 'recovery_actor_required' });
  const applied = await runTaskRecovery(pool, { tenantId: '10', mode: 'apply', confirmTenant: '10', operator: 'ops-1' });
  const recovered = applied.changed.find((t) => t.id === started.taskId);
  assert.equal(recovered.state, 'escalated', 'router tasks have no retry budget: an interrupted task needs a person');
  assert.equal(recovered.escalationReason, 'retry_budget_exhausted');
  const last = (await restarted.listEvents({ tenantId: '10', taskId: started.taskId })).at(-1);
  assert.equal(last.detail.actor, 'operator:ops-1', 'operator attribution recorded');
  assert.deepEqual(await durable(pool, mission.id), before, 'the crashed owner wrote nothing durable');
  // The killed owner's fence can never be used again.
  await assert.rejects(restarted.requireOwner({ tenantId: '10', taskId: started.taskId, ownershipToken: 'x', fenceEpoch: started.fenceEpoch }), { code: 'durable_task_fenced' });
});

test('request receipts link to their task before dispatch; replays and conflicts report live task state', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { engine, mission } = await missionFixture(pool);
  const loadTask = async (taskId) => summarizeTask(await store.get({ tenantId: '10', taskId }));
  const keyed = (run) => runKeyedRequest({ pool, tenantId: '10', scope: `amo_execute:${mission.id}`, idempotencyKey: `e2e-receipt-${seq}-key`,
    request: { intent: 'EDIT_PLAN', missionId: mission.id }, loadTask, run });
  let linkedBeforeDispatch = null;
  const first = await keyed(async (receipt) => {
    const routed = await route(engine, mission, pool, store, {
      context: {
        durableTaskKey: `receipt:${receipt.scope}:${receipt.idempotencyKey}`,
        durableTaskInput: { receiptFingerprint: receipt.requestFingerprint },
        onDurableTaskAccepted: async (task) => { await receipt.linkTask(task.id); linkedBeforeDispatch = task.state; },
      },
    });
    return { durableTask: routed.durableTask, execution: { state: routed.execution.state } };
  });
  assert.equal(first.status, 200);
  assert.equal(linkedBeforeDispatch, 'queued', 'receipt linked while the task was still queued (before any specialist ran)');
  const receiptRow = (await pool.query('SELECT task_id FROM max_request_receipts WHERE tenant_id=$1 AND idempotency_key=$2', ['10', `e2e-receipt-${seq}-key`])).rows[0];
  assert.equal(receiptRow.task_id, first.body.durableTask.id);
  let reran = false;
  const replay = await keyed(async () => { reran = true; return {}; });
  assert.equal(reran, false);
  assert.equal(replay.body.replayed, true);
  assert.equal(replay.body.durableTask.state, 'completed', 'replay carries the live task state');
  // The receipt's task link is immutable, and only same-tenant tasks may be linked.
  await assert.rejects(pool.query('UPDATE max_request_receipts SET task_id = NULL WHERE tenant_id=$1 AND idempotency_key=$2', ['10', `e2e-receipt-${seq}-key`]), { code: '23514' });
  await assert.rejects(pool.query(`INSERT INTO max_request_receipts (tenant_id, scope, idempotency_key, request_fingerprint, state, owner_token, task_id, created_at, updated_at)
    VALUES ('99','s','cross-tenant-key',$1,'in_progress','o',$2,now(),now())`, ['a'.repeat(64), first.body.durableTask.id]), { code: '23503' });
});

test('operator cancel and revalidation through the recovery service are tenant-scoped and attributed', { timeout: 60000 }, async () => {
  const { pool } = await db(); const store = createDurableExecutionOwnership(pool);
  const { mission } = await missionFixture(pool);
  const accepted = await store.accept({ tenantId: '10', logicalKey: `e2e-ops-${seq}`, specialist: 'operator', missionId: mission.id,
    dependencyRefs: [{ id: mission.id, version: '1' }], retryBudget: 2 });
  await store.claimTask({ tenantId: '10', taskId: accepted.task.id, ownerToken: 'w', leaseMs: 1000 });
  await pool.query("UPDATE max_durable_execution_tasks SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1", [accepted.task.id]);
  await runTaskRecovery(pool, { tenantId: '10', mode: 'apply', confirmTenant: '10', operator: 'ops' });
  await assert.rejects(runTaskRecovery(pool, { tenantId: '10', mode: 'revalidate', confirmTenant: '10', operator: 'ops', taskId: accepted.task.id }),
    { code: 'recovery_dependencies_required' });
  const drift = await runTaskRecovery(pool, { tenantId: '10', mode: 'revalidate', confirmTenant: '10', operator: 'ops', taskId: accepted.task.id,
    dependencyRefs: [{ id: mission.id, version: '2' }] });
  assert.equal(drift.task.state, 'blocked_dependency');
  // Another tenant cannot touch this task.
  await assert.rejects(runTaskRecovery(pool, { tenantId: '11', mode: 'cancel', confirmTenant: '11', operator: 'ops', taskId: accepted.task.id }),
    { code: 'durable_task_state_conflict' });
  const cancelled = await runTaskRecovery(pool, { tenantId: '10', mode: 'cancel', confirmTenant: '10', operator: 'ops', taskId: accepted.task.id, reason: 'superseded' });
  assert.equal(cancelled.task.state, 'cancelled');
  assert.equal((await store.listEvents({ tenantId: '10', taskId: accepted.task.id })).at(-1).detail.actor, 'operator:ops');
});

test('durable tasks are off unless explicitly enabled', async () => {
  const { pool } = await db();
  assert.equal(resolveDurableTasks({ pool }, {}), null);
  assert.equal(resolveDurableTasks({ pool }, { MAX_DURABLE_TASKS: 'false' }), null);
  assert.equal(resolveDurableTasks({}, { MAX_DURABLE_TASKS: 'true' }), null, 'never without a pool');
  assert.equal(typeof resolveDurableTasks({ pool }, { MAX_DURABLE_TASKS: 'true' }).claimTask, 'function');
});
