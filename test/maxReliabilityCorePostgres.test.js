'use strict';

/**
 * MAX-P0-003 — Reliability Core on real, disposable PostgreSQL.
 *
 * Covers: the pooled-connection advisory-lock defect (reproduced against the
 * raw session-lock pattern, then shown fixed), transaction-scoped mission
 * leases across processes and pooled connections, lock timeout, crash release,
 * tenant isolation, lost-update prevention by compare-and-set, TME commit and
 * rollback on the lease connection, the external-action ledger (atomic claims,
 * replay, fingerprint reuse, outcome-unknown, no blind retry, recovery,
 * evidence-backed reconcile), durable request receipts, and fail-closed
 * behaviour without the migration. Integration tests may not skip.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');

const amo = require('../packages/acquisition-mission');
const TP = require('../packages/acquisition-mission/TransactionalPersistence');
const { executeMissionStage, bumpMissionVersion } = require('../packages/acquisition-mission/TransactionalExecution');
const persistence = require('../services/acquisitionMissionPersistence');
const { ensureExecutionAuditSchema } = persistence;
const { ensureOutboundExecutionSchema } = require('../services/acquisitionMissionOutboundPersistence');
const { ensureAcquisitionKnowledgeSchema } = require('../services/acquisitionKnowledgePersistence');
const { acquireMissionLease, LEASE_ERRORS } = require('../services/missionLease');
const {
  createExternalActionLedger, requestFingerprint, classifyProviderResult, STATES,
} = require('../services/maxReliability/externalActionLedger');
const { guardedDispatch } = require('../services/maxReliability/guardedDispatch');
const { createRequestReceipts } = require('../services/maxReliability/requestReceipts');
const { runKeyedRequest } = require('../services/maxReliability/keyedRequest');
const { recoverInterruptedExecution } = require('../services/maxReliability/executionRecovery');
const { listReliabilityAudit } = require('../services/maxReliability/reliabilityAudit');

const MIGRATION = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const WORKER = path.join(__dirname, 'helpers/maxReliabilityWorker.cjs');

let fixture;
async function db(t) {
  if (fixture) return fixture;
  const instance = await startDisposablePostgres('max-p0-003-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 8 });
  const pool2 = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 4 });
  await persistence.ensureAcquisitionMissionSchema(pool);
  await ensureExecutionAuditSchema(pool);
  await ensureOutboundExecutionSchema(pool);
  await ensureAcquisitionKnowledgeSchema(pool);
  await pool.query(fs.readFileSync(MIGRATION, 'utf8'));
  // Re-running the operator migration is a no-op.
  await pool.query(fs.readFileSync(MIGRATION, 'utf8'));
  fixture = { instance, pool, pool2, connectionString: instance.connectionString };
  return fixture;
}
test.after(async () => {
  if (!fixture) return;
  await fixture.pool2.end().catch(() => {});
  await closePostgresFixture(fixture.pool, fixture.instance);
});

let seq = 0;
function missionRow(overrides = {}) {
  seq += 1;
  return {
    id: overrides.id || `m_p0003_${process.pid}_${seq}`,
    tenantId: '10',
    stage: 'understand',
    status: 'Planning',
    objective: 'Acquire facility buyers in Manchester NH.',
    priority: 'normal',
    version: 0,
    updatedAt: '2026-11-25T00:00:00.000Z',
    ...overrides,
  };
}

async function advisoryLocks(pool) {
  return (await pool.query("SELECT pid, objid, granted FROM pg_locks WHERE locktype = 'advisory'")).rows;
}

function runWorker(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [WORKER, JSON.stringify(args)], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => { stdout += d; });
    child.stderr.on('data', (d) => { stderr += d; });
    child.on('error', reject);
    child.on('exit', (code) => {
      const line = stdout.trim().split('\n').filter(Boolean).pop();
      try {
        resolve({ exitCode: code, ...(line ? JSON.parse(line) : {}), stderr });
      } catch (err) {
        reject(new Error(`worker output not JSON (exit ${code}): ${stdout} ${stderr}`));
      }
    });
  });
}

function tmpFile(name) {
  return path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'max-p0-003-')), name);
}

// ───────────────────────────── locking ─────────────────────────────

test('REPRODUCTION: session-level advisory lock via pool.query leaks onto a pooled connection', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  // The pre-MAX-P0-003 pattern issued lock and unlock as two independent
  // pool.query() statements; pg-pool runs each on whichever idle connection it
  // hands out. Model that explicitly: the lock lands on A, the unlock on B.
  const a = await pool.connect();
  const b = await pool.connect();
  try {
    assert.equal((await a.query('SELECT pg_try_advisory_lock($1, $2) AS locked', [750825, 424242])).rows[0].locked, true);
    const unlock = await b.query('SELECT pg_advisory_unlock($1, $2) AS unlocked', [750825, 424242]);
    assert.equal(unlock.rows[0].unlocked, false, 'unlock on another connection is a silent no-op (WARNING only)');
    const leaked = (await advisoryLocks(pool)).filter((row) => Number(row.objid) === 424242);
    assert.equal(leaked.length, 1, 'lock remains held after the "release"');
    assert.equal((await pool2.query('SELECT pg_try_advisory_lock($1, $2) AS locked', [750825, 424242])).rows[0].locked,
      false, 'a second process is locked out indefinitely');
    // Mutual-exclusion bypass: any later request handed connection A "acquires" it re-entrantly.
    assert.equal((await a.query('SELECT pg_try_advisory_lock($1, $2) AS locked', [750825, 424242])).rows[0].locked,
      true, 'session locks are re-entrant: the next holder of the pooled connection walks straight in');
    await a.query('SELECT pg_advisory_unlock_all()');
  } finally {
    a.release();
    b.release();
  }
});

test('fixed: mission lease releases on the same connection and never leaks', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  TP.resetMissionDurableLocksForTests();
  for (let i = 0; i < 20; i += 1) {
    const missionId = `leak-check-${i}`;
    const held = await TP.acquireMissionDurableLock(missionId, pool, { tryOnly: true });
    assert.equal(held.acquired, true);
    const busy = await pool.connect(); // hold another pooled connection while releasing
    await TP.releaseMissionDurableLock(missionId);
    busy.release();
    assert.deepEqual(await advisoryLocks(pool), [], `no advisory lock remains after release #${i}`);
    const other = await acquireMissionLease(pool2, { missionId, mode: 'try' });
    await other.rollback();
  }
});

test('worker advisory lock (Mira/warm-routing agents): single-flight and never leaks onto the pool', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  const { withWorkerAdvisoryLock } = require('../utils/workerAdvisoryLock');
  let release;
  const gate = new Promise((r) => { release = r; });
  const first = withWorkerAdvisoryLock(pool, 990001, async () => { await gate; return 'done'; });
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(await withWorkerAdvisoryLock(pool2, 990001, async () => assert.fail('must not run')),
    { skipped: true, reason: 'worker_already_running' });
  const busy = await Promise.all([pool.connect(), pool.connect()]);
  release();
  assert.equal(await first, 'done');
  busy.forEach((c) => c.release());
  assert.deepEqual(await advisoryLocks(pool), [], 'released on the connection that took it');
  await assert.rejects(withWorkerAdvisoryLock(pool, 990001, async () => { throw new Error('work failed'); }), /work failed/);
  assert.deepEqual(await advisoryLocks(pool), [], 'released after a failure too');
});

test('lease contention across pools: try fails fast, wait times out explicitly, release unblocks', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  const lease = await acquireMissionLease(pool, { missionId: 'contended', mode: 'try' });
  await assert.rejects(acquireMissionLease(pool2, { missionId: 'contended', mode: 'try' }), { code: LEASE_ERRORS.BUSY });
  const started = Date.now();
  await assert.rejects(acquireMissionLease(pool2, { missionId: 'contended', mode: 'wait', lockTimeoutMs: 300 }), { code: LEASE_ERRORS.TIMEOUT });
  assert.ok(Date.now() - started >= 250, 'waited for the lock timeout');
  const waiter = acquireMissionLease(pool2, { missionId: 'contended', mode: 'wait', lockTimeoutMs: 5000 });
  await new Promise((r) => setTimeout(r, 100));
  await lease.commit();
  const acquired = await waiter;
  assert.equal(acquired.isOpen(), true);
  await acquired.rollback();
  assert.deepEqual(await advisoryLocks(pool), []);
});

test('lock identity is the canonical mission id: competing versions and wrong-tenant callers contend for one lock', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  const mission = missionRow({ version: 3 });
  await persistence.persistStageCommit({ mission }, pool, { skipEnsure: true });
  const lease = await acquireMissionLease(pool, { missionId: mission.id, tenantId: '10' });
  // Same mission, padded id, other "version" in hand, no tenant supplied: still the same lock.
  await assert.rejects(acquireMissionLease(pool2, { missionId: `  ${mission.id} ` }), { code: LEASE_ERRORS.BUSY });
  await lease.rollback();
  // Tenant isolation: another tenant cannot lease (or learn about) this mission.
  await assert.rejects(acquireMissionLease(pool2, { missionId: mission.id, tenantId: '2' }), { code: LEASE_ERRORS.TENANT_MISMATCH });
  assert.deepEqual(await advisoryLocks(pool), [], 'a refused lease holds nothing');
});

test('dedicated-client session holders (Anchor recovery) and lease holders exclude each other on one key', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  const { MISSION_SESSION_LOCK_SQL, MISSION_SESSION_UNLOCK_SQL, missionLockText } = require('../services/missionLease');
  const id = 'recovery-vs-lease';
  const dedicated = await pool.connect();
  try {
    assert.equal((await dedicated.query(MISSION_SESSION_LOCK_SQL, [missionLockText(id)])).rows[0].locked, true);
    await assert.rejects(acquireMissionLease(pool2, { missionId: id }), { code: LEASE_ERRORS.BUSY }, 'lease refused while recovery holds it');
    assert.equal((await dedicated.query(MISSION_SESSION_UNLOCK_SQL, [missionLockText(id)])).rows[0].unlocked, true);
    const lease = await acquireMissionLease(pool2, { missionId: id });
    assert.equal((await dedicated.query(MISSION_SESSION_LOCK_SQL, [missionLockText(id)])).rows[0].locked, false,
      'recovery refused while a lease holds it');
    await lease.rollback();
    assert.equal((await dedicated.query(MISSION_SESSION_LOCK_SQL, [missionLockText(id)])).rows[0].locked, true);
    await dedicated.query(MISSION_SESSION_UNLOCK_SQL, [missionLockText(id)]);
  } finally {
    dedicated.release();
  }
  assert.deepEqual(await advisoryLocks(pool), []);
});

test('crash while holding a lease: no partial write, lock released, mission re-leasable', { timeout: 60000 }, async (t) => {
  const { pool, connectionString } = await db(t);
  const mission = missionRow();
  await persistence.persistStageCommit({ mission }, pool, { skipEnsure: true });
  const crashed = await runWorker({ op: 'lease-write-then-crash', connectionString,
    mission: { ...mission, objective: 'UNCOMMITTED WRITE', version: 1, updatedAt: '2026-11-25T01:00:00.000Z' } });
  assert.equal(crashed.held, true);
  assert.equal(crashed.exitCode, 137);
  // The backend notices the dead socket and aborts the transaction.
  let locks = [];
  for (let i = 0; i < 100; i += 1) {
    locks = await advisoryLocks(pool);
    if (!locks.length) break;
    await new Promise((r) => setTimeout(r, 50));
  }
  assert.deepEqual(locks, [], 'lock released by the crash');
  const row = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [mission.id])).rows[0];
  assert.equal(row.payload.objective, mission.objective, 'uncommitted write rolled back');
  const lease = await acquireMissionLease(pool, { missionId: mission.id, tenantId: '10' });
  await lease.rollback();
});

test('terminated lease backend: commit fails explicitly as mission_lease_lost and nothing is persisted', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  const mission = missionRow();
  await persistence.persistStageCommit({ mission }, pool, { skipEnsure: true });
  const lease = await acquireMissionLease(pool, { missionId: mission.id, tenantId: '10' });
  await persistence.persistStageCommit({ mission: { ...mission, objective: 'LOST', version: 1 } }, pool, { skipEnsure: true, lease });
  const pid = (await lease.client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  await pool2.query('SELECT pg_terminate_backend($1)', [pid]);
  await assert.rejects(lease.commit(), (err) => err.code === LEASE_ERRORS.LOST);
  const row = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [mission.id])).rows[0];
  assert.equal(row.payload.objective, mission.objective);
});

// ─────────────────────── lost updates / CAS ───────────────────────

test('concurrent competing updates from separate processes: exactly one commit, no lost update', { timeout: 90000 }, async (t) => {
  const { pool, connectionString } = await db(t);
  const base = missionRow({ version: 3, updatedAt: '2026-11-25T02:00:00.000Z' });
  await persistence.persistStageCommit({ mission: base }, pool, { skipEnsure: true });
  const expectedFingerprint = persistence.missionFingerprint(base);
  const goFile = tmpFile('go');
  const workers = Array.from({ length: 8 }, (_, i) => runWorker({
    op: 'stage-commit', connectionString, goFile, expectedFingerprint,
    mission: { ...base, version: 4, objective: `writer-${i}`, updatedAt: `2026-11-25T02:00:0${i}.000Z` },
  }));
  await new Promise((r) => setTimeout(r, 400));
  fs.writeFileSync(goFile, 'go');
  const results = await Promise.all(workers);
  const winners = results.filter((r) => r.ok);
  assert.equal(winners.length, 1, JSON.stringify(results));
  for (const loser of results.filter((r) => !r.ok)) assert.equal(loser.code, 'mission_version_conflict');
  const row = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [base.id])).rows[0];
  assert.equal(row.payload.objective, winners[0].objective, 'durable row is exactly the winner');
  assert.equal(Number(row.payload.version), 4);
  assert.deepEqual(await advisoryLocks(pool), []);
});

test('stale writer is refused: older version, changed fingerprint, and cross-tenant overwrite', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const v5 = missionRow({ version: 5, updatedAt: '2026-11-25T03:00:00.000Z', objective: 'current' });
  await persistence.persistStageCommit({ mission: v5 }, pool, { skipEnsure: true });
  // A writer holding version 4 cannot regress the mission, even without a fingerprint.
  await assert.rejects(persistence.persistStageCommit({ mission: { ...v5, version: 4, objective: 'stale' } }, pool, { skipEnsure: true }),
    { code: 'mission_version_conflict' });
  // Same version number but read before another writer changed it.
  await assert.rejects(persistence.persistStageCommit({ mission: { ...v5, objective: 'stale-same-version' } }, pool,
    { skipEnsure: true, expectedFingerprint: '5|2026-11-25T02:59:59.000Z' }), { code: 'mission_version_conflict' });
  // Another tenant writing the same mission id is refused and learns nothing.
  await assert.rejects(persistence.persistStageCommit({ mission: { ...v5, tenantId: '2', version: 9, objective: 'hijack' } }, pool,
    { skipEnsure: true, skipGlobalLock: true }), { code: 'mission_tenant_mismatch' });
  const row = (await pool.query('SELECT tenant_id, payload FROM acquisition_missions WHERE id = $1', [v5.id])).rows[0];
  assert.equal(row.tenant_id, '10');
  assert.equal(row.payload.objective, 'current');
});

test('hydrate baseline matches the compare-and-set: column/payload and Date/string forms never cause false conflicts', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const { createAcquisitionMissionRuntime, resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
  TP.resetMissionDurableLocksForTests();
  resetAcquisitionMissionRuntime();
  const mission = missionRow({ tenantId: '41', version: 2, updatedAt: '2026-11-25T04:00:00.123Z' });
  await persistence.persistStageCommit({ mission }, pool, { skipEnsure: true });
  // The updated_at column deliberately disagrees with the payload (as fixtures and older writers produce).
  await pool.query("UPDATE acquisition_missions SET updated_at = now() - interval '3 days' WHERE id = $1", [mission.id]);
  const runtime = createAcquisitionMissionRuntime({ pool, persist: true });
  await runtime.hydrate('41', { pool, persist: true });
  const engine = runtime.engine();
  assert.equal(engine.durableBaseline.get(mission.id), '2|2026-11-25T04:00:00.123Z');
  engine.store.putMission({ ...engine.get(mission.id, '41'), title: 'first write' });
  await runtime.persistMissionState(mission.id, { pool, persist: true });
  engine.store.putMission({ ...engine.get(mission.id, '41'), title: 'second write from the same engine' });
  await runtime.persistMissionState(mission.id, { pool, persist: true });
  const row = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [mission.id])).rows[0];
  assert.equal(row.payload.title, 'second write from the same engine');

  // A different writer changes it; the first engine (not re-hydrated) is refused.
  await persistence.persistStageCommit({ mission: { ...row.payload, title: 'other writer', updatedAt: '2026-11-25T05:00:00.000Z' } }, pool, { skipEnsure: true });
  engine.store.putMission({ ...engine.get(mission.id, '41'), title: 'stale engine' });
  await assert.rejects(runtime.persistMissionState(mission.id, { pool, persist: true }), (err) => /changed since it was read/.test(err.message));
  resetAcquisitionMissionRuntime();
});

// ─────────────────────── TME on the lease ───────────────────────

async function tmeFixture(pool) {
  TP.resetMissionDurableLocksForTests();
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: 'Acquire facility buyers in Manchester NH.', targetSegment: 'Facilities' });
  await persistence.persistStageCommit({ mission: engine.get(mission.id, '10') }, pool, { skipEnsure: true });
  engine.durableBaseline = new Map([[mission.id, persistence.missionFingerprint(engine.get(mission.id, '10'))]]);
  return { engine, mission };
}

function stageInput(engine, mission, pool, overrides = {}) {
  return {
    engine,
    missionId: mission.id,
    tenantId: '10',
    pool,
    specialist: 'operator',
    stage: 'understand',
    validatePreconditions: () => ({}),
    execute: async () => ({ ok: true }),
    commit: async ({ transactionId }) => {
      const current = engine.get(mission.id, '10');
      const next = { ...current, title: `committed-${transactionId}` };
      bumpMissionVersion(next, transactionId);
      engine.store.putMission(next);
      return {};
    },
    persistDurable: persistence.bindStagePersistDurable({ pool, preinitializedSchema: true }, engine, '10'),
    ...overrides,
  };
}

test('TME commits the stage on the lease connection and releases the lock', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const { engine, mission } = await tmeFixture(pool);
  let lockSeenDuringExecute = 0;
  const result = await executeMissionStage(stageInput(engine, mission, pool, {
    execute: async () => { lockSeenDuringExecute = (await advisoryLocks(pool)).length; return { ok: true }; },
  }));
  assert.equal(result.committed, true);
  assert.equal(lockSeenDuringExecute, 1, 'the lease is held while the specialist runs');
  const row = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [mission.id])).rows[0];
  assert.equal(Number(row.payload.version), 1);
  assert.equal(row.payload.title, `committed-${result.transactionId}`);
  assert.deepEqual(await advisoryLocks(pool), []);
  assert.equal(TP.isGlobalLockHeld(mission.id), false);
});

test('TME failure injection: specialist throw, persistence failure, and lost lease all roll back durably', { timeout: 60000 }, async (t) => {
  const { pool, pool2 } = await db(t);
  const { engine, mission } = await tmeFixture(pool);
  const durable = async () => (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [mission.id])).rows[0].payload;
  const before = await durable();

  await assert.rejects(executeMissionStage(stageInput(engine, mission, pool, {
    execute: async () => { throw new Error('specialist exploded'); },
  })), (err) => err.rollback === true);
  assert.deepEqual(await durable(), before);
  assert.deepEqual(await advisoryLocks(pool), []);

  await assert.rejects(executeMissionStage(stageInput(engine, mission, pool, {
    persistDurable: async (ctx) => {
      await persistence.persistStageCommit({ mission: engine.get(mission.id, '10') }, pool, { skipEnsure: true, lease: ctx.lease });
      throw new Error('injected failure after the durable write, before commit');
    },
  })), (err) => err.rollback === true);
  assert.deepEqual(await durable(), before, 'write inside the lease never became durable');
  assert.equal(Number(engine.get(mission.id, '10').version), 0, 'in-memory state restored');
  assert.deepEqual(await advisoryLocks(pool), []);

  await assert.rejects(executeMissionStage(stageInput(engine, mission, pool, {
    execute: async () => {
      const holder = (await advisoryLocks(pool))[0];
      await pool2.query('SELECT pg_terminate_backend($1)', [holder.pid]);
      return { ok: true };
    },
  })), (err) => err.rollback === true);
  assert.deepEqual(await durable(), before, 'lost lease: nothing persisted');
  assert.deepEqual(await advisoryLocks(pool), []);

  const ok = await executeMissionStage(stageInput(engine, mission, pool));
  assert.equal(ok.committed, true, 'mission recovers and commits after the failures');
});

test('TME against a stale engine is refused: another process committed first', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const { engine, mission } = await tmeFixture(pool);
  const other = { ...engine.get(mission.id, '10'), title: 'other process', version: 1, updatedAt: '2026-11-25T09:00:00.000Z' };
  await persistence.persistStageCommit({ mission: other }, pool, { skipEnsure: true });
  await assert.rejects(executeMissionStage(stageInput(engine, mission, pool)),
    (err) => err.rollback === true && /changed since it was read/.test(err.message));
  const row = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [mission.id])).rows[0];
  assert.equal(row.payload.title, 'other process', 'no lost update');
});

test('same-process concurrent TME attempts: one commits, the other is rejected before execution', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const { engine, mission } = await tmeFixture(pool);
  let executions = 0;
  let release;
  const gate = new Promise((r) => { release = r; });
  const slow = executeMissionStage(stageInput(engine, mission, pool, {
    execute: async () => { executions += 1; await gate; return { ok: true }; },
  }));
  await new Promise((r) => setTimeout(r, 50));
  await assert.rejects(executeMissionStage(stageInput(engine, mission, pool, {
    execute: async () => { executions += 1; return { ok: true }; },
  })), (err) => err.code === 'tme_transaction_overlap');
  release();
  assert.equal((await slow).committed, true);
  assert.equal(executions, 1);
});

// ───────────────────── external-action ledger ─────────────────────

function claimInput(tenantId, key, extra = {}) {
  return {
    tenantId, idempotencyKey: key, actionKind: 'amo_outbound_email',
    requestFingerprint: requestFingerprint({ key, to: 'owner@example.test', body: 'hello' }),
    missionId: 'm-ledger', subjectRef: 'prospect:1', provider: 'brevo', ...extra,
  };
}

test('concurrent identical dispatches from separate processes: one provider call, one receipt', { timeout: 90000 }, async (t) => {
  const { pool, connectionString } = await db(t);
  const providerLog = tmpFile('provider.log');
  fs.writeFileSync(providerLog, '');
  const goFile = tmpFile('go');
  const claim = claimInput('10', 'concurrent-send-1');
  const workers = Array.from({ length: 10 }, () => runWorker({
    op: 'claim-dispatch', connectionString, goFile, providerLog, claim, dispatchDelayMs: 150,
  }));
  await new Promise((r) => setTimeout(r, 400));
  fs.writeFileSync(goFile, 'go');
  const results = await Promise.all(workers);
  const calls = fs.readFileSync(providerLog, 'utf8').trim().split('\n').filter(Boolean);
  assert.equal(calls.length, 1, `provider invoked once: ${JSON.stringify(results)}`);
  assert.equal(results.filter((r) => r.outcome === 'dispatched').length, 1);
  for (const r of results.filter((x) => x.outcome !== 'dispatched')) {
    assert.ok(r.outcome === 'blocked' || r.outcome === 'replay', JSON.stringify(r));
  }
  const rows = (await pool.query('SELECT state, attempt FROM max_external_action_claims WHERE tenant_id = $1 AND idempotency_key = $2', ['10', claim.idempotencyKey])).rows;
  assert.deepEqual(rows, [{ state: STATES.PROVIDER_ACKNOWLEDGED, attempt: 1 }]);

  // Replay after completion returns the original receipt; still one provider call.
  const replay = await runWorker({ op: 'claim-dispatch', connectionString, providerLog, claim });
  assert.equal(replay.outcome, 'replay');
  assert.equal(fs.readFileSync(providerLog, 'utf8').trim().split('\n').length, 1);
});

test('idempotency key reused with a different request fingerprint is rejected before the provider', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const ledger = createExternalActionLedger(pool);
  let calls = 0;
  const dispatch = async () => { calls += 1; return { success: true, providerMessageId: 'x1' }; };
  assert.equal((await guardedDispatch({ ledger, claim: claimInput('10', 'fp-1'), dispatch })).outcome, 'dispatched');
  const reused = await guardedDispatch({ ledger, dispatch,
    claim: { ...claimInput('10', 'fp-1'), requestFingerprint: requestFingerprint({ different: true }) } });
  assert.equal(reused.outcome, 'blocked');
  assert.equal(reused.code, 'idempotency_key_reused');
  assert.equal(calls, 1);
});

test('crash after dispatch, before acknowledgement: outcome unknown, never retried, resolved only by evidence', { timeout: 90000 }, async (t) => {
  const { pool, connectionString } = await db(t);
  const providerLog = tmpFile('provider.log');
  fs.writeFileSync(providerLog, '');
  const claim = claimInput('10', 'crash-after-dispatch');
  const crashed = await runWorker({ op: 'dispatch-then-crash', connectionString, providerLog, claim });
  assert.equal(crashed.exitCode, 137);

  // Every retry is refused before the provider while the outcome is unknown.
  for (let i = 0; i < 3; i += 1) {
    const retry = await runWorker({ op: 'claim-dispatch', connectionString, providerLog, claim });
    assert.equal(retry.outcome, 'blocked');
    assert.equal(retry.code, 'external_action_outcome_unknown');
  }
  assert.equal(fs.readFileSync(providerLog, 'utf8').trim().split('\n').length, 1, 'no blind retry');

  // Recovery (plan → apply) escalates dispatch_attempted → outcome_unknown.
  const later = () => new Date(Date.now() + 60 * 60 * 1000);
  const plan = await recoverInterruptedExecution(pool, { tenantId: '10', now: later });
  assert.equal(plan.applied, false);
  assert.ok(plan.counts.staleDispatches >= 1);
  const applied = await recoverInterruptedExecution(pool, { tenantId: '10', now: later, apply: true, actor: 'operator:test' });
  assert.ok(applied.changed.unknown.length >= 1);
  assert.equal(applied.escalationRequired, true);
  const ledger = createExternalActionLedger(pool);
  assert.equal((await ledger.get('10', claim.idempotencyKey)).state, STATES.OUTCOME_UNKNOWN);
  assert.equal((await guardedDispatch({ ledger, claim, dispatch: async () => assert.fail('must not dispatch') })).code,
    'external_action_outcome_unknown');

  // Reconcile requires evidence; with it, the action becomes verified and replays.
  await assert.rejects(ledger.reconcile({ tenantId: '10', idempotencyKey: claim.idempotencyKey, resolution: 'verified', evidence: {} }),
    { code: 'external_action_invalid' });
  const verified = await ledger.reconcile({ tenantId: '10', idempotencyKey: claim.idempotencyKey, resolution: 'verified',
    evidence: { source: 'brevo_webhook_delivered', providerRef: 'msg-reconciled', recipient: 'owner@example.test' }, actor: 'operator:1' });
  assert.equal(verified.state, STATES.VERIFIED);
  assert.equal((await guardedDispatch({ ledger, claim, dispatch: async () => assert.fail('must not dispatch') })).outcome, 'replay');
});

test('crash before the dispatch marker: recovery abandons the claim and a retry may dispatch once', { timeout: 60000 }, async (t) => {
  const { pool, connectionString } = await db(t);
  const claim = claimInput('10', 'crash-before-dispatch');
  const crashed = await runWorker({ op: 'claim-then-crash', connectionString, claim });
  assert.equal(crashed.outcome, 'claimed');
  const ledger = createExternalActionLedger(pool);
  assert.equal((await guardedDispatch({ ledger, claim, dispatch: async () => assert.fail('held') })).code, 'external_action_in_progress');
  await recoverInterruptedExecution(pool, { tenantId: '10', apply: true, actor: 'operator:test', now: () => new Date(Date.now() + 3600e3) });
  assert.equal((await ledger.get('10', claim.idempotencyKey)).state, STATES.ABANDONED_NOT_DISPATCHED);
  let calls = 0;
  const retry = await guardedDispatch({ ledger, claim, dispatch: async () => { calls += 1; return { success: true, providerMessageId: 'm2' }; } });
  assert.equal(retry.outcome, 'dispatched');
  assert.equal(calls, 1);
  assert.equal((await ledger.get('10', claim.idempotencyKey)).attempt, 2);
});

test('provider exception → unknown; timeout → unknown; definitive rejection → re-claimable', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const ledger = createExternalActionLedger(pool);
  await assert.rejects(guardedDispatch({ ledger, claim: claimInput('10', 'throws'),
    dispatch: async () => { throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }); } }), { code: 'ECONNRESET' });
  assert.equal((await ledger.get('10', 'throws')).state, STATES.OUTCOME_UNKNOWN);

  const timedOut = await guardedDispatch({ ledger, claim: claimInput('10', 'timeout'),
    dispatch: async () => ({ success: false, providerErrorCode: 'ECONNABORTED', providerBoundaryCrossed: true }) });
  assert.equal(timedOut.ledgerState, STATES.OUTCOME_UNKNOWN);

  const rejected = await guardedDispatch({ ledger, claim: claimInput('10', 'rejected'),
    dispatch: async () => ({ success: false, providerErrorCode: 'brevo_http_400', providerBoundaryCrossed: true }) });
  assert.equal(rejected.ledgerState, STATES.FAILED_DEFINITIVE);
  const again = await guardedDispatch({ ledger, claim: claimInput('10', 'rejected'),
    dispatch: async () => ({ success: true, providerMessageId: 'ok' }) });
  assert.equal(again.outcome, 'dispatched');
  assert.equal((await ledger.get('10', 'rejected')).attempt, 2);

  assert.equal(classifyProviderResult({ success: false, providerErrorCode: 'brevo_http_429', providerBoundaryCrossed: true }).state, STATES.OUTCOME_UNKNOWN);
  assert.equal(classifyProviderResult({ success: false, providerErrorCode: 'brevo_http_503', providerBoundaryCrossed: true }).state, STATES.OUTCOME_UNKNOWN);
  assert.equal(classifyProviderResult({ success: false, providerBoundaryCrossed: false }).state, STATES.FAILED_DEFINITIVE);
  assert.equal(classifyProviderResult({ success: false }).state, STATES.OUTCOME_UNKNOWN);
});

test('pre-dispatch guard refusal releases the claim without invoking the provider', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const ledger = createExternalActionLedger(pool);
  await assert.rejects(guardedDispatch({ ledger, claim: claimInput('10', 'guarded'),
    beforeDispatch: async () => { throw Object.assign(new Error('budget'), { code: 'budget_or_uncertain_block' }); },
    dispatch: async () => assert.fail('provider must not be called') }), { code: 'budget_or_uncertain_block' });
  assert.equal((await ledger.get('10', 'guarded')).state, STATES.ABANDONED_NOT_DISPATCHED);
});

test('tenant isolation: claims, receipts, reads and recovery are tenant-scoped', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const ledger = createExternalActionLedger(pool);
  const a = await ledger.claim(claimInput('21', 'shared-key'));
  const b = await ledger.claim(claimInput('22', 'shared-key'));
  assert.equal(a.outcome, 'claimed');
  assert.equal(b.outcome, 'claimed', 'same key in another tenant is an independent action');
  assert.equal(await ledger.get('23', 'shared-key'), null, 'a third tenant sees nothing');
  const recovered = await recoverInterruptedExecution(pool, { tenantId: '22', apply: true, actor: 'operator:test', now: () => new Date(Date.now() + 3600e3) });
  assert.deepEqual(recovered.changed.abandoned, ['shared-key']);
  assert.equal((await ledger.get('21', 'shared-key')).state, STATES.CLAIMED, 'tenant 21 untouched by tenant 22 recovery');
  assert.equal((await ledger.listUnresolved('22')).length, 0);

  const receipts = createRequestReceipts(pool);
  const ra = await receipts.begin({ tenantId: '21', scope: 's', idempotencyKey: 'tenant-key-1', request: { a: 1 } });
  const rb = await receipts.begin({ tenantId: '22', scope: 's', idempotencyKey: 'tenant-key-1', request: { a: 1 } });
  assert.equal(ra.outcome, 'owner');
  assert.equal(rb.outcome, 'owner');
  await receipts.complete(ra, 200, { secretOfTenant21: true });
  assert.equal(await receipts.get('22', 's', 'tenant-key-1').then((r) => r.response), null);
});

// ─────────────────────────── receipts ───────────────────────────

test('keyed requests: concurrent identical → one execution; replay → original receipt; reuse → 422', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  let executions = 0;
  const run = async () => {
    executions += 1;
    await new Promise((r) => setTimeout(r, 100));
    return { ok: true, execution: executions, snapshot: { big: true } };
  };
  const call = (request, key = 'exec-key-0001') => runKeyedRequest({
    pool, tenantId: '10', scope: 'amo_execute:m1', idempotencyKey: key, request, correlationId: 'cor_test_000001', run, volatileFields: ['snapshot'],
  });
  const results = await Promise.all(Array.from({ length: 10 }, () => call({ intent: 'APPROVE' })));
  assert.equal(executions, 1);
  assert.equal(results.filter((r) => r.status === 200 && !r.body.replayed).length, 1);
  for (const r of results.filter((x) => x.status !== 200 || x.body.replayed)) {
    assert.ok(r.status === 409 || r.body.replayed === true, JSON.stringify(r.body));
  }
  const replay = await call({ intent: 'APPROVE' });
  assert.equal(replay.status, 200);
  assert.equal(replay.body.replayed, true);
  assert.equal(replay.body.execution, 1);
  assert.equal(replay.body.snapshot, undefined, 'volatile fields are not stored in the receipt');
  assert.equal(executions, 1);
  const reused = await call({ intent: 'CANCEL' });
  assert.equal(reused.status, 422);
  assert.equal(reused.body.error, 'idempotency_key_reused');
  assert.equal((await call({ intent: 'APPROVE' }, 'bad key!')).status, 400);
});

test('permanent receipts store only the allowlisted summary (no free text or payload)', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const live = { request: { id: 'cer_1', intent: 'APPROVE_PLAN', source: 'api', missionId: 'm9', question: 'call Bob at 555-0100',
    payload: { note: 'private operator note' } }, action: 'plan_approved', execution: { state: 'committed_internal', verified: false },
    executionResult: { transactionId: 'tme_1', rollbackReason: 'free text', blockReason: 'free text' }, snapshot: { big: true } };
  const receiptOf = (b) => ({ request: { id: b.request.id, intent: b.request.intent }, action: b.action,
    execution: { state: b.execution.state }, executionResult: { transactionId: b.executionResult.transactionId } });
  const first = await runKeyedRequest({ pool, tenantId: '71', scope: 'amo_execute:m9', idempotencyKey: 'receipt-min-1',
    request: { intent: 'APPROVE_PLAN' }, run: async () => live, receiptOf, volatileFields: ['snapshot'] });
  assert.equal(first.body.request.question, 'call Bob at 555-0100', 'the live response is unchanged');
  const stored = (await pool.query("SELECT response FROM max_request_receipts WHERE tenant_id = '71'")).rows[0].response;
  assert.equal(JSON.stringify(stored).includes('555-0100'), false);
  assert.equal(JSON.stringify(stored).includes('private operator note'), false);
  assert.equal(JSON.stringify(stored).includes('free text'), false);
  const replay = await runKeyedRequest({ pool, tenantId: '71', scope: 'amo_execute:m9', idempotencyKey: 'receipt-min-1',
    request: { intent: 'APPROVE_PLAN' }, run: async () => assert.fail('no re-run'), receiptOf });
  assert.equal(replay.body.replayed, true);
  assert.equal(replay.body.executionResult.transactionId, 'tme_1');
});

test('keyed requests: failure is the receipt; lock contention releases it; interrupted is never re-run', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const base = { pool, tenantId: '10', scope: 'amo_execute:m2', request: { intent: 'X' }, errorStatus: () => 409 };
  let runs = 0;
  const failed = await runKeyedRequest({ ...base, idempotencyKey: 'fail-key-0001', run: async () => { runs += 1; throw Object.assign(new Error('nope'), { code: 'amo_stage_blocked' }); } });
  assert.equal(failed.status, 409);
  const failedReplay = await runKeyedRequest({ ...base, idempotencyKey: 'fail-key-0001', run: async () => { runs += 1; return {}; } });
  assert.equal(failedReplay.body.replayed, true);
  assert.equal(failedReplay.body.error, 'amo_stage_blocked');
  assert.equal(runs, 1);

  const busy = await runKeyedRequest({ ...base, idempotencyKey: 'busy-key-0001', run: async () => { throw Object.assign(new Error('locked'), { code: 'tme_transaction_overlap' }); } });
  assert.equal(busy.body.retryable, true);
  const retried = await runKeyedRequest({ ...base, idempotencyKey: 'busy-key-0001', run: async () => ({ ok: 'second try' }) });
  assert.equal(retried.status, 200);
  assert.equal(retried.body.ok, 'second try');

  const receipts = createRequestReceipts(pool);
  const handle = await receipts.begin({ tenantId: '10', scope: 'amo_execute:m2', idempotencyKey: 'dead-key-0001', request: { intent: 'X' } });
  assert.equal(handle.outcome, 'owner');
  await recoverInterruptedExecution(pool, { tenantId: '10', apply: true, actor: 'operator:test', now: () => new Date(Date.now() + 3600e3) });
  const after = await runKeyedRequest({ ...base, idempotencyKey: 'dead-key-0001', run: async () => assert.fail('must not re-run') });
  assert.equal(after.status, 409);
  assert.equal(after.body.error, 'request_outcome_unknown');
  await assert.rejects(receipts.complete(handle, 200, {}), { code: 'request_receipt_state_conflict' });
});

test('audit trail is durable, correlated and redacted', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const ledger = createExternalActionLedger(pool);
  await guardedDispatch({ ledger, claim: claimInput('31', 'audit-1', { correlationId: 'cor_audit_000001' }),
    dispatch: async () => ({ success: true, providerMessageId: 'mid-1' }) });
  await ledger.reconcile({ tenantId: '31', idempotencyKey: 'audit-1', resolution: 'verified',
    evidence: { source: 'brevo_message_lookup', note: 'delivered to owner@example.test', apiKey: 'xkeysib-secret', body: 'Hello there' } });
  const rows = await listReliabilityAudit(pool, '31');
  assert.deepEqual(rows.map((r) => r.to_state), ['claimed', 'dispatch_attempted', 'provider_acknowledged', 'verified']);
  assert.ok(rows.slice(0, 3).every((r) => r.correlation_id === 'cor_audit_000001'));
  const text = JSON.stringify(rows);
  assert.equal(text.includes('owner@example.test'), false, 'email masked');
  assert.equal(text.includes('xkeysib-secret'), false, 'secret removed');
  assert.equal(text.includes('Hello there'), false, 'content removed');
  assert.equal(text.includes('audit-1'), false, 'subject key hashed');
});

test('recovery apply requires an operator identity and audits the run', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  await createExternalActionLedger(pool).claim(claimInput('61', 'recovery-actor'));
  const later = () => new Date(Date.now() + 3600e3);
  await assert.rejects(recoverInterruptedExecution(pool, { tenantId: '61', apply: true, now: later }), { code: 'recovery_actor_required' });
  assert.equal((await createExternalActionLedger(pool).get('61', 'recovery-actor')).state, STATES.CLAIMED, 'refused run changed nothing');
  const plan = await recoverInterruptedExecution(pool, { tenantId: '61', now: later });
  assert.equal(plan.applied, false);
  assert.equal((await listReliabilityAudit(pool, '61')).filter((r) => r.subject_kind === 'recovery_run').length, 0, 'plan writes nothing');
  await recoverInterruptedExecution(pool, { tenantId: '61', apply: true, now: later, actor: 'operator:jm' });
  const rows = await listReliabilityAudit(pool, '61');
  const run = rows.find((r) => r.event === 'recovery_applied');
  assert.equal(run.detail.actor, 'operator:jm');
  assert.equal(run.detail.abandoned, 1);
  assert.equal(rows.find((r) => r.event === 'recovery_abandoned_not_dispatched').detail.actor, 'operator:jm');
});

test('durable barriers cannot be expired: claims/audit undeletable, only in_progress receipts releasable', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  const ledger = createExternalActionLedger(pool);
  await guardedDispatch({ ledger, claim: claimInput('51', 'barrier-1'), dispatch: async () => ({ success: true, providerMessageId: 'b1' }) });
  await assert.rejects(pool.query("DELETE FROM max_external_action_claims WHERE tenant_id = '51'"), /durable idempotency barrier/);
  await assert.rejects(pool.query("DELETE FROM max_external_action_claims WHERE updated_at < now() + interval '1 day'"), /durable idempotency barrier/);
  await assert.rejects(pool.query('TRUNCATE max_external_action_claims'), /durable idempotency barrier/);
  await assert.rejects(pool.query("DELETE FROM max_reliability_audit WHERE tenant_id = '51'"), /durable idempotency barrier/);
  await assert.rejects(pool.query('TRUNCATE max_reliability_audit'), /durable idempotency barrier/);
  assert.equal((await ledger.get('51', 'barrier-1')).state, STATES.PROVIDER_ACKNOWLEDGED, 'claim survives');

  const receipts = createRequestReceipts(pool);
  const done = await receipts.begin({ tenantId: '51', scope: 's', idempotencyKey: 'barrier-done', request: { a: 1 } });
  await receipts.complete(done, 200, { ok: true });
  await assert.rejects(pool.query("DELETE FROM max_request_receipts WHERE tenant_id = '51' AND idempotency_key = 'barrier-done'"),
    /durable idempotency barrier/, 'a completed receipt cannot be expired');
  await assert.rejects(pool.query('TRUNCATE max_request_receipts'), /durable idempotency barrier/);
  const open = await receipts.begin({ tenantId: '51', scope: 's', idempotencyKey: 'barrier-open', request: { a: 1 } });
  assert.equal(open.outcome, 'owner');
  const released = await pool.query("DELETE FROM max_request_receipts WHERE tenant_id = '51' AND idempotency_key = 'barrier-open'");
  assert.equal(released.rowCount, 1, 'in_progress receipt may be released (pre-execution lock contention path)');
});

test('fails closed without the operator migration', { timeout: 60000 }, async (t) => {
  const { pool } = await db(t);
  await pool.query('CREATE SCHEMA IF NOT EXISTS no_migration');
  const bare = createTrackedPostgresPool({ connectionString: fixture.connectionString, max: 2, options: '-c search_path=no_migration' });
  t.after(() => bare.end());
  const ledger = createExternalActionLedger(bare);
  await assert.rejects(guardedDispatch({ ledger, claim: claimInput('10', 'no-schema'), dispatch: async () => assert.fail('no dispatch') }),
    { code: 'reliability_schema_missing' });
  const keyed = await runKeyedRequest({ pool: bare, tenantId: '10', scope: 's', idempotencyKey: 'no-schema-key', request: {}, run: async () => assert.fail('no run') });
  assert.equal(keyed.status, 503);
  assert.equal(keyed.body.error, 'idempotency_unavailable');
});
