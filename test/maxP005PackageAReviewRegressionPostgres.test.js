'use strict';

/**
 * MAX-P0-005 Package A — independent-review regression suite.
 *
 * Authored by the independent reviewer of PR #1168. Every test is tagged with
 * the finding/requirement it proves (R1–R8, NF-1…NF-6). Assertions describe the
 * REQUIRED behaviour, so tests for unresolved findings fail until remediated.
 * See README.md for setup, the expected baseline at c66b91c, and the
 * traceability matrix.
 *
 * Real PostgreSQL only (disposable instance via test/helpers/disposablePostgres),
 * including the real operator_objectives table. No memory substitutes.
 *
 * Where the review left the exact mechanism to the implementer, a test accepts
 * any of the documented safe outcomes (e.g. "replay OR a distinct explicit
 * code") and rejects only the unsafe or misleading one.
 */

const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createDurableExecutionOwnership } = require('../services/maxReliability/durableExecutionOwnership');
const { createExternalActionLedger } = require('../services/maxReliability/externalActionLedger');
const { runWithTaskFence } = require('../services/maxReliability/taskExecutionContext');
const policy = require('../services/maxReliability/executionOwnershipPolicy');
const persistence = require('../services/acquisitionMissionPersistence');
const { ensureOutboundExecutionSchema } = require('../services/acquisitionMissionOutboundPersistence');
const { ensureAcquisitionKnowledgeSchema } = require('../services/acquisitionKnowledgePersistence');
const objectives = require('../services/operatorObjectives');
const amo = require('../packages/acquisition-mission');
const TP = require('../packages/acquisition-mission/TransactionalPersistence');
const { bumpMissionVersion } = require('../packages/acquisition-mission/TransactionalExecution');
const { runTaskOwnedWork } = require('../packages/acquisition-mission/DurableTaskRouting');
const C = require('../packages/max/objectiveOrchestration/ObjectiveContract');

const MIG = (f) => fs.readFileSync(path.join(__dirname, '../migrations', f), 'utf8');
const OBJECTIVES_SQL = '2026-08-13-operator-objectives.sql';
const P003_SQL = '2026-11-25-max-p0-003-reliability-core.sql';
const P004_SQL = '2026-11-25-max-p0-004-durable-execution-ownership.sql';
const P005_SQL = '2026-11-27-max-p0-005-package-a-contract-roots.sql';
const P005_INDEX = 'max_durable_execution_tasks_one_objective_root';

/* ------------------------------------------------------------------------ */
/* Fixtures                                                                  */
/* ------------------------------------------------------------------------ */

let fixture; let pool; let tasks; let trustedProductionAdmissionAuthorizer;
const previousDurableFlag = process.env.MAX_DURABLE_TASKS;

function positiveFixtureAdmissionAuthorizer(fixturePool, candidate, tenantId) {
  if (fixturePool !== pool || !candidate || !candidate.id
    || String(candidate.authorizedTenantId) !== String(tenantId)) return { authorized: false };
  return { authorized: true, policy: 'isolated_review_objective_admission_fixture' };
}

async function withProductionAdmissionPolicy(work) {
  const policy = require('../services/maxReliability/objectiveAdmissionAuthorization');
  const suiteFixtureAuthorizer = policy.authorizeObjectiveAdmission;
  policy.authorizeObjectiveAdmission = trustedProductionAdmissionAuthorizer;
  try {
    return await work(policy);
  } finally {
    policy.authorizeObjectiveAdmission = suiteFixtureAuthorizer;
  }
}

before(async () => {
  const instance = await startDisposablePostgres('max-p0-005-review-');
  pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 24 });
  fixture = { instance, pool };
  // Acquisition-mission schema is needed only for the R4 persistence boundary.
  await persistence.ensureAcquisitionMissionSchema(pool);
  await persistence.ensureExecutionAuditSchema(pool);
  await ensureOutboundExecutionSchema(pool);
  await ensureAcquisitionKnowledgeSchema(pool);
  for (const f of [OBJECTIVES_SQL, P003_SQL, P004_SQL, P005_SQL]) await pool.query(MIG(f));
  tasks = createDurableExecutionOwnership(pool);
  const policy = require('../services/maxReliability/objectiveAdmissionAuthorization');
  trustedProductionAdmissionAuthorizer = policy.authorizeObjectiveAdmission;
  policy.authorizeObjectiveAdmission = positiveFixtureAdmissionAuthorizer;
  process.env.MAX_DURABLE_TASKS = 'true'; // ownership enforcement for R4
});

after(async () => {
  if (trustedProductionAdmissionAuthorizer) {
    require('../services/maxReliability/objectiveAdmissionAuthorization').authorizeObjectiveAdmission = trustedProductionAdmissionAuthorizer;
  }
  if (previousDurableFlag === undefined) delete process.env.MAX_DURABLE_TASKS;
  else process.env.MAX_DURABLE_TASKS = previousDurableFlag;
  if (fixture) await closePostgresFixture(fixture.pool, fixture.instance);
});

const FAR = '2030-01-01T00:00:00.000Z';
function contract(overrides = {}) {
  return {
    version: 1,
    successPredicates: ['durable_evidence'],
    allowedActionClasses: [],
    allowedSpecialists: ['scout'],
    allowedProviders: ['deterministic_fixture'],
    evidenceFreshnessMs: 0,
    deadlineAt: FAR,
    plan: { maxSteps: 2, maxChildren: 1, maxReplans: 0, maxRetries: 0 },
    budget: { caps: { provider_usd: 0 } },
    wakeConditions: ['objective_accepted', 'due_verification'],
    verification: { initialPollMs: 5000, maxPollMs: 10000, maxAttempts: 3, backoffMultiplier: 2, deadlineAt: FAR },
    escalationPolicy: 'operator_review',
    ...overrides,
  };
}
function widened() {
  return contract({
    version: 2,
    plan: { maxSteps: 64, maxChildren: 32, maxReplans: 8, maxRetries: 20 },
    budget: { caps: { provider_usd: 1e9 } },
  });
}
const ENV = Object.freeze({ MAX_AUTONOMOUS_OBJECTIVES: 'true', MAX_DURABLE_TASKS: 'true' });
const actor = (tenantId, id = `operator-${tenantId}`) => ({ id, authorizedTenantId: tenantId });
function options(extra = {}) {
  return { env: ENV, taskStore: tasks, objectiveService: objectives, objectiveOpts: { pool }, ...extra };
}
async function newObjective(tenantId, objectiveText = 'Verify a safe result.') {
  return objectives.createOperatorObjective({ tenantId, scope: 'operator', title: `review ${tenantId}`, objectiveText }, { pool });
}
function admit(tenantId, objectiveId, c, extra = {}, opts = options()) {
  return C.admitObjectiveRoot({ tenantId, objectiveId, contract: c, actor: actor(tenantId), ...extra }, opts);
}
/** Normalises an admission promise to 'new' | 'replay' | <error code>. */
function outcome(promise) {
  return promise.then((r) => (r.replay ? 'replay' : (r.admitted ? 'new' : 'not_admitted')), (e) => e.code || e.message);
}
async function objectiveTasks(tenantId, objectiveId) {
  const r = await pool.query(
    `SELECT id, logical_key, specialist, mission_id, execution_scope, immutable_input
       FROM max_durable_execution_tasks WHERE tenant_id=$1 AND objective_id=$2 ORDER BY created_at, id`,
    [tenantId, objectiveId]);
  return r.rows;
}
const rootShaped = (rows) => rows.filter((r) => r.mission_id == null
  && r.execution_scope && (r.execution_scope.kind === 'objective_orchestration' || r.execution_scope.role === 'root'));
async function objectiveContext(tenantId, objectiveId) {
  return (await objectives.getObjectiveById(objectiveId, { tenantId, pool })).context;
}
const tally = (xs) => xs.reduce((m, k) => { m[k] = (m[k] || 0) + 1; return m; }, {});

/* ------------------------------------------------------------------------ */
/* R1 — root uniqueness and atomic admission                                 */
/* ------------------------------------------------------------------------ */

test('R1-01 [ADV-2] concurrent different-version first admissions never create a second root (30 trials)', async () => {
  const outcomes = [];
  for (let i = 0; i < 30; i += 1) {
    const t = `r1-01-${i}`;
    const o = await newObjective(t);
    outcomes.push(...await Promise.all([1, 2].map((v) => outcome(admit(t, o.id, contract({ version: v }))))));
    assert.equal((await objectiveTasks(t, o.id)).length, 1, `trial ${i}: exactly one root task`);
  }
  assert.deepEqual(tally(outcomes), { new: 30, objective_contract_revision_conflict: 30 });
});

test('R1-02 [ADV-8] 20 identical concurrent submissions: one new root, 19 clean replays', async () => {
  const o = await newObjective('r1-02');
  const results = await Promise.all(Array.from({ length: 20 }, () => outcome(admit('r1-02', o.id, contract()))));
  assert.deepEqual(tally(results), { new: 1, replay: 19 });
  assert.equal((await objectiveTasks('r1-02', o.id)).length, 1);
});

test('R1-03 direct SQL cannot insert a second max_objective root for the same objective', async () => {
  const o = await newObjective('r1-03');
  await admit('r1-03', o.id, contract());
  await assert.rejects(
    pool.query(`INSERT INTO max_durable_execution_tasks
      (id, tenant_id, objective_id, logical_key, input_fingerprint, specialist, execution_scope, immutable_input, state)
      VALUES (gen_random_uuid(), 'r1-03', $1, 'sql-second-root', repeat('a',64), 'max_objective', '{}', '{}', 'queued')`, [o.id]),
    (e) => e.code === '23505' && e.constraint === P005_INDEX);
});

test('R1-04 [NF-2] a root-shaped task under a different specialist is refused (store path and direct SQL)', async () => {
  const o = await newObjective('r1-04');
  await admit('r1-04', o.id, contract());
  const base = C.rootTaskInput({ tenantId: 'r1-04', objective: o, contract: C.validateObjectiveContract(widened()) });
  await assert.rejects(tasks.accept({ ...base, logicalKey: 'alt-root', specialist: 'max_objective_v2' }),
    'P0-004 store must not accept a second objective_orchestration root for the objective');
  await assert.rejects(pool.query(`INSERT INTO max_durable_execution_tasks
      (id, tenant_id, objective_id, logical_key, input_fingerprint, specialist, execution_scope, immutable_input, state)
      VALUES (gen_random_uuid(), 'r1-04', $1, 'sql-alt-root', repeat('b',64), 'operator',
              '{"kind":"objective_orchestration","role":"root"}', '{}', 'queued')`, [o.id]),
  'direct SQL must not insert a second root-shaped row');
  assert.equal(rootShaped(await objectiveTasks('r1-04', o.id)).length, 1, 'exactly one root-shaped task per objective');
});

test('R1-05 Package A migration reruns idempotently', async () => {
  await pool.query(MIG(P005_SQL));
  await pool.query(MIG(P005_SQL));
  const idx = await pool.query('SELECT to_regclass($1) AS i', [P005_INDEX]);
  assert.ok(idx.rows[0].i, 'uniqueness index present after reruns');
});

test('R1-06 migration preflight refuses pre-existing duplicate roots and leaves nothing behind', async () => {
  const instance = await startDisposablePostgres('max-p0-005-review-preflight-');
  const p = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 2 });
  try {
    await p.query(MIG(P003_SQL)); await p.query(MIG(P004_SQL));
    for (const key of ['objective:o:v:1', 'objective:o:v:2']) {
      await p.query(`INSERT INTO max_durable_execution_tasks
        (id, tenant_id, objective_id, logical_key, input_fingerprint, specialist, execution_scope, immutable_input, state)
        VALUES (gen_random_uuid(), 't', '11111111-1111-1111-1111-111111111111', $1, repeat('a',64), 'max_objective', '{}', '{}', 'queued')`, [key]);
    }
    await assert.rejects(p.query(MIG(P005_SQL)), /preflight failed: duplicate objective roots/);
    assert.equal((await p.query('SELECT to_regclass($1) AS i', [P005_INDEX])).rows[0].i, null, 'no index after refusal');
    assert.equal((await p.query('SELECT 1 AS ok')).rows[0].ok, 1, 'connection usable; transaction rolled back');
  } finally {
    await closePostgresFixture(p, instance);
  }
});

test('N4 regression: migration preflight refuses duplicate objective wakes and rolls back the deduplication index', async () => {
  const instance = await startDisposablePostgres('max-p0-005-review-wake-preflight-');
  const p = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 2 });
  try {
    await p.query(MIG(P003_SQL)); await p.query(MIG(P004_SQL));
    const task = await p.query(`INSERT INTO max_durable_execution_tasks
      (id, tenant_id, objective_id, logical_key, input_fingerprint, specialist, execution_scope, immutable_input, state)
      VALUES (gen_random_uuid(), 't', '11111111-1111-1111-1111-111111111112', 'ordinary-task', repeat('b',64), 'operator', '{}', '{}', 'queued') RETURNING id`);
    for (let i = 0; i < 2; i += 1) await p.query(`INSERT INTO max_durable_execution_task_events
      (task_id, tenant_id, fence_epoch, event, detail) VALUES ($1, 't', 0, 'objective_wake', '{"wakeIdentity":"duplicate-wake"}')`, [task.rows[0].id]);
    await assert.rejects(p.query(MIG(P005_SQL)), /preflight failed: duplicate objective wakes/);
    assert.equal((await p.query(`SELECT to_regclass('max_durable_execution_task_events_one_objective_wake') AS i`)).rows[0].i, null,
      'transaction rollback leaves no partial wake-deduplication index');
  } finally {
    await closePostgresFixture(p, instance);
  }
});

test('N4 regression: a same-named malformed wake index is not accepted as the invariant', async () => {
  const instance = await startDisposablePostgres('max-p0-005-review-wake-index-');
  const p = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 2 });
  try {
    await p.query(MIG(P003_SQL)); await p.query(MIG(P004_SQL));
    await p.query(`CREATE UNIQUE INDEX max_durable_execution_task_events_one_objective_wake
      ON max_durable_execution_task_events (tenant_id, task_id, id)`);
    await assert.rejects(p.query(MIG(P005_SQL)), /objective-wake deduplication index is missing or invalid/);
    const wrong = await p.query(`SELECT i.indisunique, i.indisvalid, i.indisready, i.indnkeyatts::int AS keys
      FROM pg_index i WHERE i.indexrelid='max_durable_execution_task_events_one_objective_wake'::regclass`);
    assert.deepEqual(wrong.rows[0], { indisunique: true, indisvalid: true, indisready: true, keys: 3 },
      'the rejection is definition-based, not merely a missing/invalid-name check');
  } finally {
    await closePostgresFixture(p, instance);
  }
});

test('R1-07 read-only preflight/schema report exposes the Package A uniqueness index', async () => {
  const r = await pool.query('SELECT to_regclass($1) AS index_name', [P005_INDEX]);
  assert.ok(r.rows[0].index_name, 'Package A reportable uniqueness index exists');
});

test('N4 regression: read-only schema report verifies both Package A invariants by catalog definition', () => {
  const source = fs.readFileSync(path.join(__dirname, '../scripts/max-reliability/schemaReport.cjs'), 'utf8');
  assert.match(source, /canonicalRootInvariant/);
  assert.match(source, /wakeDeduplicationInvariant/);
  assert.match(source, /migrationPreflight/);
  assert.match(source, /duplicateObjectiveWakes/);
  assert.match(source, /pg_get_expr\(i\.indexprs/);
  assert.match(source, /indisunique AND indisvalid AND indisready/);
});

/* ------------------------------------------------------------------------ */
/* R2 — canonical replay                                                     */
/* ------------------------------------------------------------------------ */

test('R2-01 [ADV-1] replay survives a real JSONB round trip, reordered keys and reordered lists', async () => {
  const o = await newObjective('r2-01');
  assert.equal(await outcome(admit('r2-01', o.id, contract())), 'new');
  const reordered = {};
  for (const k of Object.keys(contract()).reverse()) reordered[k] = contract()[k];
  reordered.wakeConditions = ['due_verification', 'objective_accepted'];
  reordered.verification = { deadlineAt: FAR, backoffMultiplier: 2, maxAttempts: 3, maxPollMs: 10000, initialPollMs: 5000 };
  assert.equal(await outcome(admit('r2-01', o.id, contract())), 'replay');
  assert.equal(await outcome(admit('r2-01', o.id, reordered)), 'replay');
});

test('R2-02 replay is judged against the accepted root, not mutable objective context', async () => {
  const o = await newObjective('r2-02');
  const first = await admit('r2-02', o.id, contract());
  await objectives.updateOperatorObjective(o.id, { tenantId: 'r2-02', context: { maxP005Contract: widened() } }, { pool });
  const again = await admit('r2-02', o.id, contract());
  assert.equal(again.replay, true);
  assert.equal(again.task.id, first.task.id);
  assert.equal(await outcome(admit('r2-02', o.id, widened())), 'objective_contract_revision_conflict');
  assert.equal(await outcome(C.admitObjectiveRoot({ tenantId: 'r2-02', objectiveId: o.id, actor: actor('r2-02') }, options())),
    'objective_contract_invalid', 'context is never used as a contract source');
});

/* ------------------------------------------------------------------------ */
/* R3 — contract immutability and failed-admission consistency               */
/* ------------------------------------------------------------------------ */

test('R3-01 [ADV-3] concurrent conflicting same-version contracts: one root, loser conflicts, context untouched (20 trials)', async () => {
  const outcomes = [];
  for (let i = 0; i < 20; i += 1) {
    const t = `r3-01-${i}`;
    const o = await newObjective(t);
    const b = contract(); b.plan = { ...b.plan, maxSteps: 9 };
    outcomes.push(...await Promise.all([outcome(admit(t, o.id, contract())), outcome(admit(t, o.id, b))]));
    assert.equal((await objectiveTasks(t, o.id)).length, 1, `trial ${i}`);
    assert.deepEqual(await objectiveContext(t, o.id), {}, `trial ${i}: objective context never carries orchestration authority`);
  }
  assert.deepEqual(tally(outcomes), { new: 20, objective_contract_revision_conflict: 20 });
});

test('R3-02 [ADV-4] direct SQL cannot widen the accepted root contract', async () => {
  const o = await newObjective('r3-02');
  const root = (await admit('r3-02', o.id, contract())).task;
  await assert.rejects(pool.query(
    `UPDATE max_durable_execution_tasks
        SET immutable_input = jsonb_set(immutable_input, '{contract,budget,caps,provider_usd}', '1e9')
      WHERE id=$1`, [root.id]), /accepted task identity and input are immutable/);
  const row = (await objectiveTasks('r3-02', o.id))[0];
  assert.equal(row.immutable_input.contract.budget.caps.provider_usd, 0);
});

test('R3-03 a conflicting admission changes nothing; a failing task store leaves no state', async () => {
  const o = await newObjective('r3-03');
  await admit('r3-03', o.id, contract());
  const before = JSON.stringify(await objectiveTasks('r3-03', o.id));
  const conflicting = contract(); conflicting.plan = { ...conflicting.plan, maxSteps: 5 };
  assert.equal(await outcome(admit('r3-03', o.id, conflicting)), 'objective_contract_revision_conflict');
  assert.equal(JSON.stringify(await objectiveTasks('r3-03', o.id)), before);
  assert.deepEqual(await objectiveContext('r3-03', o.id), {});

  const o2 = await newObjective('r3-03b');
  const failing = { accept: async () => { throw Object.assign(new Error('injected'), { code: 'injected_store_failure' }); } };
  assert.equal(await outcome(admit('r3-03b', o2.id, contract(), {}, options({ taskStore: failing }))), 'injected_store_failure');
  assert.deepEqual(await objectiveContext('r3-03b', o2.id), {});
  assert.equal((await objectiveTasks('r3-03b', o2.id)).length, 0);
});

test('R3-04 [NF-4] an identical contract is not misreported as a revision conflict after an objective-text edit', async () => {
  const o = await newObjective('r3-04', 'original text');
  const first = await admit('r3-04', o.id, contract());
  await objectives.updateOperatorObjective(o.id, { tenantId: 'r3-04', objectiveText: 'edited text' }, { pool });
  const res = await admit('r3-04', o.id, contract()).then((r) => r, (e) => e);
  if (res instanceof Error) {
    assert.notEqual(res.code, 'objective_contract_revision_conflict',
      'the contract did not change; reject with a distinct, explicit code (e.g. objective_text_changed) or replay');
  } else {
    assert.equal(res.replay, true); assert.equal(res.task.id, first.task.id);
  }
  assert.equal((await objectiveTasks('r3-04', o.id)).length, 1);
});

test('R3-05 [NF-4] after the deadline, resubmission replays the root or fails with an explicit expiry code', async () => {
  const soon = new Date(Date.now() + 2500).toISOString();
  const short = contract({ deadlineAt: soon, verification: { initialPollMs: 5000, maxPollMs: 5000, maxAttempts: 2, backoffMultiplier: 1, deadlineAt: soon } });
  const o = await newObjective('r3-05');
  assert.equal(await outcome(admit('r3-05', o.id, short)), 'new');
  await new Promise((r) => setTimeout(r, 3000));
  const res = await outcome(admit('r3-05', o.id, short));
  assert.ok(res === 'replay' || /expired|deadline/i.test(res), `got ${res}; generic objective_contract_invalid is misleading`);
});

/* ------------------------------------------------------------------------ */
/* R4 — null-mission root fence (routing, persistence, external actions)     */
/* ------------------------------------------------------------------------ */

let missionSeq = 0;
async function missionFixture() {
  TP.resetMissionDurableLocksForTests();
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: `Acquire facility buyers p005 review ${process.pid}-${++missionSeq}.`, targetSegment: 'Facilities' });
  await policy.runWithOwnershipExemption('mission_creation',
    () => persistence.persistStageCommit({ mission: engine.get(mission.id, '10') }, pool, { skipEnsure: true }));
  return { engine, mission: engine.get(mission.id, '10') };
}
async function claimAndStart(taskId, token) {
  const claimed = await tasks.claimTask({ tenantId: '10', taskId, ownerToken: token, leaseMs: 60000 });
  assert.ok(claimed, `claim ${token}`);
  await tasks.start({ tenantId: '10', taskId, ownershipToken: token, fenceEpoch: claimed.fenceEpoch });
  return claimed.fenceEpoch;
}
/** Root (mission_id NULL) active alongside a mission-bound child — the P0-005 topology. */
async function rootAndChild() {
  const o = await newObjective('10');
  const root = (await admit('10', o.id, contract())).task;
  const a = await missionFixture();
  const child = (await tasks.accept({ tenantId: '10', objectiveId: o.id, parentTaskId: root.id, missionId: a.mission.id,
    logicalKey: `review-child-${missionSeq}`, specialist: 'fixture', immutableInput: { fixture: true }, retryBudget: 0 })).task;
  const childEpoch = await claimAndStart(child.id, `child-${missionSeq}`);
  const rootEpoch = await claimAndStart(root.id, `root-${missionSeq}`);
  return {
    a, root, child,
    rootOwner: { tenantId: '10', taskId: root.id, ownershipToken: `root-${missionSeq}`, fenceEpoch: rootEpoch, missionId: null },
    childOwner: { tenantId: '10', taskId: child.id, ownershipToken: `child-${missionSeq}`, fenceEpoch: childEpoch, missionId: a.mission.id },
  };
}
const ledger = () => createExternalActionLedger(pool);
async function claim(key, taskId, missionId) {
  const c = await ledger().claim({ tenantId: '10', idempotencyKey: key, actionKind: 'outbound_email',
    requestFingerprint: 'f'.repeat(64), missionId, taskId, subjectRef: key });
  assert.ok(c.ownerToken, `claim ${key}: ${c.outcome}/${c.code}`);
  return c.ownerToken;
}
const markFenced = (key, ownerToken, taskFence) => ledger().markDispatchAttempted({ tenantId: '10', idempotencyKey: key, ownerToken, taskFence });

test('R4-01 [ADV-7 routing] a root fence never lets mission work inherit it', { timeout: 60000 }, async () => {
  const { a, rootOwner } = await rootAndChild();
  let ran = false;
  await assert.rejects(
    runWithTaskFence(rootOwner, () => runTaskOwnedWork({ store: tasks, tenantId: '10', missionId: a.mission.id,
      logicalKey: `r4-01-${missionSeq}`, specialist: 'operator' }, async () => { ran = true; })),
    { code: 'durable_task_mission_busy' }, 'must take its own task, which the active child blocks');
  assert.equal(ran, false);
});

test('R4-02 [ADV-7 persistence] a root fence cannot persist a mission stage commit', { timeout: 60000 }, async () => {
  const { a, rootOwner } = await rootAndChild();
  const next = { ...a.engine.get(a.mission.id, '10'), title: 'root-write' };
  bumpMissionVersion(next, `tx-r4-02-${missionSeq}`);
  await assert.rejects(runWithTaskFence(rootOwner,
    () => persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true })), { code: 'durable_task_fenced' });
});

test('R4-03 [ADV-7 external] a root fence cannot mark an external dispatch, with or without a mission', { timeout: 60000 }, async () => {
  const { a, root, rootOwner } = await rootAndChild();
  const k1 = `r4-03-m-${missionSeq}`; const k2 = `r4-03-n-${missionSeq}`;
  await assert.rejects(markFenced(k1, await claim(k1, root.id, a.mission.id), rootOwner), { code: 'durable_task_fenced' });
  await assert.rejects(markFenced(k2, await claim(k2, root.id, null), rootOwner), { code: 'durable_task_fenced' });
});

test('R4-04 a mission child cannot dispatch an action claimed for a different mission', { timeout: 60000 }, async () => {
  const { child, childOwner } = await rootAndChild();
  const k = `r4-04-${missionSeq}`;
  await assert.rejects(markFenced(k, await claim(k, child.id, 'some-other-mission'), childOwner), { code: 'durable_task_fenced' });
});

test('R4-05 legitimate same-mission work still succeeds (persist, nested inherit, own-mission dispatch)', { timeout: 60000 }, async () => {
  const { a, child, childOwner } = await rootAndChild();
  const next = { ...a.engine.get(a.mission.id, '10'), title: 'child-write' };
  bumpMissionVersion(next, `tx-r4-05-${missionSeq}`);
  await runWithTaskFence(childOwner, () => persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true }));
  const nested = await runWithTaskFence(childOwner, () => runTaskOwnedWork({ store: tasks, tenantId: '10',
    missionId: a.mission.id, logicalKey: `r4-05-${missionSeq}`, specialist: 'operator' }, async () => 'ok'));
  assert.equal(nested.inherited, true); assert.equal(nested.result, 'ok');
  const k = `r4-05-${missionSeq}`;
  const receipt = await markFenced(k, await claim(k, child.id, a.mission.id), childOwner);
  assert.equal(receipt.state, 'dispatch_attempted');
});

test('R4-06 [NF-6] a claim linked to a root task cannot be dispatched through the unfenced marker path', { timeout: 60000 }, async () => {
  const { a, root } = await rootAndChild();
  const k = `r4-06-${missionSeq}`;
  const ownerToken = await claim(k, root.id, a.mission.id);
  await assert.rejects(ledger().markDispatchAttempted({ tenantId: '10', idempotencyKey: k, ownerToken }),
    'a task-linked claim (here: root-linked) must require its task fence at the ledger');
});

test('R4-07 P0-004 one-active-task-per-mission still holds with an active root', { timeout: 60000 }, async () => {
  const { a, root } = await rootAndChild();
  const competing = (await tasks.accept({ tenantId: '10', objectiveId: root.objectiveId, parentTaskId: root.id, missionId: a.mission.id,
    logicalKey: `r4-07-${missionSeq}`, specialist: 'fixture', immutableInput: { competing: true }, retryBudget: 0 })).task;
  assert.equal(await tasks.claimTask({ tenantId: '10', taskId: competing.id, ownerToken: 'competing', leaseMs: 5000 }), null);
});

/* ------------------------------------------------------------------------ */
/* R5 — actor authorization                                                  */
/* ------------------------------------------------------------------------ */

test('R5-01 missing, malformed, cross-tenant, or trusted-policy-denied actors fail closed', async () => {
  const o = await newObjective('r5-01');
  const base = { tenantId: 'r5-01', objectiveId: o.id, contract: contract() };
  const cases = [
    ['no actor', base, options()],
    ['actor without id', { ...base, actor: { authorizedTenantId: 'r5-01' } }, options()],
    ['cross-tenant actor', { ...base, actor: actor('other-tenant') }, options()],
    ['production default-deny', { ...base, actor: actor('r5-01') }, options()],
  ];
  await withProductionAdmissionPolicy(async () => {
    for (const [label, input, opts] of cases) {
      assert.equal(await outcome(C.admitObjectiveRoot(input, opts)), 'objective_contract_actor_unauthorized', label);
    }
  });
  assert.equal((await objectiveTasks('r5-01', o.id)).length, 0);
});

test('R5-02 [N1 regression] production callers cannot override the trusted authorization decision', async () => {
  const o = await newObjective('r5-02');
  await withProductionAdmissionPolicy(async () => {
    for (const [label, callerOverride] of [
      ['boolean approval', async () => true],
      ['object approval', async () => ({ authorized: true, policy: 'caller_controlled' })],
      ['truthy approval', async () => 'approved'],
      ['throwing approval', async () => { throw new Error('caller authorizer must not run'); }],
    ]) {
      assert.equal(await outcome(admit('r5-02', o.id, contract(), {}, options({ authorizeActor: callerOverride }))),
        'objective_contract_actor_unauthorized', label);
    }
  });
  assert.equal((await objectiveTasks('r5-02', o.id)).length, 0);
});

test('R5-03 a trusted authorization-policy failure fails closed with no root', async () => {
  const o = await newObjective('r5-03');
  let res;
  await withProductionAdmissionPolicy(async (productionPolicy) => {
    const originalAuthorizer = productionPolicy.authorizeObjectiveAdmission;
    productionPolicy.authorizeObjectiveAdmission = async () => { throw new Error('policy store down'); };
    try {
      res = await outcome(admit('r5-03', o.id, contract()));
    } finally {
      productionPolicy.authorizeObjectiveAdmission = originalAuthorizer;
    }
  });
  assert.equal(res, 'objective_contract_actor_unauthorized');
  assert.equal((await objectiveTasks('r5-03', o.id)).length, 0);
});

test('R5-04 [R5 attribution] the admitting actor is durably attributable on the root or its accepted event', async () => {
  const o = await newObjective('r5-04');
  const root = (await admit('r5-04', o.id, contract(), { actor: actor('r5-04', 'operator-alice') })).task;
  const row = (await objectiveTasks('r5-04', o.id))[0];
  const events = (await pool.query('SELECT detail FROM max_durable_execution_task_events WHERE task_id=$1', [root.id])).rows;
  const haystack = JSON.stringify([row.immutable_input, row.execution_scope, events]);
  assert.match(haystack, /operator-alice/, 'actor id must be recorded for audit');
});

test('R5-05 authorization reuses an existing repository policy', async () => {
  await withProductionAdmissionPolicy(async (policy) => {
    assert.deepEqual(await policy.authorizeObjectiveAdmission(pool, { id: 'not-an-id' }, '10'), { authorized: false });
  });
});

test('R5-06 [fixture-only] authenticated identities stay denied in production while a disposable policy fixture admits and replays', async () => {
  /*
   * This authorizer is intentionally declared inside this disposable-PostgreSQL
   * test.  It is not exported by, configurable from, or reachable through the
   * production admission boundary.  It lets this suite exercise the positive
   * lifecycle while production has no objective-admission permission yet.
   */
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id integer PRIMARY KEY,
      client_id integer NOT NULL,
      role text NOT NULL,
      active boolean NOT NULL DEFAULT true
    )`);
  const tenantId = '50506';
  const adminId = 505061;
  const managerId = 505062;
  await pool.query(
    `INSERT INTO users(id, client_id, role, active) VALUES
       ($1, $2, 'admin', true), ($3, $2, 'manager', true)
     ON CONFLICT (id) DO UPDATE SET client_id=EXCLUDED.client_id, role=EXCLUDED.role, active=EXCLUDED.active`,
    [adminId, Number(tenantId), managerId],
  );

  const productionPolicy = require('../services/maxReliability/objectiveAdmissionAuthorization');
  // Trusted identity and matching tenant membership are not an admission grant.
  await withProductionAdmissionPolicy(async () => {
    for (const id of [adminId, managerId]) {
      const decision = await productionPolicy.authorizeObjectiveAdmission(pool, { id: String(id) }, tenantId);
      assert.equal(decision.authorized, false);
      assert.equal(decision.reason, 'objective_admission_permission_absent');
      assert.equal(decision.actorId, String(id));
    }
  });

  // Build the candidate from the trusted, persisted identity fixture rather
  // than accepting a caller-supplied role or tenant claim.
  const identity = (await pool.query(
    'SELECT id, client_id, role, active FROM users WHERE id=$1', [adminId],
  )).rows[0];
  const trustedActor = Object.freeze({ id: String(identity.id), authorizedTenantId: String(identity.client_id) });
  const fixturePolicy = async (fixturePool, candidate, scopeTenantId) => {
    // Admission receives the authenticated identity value across the module
    // boundary, so bind the fixture to the identity loaded from this fixture's
    // real users row rather than JavaScript object identity.
    if (!candidate
      || fixturePool !== pool
      || String(candidate.id) !== trustedActor.id
      || String(candidate.authorizedTenantId) !== trustedActor.authorizedTenantId
      || String(scopeTenantId) !== tenantId) return { authorized: false };
    return Object.freeze({ authorized: true, policy: 'disposable_postgres_objective_admission_fixture_v1' });
  };

  const objective = await newObjective(tenantId, 'Exercise only the disposable positive-admission fixture.');
  // Exercise the production call shape (no caller-provided authorizer).  The
  // temporary replacement is confined to this Node test process and restored
  // before the test ends; production code has no switch that can select it.
  const suiteFixtureAuthorizer = productionPolicy.authorizeObjectiveAdmission;
  let first; let replay;
  productionPolicy.authorizeObjectiveAdmission = fixturePolicy;
  try {
    const fixtureOptions = options();
    first = await C.admitObjectiveRoot({ tenantId, objectiveId: objective.id, contract: contract(), actor: trustedActor }, fixtureOptions);
    replay = await C.admitObjectiveRoot({
      tenantId,
      objectiveId: objective.id,
      // Deliberately reorder JSON members: replay is fingerprint based, not
      // dependent on JavaScript insertion order or PostgreSQL JSONB ordering.
      contract: { ...contract(), plan: { maxRetries: 0, maxReplans: 0, maxChildren: 1, maxSteps: 2 } },
      actor: trustedActor,
    }, fixtureOptions);
  } finally {
    productionPolicy.authorizeObjectiveAdmission = suiteFixtureAuthorizer;
  }
  assert.equal(first.admitted, true);
  assert.equal(replay.replay, true);
  assert.equal(replay.task.id, first.task.id);
  const roots = await objectiveTasks(tenantId, objective.id);
  assert.equal(roots.length, 1, 'positive lifecycle creates one immutable canonical root');
  const events = (await pool.query(
    `SELECT detail FROM max_durable_execution_task_events
       WHERE task_id=$1 AND event='accepted'`, [first.task.id],
  )).rows;
  assert.equal(events.length, 1);
  assert.equal(events[0].detail.actorId, String(adminId));
  assert.equal(events[0].detail.authorizationPolicy, 'disposable_postgres_objective_admission_fixture_v1');
  assert.equal(events[0].detail.admission, 'objective_contract');
});

/* ------------------------------------------------------------------------ */
/* R6 — provider / specialist / action-class governance                      */
/* ------------------------------------------------------------------------ */

const rejects = (o) => assert.throws(() => C.validateObjectiveContract(contract(o)), (e) => e instanceof C.ObjectiveContractError);

test('R6-01 [ADV-5] unknown, unapproved, confusable and invisible-character identifiers are rejected', () => {
  for (const o of [
    { allowedProviders: ['openai'] }, { allowedProviders: ['gemini'] },
    { allowedProviders: ['Deterministic_Fixture'] },
    { allowedProviders: ['deterministic_fixturе'] }, // Cyrillic e
    { allowedProviders: ['deterministic_fixture​'] }, // zero-width space
    { allowedProviders: ['deterministic‐fixture'] }, // Unicode hyphen
    { allowedSpecialists: ['riley'] }, { allowedSpecialists: ['scout​'] },
    { allowedActionClasses: ['outbound_email'] },
  ]) rejects(o);
});

test('R6-02 Anthropic stays retired in every role and alias', () => {
  for (const o of [
    { allowedProviders: ['anthropic'] }, { allowedProviders: ['ANTHROPIC'] }, { allowedProviders: ['anthrоpic'] },
    { allowedProviders: ['claude-3'] }, { allowedProviders: ['claude'] },
    { allowedSpecialists: ['anthropic'] }, { allowedActionClasses: ['anthropic_messages'] },
    { providerPolicy: { fallback: 'Anthropic' } }, { providerPolicy: { default: 'claude' } },
  ]) rejects(o);
});

test('R6-03 [ADV-5] non-string identifiers are rejected, not coerced', () => {
  for (const o of [
    { allowedProviders: [{ toString: () => 'deterministic_fixture' }] },
    { allowedProviders: [['deterministic_fixture']] },
    { allowedSpecialists: [{ toString: () => 'scout' }] },
    { allowedProviders: [42] },
  ]) rejects(o);
});

test('R6-04 providerPolicy naming an unapproved provider is rejected', () => {
  rejects({ providerPolicy: { fallback: 'gemini' } });
  rejects({ providerPolicy: { default: 'openai' } });
});

test('R6-05 a supplied providerPolicy is persisted in the immutable contract (or rejected outright)', () => {
  let validated;
  try { validated = C.validateObjectiveContract(contract({ providerPolicy: { default: 'deterministic_fixture' } })); } catch (e) {
    assert.ok(e instanceof C.ObjectiveContractError); return; // rejecting an unsupported field is acceptable
  }
  assert.ok(Object.prototype.hasOwnProperty.call(validated, 'providerPolicy'),
    'an accepted providerPolicy must not be silently dropped');
});

test('R6-06 specialist/action registries are sourced from the approved repository registries', () => {
  const registry = require('../packages/max/specialistDelegation/CapabilityRegistry').createDefaultCapabilityRegistry();
  assert.ok(registry.listCallable().some(x => x.specialist === 'scout'));
  assert.deepEqual(C.APPROVED_ACTION_CLASSES, []);
});

/* ------------------------------------------------------------------------ */
/* R7 — deadlines, jitter/backoff, wake policy, status projection            */
/* ------------------------------------------------------------------------ */

test('R7-01 expired objective deadlines are rejected at validation', () => {
  rejects({ deadlineAt: '2001-01-01T00:00:00Z',
    verification: { initialPollMs: 5000, maxPollMs: 5000, maxAttempts: 1, backoffMultiplier: 1, deadlineAt: '2000-01-01T00:00:00Z' } });
});

test('R7-02 [NF-3] jittered delays stay within [max(MIN_POLL_MS, initialPollMs), maxPollMs] for every attempt', () => {
  const v = C.validateObjectiveContract(contract());
  const lo = Math.max(C.MIN_POLL_MS, v.verification.initialPollMs); const hi = v.verification.maxPollMs;
  for (let attempt = 0; attempt < v.verification.maxAttempts; attempt += 1) {
    for (const r of [0, 1e-9, 0.25, 0.5, 0.75, 0.999999]) {
      const d = C.nextVerificationWake(v, attempt, 'pending', () => r);
      assert.ok(Number.isFinite(d) && d >= lo && d <= hi, `attempt ${attempt}, random ${r}: ${d} outside [${lo}, ${hi}]`);
    }
  }
});

test('R7-03 [NF-3] an out-of-range or non-numeric random source cannot produce an unbounded delay or a silent stop', () => {
  const v = C.validateObjectiveContract(contract());
  for (const r of [7, -3, NaN, Infinity, undefined]) {
    let d; let threw = false;
    try { d = C.nextVerificationWake(v, 0, 'pending', () => r); } catch (_) { threw = true; }
    if (!threw) assert.ok(Number.isFinite(d) && d >= C.MIN_POLL_MS && d <= v.verification.maxPollMs, `random ${r} produced ${d}`);
  }
});

test('R7-04 [NF-4] after the verification deadline the next wake is null, not an exception', async () => {
  const soon = new Date(Date.now() + 1500).toISOString();
  const v = C.validateObjectiveContract(contract({ deadlineAt: soon,
    verification: { initialPollMs: 5000, maxPollMs: 5000, maxAttempts: 2, backoffMultiplier: 1, deadlineAt: soon } }));
  const stored = JSON.parse(JSON.stringify(v)); // as read back from the root's JSONB
  await new Promise((r) => setTimeout(r, 2000));
  assert.equal(C.nextVerificationWake(stored, 0, 'pending', () => 0.5), null);
});

test('R7-05 terminal outcomes and exhausted attempts stop polling', () => {
  const v = C.validateObjectiveContract(contract());
  for (const o of ['verified', 'contradicted', 'unverifiable', 'error']) assert.equal(C.nextVerificationWake(v, 0, o), null);
  assert.equal(C.nextVerificationWake(v, v.verification.maxAttempts, 'pending', () => 0.5), null);
});

test('R7-06 status projection is read-only and derived from the authoritative root', async () => {
  const o = await newObjective('r7-06');
  const root = (await admit('r7-06', o.id, contract())).task;
  const before = JSON.stringify(await objectiveTasks('r7-06', o.id));
  const status = C.projectObjectiveStatus(await tasks.get({ tenantId: 'r7-06', taskId: root.id }));
  assert.equal(status.rootTaskId, root.id); assert.equal(status.state, 'queued');
  assert.equal(status.externalActionsEnabled, false);
  assert.ok(Object.isFrozen(status));
  assert.equal(JSON.stringify(await objectiveTasks('r7-06', o.id)), before, 'projection writes nothing');
  assert.equal(C.projectObjectiveStatus(null), null);
});

test('R7-07 wake-deduplication contract is defined and persisted (objective/task/reason/version key)', async () => {
  const o = await newObjective('r7-07'); const root = (await admit('r7-07', o.id, contract())).task;
  const stored = await tasks.get({ tenantId: 'r7-07', taskId: root.id });
  assert.equal(stored.immutableInput.contract.wakeDeduplication.persistence, 'max_durable_execution_task_events');
  assert.equal(C.wakeIdentity({ objectiveId:o.id, rootTaskId:root.id, reason:'due_verification', version:1 }), C.wakeIdentity({ objectiveId:o.id, rootTaskId:root.id, reason:'due_verification', version:1 }));
});

test('R7-08 Package A creates no timers', async () => {
  const realSetTimeout = global.setTimeout; const realSetInterval = global.setInterval; let timers = 0;
  global.setTimeout = (...a) => { timers += 1; return realSetTimeout(...a); };
  global.setInterval = (...a) => { timers += 1; return realSetInterval(...a); };
  try {
    const v = C.validateObjectiveContract(contract());
    C.nextVerificationWake(v, 0, 'pending', () => 0.5);
    C.projectObjectiveStatus({ id: 'x', state: 'queued', immutableInput: { contract: v } });
  } finally { global.setTimeout = realSetTimeout; global.setInterval = realSetInterval; }
  assert.equal(timers, 0);
});

/* ------------------------------------------------------------------------ */
/* Default-off                                                               */
/* ------------------------------------------------------------------------ */

test('DEF-01 with either flag off, admission touches neither the task store nor the objective', async () => {
  const o = await newObjective('def-01');
  let calls = 0;
  const spy = { accept: async () => { calls += 1; } };
  for (const env of [{}, { MAX_AUTONOMOUS_OBJECTIVES: 'true' }, { MAX_DURABLE_TASKS: 'true' }]) {
    const r = await C.admitObjectiveRoot({ tenantId: 'def-01', objectiveId: o.id, contract: contract(), actor: actor('def-01') },
      options({ env, taskStore: spy }));
    assert.deepEqual(r, { enabled: false, admitted: false, reason: 'max_p0_005_disabled' });
  }
  assert.equal(calls, 0);
  assert.deepEqual(await objectiveContext('def-01', o.id), {});
});
