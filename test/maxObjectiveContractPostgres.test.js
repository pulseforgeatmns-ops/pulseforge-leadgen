'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createDurableExecutionOwnership } = require('../services/maxReliability/durableExecutionOwnership');
const { validateObjectiveContract, admitObjectiveRoot } = require('../packages/max/objectiveOrchestration/ObjectiveContract');
const objectives = require('../services/operatorObjectives');

const P003 = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const P004 = path.join(__dirname, '../migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql');
const P005 = path.join(__dirname, '../migrations/2026-11-27-max-p0-005-package-a-contract-roots.sql');
const OBJECTIVES = path.join(__dirname, '../migrations/2026-08-13-operator-objectives.sql');
let fixture;
async function db() {
  if (fixture) return fixture;
  const instance = await startDisposablePostgres('max-p0-005-a-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 6 });
  await pool.query(fs.readFileSync(OBJECTIVES, 'utf8'));
  await pool.query(fs.readFileSync(P003, 'utf8'));
  await pool.query(fs.readFileSync(P004, 'utf8'));
  await pool.query(fs.readFileSync(P005, 'utf8'));
  fixture = { instance, pool };
  return fixture;
}
test.after(async () => { if (fixture) await closePostgresFixture(fixture.pool, fixture.instance); });

function contract() {
  return {
    version: 1, successPredicates: ['durable_evidence'], allowedActionClasses: [],
    allowedSpecialists: ['scout'], allowedProviders: ['deterministic_fixture'],
    evidenceFreshnessMs: 0, deadlineAt: '2030-01-01T00:00:00.000Z',
    plan: { maxSteps: 2, maxChildren: 1, maxReplans: 0, maxRetries: 0 },
    budget: { caps: { provider_usd: 0 } }, wakeConditions: ['objective_accepted', 'due_verification'],
    verification: { initialPollMs: 5000, maxPollMs: 10000, maxAttempts: 2, backoffMultiplier: 2, deadlineAt: '2030-01-01T00:00:00.000Z' },
    escalationPolicy: 'operator_review',
  };
}
function actor(tenantId) { return { id: `operator-${tenantId}`, authorizedTenantId: tenantId }; }
function opts(tasks, pool) { return { env: { MAX_AUTONOMOUS_OBJECTIVES: 'true', MAX_DURABLE_TASKS: 'true' }, taskStore: tasks, objectiveService: objectives, objectiveOpts: { pool } }; }

// Positive admission is exercised only by this isolated PostgreSQL fixture.
// The production boundary still obtains its decision from the same trusted
// policy module; this test temporarily replaces that module's implementation
// and restores it before returning.
async function withFixtureAdmission(pool, work) {
  const policy = require('../services/maxReliability/objectiveAdmissionAuthorization');
  const original = policy.authorizeObjectiveAdmission;
  policy.authorizeObjectiveAdmission = async (fixturePool, candidate, tenantId) => {
    if (fixturePool !== pool || !candidate || !candidate.id
      || String(candidate.authorizedTenantId) !== String(tenantId)) return { authorized: false };
    return { authorized: true, policy: 'isolated_postgres_objective_admission_fixture' };
  };
  try {
    return await work();
  } finally {
    policy.authorizeObjectiveAdmission = original;
  }
}

test('Package A root is durable and does not consume P0-004 mission ownership', async () => {
  const { pool } = await db();
  const objective = await objectives.createOperatorObjective({ tenantId: 'p005-a', scope: 'operator', title: 'Durable research', objectiveText: 'Verify a safe result.' }, { pool });
  const tasks = createDurableExecutionOwnership(pool);
  await withFixtureAdmission(pool, async () => {
    const admitted = await admitObjectiveRoot({ tenantId: 'p005-a', objectiveId: objective.id, contract: contract(), actor: actor('p005-a') }, opts(tasks, pool));
    const replay = await admitObjectiveRoot({ tenantId: 'p005-a', objectiveId: objective.id, contract: contract(), actor: actor('p005-a') }, opts(tasks, pool));
    assert.equal(replay.task.id, admitted.task.id);
    assert.equal(admitted.task.missionId, null);

    const root = await tasks.claimTask({ tenantId: 'p005-a', taskId: admitted.task.id, ownerToken: 'root', leaseMs: 1000 });
    assert.ok(root, 'root can be active');
    const child = (await tasks.accept({ tenantId: 'p005-a', objectiveId: objective.id, parentTaskId: admitted.task.id, missionId: 'mission-1', logicalKey: `child:${objective.id}`, specialist: 'fixture', immutableInput: { fixture: true }, dependencyRefs: [], retryBudget: 0 })).task;
    const childOwner = await tasks.claimTask({ tenantId: 'p005-a', taskId: child.id, ownerToken: 'child', leaseMs: 1000 });
    assert.ok(childOwner, 'root active with no mission leaves mission child claimable');
    const competing = (await tasks.accept({ tenantId: 'p005-a', objectiveId: objective.id, parentTaskId: admitted.task.id, missionId: 'mission-1', logicalKey: `child-competing:${objective.id}`, specialist: 'fixture', immutableInput: { fixture: 2 }, dependencyRefs: [], retryBudget: 0 })).task;
    assert.equal(await tasks.claimTask({ tenantId: 'p005-a', taskId: competing.id, ownerToken: 'competing', leaseMs: 1000 }), null, 'P0-004 one-active-task-per-mission remains enforced');
    assert.equal((await tasks.get({ tenantId: 'p005-a', taskId: admitted.task.id })).missionId, null);
    assert.equal(validateObjectiveContract(contract()).idlePolicy.noProviderCalls, true);
  });
});

test('Package A concurrent admission is tenant-scoped, idempotent, and preserves the immutable root contract', async () => {
  const { pool } = await db();
  const objective = await objectives.createOperatorObjective({ tenantId: 'p005-concurrent', scope: 'operator', title: 'Concurrent objective', objectiveText: 'Verify exactly once.' }, { pool });
  const tasks = createDurableExecutionOwnership(pool);
  const options = opts(tasks, pool);
  await withFixtureAdmission(pool, async () => {
    const [first, second] = await Promise.all([
      admitObjectiveRoot({ tenantId: 'p005-concurrent', objectiveId: objective.id, contract: contract(), actor: actor('p005-concurrent') }, options),
      admitObjectiveRoot({ tenantId: 'p005-concurrent', objectiveId: objective.id, contract: contract(), actor: actor('p005-concurrent') }, options),
    ]);
    assert.equal(first.task.id, second.task.id, 'the P0-004 logical key admits one root under concurrent submission');
    const roots = await pool.query("SELECT id, mission_id, immutable_input FROM max_durable_execution_tasks WHERE tenant_id=$1 AND objective_id=$2", ['p005-concurrent', objective.id]);
    assert.equal(roots.rows.length, 1);
    assert.equal(roots.rows[0].mission_id, null);
    assert.deepEqual(roots.rows[0].immutable_input.contract, validateObjectiveContract(contract()));
    await assert.rejects(
      () => admitObjectiveRoot({ tenantId: 'other-tenant', objectiveId: objective.id, contract: contract(), actor: actor('other-tenant') }, options),
      error => error.code === 'objective_contract_not_found',
      'the objective lookup never crosses the tenant boundary',
    );
    const changed = contract(); changed.plan.maxSteps = 3;
    await assert.rejects(
      () => admitObjectiveRoot({ tenantId: 'p005-concurrent', objectiveId: objective.id, contract: changed, actor: actor('p005-concurrent') }, options),
      error => error.code === 'objective_contract_revision_conflict',
      'a root cannot be revised through duplicate submission',
    );
  });
});

test('Package A objective wakes deduplicate durably across concurrent store instances', async () => {
  const { pool } = await db(); const objective = await objectives.createOperatorObjective({ tenantId: 'p005-wake', scope: 'operator', title: 'Wake', objectiveText: 'Wake safely.' }, { pool });
  await withFixtureAdmission(pool, async () => {
    const root = (await admitObjectiveRoot({ tenantId:'p005-wake', objectiveId:objective.id, contract:contract(), actor:actor('p005-wake') }, opts(createDurableExecutionOwnership(pool), pool))).task;
    const wake = `objective-wake:${objective.id}:${root.id}:due_verification:v1`;
    const [a,b] = await Promise.all([createDurableExecutionOwnership(pool).registerObjectiveWake({ tenantId:'p005-wake', taskId:root.id, wakeIdentity:wake, reason:'due_verification' }), createDurableExecutionOwnership(pool).registerObjectiveWake({ tenantId:'p005-wake', taskId:root.id, wakeIdentity:wake, reason:'due_verification' })]);
    assert.equal(Number(a.registered) + Number(b.registered), 1);
    assert.equal((await createDurableExecutionOwnership(pool).registerObjectiveWake({ tenantId:'p005-wake', taskId:root.id, wakeIdentity:`${wake}:other`, reason:'operator_reverify' })).registered, true);
    const rows = await pool.query("SELECT detail->>'wakeIdentity' AS identity FROM max_durable_execution_task_events WHERE task_id=$1 AND event='objective_wake'", [root.id]);
    assert.equal(rows.rows.length, 2);
  });
});
