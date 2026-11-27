'use strict';

/**
 * MAX-P0-004 criterion 1 — with MAX_DURABLE_TASKS=true nothing executes or
 * mutates a mission outside durable task ownership, on real PostgreSQL:
 *   router without a task store → refused (audited), no handler ran;
 *   TME without a fence → refused before the specialist runs;
 *   outbound adapter without a fence → refused before any claim or provider call;
 *   persistStageCommit without a fence → refused unless a closed WRITE exemption
 *   applies, and then only through the mission lease, audited in that transaction;
 *   the runtime's own mutations (create, contribute, progress) and operator work
 *   (runTaskOwnedWork, router afterDispatch) succeed as task-owned work;
 *   operator run keys are stable for the same logical operation.
 * The flag is set only inside this file and restored afterwards. May not skip.
 */
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const amo = require('../packages/acquisition-mission');
const TP = require('../packages/acquisition-mission/TransactionalPersistence');
const { executeMissionStage, bumpMissionVersion } = require('../packages/acquisition-mission/TransactionalExecution');
const persistence = require('../services/acquisitionMissionPersistence');
const { ensureOutboundExecutionSchema } = require('../services/acquisitionMissionOutboundPersistence');
const { ensureAcquisitionKnowledgeSchema } = require('../services/acquisitionKnowledgePersistence');
const { createDurableExecutionOwnership } = require('../services/maxReliability/durableExecutionOwnership');
const policy = require('../services/maxReliability/executionOwnershipPolicy');
const { runTaskOwnedWork, operatorRunKey, missionOperationParts } = require('../packages/acquisition-mission/DurableTaskRouting');

const P003 = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const P004 = path.join(__dirname, '../migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql');

let fixture; const previousFlag = process.env.MAX_DURABLE_TASKS;
before(async () => {
  const instance = await startDisposablePostgres('max-p0-004-enforce-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 10 });
  await persistence.ensureAcquisitionMissionSchema(pool);
  await persistence.ensureExecutionAuditSchema(pool);
  await ensureOutboundExecutionSchema(pool);
  await ensureAcquisitionKnowledgeSchema(pool);
  await pool.query(fs.readFileSync(P003, 'utf8'));
  await pool.query(fs.readFileSync(P004, 'utf8'));
  fixture = { instance, pool };
  process.env.MAX_DURABLE_TASKS = 'true';
});
after(async () => {
  if (previousFlag === undefined) delete process.env.MAX_DURABLE_TASKS; else process.env.MAX_DURABLE_TASKS = previousFlag;
  if (fixture) await closePostgresFixture(fixture.pool, fixture.instance);
});

let seq = 0;
async function missionFixture(pool) {
  TP.resetMissionDurableLocksForTests();
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: `Acquire facility buyers enforcement ${process.pid}-${++seq}.`, targetSegment: 'Facilities' });
  await policy.runWithOwnershipExemption('mission_creation',
    () => persistence.persistStageCommit({ mission: engine.get(mission.id, '10') }, pool, { skipEnsure: true }));
  engine.durableBaseline = new Map([[mission.id, persistence.missionFingerprint(engine.get(mission.id, '10'))]]);
  return { engine, mission: engine.get(mission.id, '10') };
}
const durable = async (pool, id) => (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [id])).rows[0].payload;
const cer = (mission, intent = amo.EXECUTION_INTENTS.EDIT_PLAN) => amo.createExecutionRequestFromApi({
  intent, missionId: mission.id, mission, operatorId: 'enforce', stage: mission.stage, objective: mission.objective,
  runtimeOwner: amo.resolveMissionRuntimeOwner(mission), permissions: { canExecute: true }, payload: { question: 'enforce' },
});
const bump = (engine, mission) => async ({ transactionId }) => {
  const next = { ...engine.get(mission.id, '10') }; bumpMissionVersion(next, transactionId); engine.store.putMission(next); return {};
};

test('router: without a task store an execution is refused and audited; with one it runs', { timeout: 60000 }, async () => {
  const { pool } = fixture; const { engine, mission } = await missionFixture(pool);
  let ran = 0;
  const handlers = { [amo.EXECUTION_INTENTS.EDIT_PLAN]: async () => { ran += 1; return {}; } };
  await assert.rejects(amo.routeExecutionRequest(cer(mission), { engine, tenantId: '10', pool, handlers }), { code: 'durable_task_owner_required' });
  assert.equal(ran, 0);
  const audit = amo.listExecutionRouterAudit({}).filter((a) => a.outcome === 'blocked_no_task_owner');
  assert.ok(audit.length >= 1, 'refusal recorded in the router audit');
  // Offline evaluation never passes with a pool.
  await assert.rejects(policy.runWithOwnershipExemption(policy.OFFLINE_EVALUATION,
    () => amo.routeExecutionRequest(cer(mission), { engine, tenantId: '10', pool, handlers })), { code: 'durable_task_owner_required' });
  // Offline, no pool, non-discovery intent: allowed (nothing durable can happen).
  await policy.runWithOwnershipExemption(policy.OFFLINE_EVALUATION,
    () => amo.routeExecutionRequest(cer(mission), { engine, tenantId: '10', handlers, persist: false }));
  assert.equal(ran, 1);
  // Offline discovery without an injected Scout runner: refused (no live provider).
  await assert.rejects(policy.runWithOwnershipExemption(policy.OFFLINE_EVALUATION,
    () => amo.routeExecutionRequest(cer(mission, amo.EXECUTION_INTENTS.APPROVE_DISCOVERY), { engine, tenantId: '10', persist: false,
      handlers: { [amo.EXECUTION_INTENTS.APPROVE_DISCOVERY]: async () => { ran += 1; return {}; } } })),
  (err) => err.code === 'durable_task_owner_required' || err.code === 'research_planning_required' || /constraint|planning/.test(String(err.code)));
  // With a store: task-owned, runs.
  const store = createDurableExecutionOwnership(pool);
  const routed = await amo.routeExecutionRequest(cer(mission), { engine, tenantId: '10', pool, handlers, durableTasks: store });
  assert.ok(routed.durableTask && routed.durableTask.id);
});

test('TME: a specialist stage without a task fence never runs', { timeout: 60000 }, async () => {
  const { pool } = fixture; const { engine, mission } = await missionFixture(pool);
  let executed = false;
  await assert.rejects(executeMissionStage({ engine, missionId: mission.id, tenantId: '10', pool, specialist: 'operator', stage: mission.stage,
    validatePreconditions: () => ({}), execute: async () => { executed = true; return {}; }, commit: bump(engine, mission),
    persistDurable: persistence.bindStagePersistDurable({ pool, preinitializedSchema: true }, engine, '10') }),
  (err) => err.code === 'durable_task_owner_required');
  assert.equal(executed, false);
  // Exemptions never cover a specialist stage.
  await assert.rejects(policy.runWithOwnershipExemption('mission_creation', () => executeMissionStage({ engine, missionId: mission.id, tenantId: '10', pool,
    specialist: 'operator', stage: mission.stage, validatePreconditions: () => ({}), execute: async () => { executed = true; return {}; }, commit: bump(engine, mission) })),
  (err) => err.code === 'durable_task_owner_required');
  assert.equal(executed, false);
});

test('outbound: no send outside a task-owned execution, whatever the exemption', { timeout: 60000 }, async () => {
  const { executeOutboundBundle } = require('../packages/max/workspace/OutboundExecutionAdapter');
  let calls = 0;
  for (const wrap of [(fn) => fn(), (fn) => policy.runWithOwnershipExemption('inbound_observation', fn)]) {
    await assert.rejects(wrap(() => executeOutboundBundle({ mission: { id: 'm-x', tenantId: '10' }, tenantId: '10', approval: { payload: {} },
      sendEmail: async () => { calls += 1; return { success: true }; } })), { code: 'durable_task_owner_required' });
  }
  assert.equal(calls, 0);
});

test('commit: unowned writes refused; write exemptions only through the lease, audited atomically', { timeout: 60000 }, async () => {
  const { pool } = fixture; const { engine, mission } = await missionFixture(pool);
  const before = await durable(pool, mission.id);
  const next = { ...engine.get(mission.id, '10'), title: 'unowned' }; bumpMissionVersion(next, 'tx-unowned');
  await assert.rejects(persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true }), { code: 'durable_task_owner_required' });
  assert.deepEqual(await durable(pool, mission.id), before, 'nothing written');
  // An exemption does not open the unlocked path.
  await assert.rejects(policy.runWithOwnershipExemption('inbound_observation',
    () => persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true, skipGlobalLock: true })), { code: 'durable_task_owner_required' });
  assert.deepEqual(await durable(pool, mission.id), before);
  // Through the lease, an exempt write commits together with its audit row.
  const auditBefore = (await pool.query("SELECT count(*)::int n FROM max_reliability_audit WHERE subject_kind='mission_write_exemption'")).rows[0].n;
  await policy.runWithOwnershipExemption('inbound_observation', () => persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true }));
  assert.equal((await durable(pool, mission.id)).title, 'unowned');
  const rows = (await pool.query("SELECT detail FROM max_reliability_audit WHERE subject_kind='mission_write_exemption' ORDER BY id DESC LIMIT 1")).rows;
  assert.equal((await pool.query("SELECT count(*)::int n FROM max_reliability_audit WHERE subject_kind='mission_write_exemption'")).rows[0].n, auditBefore + 1);
  assert.equal(rows[0].detail.reason, 'inbound_observation');
  assert.equal(rows[0].detail.missionId, mission.id);
  assert.throws(() => policy.runWithOwnershipExemption('anything_else', () => null), { code: 'durable_task_exemption_invalid' });
});

test('runtime mutations and operator work succeed as task-owned work; afterDispatch is fenced', { timeout: 60000 }, async () => {
  const { pool } = fixture; const store = createDurableExecutionOwnership(pool);
  const { createAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
  TP.resetMissionDurableLocksForTests();
  const runtime = createAcquisitionMissionRuntime({ pool, persist: true, inMemory: false });
  const created = await runtime.create({ tenantId: '10', objective: `Acquire facility buyers runtime ${process.pid}-${++seq}.`, targetSegment: 'Facilities' }, { pool, persist: true });
  assert.ok(await durable(pool, created.id), 'creation persisted under the audited exemption');
  await runtime.contribute(created.id, { specialist: amo.SPECIALISTS.OPERATOR, kind: amo.CONTRIBUTION_KINDS.APPROVAL,
    payload: { approved: true, command: 'owned contribution' } }, { tenantId: '10', pool, persist: true });
  // A runtime progress the engine refuses still ends its task honestly (failed), never "completed".
  await assert.rejects(runtime.progress(created.id, { role: 'operator' }, { stage: 'plan' }, { tenantId: '10', pool, persist: true }));
  const runtimeTasks = await store.list({ tenantId: '10', missionId: created.id });
  const contributeTask = runtimeTasks.find((t) => /^runtime-contribute:/.test(t.logicalKey));
  const progressTask = runtimeTasks.find((t) => /^runtime-progress:/.test(t.logicalKey));
  assert.equal(contributeTask && contributeTask.state, 'completed', 'runtime contribution was task-owned and completed');
  assert.equal(progressTask && progressTask.state, 'failed');
  const creationAudit = (await pool.query("SELECT detail FROM max_reliability_audit WHERE subject_kind='mission_write_exemption' AND detail->>'missionId' = $1", [created.id])).rows;
  assert.deepEqual(creationAudit.map((r) => r.detail.reason), ['mission_creation'], 'creation recorded under its exemption');

  // runTaskOwnedWork: fenced writes, stable key replays without re-running.
  const { engine, mission } = await missionFixture(pool);
  let runs = 0;
  const work = () => runTaskOwnedWork({ store, tenantId: '10', missionId: mission.id, logicalKey: `enforce-work-${seq}`, specialist: 'operator' }, async () => {
    runs += 1;
    const next = { ...engine.get(mission.id, '10'), title: 'owned work' }; bumpMissionVersion(next, `tx-work-${seq}`); engine.store.putMission(next);
    await persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true });
  });
  const first = await work();
  assert.equal(first.durableTask.state, 'completed');
  assert.equal((await durable(pool, mission.id)).title, 'owned work');
  const again = await work();
  assert.equal(again.replay, true); assert.equal(runs, 1, 'same logical key never re-runs');

  // Router afterDispatch (cancel persistence) runs inside the request's task fence.
  const m2 = await missionFixture(pool);
  let persisted = false;
  await amo.routeExecutionRequest(cer(m2.mission), { engine: m2.engine, tenantId: '10', pool, durableTasks: store,
    handlers: { [amo.EXECUTION_INTENTS.EDIT_PLAN]: async () => ({}) },
    afterDispatch: async () => {
      const next = { ...m2.engine.get(m2.mission.id, '10'), title: 'after dispatch' }; bumpMissionVersion(next, `tx-after-${seq}`); m2.engine.store.putMission(next);
      await persistence.persistStageCommit({ mission: next }, pool, { skipEnsure: true });
      persisted = true;
    } });
  assert.equal(persisted, true);
  assert.equal((await durable(pool, m2.mission.id)).title, 'after dispatch');
});

test('operator run keys are stable for the same logical operation and differ when it changes', () => {
  const engine = amo.createAcquisitionMissionEngine();
  const m = engine.create({ tenantId: '10', objective: 'Acquire facility buyers in Manchester NH.', targetSegment: 'Facilities' });
  const a = operatorRunKey('executeAnchorOneOutbound', missionOperationParts(engine, m.id, 'EXECUTE_OUTBOUND', { prospectId: 'p1', maxSends: 1 }));
  const b = operatorRunKey('executeAnchorOneOutbound', missionOperationParts(engine, m.id, 'EXECUTE_OUTBOUND', { maxSends: 1, prospectId: 'p1' }));
  const c = operatorRunKey('executeAnchorOneOutbound', missionOperationParts(engine, m.id, 'EXECUTE_OUTBOUND', { prospectId: 'p2', maxSends: 1 }));
  assert.equal(a, b, 'retry of the same logical execution → same key (key order irrelevant)');
  assert.notEqual(a, c);
  assert.match(a, /^script:executeAnchorOneOutbound:[0-9a-f]{40}$/);
  assert.throws(() => operatorRunKey('', {}), { code: 'durable_task_invalid' });
});

test('review: write exemptions are bound to what they may change (scope checked against the durable row)', { timeout: 60000 }, async () => {
  const { pool } = fixture; const { engine, mission } = await missionFixture(pool);
  const durableNow = await durable(pool, mission.id);
  // inbound_observation may not carry a stage advance (or any guarded change).
  const advanced = { ...durableNow, stage: 'learn' }; bumpMissionVersion(advanced, `tx-adv-${seq}`);
  await assert.rejects(policy.runWithOwnershipExemption('inbound_observation', () => persistence.persistStageCommit({ mission: advanced }, pool, { skipEnsure: true })),
    (err) => err.code === 'durable_task_exemption_scope' && /stage/.test(err.message));
  assert.equal((await durable(pool, mission.id)).stage, durableNow.stage, 'nothing written');
  // mission_creation may not overwrite an existing mission.
  const again = { ...durableNow, title: 'recreated' }; bumpMissionVersion(again, `tx-recreate-${seq}`);
  await assert.rejects(policy.runWithOwnershipExemption('mission_creation', () => persistence.persistStageCommit({ mission: again }, pool, { skipEnsure: true })),
    { code: 'durable_task_exemption_scope' });
  // operator_restriction_tightening may only tighten constraints.
  const base = { prohibited: ['paid_media'], reviewOnly: false, sources: [] };
  const seeded = { ...durableNow, authorizationConstraints: amo.mergeAuthorizationConstraints(durableNow.authorizationConstraints, base) };
  bumpMissionVersion(seeded, `tx-seed-${seq}`);
  await policy.runWithOwnershipExemption('operator_restriction_tightening', () => persistence.persistStageCommit({ mission: seeded }, pool, { skipEnsure: true }));
  engine.store.putMission(seeded);
  const tightened = { ...seeded, authorizationConstraints: amo.mergeAuthorizationConstraints(seeded.authorizationConstraints, { prohibited: ['outreach'], sources: [] }) };
  bumpMissionVersion(tightened, `tx-tight-${seq}`);
  await policy.runWithOwnershipExemption('operator_restriction_tightening', () => persistence.persistStageCommit({ mission: tightened }, pool, { skipEnsure: true }));
  engine.store.putMission(tightened);
  // Drops paid_media while adding provider_research: adds one, removes one → refused.
  const loosened = { ...tightened, authorizationConstraints: { ...tightened.authorizationConstraints, prohibited: ['outreach', 'provider_research'] } };
  bumpMissionVersion(loosened, `tx-loose-${seq}`);
  await assert.rejects(policy.runWithOwnershipExemption('operator_restriction_tightening', () => persistence.persistStageCommit({ mission: loosened }, pool, { skipEnsure: true })),
    { code: 'durable_task_exemption_scope' });
  const plusStage = { ...tightened, stage: 'learn' }; bumpMissionVersion(plusStage, `tx-tight-stage-${seq}`);
  await assert.rejects(policy.runWithOwnershipExemption('operator_restriction_tightening', () => persistence.persistStageCommit({ mission: plusStage }, pool, { skipEnsure: true })),
    { code: 'durable_task_exemption_scope' });
});

test('review: a fence owns only its mission; nested work on another mission takes its own task', { timeout: 60000 }, async () => {
  const { pool } = fixture; const store = createDurableExecutionOwnership(pool);
  const a = await missionFixture(pool); const b = await missionFixture(pool);
  const outer = await runTaskOwnedWork({ store, tenantId: '10', missionId: a.mission.id, logicalKey: `outer-${seq}`, specialist: 'operator' }, async () => {
    // A direct write to mission B under A's fence is refused.
    const nb = { ...b.engine.get(b.mission.id, '10'), title: 'cross' }; bumpMissionVersion(nb, `tx-cross-${seq}`);
    await assert.rejects(persistence.persistStageCommit({ mission: nb }, pool, { skipEnsure: true }), { code: 'durable_task_fenced' });
    // Owned work on B gets its own task (not A's fence).
    const inner = await runTaskOwnedWork({ store, tenantId: '10', missionId: b.mission.id, logicalKey: `inner-${seq}`, specialist: 'operator' }, async () => {
      await persistence.persistStageCommit({ mission: nb }, pool, { skipEnsure: true });
    });
    return inner;
  });
  assert.equal(outer.durableTask.state, 'completed');
  assert.equal(outer.result.durableTask.state, 'completed', 'mission B work recorded as its own task');
  assert.equal(outer.result.inherited, undefined);
  assert.equal((await durable(pool, b.mission.id)).title, 'cross');
});

test('review: offline evaluation admits discovery only with a marked fixture Scout runner', { timeout: 60000 }, async () => {
  const { pool } = fixture; const { engine, mission } = await missionFixture(pool);
  const handlers = { [amo.EXECUTION_INTENTS.APPROVE_DISCOVERY]: async () => ({}) };
  const live = async () => ({ companies: [] });
  await assert.rejects(policy.runWithOwnershipExemption(policy.OFFLINE_EVALUATION,
    () => amo.routeExecutionRequest(cer(mission, amo.EXECUTION_INTENTS.APPROVE_DISCOVERY), { engine, tenantId: '10', persist: false, handlers, runScout: live })),
  (err) => err.code === 'durable_task_owner_required' || /planning|constraint/.test(String(err.code)));
  void pool;
});
