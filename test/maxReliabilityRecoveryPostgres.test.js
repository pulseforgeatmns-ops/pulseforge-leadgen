'use strict';

/**
 * P0 Max reliability recovery — durable mission state on real, disposable
 * PostgreSQL. A "restart" is a fresh Acquisition Mission Runtime (new
 * in-memory engine) over the same database: committed missions must be
 * recovered, tenants must stay isolated, and a lost database must fail closed.
 *
 * Never touches DATABASE_URL or any shared database.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const { maybeHandleAcquisitionOwnershipTurn } = require('../packages/max/workspace/AcquisitionOwnership');

const NEW_MISSION =
  'Create a new acquisition mission. Objective: Acquire one recurring commercial cleaning client ' +
  'from a property management company in the Greater Manchester area.';
const OBJECTIVE_ONLY =
  'Acquire one recurring commercial cleaning client from a property management company in the Greater Manchester area.';
const ROUTED = {
  primaryObjective: 'mission_creation',
  routingDecision: { owner: 'mission_creation', pipeline: 'MissionRuntime', reason: 'primary_objective:mission_creation' },
};
const NO_CIE = { getApprovedClientBlueprint: async () => null };

function runtimeFor(pool) {
  return createAcquisitionMissionRuntime({ pool, persist: true, production: false });
}

async function createMission(runtime, tenantId, question = NEW_MISSION) {
  return maybeHandleAcquisitionOwnershipTurn({
    question,
    context: { tenantId },
    objectiveResolution: ROUTED,
    acquisitionMissionRuntime: runtime,
    cieService: NO_CIE,
  });
}

test('committed missions survive restart, stay tenant-isolated, and a lost database fails closed', async (t) => {
  const pg = await startDisposablePostgres('max-p0-recovery-');
  const pool = new Pool({ connectionString: pg.connectionString });
  pool.on('error', () => {}); // idle clients are terminated when the DB stops
  let stopped = false;
  t.after(async () => {
    await pool.end().catch(() => {});
    if (!stopped) await pg.stop();
  });

  // Process 1 — create and commit.
  const first = await createMission(runtimeFor(pool), '10');
  assert.equal(first.created, true);
  const missionId = first.mission.id;
  const { rows } = await pool.query('SELECT id FROM acquisition_missions WHERE id = $1', [missionId]);
  assert.equal(rows.length, 1, 'mission row committed to PostgreSQL');

  // Process 2 — restart: a fresh runtime recovers committed state from the DB.
  const restarted = runtimeFor(pool);
  const recovered = await restarted.list('10');
  assert.deepEqual(recovered.map((m) => m.id), [missionId]);
  assert.equal(recovered[0].stage, first.mission.stage, 'stage recovered unchanged');

  // Re-issuing the objective after restart (no explicit "new") resumes the
  // recovered mission instead of duplicating it (SPEC-200 duplicate prevention).
  const retried = await createMission(restarted, '10', OBJECTIVE_ONLY);
  assert.equal(retried.created, false);
  assert.equal(retried.mission.id, missionId);
  const count = await pool.query('SELECT count(*)::int AS n FROM acquisition_missions');
  assert.equal(count.rows[0].n, 1);

  // Tenant isolation across the shared store.
  assert.deepEqual(await runtimeFor(pool).list('11'), []);

  // Lost database — fail closed, no phantom mission.
  await pool.end();
  await pg.stop();
  stopped = true;
  const deadPool = new Pool({ connectionString: pg.connectionString, connectionTimeoutMillis: 1000 });
  deadPool.on('error', () => {});
  t.after(() => deadPool.end().catch(() => {}));
  const blockedRuntime = runtimeFor(deadPool);
  const blocked = await createMission(blockedRuntime, '10');
  assert.equal(blocked.reason, 'acquisition_mission_persistence_unavailable');
  assert.equal(blocked.created, false);
  assert.equal(blocked.mission, null);
  assert.equal(blockedRuntime.engine().list('10').length, 0);
});

test('authorization constraints are durable across restart and only ever tighten', async (t) => {
  const pg = await startDisposablePostgres('max-p0-constraints-');
  const pool = new Pool({ connectionString: pg.connectionString });
  pool.on('error', () => {});
  t.after(async () => {
    await pool.end().catch(() => {});
    await pg.stop();
  });

  const REVIEW_ONLY =
    'Set up a review-only contact intake mission. Objective: Acquire one recurring commercial cleaning client ' +
    'from a property management company in the Greater Manchester area. No outreach.';
  const created = await createMission(runtimeFor(pool), '10', REVIEW_ONLY);
  assert.equal(created.created, true);
  assert.equal(created.mission.authorizationConstraints.reviewOnly, true);

  // Restart: constraints come back from the durable payload.
  const restarted = runtimeFor(pool);
  const [recovered] = await restarted.list('10');
  assert.equal(recovered.id, created.mission.id);
  assert.equal(recovered.authorizationConstraints.reviewOnly, true);
  assert.ok(recovered.authorizationConstraints.prohibited.includes('outreach'));

  // Re-issuing the objective without restrictions resumes and cannot loosen.
  const resumed = await createMission(restarted, '10', OBJECTIVE_ONLY);
  assert.equal(resumed.mission.id, created.mission.id);
  const [afterResume] = await runtimeFor(pool).list('10');
  assert.deepEqual(afterResume.authorizationConstraints.prohibited, recovered.authorizationConstraints.prohibited);

  // A resumed request that adds a restriction tightens durably.
  const tightened = await createMission(runtimeFor(pool), '10', `${OBJECTIVE_ONLY} No content.`);
  assert.equal(tightened.mission.id, created.mission.id);
  const { rows } = await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [created.mission.id]);
  const durable = rows[0].payload.authorizationConstraints;
  assert.equal(durable.reviewOnly, true);
  assert.ok(durable.prohibited.includes('content'), 'added restriction committed');
  assert.ok(durable.prohibited.includes('outreach'), 'existing restriction kept');
});

test('governed amendments and synthetic stamps are durable and replay-safe across restart', async (t) => {
  const pg = await startDisposablePostgres('max-p0-amend-');
  const pool = new Pool({ connectionString: pg.connectionString });
  pool.on('error', () => {});
  t.after(async () => {
    await pool.end().catch(() => {});
    await pg.stop();
  });
  const service = require('../services/acquisitionMission');
  const amo = require('../packages/acquisition-mission');
  const approval = require('../packages/max/workspace/AmoOperatorApproval');

  const created = await createMission(runtimeFor(pool), '10',
    'Create a new acquisition mission. Objective: Acquire one recurring commercial cleaning client ' +
    'from a property management company in the Greater Manchester area. No outreach.');
  const missionId = created.mission.id;

  // Restart, preview, amend through the service (durable TME commit).
  const preview = await service.previewMissionAuthorization({ tenantId: '10', missionId },
    { pool, acquisitionMissionRuntime: runtimeFor(pool) });
  const input = {
    tenantId: '10', missionId,
    change: { lift: ['outreach', 'outreach_authorization'], reason: 'Approved live outreach after review.' },
    expectedConstraintsHash: preview.constraintsHash,
    amendmentId: 'amend_pg_restart_0001',
    approver: { id: '7', role: 'admin', name: 'Admin' },
  };
  const applied = await service.amendMissionAuthorization(input, { pool, acquisitionMissionRuntime: runtimeFor(pool) });
  assert.equal(applied.alreadyApplied, false);

  // Restart: amended constraints and the audit contribution come back from PostgreSQL.
  const [recovered] = await runtimeFor(pool).list('10');
  assert.equal(recovered.authorizationConstraints.revision, 1);
  assert.ok(!recovered.authorizationConstraints.prohibited.includes('outreach'));
  const rows = (await pool.query(
    `SELECT payload FROM acquisition_mission_contributions WHERE mission_id = $1 AND kind = 'constraints'`, [missionId]
  )).rows;
  assert.equal(rows.length, 1);
  // The payload column stores the whole contribution row; the operator record is nested.
  const stored = rows[0].payload.payload || rows[0].payload;
  assert.equal(stored.action, 'authorization_amendment');
  assert.equal(stored.approvedBy.role, 'admin');
  assert.deepEqual(stored.lifted, ['outreach', 'outreach_authorization']);

  // Replay after restart applies nothing twice.
  const replay = await service.amendMissionAuthorization(input, { pool, acquisitionMissionRuntime: runtimeFor(pool) });
  assert.equal(replay.alreadyApplied, true);
  const count = await pool.query(
    `SELECT count(*)::int AS n FROM acquisition_mission_contributions WHERE mission_id = $1 AND kind = 'constraints'`, [missionId]
  );
  assert.equal(count.rows[0].n, 1);

  // Synthetic stamp: a fixture discovery committed durably stays synthetic after restart.
  const runtime = runtimeFor(pool);
  await runtime.hydrate('10');
  const engine = runtime.engine();
  const persistDurable = require('../services/acquisitionMissionPersistence')
    .bindStagePersistDurable({ pool }, engine, '10');
  await approval.advancePlanAfterApproval({ engine, mission: engine.get(missionId, '10'), tenantId: '10',
    question: 'Approved.', pool, persistStage: persistDurable });
  await approval.advanceDiscoveryAfterApproval({ engine, mission: engine.get(missionId, '10'), tenantId: '10',
    question: 'Approved.', allowFixtureFallback: true, pool, persistStage: persistDurable });
  const [afterRestart] = await runtimeFor(pool).list('10');
  assert.equal(afterRestart.syntheticEvidence && afterRestart.syntheticEvidence.provenance, 'synthetic_fixture');
  assert.ok(amo.missionHasSyntheticEvidence(afterRestart));
});
