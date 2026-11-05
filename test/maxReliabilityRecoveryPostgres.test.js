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
