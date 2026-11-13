'use strict';

/**
 * P0 — compatibility of missions persisted before PR #1127, against a
 * disposable PostgreSQL instance (never a shared database).
 *
 * Pre-deploy rows are simulated by writing the pre-PR status ("Discovering")
 * into durable rows whose plans carry no researchPlanning marker:
 *  - an ordinary acquisition mission awaiting discovery approval;
 *  - a review-only mission planned before review-only planning existed (its
 *    plan was locked with market/region and it advertises discovery approval).
 *
 * After restart: stored contract hashes are unchanged, pending decisions are
 * unchanged, hydration writes nothing, statuses are reconciled in memory and
 * reach the database only on the next committed write, and discovery on the
 * legacy review-only mission is still refused without any Scout call.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const TENANT = '25';
const OBJECTIVE = 'Acquire one recurring commercial cleaning client from a property management company in Nashua NH.';
const ADMIN = { id: 'admin-1', role: 'admin', name: 'Admin' };

test('legacy persisted missions hydrate unchanged and stay safe', async (t) => {
  const pg = await startDisposablePostgres('p0-legacy-hydration-');
  const pool = new Pool({ connectionString: pg.connectionString });
  pool.on('error', () => {});
  t.after(async () => {
    await pool.end().catch(() => {});
    await pg.stop();
  });

  const { createAcquisitionMissionRuntime, setAcquisitionMissionRuntimeForTests } =
    require('../services/acquisitionMissionRuntime');
  const svc = require('../services/acquisitionMission');
  const amo = require('../packages/acquisition-mission');
  const { planFromObjective } = require('../packages/acquisition-mission/MissionPlanner');
  const { freezeStructuredMission } = require('../packages/acquisition-mission/StructuredMission');
  const { deriveProgressionStage, PROGRESSION_STAGES } = require('../packages/acquisition-mission/MissionProgression');
  const runtimeFor = () => createAcquisitionMissionRuntime({ pool, persist: true, production: false });
  const writer = runtimeFor();
  setAcquisitionMissionRuntimeForTests(writer);
  t.after(() => setAcquisitionMissionRuntimeForTests(null));

  // Plans locked the pre-PR way: market and region present, no researchPlanning.
  const legacyPlan = () => freezeStructuredMission(planFromObjective(OBJECTIVE, {}).draft, { approvedBy: 'legacy' });
  const ordinary = await writer.create({
    tenantId: TENANT, clientId: Number(TENANT), objective: OBJECTIVE, createdBy: 'legacy',
    structuredMission: legacyPlan(),
  }, { pool, persist: true });
  const legacyReviewOnly = await writer.create({
    tenantId: TENANT, clientId: Number(TENANT), objective: `${OBJECTIVE} Review only.`, createdBy: 'legacy',
    structuredMission: legacyPlan(),
    authorizationConstraints: amo.extractAuthorizationConstraints('Review only. No outreach, calls, content, or campaign activation.'),
  }, { pool, persist: true });

  // Rewrite both rows to the status the pre-PR code stored.
  for (const id of [ordinary.id, legacyReviewOnly.id]) {
    await pool.query(
      `UPDATE acquisition_missions
          SET status = 'Discovering', payload = jsonb_set(payload, '{status}', '"Discovering"')
        WHERE id = $1`,
      [id]
    );
  }
  const legacyRows = Object.fromEntries((await pool.query(
    'SELECT id, status, payload FROM acquisition_missions WHERE tenant_id = $1', [TENANT])).rows.map((r) => [r.id, r]));
  for (const id of [ordinary.id, legacyReviewOnly.id]) {
    assert.equal(legacyRows[id].status, 'Discovering');
    assert.equal(legacyRows[id].payload.pendingOperatorDecision.kind, 'discovery_approval');
    assert.equal('researchPlanning' in legacyRows[id].payload.structuredMission, false);
  }

  // --- Restart.
  const restarted = runtimeFor();
  setAcquisitionMissionRuntimeForTests(restarted);
  const hydrated = Object.fromEntries((await restarted.list(TENANT)).map((m) => [m.id, m]));

  for (const id of [ordinary.id, legacyReviewOnly.id]) {
    const mission = hydrated[id];
    const stored = legacyRows[id].payload.structuredMission;
    assert.equal(mission.structuredMission.contractHash, stored.contractHash, 'stored contract hash unchanged');
    assert.equal('researchPlanning' in mission.structuredMission, false, 'legacy plan gains no marker');
    assert.equal(mission.pendingOperatorDecision.kind, 'discovery_approval', 'pending decision unchanged');
  }
  assert.equal(hydrated[ordinary.id].status, 'Awaiting discovery approval');
  assert.equal(hydrated[legacyReviewOnly.id].status, 'Discovery not authorized');
  assert.equal(
    deriveProgressionStage(restarted.engine().inspect(legacyReviewOnly.id, { tenantId: TENANT })),
    PROGRESSION_STAGES.DISCOVERY_APPROVAL,
    'a legacy plan is not reclassified as a review-only resting plan'
  );

  // Hydration writes nothing.
  const afterHydrate = Object.fromEntries((await pool.query(
    'SELECT id, status, updated_at FROM acquisition_missions WHERE tenant_id = $1', [TENANT])).rows.map((r) => [r.id, r]));
  for (const id of [ordinary.id, legacyReviewOnly.id]) {
    assert.equal(afterHydrate[id].status, 'Discovering');
  }

  // Discovery on the legacy review-only mission is still refused; no Scout call.
  let scoutCalls = 0;
  await assert.rejects(
    svc.executeCanonical({
      tenantId: TENANT, missionId: legacyReviewOnly.id, intent: 'APPROVE_DISCOVERY', role: 'operator', operatorId: 'op',
      runScout: async () => { scoutCalls += 1; return {}; },
    }, { pool }),
    (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED
  );
  assert.equal(scoutCalls, 0);

  // The reconciled label reaches the database on the next committed write
  // (a tightening amendment here), not before.
  const preview = await svc.previewMissionAuthorization({ tenantId: TENANT, missionId: ordinary.id }, { pool });
  await svc.amendMissionAuthorization({
    tenantId: TENANT, missionId: ordinary.id,
    change: { add: ['content'], lift: [], reason: 'tighten' },
    expectedConstraintsHash: preview.constraintsHash,
    amendmentId: 'p0-legacy-tighten-1', approver: ADMIN,
  }, { pool, persist: true });
  const written = (await pool.query('SELECT status, payload FROM acquisition_missions WHERE id = $1', [ordinary.id])).rows[0];
  assert.equal(written.status, 'Awaiting discovery approval');
  assert.equal(written.payload.structuredMission.contractHash, legacyRows[ordinary.id].payload.structuredMission.contractHash);
  assert.equal(written.payload.pendingOperatorDecision.kind, 'discovery_approval');
});
