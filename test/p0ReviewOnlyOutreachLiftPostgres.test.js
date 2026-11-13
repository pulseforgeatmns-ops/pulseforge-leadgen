'use strict';

/**
 * P0 — a governed amendment that lifts outreach on an approved review-only
 * mission, but leaves provider research prohibited, against a disposable
 * PostgreSQL instance (never a shared database).
 *
 * The mission is no longer review-only, so it must not stay classified as
 * Review Only; it reads "discovery not authorized" consistently (status,
 * progression stage, workspace mode) before and after a restart, nothing is
 * reopened or re-versioned, and discovery is still refused with no Scout call.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const TENANT = '25';
const REVIEW_ONLY =
  'Set up a review-only contact intake mission for an existing customer. ' +
  'No outreach, calls, content, or campaign activation.';
const ROUTED = {
  primaryObjective: 'mission_creation',
  routingDecision: { owner: 'mission_creation', pipeline: 'MissionRuntime', reason: 'primary_objective:mission_creation' },
};
const ADMIN = { id: 'admin-1', role: 'admin', name: 'Admin' };

test('outreach lift on a review-only plan is durable and classified from current authorization', async (t) => {
  const pg = await startDisposablePostgres('p0-outreach-lift-');
  const pool = new Pool({ connectionString: pg.connectionString });
  pool.on('error', () => {});
  t.after(async () => {
    await pool.end().catch(() => {});
    await pg.stop();
  });

  const { createAcquisitionMissionRuntime, setAcquisitionMissionRuntimeForTests } =
    require('../services/acquisitionMissionRuntime');
  const { maybeHandleAcquisitionOwnershipTurn } = require('../packages/max/workspace/AcquisitionOwnership');
  const svc = require('../services/acquisitionMission');
  const amo = require('../packages/acquisition-mission');
  const { deriveProgressionStage, PROGRESSION_STAGES } = require('../packages/acquisition-mission/MissionProgression');
  const { deriveWorkspaceMode, WORKSPACE_MODES } = require('../packages/acquisition-mission/WorkspaceMode');
  const runtimeFor = () => createAcquisitionMissionRuntime({ pool, persist: true, production: false });
  const runtime = runtimeFor();
  setAcquisitionMissionRuntimeForTests(runtime);
  t.after(() => setAcquisitionMissionRuntimeForTests(null));

  const created = await maybeHandleAcquisitionOwnershipTurn({
    question: REVIEW_ONLY, context: { tenantId: TENANT }, objectiveResolution: ROUTED,
    acquisitionMissionRuntime: runtime, cieService: { getApprovedClientBlueprint: async () => null },
  });
  const missionId = created.mission.id;
  await svc.executeCanonical({ tenantId: TENANT, missionId, intent: 'APPROVE_PLAN', role: 'operator', operatorId: 'op' }, { pool });
  let row = (await pool.query('SELECT status, payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0];
  assert.equal(row.status, 'Plan approved (review only)');
  const versionBefore = row.payload.version;
  const contractHash = row.payload.structuredMission.contractHash;

  const preview = await svc.previewMissionAuthorization({ tenantId: TENANT, missionId }, { pool });
  const amended = await svc.amendMissionAuthorization({
    tenantId: TENANT, missionId,
    change: { add: [], lift: ['outreach'], reason: 'Acceptance: allow outreach later' },
    expectedConstraintsHash: preview.constraintsHash,
    amendmentId: 'p0-pg-lift-outreach-1', approver: ADMIN,
  }, { pool, persist: true });
  assert.equal(amended.loosens, true);

  row = (await pool.query('SELECT status, payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0];
  assert.equal(row.payload.authorizationConstraints.reviewOnly, false);
  assert.ok(row.payload.authorizationConstraints.prohibited.includes('provider_research'));
  assert.equal(row.payload.authorizationConstraints.prohibited.includes('outreach'), false);
  assert.equal(row.status, 'Plan approved (discovery not authorized)');
  assert.equal(row.payload.pendingOperatorDecision, null);
  assert.equal(row.payload.structuredMission.immutable, true, 'not reopened while research is prohibited');
  assert.equal(row.payload.structuredMission.contractHash, contractHash);
  assert.equal(row.payload.version, versionBefore, 'constraints-only amendment: version unchanged');
  assert.equal(row.payload.authorizationConstraints.amendments.at(-1).planningReopened, undefined);

  // Restart: the same consistent classification is recovered.
  const restarted = runtimeFor();
  setAcquisitionMissionRuntimeForTests(restarted);
  const [recovered] = await restarted.list(TENANT);
  const snapshot = restarted.engine().inspect(missionId, { tenantId: TENANT });
  assert.equal(recovered.status, 'Plan approved (discovery not authorized)');
  assert.equal(deriveProgressionStage(snapshot), PROGRESSION_STAGES.DISCOVERY_NOT_AUTHORIZED);
  assert.equal(deriveWorkspaceMode({ missionId, snapshot }), WORKSPACE_MODES.DISCOVERY_NOT_AUTHORIZED);
  assert.equal(recovered.pendingOperatorDecision, null);

  // Replay after restart is idempotent.
  const replay = await svc.amendMissionAuthorization({
    tenantId: TENANT, missionId,
    change: { add: [], lift: ['outreach'], reason: 'Acceptance: allow outreach later' },
    expectedConstraintsHash: preview.constraintsHash,
    amendmentId: 'p0-pg-lift-outreach-1', approver: ADMIN,
  }, { pool, persist: true });
  assert.equal(replay.alreadyApplied, true);

  // Discovery is still refused after restart; no Scout call.
  let scoutCalls = 0;
  await assert.rejects(
    svc.executeCanonical({
      tenantId: TENANT, missionId, intent: 'APPROVE_DISCOVERY', role: 'operator', operatorId: 'op',
      runScout: async () => { scoutCalls += 1; return {}; },
    }, { pool }),
    (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED
  );
  assert.equal(scoutCalls, 0);
  // Tenant isolation: another tenant sees nothing.
  assert.deepEqual(await runtimeFor().list('26'), []);
});
