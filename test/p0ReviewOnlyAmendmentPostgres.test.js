'use strict';

/**
 * P0 Max reliability — review-only lifecycle, governed amendment and REST
 * sender ordering against a disposable PostgreSQL instance.
 *
 * Never touches a shared database: DATABASE_URL is pointed at the disposable
 * instance inside this process before the route or pool modules load.
 *
 * Covers:
 *  - a review-only mission persists at plan approval with research planning
 *    deferred and, once approved, rests with no actionable decision;
 *  - the amendment that permits research reopens planning in one durable
 *    commit that survives restart, and discovery cannot run before the
 *    research plan is approved again;
 *  - the execute route refuses prohibited actions with 403 even when the
 *    tenant's sender is incomplete, returns 422 (not 500) for an incomplete
 *    sender on an outbound intent, and never reads the sender for intents that
 *    do not send.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const TENANT = '25';
const REVIEW_ONLY =
  'Set up a review-only contact intake mission for an existing customer. ' +
  'No outreach, calls, content, or campaign activation.';
const ORDINARY =
  'Create a new acquisition mission. Objective: Acquire one recurring commercial cleaning client ' +
  'from a property management company in Nashua NH.';
const ROUTED = {
  primaryObjective: 'mission_creation',
  routingDecision: { owner: 'mission_creation', pipeline: 'MissionRuntime', reason: 'primary_objective:mission_creation' },
};
const NO_CIE = { getApprovedClientBlueprint: async () => null };
const ADMIN = { id: 'admin-1', role: 'admin', name: 'Admin' };

function post(port, path, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = http.request({
      host: '127.0.0.1', port, path, method: 'POST',
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) },
    }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: data ? JSON.parse(data) : null }));
    });
    req.on('error', reject);
    req.end(payload);
  });
}

test('review-only lifecycle, governed amendment and REST sender ordering are durable', async (t) => {
  const pg = await startDisposablePostgres('p0-review-only-');
  const previousUrl = process.env.DATABASE_URL;
  const previousSsl = process.env.DATABASE_SSL;
  process.env.DATABASE_URL = pg.connectionString;
  process.env.DATABASE_SSL = 'false';
  const pool = new Pool({ connectionString: pg.connectionString });
  pool.on('error', () => {});
  let server = null;
  t.after(async () => {
    if (server) await new Promise((resolve) => server.close(resolve));
    await pool.end().catch(() => {});
    await require('../db').end().catch(() => {});
    await pg.stop();
    if (previousUrl === undefined) delete process.env.DATABASE_URL; else process.env.DATABASE_URL = previousUrl;
    if (previousSsl === undefined) delete process.env.DATABASE_SSL; else process.env.DATABASE_SSL = previousSsl;
  });

  // Tenant 25 exists with an INCOMPLETE sender identity (no sending_domain).
  await pool.query(`CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY, name TEXT, sender_email TEXT, sender_name TEXT, sending_domain TEXT, active BOOLEAN DEFAULT TRUE)`);
  await pool.query(`INSERT INTO clients (id, name, sender_email, sender_name, sending_domain)
    VALUES (25, 'P0 Acceptance', 'ops@example.invalid', 'Ops', NULL)`);

  const { createAcquisitionMissionRuntime, setAcquisitionMissionRuntimeForTests } =
    require('../services/acquisitionMissionRuntime');
  const { maybeHandleAcquisitionOwnershipTurn } = require('../packages/max/workspace/AcquisitionOwnership');
  const svc = require('../services/acquisitionMission');
  const amo = require('../packages/acquisition-mission');
  const runtimeFor = () => createAcquisitionMissionRuntime({ pool, persist: true, production: false });
  const runtime = runtimeFor();
  setAcquisitionMissionRuntimeForTests(runtime);
  t.after(() => setAcquisitionMissionRuntimeForTests(null));

  // Real execute route behind a stub admin session for tenant 25.
  const express = require('express');
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    req.session = { user: { id: 1, email: 'admin@example.invalid', role: 'admin' }, active_client_id: Number(TENANT) };
    next();
  });
  app.use('/', require('../routes/acquisitionMissions'));
  server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  const { port } = server.address();
  const execute = (missionId, intent) => post(port, `/api/v1/amo/missions/${missionId}/execute`, { intent });

  // --- Review-only creation persists at plan approval, research deferred.
  const created = await maybeHandleAcquisitionOwnershipTurn({
    question: REVIEW_ONLY, context: { tenantId: TENANT }, objectiveResolution: ROUTED,
    acquisitionMissionRuntime: runtime, cieService: NO_CIE,
  });
  assert.equal(created.created, true);
  const missionId = created.mission.id;
  let row = (await pool.query('SELECT status, payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0];
  assert.equal(row.status, 'Plan ready for approval');
  assert.equal(row.payload.pendingOperatorDecision.kind, 'plan_approval');
  assert.equal(row.payload.missionPlanDraft.researchPlanning.deferred, true);
  assert.equal(row.payload.missionPlanDraft.successMetric.type, 'operator_review');
  assert.equal(row.payload.authorizationConstraints.reviewOnly, true);
  assert.deepEqual(row.payload.authorizationConstraints.prohibited,
    ['content', 'outreach', 'outreach_authorization', 'paid_media', 'provider_research']);

  // --- Prohibited actions: 403 although the sender is incomplete.
  for (const intent of ['EXECUTE_OUTBOUND', 'APPROVE_EXECUTION', 'APPROVE_DISCOVERY', 'GENERATE_VARIANTS']) {
    const res = await execute(missionId, intent);
    assert.equal(res.status, 403, `${intent}: ${JSON.stringify(res.body)}`);
    assert.equal(res.body.error, 'authorization_constraint_blocked', intent);
  }

  // --- Plan approval needs no sender: it is not blocked by the incomplete sender.
  const approved = await execute(missionId, 'APPROVE_PLAN');
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  row = (await pool.query('SELECT status, payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0];
  assert.equal(row.payload.structuredMission.immutable, true);
  assert.equal(row.payload.structuredMission.researchPlanning.deferred, true);
  assert.equal(row.payload.pendingOperatorDecision, null, 'no discovery approval is advertised');
  assert.equal(row.status, 'Plan approved (review only)');

  // Restart: the resting state is recovered unchanged.
  let [recovered] = await runtimeFor().list(TENANT);
  assert.equal(recovered.pendingOperatorDecision, null);
  assert.equal(recovered.status, 'Plan approved (review only)');

  // --- Governed amendment permitting research: one durable commit reopens planning.
  const versionBeforeAmendment = row.payload.version;
  const approvedContractHash = row.payload.structuredMission.contractHash;
  const preview = await svc.previewMissionAuthorization({ tenantId: TENANT, missionId }, { pool });
  const amendment = await svc.amendMissionAuthorization({
    tenantId: TENANT,
    missionId,
    change: { add: [], lift: ['provider_research'], reason: 'Acceptance: allow research' },
    expectedConstraintsHash: preview.constraintsHash,
    amendmentId: 'p0-pg-lift-research-1',
    approver: ADMIN,
  }, { pool, persist: true });
  row = (await pool.query('SELECT status, payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0];
  assert.equal(row.payload.structuredMission, null);
  assert.equal(row.payload.pendingOperatorDecision.kind, 'plan_clarification');
  assert.equal(row.payload.pendingOperatorDecision.field, 'geography.region');
  assert.equal(row.payload.missionPlanDraft.successMetric.type, 'customers');
  assert.equal('researchPlanning' in row.payload.missionPlanDraft, false);
  assert.equal(row.status, 'Clarification required');
  assert.deepEqual(row.payload.authorizationConstraints.prohibited,
    ['content', 'outreach', 'outreach_authorization', 'paid_media']);
  // M1 — the reopen is a versioned mutation carrying the amendment's transaction.
  assert.equal(row.payload.version, versionBeforeAmendment + 1, 'mission version bumped durably');
  assert.equal(row.payload.lastTransactionId, amendment.transactionId);
  const durableRecord = row.payload.authorizationConstraints.amendments.at(-1);
  assert.equal(durableRecord.planningReopened, true);
  assert.deepEqual(durableRecord.supersededPlan, { contractHash: approvedContractHash, approved: true });
  assert.equal(durableRecord.transactionId, amendment.transactionId);
  const amendments = (await pool.query(
    `SELECT count(*)::int AS n FROM acquisition_mission_contributions
      WHERE mission_id = $1 AND payload->'payload'->>'amendmentId' = 'p0-pg-lift-research-1'
         OR (mission_id = $1 AND payload->>'amendmentId' = 'p0-pg-lift-research-1')`, [missionId])).rows[0].n;
  assert.ok(amendments >= 1, 'amendment contribution committed');

  // Restart: the reopened planning state is recovered unchanged.
  [recovered] = await runtimeFor().list(TENANT);
  assert.equal(recovered.pendingOperatorDecision.kind, 'plan_clarification');
  assert.equal(recovered.structuredMission, null);

  // Replaying the amendment is idempotent and does not reopen again.
  const replay = await svc.amendMissionAuthorization({
    tenantId: TENANT, missionId,
    change: { add: [], lift: ['provider_research'], reason: 'Acceptance: allow research' },
    expectedConstraintsHash: preview.constraintsHash, amendmentId: 'p0-pg-lift-research-1', approver: ADMIN,
  }, { pool, persist: true });
  assert.equal(replay.alreadyApplied, true);
  const afterReplay = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [missionId])).rows[0];
  assert.equal(afterReplay.payload.version, versionBeforeAmendment + 1, 'replay does not bump again');

  // Discovery before the research plan is approved never reaches Scout.
  let scoutCalls = 0;
  const discovery = await svc.executeCanonical({
    tenantId: TENANT, missionId, intent: 'APPROVE_DISCOVERY', role: 'operator', operatorId: 'op',
    runScout: async () => { scoutCalls += 1; return {}; },
  }, { pool }).catch((err) => ({ error: err }));
  assert.ok(discovery.error || (discovery.execution && discovery.execution.state !== 'completed_verified'));
  assert.equal(scoutCalls, 0);
  // Outreach is still prohibited: 403, sender still incomplete.
  const send = await execute(missionId, 'EXECUTE_OUTBOUND');
  assert.equal(send.status, 403);
  assert.equal(send.body.error, 'authorization_constraint_blocked');

  // --- Ordinary mission, outbound intent, incomplete sender: 422, never 500.
  const ordinary = await maybeHandleAcquisitionOwnershipTurn({
    question: ORDINARY, context: { tenantId: '26' }, objectiveResolution: ROUTED,
    acquisitionMissionRuntime: runtime, cieService: NO_CIE,
  });
  await pool.query(`INSERT INTO clients (id, name, sender_email, sender_name, sending_domain)
    VALUES (26, 'Ordinary', 'ops@example.invalid', 'Ops', NULL)`);
  const ordinaryApp = express();
  ordinaryApp.use(express.json());
  ordinaryApp.use((req, _res, next) => {
    req.session = { user: { id: 1, email: 'admin@example.invalid', role: 'admin' }, active_client_id: 26 };
    next();
  });
  ordinaryApp.use('/', require('../routes/acquisitionMissions'));
  const ordinaryServer = ordinaryApp.listen(0, '127.0.0.1');
  t.after(() => new Promise((resolve) => ordinaryServer.close(resolve)));
  await new Promise((resolve) => ordinaryServer.once('listening', resolve));
  const ordinaryPort = ordinaryServer.address().port;
  const outbound = await post(ordinaryPort, `/api/v1/amo/missions/${ordinary.mission.id}/execute`, { intent: 'EXECUTE_OUTBOUND' });
  assert.equal(outbound.status, 422, JSON.stringify(outbound.body));
  assert.equal(outbound.body.error, 'canonical_sender_incomplete');
  assert.doesNotMatch(JSON.stringify(outbound.body), /ops@example\.invalid/, 'no sender values in the error');

  // Tenant isolation: tenant 26's session cannot execute tenant 25's mission.
  const crossTenant = await post(ordinaryPort, `/api/v1/amo/missions/${missionId}/execute`, { intent: 'APPROVE_PLAN' });
  assert.equal(crossTenant.status, 400, JSON.stringify(crossTenant.body));
  assert.ok(['amo_tenant_mismatch', 'amo_mission_not_found'].includes(crossTenant.body.error), JSON.stringify(crossTenant.body));
  const [stillApproved] = await runtimeFor().list(TENANT);
  assert.equal(stillApproved.pendingOperatorDecision.kind, 'plan_clarification', 'cross-tenant request changed nothing');
  assert.ok(amo);
});
