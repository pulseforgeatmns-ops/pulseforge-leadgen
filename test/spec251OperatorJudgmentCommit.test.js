'use strict';

const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const express = require('express');
const { after, before, beforeEach, describe, it } = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const v1Seed = require('../migrations/2026-09-02-spec-224-production-registry-artifact');
const spec250Seed = require('../migrations/2026-09-11-spec-250-operator-judgment-registry');
const { ANCHOR_AO_ALLOCATION_FIXTURE } = require('./fixtures/spec250-anchor-ao-allocation');
const { reconstructCanonicalSemanticProjection } = require('../lib/canonicalSemanticProjection');
const { isOperatorJudgmentFact } = require('../lib/operatorJudgmentCanonicalAdapter');
const {
  commitTypedOperatorJudgment,
  OperatorJudgmentCommitError,
} = require('../services/operatorJudgmentCommit');

const spec223aMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-09-01-spec-223a-canonical-semantic-persistence.sql'),
  'utf8'
);
const cieMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-08-06-client-intelligence-engine.sql'),
  'utf8'
);

function apiBody(overrides = {}) {
  const {
    tenant_id: _tenant,
    client_id: _client,
    operator: _operator,
    provenance: _provenance,
    ...rest
  } = ANCHOR_AO_ALLOCATION_FIXTURE;
  return {
    kind: 'OPERATOR_JUDGMENT',
    judgment_key: rest.judgment_key,
    judgment_kind: rest.judgment_kind,
    label: rest.label,
    propositions: rest.propositions.map(item => ({ ...item })),
    associated_mission_id: rest.associated_mission_id,
    associated_objective_identity_key: rest.associated_objective_identity_key,
    ...overrides,
  };
}

function adminReq(activeClientId, overrides = {}) {
  return {
    user: { id: 3, email: 'admin@test.local', role: 'admin', client_id: null, name: 'Admin' },
    session: { active_client_id: activeClientId, user: { role: 'admin', id: 3 } },
    ...overrides,
  };
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        server,
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

async function request(base, method, urlPath, body) {
  const url = new URL(urlPath, base);
  const res = await fetch(url, {
    method,
    headers: {
      Accept: 'application/json',
      ...(body == null ? {} : { 'Content-Type': 'application/json' }),
    },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }
  return { status: res.status, json, text };
}

function mountTestApp(activeClientId, pool, inspectMission) {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    Object.assign(req, adminReq(activeClientId));
    next();
  });
  app.post('/api/v1/operator-judgments/commit', async (req, res) => {
    try {
      const result = await commitTypedOperatorJudgment(
        { req, body: req.body, pool },
        { inspectMission, pool }
      );
      res.set('Cache-Control', 'no-store');
      return res.status(result.replayed ? 200 : 201).json(result);
    } catch (err) {
      const status = err.status || 500;
      return res.status(status).json({
        error: err.code || 'operator_judgment_commit_failed',
        message: String(err.message || err),
      });
    }
  });
  return app;
}

describe('SPEC-251 typed operator judgment commit boundary', () => {
  let postgres;
  let pool;

  before(async () => {
    postgres = await startDisposablePostgres('spec-251-pg-');
    pool = new Pool({ connectionString: postgres.connectionString });
    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
      CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE tenant_workspaces (client_id INTEGER PRIMARY KEY REFERENCES clients(id), tenant_key TEXT NOT NULL UNIQUE);
      INSERT INTO clients VALUES (10,'Anchor Cleaning'),(99,'Other Tenant');
      INSERT INTO tenant_workspaces VALUES (10,'tenant:anchor'),(99,'tenant:other');`);
    await pool.query(cieMigration);
    await pool.query(spec223aMigration);
    await v1Seed.up(pool);
    await spec250Seed.up(pool);
  });

  after(async () => {
    await pool.end();
    await postgres.stop();
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE canonical_interpretation_batches CASCADE');
    await pool.query('TRUNCATE cie_evidence CASCADE');
    await pool.query('TRUNCATE cie_interview_sessions CASCADE');
  });

  it('1-6. authenticated operator commits AO allocation with server-bound tenant/provenance', async () => {
    const result = await commitTypedOperatorJudgment({
      req: adminReq(10),
      body: apiBody(),
      pool,
    }, {
      inspectMission: async () => ({ mission: { id: 'acquisition-mission:anchor-ao-initial-allocation', tenantId: '10' } }),
      pool,
    });

    assert.equal(result.spec, 'SPEC-251');
    assert.equal(result.client_id, 10);
    assert.equal(result.tenant_id, 'tenant:anchor');
    assert.equal(result.judgment_key, 'ao-initial-prospect-allocation');
    assert.equal(result.newly_committed, true);
    assert.equal(result.replayed, false);

    const projection = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id: 'tenant:anchor',
      snapshot_id: result.snapshot_id,
    });
    const entities = new Map(projection.entities.map(entity => [entity.id, entity]));
    const zack = projection.facts.find(fact => fact.qualifiers?.judgment_slot === 'allocation:zack_bunker');
    assert.ok(zack);
    assert.equal(isOperatorJudgmentFact(zack, entities), true);
    assert.equal(zack.qualifiers.provenance.origin, 'OPERATOR');
    assert.equal(zack.qualifiers.provenance.origin_kind, 'operator_authored');
    assert.equal(zack.qualifiers.provenance.actor_id, '3');
    const learning = projection.facts.find(fact => fact.qualifiers?.judgment_slot === 'learning_hypothesis:ao_segment_fit');
    assert.equal(learning.epistemic_state, 'HYPOTHESIS');
  });

  it('4. caller cannot override tenant identity via body', async () => {
    await assert.rejects(
      () => commitTypedOperatorJudgment({
        req: adminReq(10),
        body: apiBody({ tenant_id: 'tenant:other', client_id: 99 }),
        pool,
      }, {
        inspectMission: async () => ({ mission: { id: 'acquisition-mission:anchor-ao-initial-allocation', tenantId: '10' } }),
        pool,
      }),
      err => err instanceof OperatorJudgmentCommitError && err.code === 'JUDGMENT_OVERRIDE_FORBIDDEN'
    );
  });

  it('7-10. rejects plain text and malformed payloads', async () => {
    const deps = {
      inspectMission: async () => ({ mission: { id: 'm1', tenantId: '10' } }),
      pool,
    };
    await assert.rejects(
      () => commitTypedOperatorJudgment({ req: adminReq(10), body: 'Zack should get PM accounts', pool }, deps),
      err => err.code === 'JUDGMENT_BODY_INVALID'
    );
    await assert.rejects(
      () => commitTypedOperatorJudgment({ req: adminReq(10), body: { text: 'Zack should get PM accounts' }, pool }, deps),
      err => err.code === 'JUDGMENT_OVERRIDE_FORBIDDEN'
    );
    await assert.rejects(
      () => commitTypedOperatorJudgment({ req: adminReq(10), body: apiBody({ kind: undefined }), pool }, deps),
      err => err.code === 'JUDGMENT_KIND_REQUIRED'
    );
    await assert.rejects(
      () => commitTypedOperatorJudgment({ req: adminReq(10), body: apiBody({ kind: 'BUSINESS_FACT' }), pool }, deps),
      err => err.code === 'JUDGMENT_KIND_INVALID'
    );
    await assert.rejects(
      () => commitTypedOperatorJudgment({ req: adminReq(10), body: apiBody({ propositions: [] }), pool }, deps),
      err => err.code === 'JUDGMENT_PROPOSITIONS_REQUIRED'
    );
    await assert.rejects(
      () => commitTypedOperatorJudgment({
        req: adminReq(10),
        body: apiBody({ propositions: [{ statement: 'missing slot' }] }),
        pool,
      }, deps),
      err => err.code === 'JUDGMENT_SLOT_REQUIRED'
    );
  });

  it('11-12. invalid and cross-tenant mission binding fail closed', async () => {
    await assert.rejects(
      () => commitTypedOperatorJudgment({
        req: adminReq(10),
        body: apiBody({ associated_mission_id: 'missing-mission' }),
        pool,
      }, {
        inspectMission: async () => {
          const err = new Error('Unknown mission: missing-mission');
          err.code = 'amo_mission_not_found';
          throw err;
        },
        pool,
      }),
      err => err.code === 'MISSION_BINDING_INVALID' && err.status === 404
    );

    await assert.rejects(
      () => commitTypedOperatorJudgment({
        req: adminReq(10),
        body: apiBody(),
        pool,
      }, {
        inspectMission: async () => ({
          mission: { id: 'acquisition-mission:anchor-ao-initial-allocation', tenantId: '99' },
        }),
        pool,
      }),
      err => err.code === 'MISSION_TENANT_MISMATCH' && err.status === 403
    );
  });

  it('13. duplicate submission remains idempotent', async () => {
    const deps = {
      inspectMission: async () => ({ mission: { id: 'acquisition-mission:anchor-ao-initial-allocation', tenantId: '10' } }),
      pool,
    };
    const first = await commitTypedOperatorJudgment({ req: adminReq(10), body: apiBody(), pool }, deps);
    const second = await commitTypedOperatorJudgment({ req: adminReq(10), body: apiBody(), pool }, deps);
    assert.equal(first.newly_committed, true);
    assert.equal(second.replayed, true);
    assert.equal(first.snapshot_id, second.snapshot_id);
  });

  it('14. revised submission preserves SPEC-250 revision semantics', async () => {
    const deps = {
      inspectMission: async () => ({ mission: { id: 'acquisition-mission:anchor-ao-initial-allocation', tenantId: '10' } }),
      pool,
    };
    const first = await commitTypedOperatorJudgment({ req: adminReq(10), body: apiBody(), pool }, deps);
    const revisedBody = apiBody();
    const rory = revisedBody.propositions.find(item => item.judgment_slot === 'allocation:rory_matthews');
    rory.statement = 'Rory Matthews should now focus primarily on property managers.';
    rory.rationale_points = ['Later operator decision: shift Rory toward property managers.'];
    const second = await commitTypedOperatorJudgment({ req: adminReq(10), body: revisedBody, pool }, deps);
    assert.notEqual(first.snapshot_id, second.snapshot_id);

    const latest = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id: 'tenant:anchor',
      snapshot_id: second.snapshot_id,
    });
    const currentRory = latest.facts.find(fact => fact.qualifiers?.judgment_slot === 'allocation:rory_matthews'
      && fact.selected_in_conflict);
    assert.match(currentRory.object_value.value, /property managers/);
    assert.ok(latest.relations.some(rel => rel.relation_type === 'SUPERSEDES'));
  });

  it('HTTP route commits typed judgment and rejects overrides', { timeout: 30000 }, async () => {
    const bodyWithoutMission = apiBody({
      associated_mission_id: undefined,
      associated_objective_identity_key: undefined,
    });
    delete bodyWithoutMission.associated_mission_id;
    delete bodyWithoutMission.associated_objective_identity_key;

    const app = mountTestApp(10, pool, async () => ({ mission: { id: 'unused', tenantId: '10' } }));
    const { base, close } = await listen(app);
    try {
      const ok = await request(base, 'POST', '/api/v1/operator-judgments/commit', bodyWithoutMission);
      assert.equal(ok.status, 201);
      assert.equal(ok.json.spec, 'SPEC-251');
      assert.equal(ok.json.tenant_id, 'tenant:anchor');

      const blocked = await request(base, 'POST', '/api/v1/operator-judgments/commit', {
        ...bodyWithoutMission,
        tenant_id: 'tenant:other',
      });
      assert.equal(blocked.status, 400);
      assert.equal(blocked.json.error, 'JUDGMENT_OVERRIDE_FORBIDDEN');
    } finally {
      await close();
    }
  });

  it('15. execution approval path does not invoke operator judgment persistence', () => {
    const executionSource = fs.readFileSync(
      path.join(__dirname, '..', 'packages', 'max', 'workspace', 'AcquisitionMissionExecution.js'),
      'utf8'
    );
    const amoApprovalSource = fs.readFileSync(
      path.join(__dirname, '..', 'packages', 'max', 'workspace', 'AmoOperatorApproval.js'),
      'utf8'
    );
    assert.equal(executionSource.includes('commitOperatorJudgment'), false);
    assert.equal(executionSource.includes('commitTypedOperatorJudgment'), false);
    assert.equal(amoApprovalSource.includes('commitOperatorJudgment'), false);
    assert.equal(amoApprovalSource.includes('commitTypedOperatorJudgment'), false);
  });

  it('16. generic Max workspace ask still does not invoke operator judgment persistence', () => {
    const workspaceSource = fs.readFileSync(
      path.join(__dirname, '..', 'packages', 'max', 'workspace', 'WorkspaceEngine.js'),
      'utf8'
    );
    assert.equal(workspaceSource.includes('commitOperatorJudgment'), false);
    assert.equal(workspaceSource.includes('commitTypedOperatorJudgment'), false);
    assert.equal(workspaceSource.includes('operatorJudgmentCanonical'), false);
  });

  it('17. recommendation disposition still does not invoke operator judgment persistence', () => {
    const directionSource = fs.readFileSync(
      path.join(__dirname, '..', 'services', 'specialistDirection.js'),
      'utf8'
    );
    assert.equal(directionSource.includes('commitOperatorJudgment'), false);
    assert.equal(directionSource.includes('commitTypedOperatorJudgment'), false);
  });

  it('route is mounted from server.js', () => {
    const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
    assert.match(serverSource, /require\('\.\/routes\/operatorJudgments'\)/);
  });
});
