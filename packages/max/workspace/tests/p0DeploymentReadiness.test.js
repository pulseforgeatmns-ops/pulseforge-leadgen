'use strict';

/**
 * P0 Max reliability — deployment readiness:
 *   A. governed restriction amendments
 *   B. synthetic (fixture) data isolation
 *
 * Deterministic: no network, no database, no provider.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const approval = require('../AmoOperatorApproval');
const { createWorkspaceEngine } = require('..');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { extractLearningCandidateRows } = require('../../../../services/acquisitionKnowledgePersistence');
const { admitMissionBoundCandidates } = require('../MissionBoundCrmAdmission');

const {
  extractAuthorizationConstraints: extract,
  mergeAuthorizationConstraints: merge,
  authorizationConstraintsHash: hashOf,
  applyAuthorizationAmendment: amend,
  previewAuthorization,
  routeExecutionRequest,
  createExecutionRequest,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  clearExecutionRouterAudit,
  listExecutionRouterAudit,
} = amo;

const OBJECTIVE = 'Acquire one recurring commercial cleaning client from a law firm in the Greater Manchester area.';
const ADMIN = { id: '7', role: 'admin', name: 'Admin Operator' };
const MANAGER = { id: '8', role: 'manager', name: 'Manager Operator' };

function constrained(engine, text = 'Review only. No outreach.', tenantId = '10') {
  return engine.create({ tenantId, objective: OBJECTIVE, targetSegment: 'Law Firms', authorizationConstraints: extract(text) });
}

function cer(mission, intent) {
  return createExecutionRequest({ source: EXECUTION_SOURCES.API, missionId: mission.id, mission, operatorId: 'operator_test', intent });
}

function lift(engine, mission, classes, overrides = {}) {
  return amend({
    engine,
    missionId: mission.id,
    tenantId: '10',
    change: { lift: classes, reason: 'Operator approved live outreach for this mission.' },
    expectedConstraintsHash: hashOf(engine.get(mission.id, '10').authorizationConstraints),
    amendmentId: `amend_${Math.random().toString(16).slice(2, 12)}`,
    approver: ADMIN,
    ...overrides,
  });
}

describe('A — governed restriction amendments', () => {
  beforeEach(() => clearExecutionRouterAudit());

  it('Max, autonomous, client or anonymous actors can never amend', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine);
    for (const approver of [
      { id: 'max', role: 'admin' },
      { id: 'autonomous_operator', role: 'admin' },
      { id: '9', role: 'client' },
      { id: '', role: 'admin' },
      { id: '9', role: 'viewer' },
    ]) {
      await assert.rejects(lift(engine, mission, ['outreach'], { approver }),
        (err) => err.code === 'authorization_amendment_forbidden', JSON.stringify(approver));
    }
    assert.ok(engine.get(mission.id, '10').authorizationConstraints.prohibited.includes('outreach'));
  });

  it('managers may tighten but never lift', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach.');
    await assert.rejects(lift(engine, mission, ['outreach'], { approver: MANAGER }),
      (err) => err.code === 'authorization_amendment_forbidden');
    const tightened = await amend({
      engine, missionId: mission.id, tenantId: '10',
      change: { add: ['content'], reason: 'No drafts until legal review.' },
      expectedConstraintsHash: hashOf(engine.get(mission.id, '10').authorizationConstraints),
      amendmentId: 'amend_manager_tighten', approver: MANAGER,
    });
    assert.ok(tightened.constraints.prohibited.includes('content'));
    assert.equal(tightened.loosens, false);
  });

  it('an admin-approved lift commits atomically, is audited, and unblocks only what it names', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach. No spending.');
    const handlers = { [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: () => ({ transactionId: 'tx', records: [] }) };
    await assert.rejects(routeExecutionRequest(cer(mission, EXECUTION_INTENTS.EXECUTE_OUTBOUND),
      { engine, tenantId: '10', handlers, persist: false }), (err) => err.code === 'authorization_constraint_blocked');

    const result = await lift(engine, mission, ['outreach', 'outreach_authorization']);
    assert.equal(result.alreadyApplied, false);
    const after = engine.get(mission.id, '10').authorizationConstraints;
    assert.deepEqual(after.prohibited, ['paid_media', 'provider_research'], 'only the named classes lifted');
    assert.equal(after.revision, 1);
    assert.equal(after.amendments.length, 1);
    assert.equal(after.amendments[0].approvedBy.id, ADMIN.id);
    assert.deepEqual(after.amendments[0].lifted, ['outreach', 'outreach_authorization']);

    const record = engine.store.listContributions(mission.id)
      .find((row) => row.specialist === 'operator' && row.kind === 'constraints');
    assert.ok(record, 'durable operator contribution recorded');
    assert.equal(record.payload.action, 'authorization_amendment');
    assert.equal(record.payload.approvedBy.role, 'admin');
    assert.ok(record.payload.before.prohibited.includes('outreach'));

    const routed = await routeExecutionRequest(cer(engine.get(mission.id, '10'), EXECUTION_INTENTS.EXECUTE_OUTBOUND),
      { engine, tenantId: '10', handlers, persist: false });
    assert.ok(routed.routed);
    await assert.rejects(routeExecutionRequest(cer(engine.get(mission.id, '10'), EXECUTION_INTENTS.APPROVE_DISCOVERY),
      { engine, tenantId: '10', handlers: {}, persist: false }), (err) => err.code === 'authorization_constraint_blocked',
      'spending restriction still enforced');
  });

  it('an approval of a stale view is refused and changes nothing', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach.');
    const reviewedHash = hashOf(engine.get(mission.id, '10').authorizationConstraints);
    await amend({ engine, missionId: mission.id, tenantId: '10', change: { add: ['content'], reason: 'tighten' },
      expectedConstraintsHash: reviewedHash, amendmentId: 'amend_tighten_first', approver: MANAGER });
    await assert.rejects(lift(engine, mission, ['outreach'], { expectedConstraintsHash: reviewedHash }),
      (err) => String(err.code).includes('authorization_amendment_stale') ||
        (err.cause && err.cause.code === 'authorization_amendment_stale'));
    assert.ok(engine.get(mission.id, '10').authorizationConstraints.prohibited.includes('outreach'));
  });

  it('replaying the same amendmentId (retry / restart) applies it once', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach.');
    const input = {
      engine, missionId: mission.id, tenantId: '10',
      change: { lift: ['outreach', 'outreach_authorization'], reason: 'approved' },
      expectedConstraintsHash: hashOf(engine.get(mission.id, '10').authorizationConstraints),
      amendmentId: 'amend_replay_0001', approver: ADMIN,
    };
    const first = await amend(input);
    const second = await amend(input);
    assert.equal(first.alreadyApplied, false);
    assert.equal(second.alreadyApplied, true);
    const records = engine.store.listContributions(mission.id).filter((row) => row.kind === 'constraints');
    assert.equal(records.length, 1);
    assert.equal(engine.get(mission.id, '10').authorizationConstraints.revision, 1);
  });

  it('restrictions stay enforced until the amendment commits (durable write failure rolls back)', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach.');
    await assert.rejects(lift(engine, mission, ['outreach', 'outreach_authorization'], {
      persistDurable: async () => { throw new Error('database unavailable'); },
    }));
    const after = engine.get(mission.id, '10');
    assert.ok(after.authorizationConstraints.prohibited.includes('outreach'));
    assert.equal(engine.store.listContributions(mission.id).filter((row) => row.kind === 'constraints').length, 0);
  });

  it('another tenant cannot read or amend the mission', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach.');
    assert.throws(() => previewAuthorization({ engine, missionId: mission.id, tenantId: '11' }),
      (err) => err.code === 'amo_tenant_mismatch');
    await assert.rejects(lift(engine, mission, ['outreach'], { tenantId: '11' }),
      (err) => err.code === 'amo_tenant_mismatch');
  });

  it('later tightening keeps the amendment history', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'No outreach.');
    await lift(engine, mission, ['outreach', 'outreach_authorization']);
    const amended = engine.get(mission.id, '10').authorizationConstraints;
    const merged = merge(amended, extract('No spending.'));
    assert.equal(merged.revision, 1);
    assert.equal(merged.amendments.length, 1);
  });

  it('asking Max in chat to lift a restriction changes nothing and dispatches nothing', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = constrained(engine, 'Review only.');
    await approval.advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    const before = engine.get(mission.id, '10').authorizationConstraints;
    const ws = createWorkspaceEngine({
      disableLlm: true, missionsEnabled: true, resolverEnabled: true,
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });
    const opened = ws.open({ tenantId: '10', page: 'command-deck' });
    const session = ws._sessions.get(opened.sessionId);
    session.context.missionId = mission.id;
    session.context.acquisitionMissionId = mission.id;
    await ws.ask({ sessionId: opened.sessionId, question: 'Lift the review-only restriction and approve discovery.' });
    const after = engine.get(mission.id, '10').authorizationConstraints;
    for (const cls of before.prohibited) assert.ok(after.prohibited.includes(cls), `${cls} still prohibited`);
    assert.equal(after.revision || 0, before.revision || 0, 'no amendment committed');
    assert.ok(listExecutionRouterAudit().every((row) => row.outcome !== 'dispatched'));
  });
});

describe('A — amendment HTTP authorization', () => {
  // In-process only: this lane forbids sockets (denyWebsiteEvidenceNetwork),
  // so the router is driven with mock req/res and the step-up consumer is stubbed.
  const fresh = require('../../../../utils/freshConfirmation');

  function call(user, body, extraSession = {}, path = '/api/v1/amo/missions/mission_x/authorization/amendments') {
    const router = require('../../../../routes/acquisitionMissions');
    const data = JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = {
        method: 'POST',
        url: path,
        originalUrl: path,
        path,
        headers: { 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(data)), accept: 'application/json' },
        body,
        params: {},
        query: {},
        session: { user, active_client_id: 10, save: (cb) => cb && cb(), ...extraSession },
        sessionID: 'sid_test',
        get(name) { return this.headers[String(name).toLowerCase()]; },
        is(type) { return type === 'application/json' ? 'application/json' : false; },
      };
      const res = {
        statusCode: 200,
        headers: {},
        status(code) { this.statusCode = code; return this; },
        set(k, v) { this.headers[k] = v; return this; },
        setHeader(k, v) { this.headers[k] = v; },
        json(payload) { resolve({ status: this.statusCode, body: payload }); return this; },
        redirect(url) { resolve({ status: 302, body: { redirect: url } }); },
      };
      router.handle(req, res, (err) => (err ? reject(err) : resolve({ status: 404, body: null })));
    });
  }

  it('a manager cannot lift, and an admin cannot lift without a fresh password confirmation', async () => {
    const original = fresh.consume;
    const consumed = [];
    fresh.consume = async (_db, args) => { consumed.push(args); return false; };
    try {
      const manager = await call({ id: 8, role: 'manager', client_id: 10 }, { lift: ['outreach'], reason: 'x' });
      assert.equal(manager.status, 403);
      assert.equal(manager.body.error, 'authorization_amendment_forbidden');
      assert.equal(consumed.length, 0, 'a manager never reaches the step-up');

      const admin = await call({ id: 7, role: 'admin', client_id: 10 }, { lift: ['outreach'], reason: 'x' });
      assert.equal(admin.status, 403);
      assert.equal(admin.body.error, 'fresh_confirmation_required');
      assert.equal(consumed.length, 1);
      assert.equal(consumed[0].purpose, 'mission_authorization_amendment');
      assert.equal(consumed[0].sessionId, 'sid_test');

      const viewer = await call({ id: 9, role: 'viewer', client_id: 10 }, { add: ['content'], reason: 'x' });
      assert.equal(viewer.status, 403);
    } finally {
      fresh.consume = original;
    }
  });

  it('an impersonating admin cannot amend, even to tighten', async () => {
    const original = fresh.consume;
    const consumed = [];
    fresh.consume = async (_db, args) => { consumed.push(args); return true; };
    try {
      const res = await call({ id: 7, role: 'admin', client_id: 10 }, { add: ['content'], reason: 'x' }, {
        impersonation: { active: true, authenticatedUserId: 7, effectiveUserId: 5, tenantId: 10,
          effectiveUser: { id: 5, role: 'client', client_id: 10 } },
      });
      assert.equal(res.status, 403);
      assert.equal(res.body.error, 'authorization_amendment_forbidden');
      assert.equal(consumed.length, 0);
    } finally {
      fresh.consume = original;
    }
  });
});

describe('Release review fixes', () => {
  it('the engine core never infers restrictions from objective text (governed objectives stay sendable)', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire facility buyers with no prior contact. Do not email existing customers.',
      targetSegment: 'Law Firms',
    });
    assert.equal(mission.authorizationConstraints, null);
  });

  it('creating with an existing id never replaces the mission or moves it across tenants', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const runtime = createTestAmoRuntime({ engine });
    const original = await runtime.create({ tenantId: '10', objective: OBJECTIVE, authorizationConstraints: extract('Review only.') });
    await assert.rejects(runtime.create({ id: original.id, tenantId: '10', objective: OBJECTIVE }),
      (err) => err.code === 'amo_mission_exists');
    await assert.rejects(runtime.create({ id: original.id, tenantId: '11', objective: OBJECTIVE }),
      (err) => err.code === 'amo_mission_exists');
    const kept = engine.get(original.id, '10');
    assert.equal(kept.authorizationConstraints.reviewOnly, true);
  });

  it('REST create ignores server-owned fields and derives restrictions from the operator objective', async () => {
    const service = require('../../../../services/acquisitionMission');
    const engine = amo.createAcquisitionMissionEngine();
    const runtime = createTestAmoRuntime({ engine });
    const existing = await runtime.create({ tenantId: '10', objective: OBJECTIVE, authorizationConstraints: extract('Review only.') });
    const originalCreate = service.createMission;
    const seen = [];
    // Capture what the route hands to the service; do not touch a database.
    require.cache[require.resolve('../../../../services/acquisitionMission')].exports.createMission =
      async (input) => { seen.push(input); return runtime.create(input); };
    delete require.cache[require.resolve('../../../../routes/acquisitionMissions')];
    try {
      const router = require('../../../../routes/acquisitionMissions');
      const body = { id: existing.id, objective: `${OBJECTIVE} Review only.`, planApproved: true,
        authorizationConstraints: null, syntheticEvidence: null, version: 99 };
      const result = await new Promise((resolve, reject) => {
        const req = { method: 'POST', url: '/api/v1/amo/missions', originalUrl: '/api/v1/amo/missions', path: '/api/v1/amo/missions',
          headers: { 'content-type': 'application/json', accept: 'application/json' }, body, params: {}, query: {},
          session: { user: { id: 7, role: 'admin', client_id: 10 }, active_client_id: 10, save: (cb) => cb && cb() },
          sessionID: 'sid', get(n) { return this.headers[String(n).toLowerCase()]; }, is: () => 'application/json' };
        const res = { statusCode: 200, status(c) { this.statusCode = c; return this; }, set() { return this; },
          setHeader() {}, json(p) { resolve({ status: this.statusCode, body: p }); return this; } };
        router.handle(req, res, (err) => (err ? reject(err) : resolve({ status: 404 })));
      });
      assert.equal(result.status, 201);
      assert.equal(seen[0].id, undefined, 'id is not taken from the body');
      assert.equal(seen[0].planApproved, undefined, 'plan approval is not taken from the body');
      assert.equal(seen[0].version, undefined);
      assert.equal(seen[0].authorizationConstraints.reviewOnly, true, 'derived from the operator objective');
      assert.notEqual(result.body.mission.id, existing.id);
      assert.equal(engine.get(existing.id, '10').authorizationConstraints.reviewOnly, true, 'existing mission untouched');
    } finally {
      require.cache[require.resolve('../../../../services/acquisitionMission')].exports.createMission = originalCreate;
      delete require.cache[require.resolve('../../../../routes/acquisitionMissions')];
    }
  });

  it('a failure after the mission row committed is not reported as "not created"', async () => {
    const committedRows = new Set();
    const pool = {
      query: async (sql, params) => {
        if (/SELECT 1 FROM acquisition_missions/.test(sql)) return { rows: committedRows.has(params[0]) ? [{ '?column?': 1 }] : [] };
        return { rows: [] };
      },
    };
    const runtime = require('../../../../services/acquisitionMissionRuntime').createAcquisitionMissionRuntime({ persist: false, pool: null, production: false });
    runtime.persistOpts = () => ({ persist: true, pool });
    runtime.hydrate = async () => runtime.engine();
    runtime.persistMissionState = async (missionId) => { committedRows.add(missionId); throw new Error('learning write failed'); };
    const mission = await runtime.create({ tenantId: '10', objective: OBJECTIVE });
    assert.ok(runtime.engine().get(mission.id, '10'), 'committed mission stays visible');

    const unknownRuntime = require('../../../../services/acquisitionMissionRuntime').createAcquisitionMissionRuntime({ persist: false, pool: null, production: false });
    unknownRuntime.persistOpts = () => ({ persist: true, pool: { query: async () => { throw new Error('connection reset'); } } });
    unknownRuntime.hydrate = async () => unknownRuntime.engine();
    unknownRuntime.persistMissionState = async () => { throw new Error('connection reset'); };
    await assert.rejects(unknownRuntime.create({ tenantId: '10', objective: OBJECTIVE }),
      (err) => err.code === 'MISSION_PERSISTENCE_UNAVAILABLE' && err.outcome === 'unknown');
  });
});

describe('B — synthetic data isolation', () => {
  beforeEach(() => clearExecutionRouterAudit());

  async function syntheticMission() {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({ tenantId: '10', objective: OBJECTIVE, targetSegment: 'Law Firms' });
    await approval.advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    // Explicitly requested fixture fallback with no live provider.
    await approval.advanceDiscoveryAfterApproval({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
    });
    return { engine, mission: engine.get(mission.id, '10') };
  }

  it('a fixture-backed discovery is stamped durably on the mission and the discovery record', async () => {
    const { engine, mission } = await syntheticMission();
    assert.equal(mission.syntheticEvidence.provenance, 'synthetic_fixture');
    const discovery = engine.store.listContributions(mission.id).filter((row) => row.kind === 'discovery').pop();
    assert.equal(discovery.payload.provenance, 'synthetic_fixture');
    // Survives a snapshot/restore (the durable payload round-trip).
    const restored = amo.createAcquisitionMissionEngine();
    restored.store.restore(engine.store.snapshot());
    assert.equal(restored.get(mission.id, '10').syntheticEvidence.provenance, 'synthetic_fixture');
  });

  it('synthetic missions can never authorize or perform outreach (router and TME)', async () => {
    const { engine, mission } = await syntheticMission();
    let sent = 0;
    for (const intent of [EXECUTION_INTENTS.APPROVE_EXECUTION, EXECUTION_INTENTS.EXECUTE_OUTBOUND]) {
      await assert.rejects(routeExecutionRequest(cer(mission, intent), {
        engine, tenantId: '10', persist: false,
        handlers: { [intent]: () => { sent += 1; return {}; } },
      }), (err) => err.code === 'synthetic_evidence_blocked');
    }
    assert.equal(sent, 0);
    assert.ok(listExecutionRouterAudit().every((row) => row.outcome === 'blocked_synthetic_evidence'));
    const { executeMissionStage } = require('../../../acquisition-mission/TransactionalExecution');
    await assert.rejects(executeMissionStage({
      engine, missionId: mission.id, tenantId: '10', specialist: 'emmett', stage: 'execute',
      execute: async () => { sent += 1; return {}; }, commit: () => ({}),
    }), (err) => amo.isSyntheticEvidenceError(err));
    assert.equal(sent, 0);
  });

  it('synthetic missions are refused CRM admission before any database write', async () => {
    const { engine, mission } = await syntheticMission();
    let queries = 0;
    const pool = { query: async () => { queries += 1; return { rows: [] }; } };
    await assert.rejects(admitMissionBoundCandidates(pool, mission, engine.store.listContributions(mission.id), {}),
      (err) => err.code === 'synthetic_evidence_blocked');
    assert.equal(queries, 0);
  });

  it('synthetic missions never produce acquisition learning', async () => {
    const { engine, mission } = await syntheticMission();
    const rows = extractLearningCandidateRows({
      mission: { ...mission, stage: 'learn' },
      outcomes: [{ type: 'sent' }],
      contributions: engine.store.listContributions(mission.id),
    });
    assert.deepEqual(rows, []);
  });

  it('later Max turns on a synthetic mission say so', async () => {
    const { engine, mission } = await syntheticMission();
    const ws = createWorkspaceEngine({
      disableLlm: true, missionsEnabled: true, resolverEnabled: true,
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });
    const opened = ws.open({ tenantId: '10', page: 'command-deck' });
    const session = ws._sessions.get(opened.sessionId);
    session.context.missionId = mission.id;
    session.context.acquisitionMissionId = mission.id;
    const result = await ws.ask({ sessionId: opened.sessionId, question: 'approved' });
    assert.match(result.prose, /synthetic/i);
  });
});
