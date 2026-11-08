'use strict';

/**
 * P0 #1087 — read-only operational status routing and evidence.
 *
 * Reproduces the exact operator request that, after #1086, rendered a
 * Blueprint-only "Mission Updated — Acquisition Mission Planning / Approve
 * discovery?" card. The request must be owned by operational inspection,
 * read persisted tenant state only, and change nothing.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { advancePlanAfterApproval } = require('../AmoOperatorApproval');
const { PresentationEngine } = require('../PresentationEngine');
const {
  detectOperationalInspection,
  readOnlyQueryable,
  classifyMission,
  CATEGORIES,
  STALE_AFTER_DAYS,
} = require('../OperationalInspection');

const ORIGINAL_REQUEST =
  'Give me a read only operational status report for anchor cleaning. Identify the current mission state, active execution blockers, and supporting evidence. Distinguish active business missions from completed, stale, or development/test mission, but do not archive, delete, or modify anything.';

const NOW = new Date('2026-11-08T12:00:00Z');
const daysAgo = (n) => new Date(NOW.getTime() - n * 24 * 60 * 60 * 1000).toISOString();

const BLUEPRINT = {
  id: 'bp_anchor',
  clientId: 10,
  status: 'approved',
  summary: {
    identity: 'Anchor Cleaning — commercial cleaning for professional offices.',
    idealCustomers: 'Property managers, facility managers, and professional offices.',
    geography: 'Greater Manchester including Bedford and Hooksett.',
    campaignGoals: 'Grow commercial cleaning in Greater Manchester.',
  },
};

function amoRow(tenantId, id, fields) {
  const payload = { id, tenantId, ...fields.payload };
  return {
    id,
    tenant_id: tenantId,
    client_id: Number(tenantId),
    stage: fields.stage,
    status: fields.status,
    title: fields.title,
    objective: fields.objective || fields.title,
    payload,
    created_at: fields.created_at || daysAgo(40),
    updated_at: fields.updated_at,
  };
}

/** Persisted fixture: tenant 10 (Anchor) plus a tenant 1 row that must never leak. */
function fixtureRows() {
  return {
    acquisition_missions: [
      amoRow('10', 'am_active', {
        stage: 'prepare', status: 'Preparing', title: 'Facility buyers — Greater Manchester',
        updated_at: daysAgo(2),
        payload: { blockers: [{ reason: 'Paige copy awaiting review' }] },
      }),
      amoRow('10', 'am_stale', {
        stage: 'discover', status: 'Discovering', title: 'Law firms — Bedford',
        updated_at: daysAgo(30),
        payload: { pendingOperatorDecision: { kind: 'discovery_approval', prompt: 'Approve discovery?' } },
      }),
      amoRow('10', 'am_done', { stage: 'improve', status: 'Cancelled', title: 'Accounting practices pilot', updated_at: daysAgo(20), payload: { planCancelled: true } }),
      amoRow('10', 'am_synth', {
        stage: 'prepare', status: 'Preparing', title: 'Restaurants FOH', updated_at: daysAgo(1),
        payload: { syntheticEvidence: { provenance: 'synthetic_fixture', at: daysAgo(1) } },
      }),
      amoRow('10', 'am_named_test', { stage: 'discover', status: 'Discovering', title: 'Test mission probe', updated_at: daysAgo(3), payload: {} }),
      amoRow('1', 'am_other_tenant', { stage: 'execute', status: 'Executing', title: 'Pulseforge NH cleaning', updated_at: daysAgo(1), payload: {} }),
    ],
    acquisition_mission_events: [
      { mission_id: 'am_active', tenant_id: '10', kind: 'stage_committed', label: 'Prepare committed', at: daysAgo(1) },
      { mission_id: 'am_other_tenant', tenant_id: '1', kind: 'outbound_sent', label: 'Sent', at: daysAgo(1) },
    ],
    clients: [
      { id: 1, name: 'Pulseforge', slug: 'pulseforge' },
      { id: 2, name: 'MSHI', slug: 'mshi' },
      { id: 10, name: 'Anchor Cleaning', slug: 'cleaning-co' },
    ],
    prospects: { 10: { total: 42, qualified: 17, setter_visible: 9, do_not_contact: 3, last_added_at: daysAgo(4), last_contacted_at: daysAgo(6) }, 1: { total: 999 } },
    touchpoints: { 10: [{ channel: 'email', last_7d: 0, last_30d: 12, last_at: daysAgo(9) }], 1: [{ channel: 'sms', last_7d: 50, last_30d: 50, last_at: daysAgo(0) }] },
    agent_log: { 10: [{ agent_name: 'scout', last_run_at: daysAgo(1), runs_7d: 6, errors_7d: 2 }, { agent_name: 'emmett', last_run_at: daysAgo(9), runs_7d: 0, errors_7d: 0 }] },
  };
}

/**
 * Minimal pg stand-in. Records every statement; answers by table and by the
 * tenant parameter so cross-tenant leakage would be visible.
 */
function createFixturePool({ failMissionsTable = false } = {}) {
  const rows = fixtureRows();
  const statements = [];
  return {
    statements,
    async query(sql, params = []) {
      const text = String(sql);
      statements.push(text.replace(/\s+/g, ' ').trim());
      const key = String(params[0]);
      if (/FROM clients/.test(text)) return { rows: rows.clients };
      if (/FROM acquisition_mission_events/.test(text)) {
        return { rows: rows.acquisition_mission_events.filter((r) => r.tenant_id === key) };
      }
      if (/FROM acquisition_missions/.test(text)) {
        return { rows: rows.acquisition_missions.filter((r) => r.tenant_id === key) };
      }
      if (/FROM missions/.test(text)) {
        if (failMissionsTable) throw new Error('connection terminated');
        return { rows: [] };
      }
      if (/FROM prospects/.test(text)) return { rows: [rows.prospects[key] || { total: 0 }] };
      if (/FROM touchpoints/.test(text)) return { rows: rows.touchpoints[key] || [] };
      if (/FROM agent_log/.test(text)) return { rows: rows.agent_log[key] || [] };
      if (/to_regclass/.test(text)) return { rows: [{ control_table: 'anchor_email_recovery_control' }] };
      if (/FROM anchor_email_recovery_control/.test(text)) {
        return { rows: [{ singleton: true, maintenance_armed: true, manifest: null }] };
      }
      throw new Error(`unexpected statement: ${text.slice(0, 80)}`);
    },
  };
}

function seedAmo() {
  const amoEngine = amo.createAcquisitionMissionEngine();
  const mission = amoEngine.create({
    tenantId: '10',
    objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    targetSegment: 'Law Firms',
  });
  return { amoEngine, mission };
}

function amoFingerprint(amoEngine, tenantId = '10') {
  return JSON.stringify(
    amoEngine.list(tenantId).map((m) => ({
      m,
      events: amoEngine.store.listEvents(m.id),
      contributions: amoEngine.store.listContributions(m.id),
    }))
  );
}

async function openWorkspace({ pool, amoEngine, tenantId = '10', activeMissionId = null, env }) {
  const workspace = createHermeticWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    resolverEnabled: true,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => BLUEPRINT },
    acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
    operationalInspectionOpts: {
      pool,
      now: NOW,
      env: env || { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' },
    },
  });
  const opened = await workspace.open({ tenantId });
  if (activeMissionId) {
    const session = workspace._sessions.get(opened.sessionId);
    session.context.missionId = activeMissionId;
    session.context.acquisitionMissionId = activeMissionId;
  }
  return { workspace, sessionId: opened.sessionId };
}

function assertOnlyReads(pool) {
  assert.ok(pool.statements.length > 0, 'expected persisted reads');
  for (const s of pool.statements) {
    assert.match(s, /^(SELECT|WITH)\b/i, `non-read statement reached the database: ${s}`);
    assert.doesNotMatch(s, /\b(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE)\b/i, s);
  }
}

function assertNotMissionPlanning(result) {
  assert.equal(result.workspaceOwnership.owner, 'operational_inspection');
  assert.equal(result.resolution.action, 'operational_inspected');
  assert.equal(result.mission, null);
  assert.doesNotMatch(result.prose, /Mission Updated/);
  assert.doesNotMatch(result.prose, /Acquisition Mission Planning/);
  assert.doesNotMatch(result.prose, /Approve discovery\?\s*$/m);
  assert.doesNotMatch(result.prose, /Sources\s*\n\s*Blueprint/);
  assert.equal(result.structured.metadata.missionCommunication, undefined);
  assert.deepEqual(result.recommendedActions, []);
}

describe('P0 #1087 — intent detection', () => {
  it('recognizes the original request and related read-only status reports', () => {
    for (const q of [
      ORIGINAL_REQUEST,
      'What is the operational status for Anchor Cleaning?',
      'Read-only status report of all missions, please.',
      'Give me a status report across missions — which are stale and which are active?',
    ]) {
      assert.equal(detectOperationalInspection(q).matched, true, q);
    }
  });

  it('does not hijack mission questions, approvals, execution, or session configuration', () => {
    for (const q of [
      "What's the status of this mission?",
      'Show me the progress.',
      'Approve discovery',
      'Approved.',
      'Give me an operational status report and then approve discovery.',
      'Run Scout for law firms in Bedford.',
      'Switch to read-only mode.',
      'Why did you stop?',
      'Create a new acquisition mission for dentists.',
    ]) {
      assert.equal(detectOperationalInspection(q).matched, false, q);
    }
  });
});

describe('P0 #1087 — read-only data boundary', () => {
  it('rejects every non-read statement before it reaches the pool', async () => {
    const reached = [];
    const boundary = { reads: 0, writesBlocked: 0 };
    const db = readOnlyQueryable({ query: async (sql) => { reached.push(sql); return { rows: [] }; } }, boundary);
    for (const sql of [
      'INSERT INTO acquisition_missions (id) VALUES ($1)',
      'UPDATE acquisition_missions SET status = $1',
      'DELETE FROM missions',
      'WITH x AS (DELETE FROM missions RETURNING *) SELECT * FROM x',
      'SELECT * FROM acquisition_missions FOR UPDATE',
      'CREATE TABLE IF NOT EXISTS t (id int)',
    ]) {
      await assert.rejects(db.query(sql), /read_only_violation/, sql);
    }
    await db.query('SELECT updated_at, do_not_contact FROM prospects WHERE client_id = $1', [10]);
    assert.equal(reached.length, 1);
    assert.equal(boundary.writesBlocked, 6);
    assert.equal(boundary.reads, 1);
  });

  it('classifies with documented criteria and labels uncertainty', () => {
    const base = { updatedAt: daysAgo(1) };
    assert.equal(classifyMission({ ...base, synthetic: true, status: 'Completed' }, NOW).category, CATEGORIES.DEVELOPMENT_TEST);
    assert.equal(classifyMission({ ...base, status: 'completed' }, NOW).category, CATEGORIES.COMPLETED);
    assert.equal(classifyMission({ ...base, status: 'Cancelled' }, NOW).category, CATEGORIES.COMPLETED);
    const named = classifyMission({ ...base, status: 'Discovering', title: 'Demo run' }, NOW);
    assert.equal(named.category, CATEGORIES.UNCERTAIN);
    assert.equal(named.certainty, 'uncertain');
    assert.equal(classifyMission({ status: 'Discovering' }, NOW).category, CATEGORIES.UNCERTAIN);
    assert.equal(classifyMission({ status: 'Discovering', updatedAt: daysAgo(STALE_AFTER_DAYS + 1) }, NOW).category, CATEGORIES.STALE);
    assert.equal(classifyMission({ status: 'Discovering', updatedAt: daysAgo(STALE_AFTER_DAYS - 1) }, NOW).category, CATEGORIES.ACTIVE);
  });
});

describe('P0 #1087 — end-to-end workspace regression (original request)', { concurrency: false }, () => {
  it('session WITH an active mission: operational report from persisted evidence, zero mutations', async () => {
    const { amoEngine, mission } = seedAmo();
    const pool = createFixturePool();
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine, activeMissionId: mission.id });
    const before = amoFingerprint(amoEngine);

    const result = await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });

    assertNotMissionPlanning(result);
    assert.equal(amoFingerprint(amoEngine), before, 'acquisition mission state must not change');
    assert.equal(amoEngine.get(mission.id, '10').pendingOperatorDecision.kind, 'plan_approval');
    assertOnlyReads(pool);
    // A turn-level "read only" qualifier must not become a sticky session policy.
    const session = workspace._sessions.get(sessionId);
    assert.notEqual(session.context.sessionState && session.context.sessionState.executionPolicy, 'read_only');
    assert.equal(session.context.missionId, mission.id, 'active mission context is preserved');

    const snap = result.operationalSnapshot;
    assert.equal(snap.tenantId, '10');
    assert.equal(snap.mutationBoundary.writesBlocked, 0);
    const byId = Object.fromEntries(snap.missions.map((m) => [m.id, m]));
    assert.equal(byId.am_active.category, 'active');
    assert.equal(byId.am_active.lastEvent.kind, 'stage_committed');
    assert.equal(byId.am_stale.category, 'stale');
    assert.equal(byId.am_done.category, 'completed');
    assert.equal(byId.am_synth.category, 'development_test');
    assert.equal(byId.am_named_test.category, 'uncertain');
    assert.equal(byId.am_other_tenant, undefined, 'tenant 1 mission must not appear');

    const p = result.prose;
    assert.match(p, /Operational status — Anchor Cleaning \(tenant 10\)/);
    assert.match(p, /Active business missions \(1\)[\s\S]*Facility buyers — Greater Manchester/);
    assert.match(p, /Stale missions \(1\)[\s\S]*Law firms — Bedford/);
    assert.match(p, /Completed \/ closed missions \(1\)/);
    assert.match(p, /Development \/ test missions \(1\)[\s\S]*Restaurants FOH/);
    assert.match(p, /Uncertain classification \(1\)[\s\S]*not classified as test from name alone/);
    assert.match(p, /Outbound email gate is in maintenance \(recovery_kill_switch\)/);
    assert.match(p, /scout: 2 error run\(s\) in the last 7 days/);
    assert.match(p, /Paige copy awaiting review/);
    assert.match(p, /Prospects: 42 total, 17 at ICP ≥ 70/);
    assert.match(p, /Touchpoints \/ email: 0 in 7d, 12 in 30d/);
    assert.doesNotMatch(p, /999|Pulseforge NH cleaning|sms/);
    assert.match(p, /✓ acquisition_missions: Postgres acquisition_missions/);
    assert.match(p, /Nothing was created, approved, executed, archived, deleted, or modified/);
  });

  it('session WITHOUT an active mission: same owner, no mission created', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const pool = createFixturePool();
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine });

    const result = await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });

    assertNotMissionPlanning(result);
    assert.equal(amoEngine.list('10').length, 0, 'no acquisition mission may be created');
    assertOnlyReads(pool);
    assert.equal(result.operationalSnapshot.missionCounts.active, 1);
  });

  it('discloses unavailable sources instead of substituting Blueprint evidence', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const pool = createFixturePool({ failMissionsTable: true });
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine });

    const result = await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });

    assert.match(result.prose, /✗ missions: UNAVAILABLE \(mission_store_unavailable\)/);
    assert.ok(result.structured.metadata.unavailable.includes('missions: mission_store_unavailable'));
    assert.doesNotMatch(result.prose, /Blueprint\b(?! content is not used)/);
  });

  it('with no database at all, mission/prospect sources are disclosed as unavailable, never healthy', async () => {
    const { amoEngine } = seedAmo();
    const { workspace, sessionId } = await openWorkspace({ pool: null, amoEngine });
    const result = await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });
    assert.equal(result.workspaceOwnership.owner, 'operational_inspection');
    assert.match(result.prose, /non-durable view/);
    assert.match(result.prose, /✗ prospects: UNAVAILABLE/);
    assert.match(result.prose, /✗ touchpoints: UNAVAILABLE/);
    assert.match(result.prose, /✗ outbound_gate: UNAVAILABLE/);
  });

  it('read-only session policy does not block inspection', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const pool = createFixturePool();
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine });
    await workspace.ask({ sessionId, question: 'Use read-only mode.' });
    const session = workspace._sessions.get(sessionId);
    assert.equal(session.context.sessionState.executionPolicy, 'read_only');

    const result = await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });
    assertNotMissionPlanning(result);
    assert.equal(result.operationalSnapshot.missions.length, 5);
  });
});

describe('P0 #1087 — tenant isolation', { concurrency: false }, () => {
  it('a different named client is refused before any tenant data is read', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const pool = createFixturePool();
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine, tenantId: '1' });

    const result = await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });

    assert.equal(result.workspaceOwnership.owner, 'operational_inspection');
    assert.equal(result.structured.metadata.operationalInspectionRefused, 'tenant_mismatch');
    assert.match(result.prose, /did not read another tenant's data/);
    assert.deepEqual(pool.statements.filter((s) => !/FROM clients/.test(s)), [], 'only the client directory may be read');
    assert.doesNotMatch(result.prose, /Facility buyers|Pulseforge NH cleaning/);
  });

  it('a client-supplied tenantId in context cannot redirect the inspection', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const pool = createFixturePool();
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine, tenantId: '10' });
    // Envelope evidence is not authorization — the HTTP route rejects a
    // mismatched context.tenantId with 403; at the engine the session tenant wins.
    const result = await workspace.ask({ sessionId, question: 'What is the operational status?' });
    assert.equal(result.operationalSnapshot.tenantId, '10');
    const scoped = pool.statements.filter((x) => !/FROM clients|to_regclass|anchor_email_recovery_control/.test(x));
    assert.ok(scoped.length >= 5);
    for (const stmt of scoped) assert.match(stmt, /(client_id|tenant_id) = \$1/, stmt);
    assert.ok(!result.operationalSnapshot.missions.some((m) => m.id === 'am_other_tenant'));
  });
});

describe('P0 #1087 — legitimate mission routing is preserved', { concurrency: false }, () => {
  it('explicit plan approval still advances the mission after an inspection turn', async () => {
    const { amoEngine, mission } = seedAmo();
    const pool = createFixturePool();
    const { workspace, sessionId } = await openWorkspace({ pool, amoEngine, activeMissionId: mission.id });

    await workspace.ask({ sessionId, question: ORIGINAL_REQUEST });
    assert.equal(amoEngine.get(mission.id, '10').pendingOperatorDecision.kind, 'plan_approval');

    const approved = await workspace.ask({ sessionId, question: 'Approved.' });
    assert.notEqual(approved.workspaceOwnership && approved.workspaceOwnership.owner, 'operational_inspection');
    const after = amoEngine.get(mission.id, '10');
    assert.notEqual(after.pendingOperatorDecision && after.pendingOperatorDecision.kind, 'plan_approval');
  });

  it('mission status questions keep their mission-scoped owner', async () => {
    const { amoEngine, mission } = seedAmo();
    await advancePlanAfterApproval({ engine: amoEngine, mission, tenantId: '10', question: 'Approved.' });
    const { workspace, sessionId } = await openWorkspace({ pool: createFixturePool(), amoEngine, activeMissionId: mission.id });
    const result = await workspace.ask({ sessionId, question: "What's the status of this mission?" });
    assert.notEqual(result.workspaceOwnership && result.workspaceOwnership.owner, 'operational_inspection');
    assert.notEqual(result.resolution && result.resolution.action, 'operational_inspected');
  });

  it('ordinary conversation is untouched', async () => {
    const { workspace, sessionId } = await openWorkspace({ pool: createFixturePool(), amoEngine: amo.createAcquisitionMissionEngine() });
    const result = await workspace.ask({ sessionId, question: 'What do you understand about my business?' });
    assert.notEqual(result.workspaceOwnership && result.workspaceOwnership.owner, 'operational_inspection');
  });
});

describe('P0 #1087 — presentation never rewrites the operational report', () => {
  it('returns the persisted-state report verbatim even when a model is configured', async () => {
    const prev = process.env.ANTHROPIC_API_KEY;
    process.env.ANTHROPIC_API_KEY = 'test-key';
    try {
      const presentation = new PresentationEngine({
        disableLlm: false,
        anthropic: { messages: { create: async () => { throw new Error('model must not be called'); } } },
      });
      const out = await presentation.present({
        answer: 'Operational status — tenant 10\n- ✗ missions: UNAVAILABLE (x)',
        metadata: { operationalInspection: true },
      });
      assert.equal(out.presentation, 'operational_inspection');
      assert.equal(out.prose, 'Operational status — tenant 10\n- ✗ missions: UNAVAILABLE (x)');
    } finally {
      if (prev === undefined) delete process.env.ANTHROPIC_API_KEY;
      else process.env.ANTHROPIC_API_KEY = prev;
    }
  });
});
