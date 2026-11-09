'use strict';

/**
 * Semantic generalization evaluation — runner.
 *
 * Drives the REAL WorkspaceEngine.ask() path (hermetic: no network, no model,
 * fixture Postgres) and records, per turn, everything the graders need:
 * prose, response owner, routing trace, which persisted sources were read,
 * every SQL statement, and before/after fingerprints of mission state, the
 * bound pending decision, session execution policy and the execution-router
 * audit. Nothing here influences routing.
 */

const amo = require('../../../../acquisition-mission');
const A = require('../../AmoOperatorApproval');
const askPathTrace = require('../../audit/AskPathTrace');
const { createHermeticWorkspaceEngine } = require('../../tests/workspaceTestHarness');
const { createTestAmoRuntime, liveScoutStub } = require('../../tests/amoTestRuntime');
const { NOW, TENANTS, createFixturePool } = require('./tenants');

const SOURCE_TABLES = ['acquisition_missions', 'acquisition_mission_events', 'prospects', 'touchpoints', 'agent_log', 'anchor_email_recovery_control', 'clients', 'missions'];

async function readyMission(engine, tenantId) {
  const tenant = TENANTS[tenantId];
  const mission = engine.create({ tenantId, objective: `Acquire new customers for ${tenant.name}: ${tenant.blueprintGoal}`, targetSegment: 'Primary segment' });
  const g = () => engine.get(mission.id, tenantId);
  await A.advancePlanAfterApproval({ engine, mission, tenantId, question: 'Approved.' });
  await A.advanceDiscoveryAfterApproval({ engine, mission, tenantId, question: 'Approved. Begin Discovery.', runScout: liveScoutStub });
  await A.advancePrioritizationAfterApproval({ engine, mission: g(), tenantId, question: 'Approved prioritization.' });
  await A.advanceMaxPrioritization({ engine, mission: g(), tenantId, allowFixtureFallback: true });
  await A.advanceAcquisitionApproach({ engine, mission: g(), tenantId, allowFixtureFallback: true });
  await A.advancePaigeVariants({ engine, mission: g(), tenantId, allowFixtureFallback: true, question: 'Generate variants.' });
  await A.advanceEmmettCapacity({ engine, mission: g(), tenantId, allowFixtureFallback: true, question: 'Plan capacity.' });
  const ready = g();
  if (!ready.pendingOperatorDecision || ready.pendingOperatorDecision.kind !== 'execution_approval') {
    throw new Error('fixture mission did not reach execution_approval');
  }
  return ready;
}

function missionFingerprint(engine, tenantId) {
  return JSON.stringify(engine.list(tenantId).map((m) => ({ m, e: engine.store.listEvents(m.id), c: engine.store.listContributions(m.id) })));
}

function boundState(engine, tenantId, missionId) {
  if (!missionId) return null;
  const m = engine.get(missionId, tenantId);
  if (!m) return null;
  return {
    stage: m.stage || null,
    status: m.status || null,
    pending: m.pendingOperatorDecision ? m.pendingOperatorDecision.kind : null,
  };
}

/** Execution-relevant session policy only (null = defaults, not a change). */
function policyFingerprint(session) {
  const st = (session && session.context && session.context.sessionState) || {};
  return JSON.stringify({
    executionPolicy: st.executionPolicy || 'normal',
    operatingMode: st.operatingMode || 'business_operation',
    evaluationMode: st.evaluationMode || 'none',
  });
}

function tablesRead(statements) {
  const read = new Set();
  for (const s of statements) {
    for (const t of SOURCE_TABLES) if (new RegExp(`\\bFROM\\s+${t}\\b`, 'i').test(s)) read.add(t);
  }
  return [...read];
}

function compactTrace(events) {
  return events
    .filter((e) => e.kind === 'BRANCH' || e.kind === 'EARLY_RETURN' || e.kind === 'OWNER_FALLBACK' || e.kind === 'RESPONSE_OWNER')
    .map((e) => {
      const p = e.payload || {};
      if (e.kind === 'BRANCH') {
        const detail = p.reason || p.subject || p.intent || (p.matched != null ? `matched=${p.matched}` : '');
        return `${p.label}${detail ? `(${detail})` : ''}`;
      }
      if (e.kind === 'EARLY_RETURN') return `return:${p.reason}`;
      if (e.kind === 'OWNER_FALLBACK') return `fallback:${p.fromOwner}->${p.toOwner}(${p.reason})`;
      return `owner:${p.owner}`;
    });
}

/**
 * Open a hermetic workspace for one case.
 */
async function openCase(testCase) {
  const tenant = TENANTS[testCase.tenant];
  if (!tenant) throw new Error(`unknown tenant ${testCase.tenant}`);
  const engine = amo.createAcquisitionMissionEngine();
  const pool = createFixturePool();
  let bound = null;
  if (testCase.bindPending === 'plan') {
    bound = engine.create({ tenantId: tenant.id, objective: `Acquire new customers for ${tenant.name}: ${tenant.blueprintGoal}`, targetSegment: 'Primary segment' });
    if (!bound.pendingOperatorDecision || bound.pendingOperatorDecision.kind !== 'plan_approval') throw new Error('fixture mission is not at plan_approval');
  } else if (testCase.bindPending) {
    bound = await readyMission(engine, tenant.id);
  }
  const ws = createHermeticWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    resolverEnabled: true,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => ({ id: `bp_${tenant.id}`, clientId: Number(tenant.id), status: 'approved', summary: { campaignGoals: tenant.blueprintGoal } }) },
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    operationalInspectionOpts: { pool, now: NOW, env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' } },
    // The business-intelligence evidence stack reads the SAME fixture world
    // (its own SQL, through the same recording pool). Stores the fixture does
    // not model are reported unavailable, never fetched from the network.
    operatingEvidenceOpts: {
      pool,
      loadCampaignAo: async () => ({ available: false, reason: 'campaign_ao_not_in_fixture' }),
      loadScout: async () => ({ available: false, reason: 'scout_acquisition_state_not_in_fixture', launchedNewWork: false }),
      // defaultLoadCapabilityPolicy ignores the injected pool (utils/clientContext
      // uses the global db pool) — an audit finding; stubbed to stay hermetic.
      loadCapability: async () => ({ available: false, reason: 'capability_policy_not_in_fixture' }),
    },
  });
  const opened = await ws.open({ tenantId: tenant.id, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  if (bound) {
    session.context.missionId = bound.id;
    session.context.acquisitionMissionId = bound.id;
  }
  return { tenant, engine, pool, ws, session, sessionId: opened.sessionId, boundMissionId: bound ? bound.id : null };
}

/**
 * Run every turn of a case through the engine.
 * @returns {Promise<{ case: object, turns: object[] }>}
 */
async function runCaseOnEngine(testCase) {
  amo.clearExecutionRouterAudit();
  const ctx = await openCase(testCase);
  const turns = [];
  const readSoFar = new Set();
  for (const turn of testCase.turns) {
    const before = {
      missions: missionFingerprint(ctx.engine, ctx.tenant.id),
      bound: boundState(ctx.engine, ctx.tenant.id, ctx.boundMissionId),
      policy: policyFingerprint(ctx.ws._sessions.get(ctx.sessionId) || ctx.session),
      sqlCount: ctx.pool.statements.length,
      routerAudit: amo.listExecutionRouterAudit().length,
    };
    let result = null;
    let error = null;
    let errorStack = null;
    let events = [];
    const started = Date.now();
    try {
      const collected = await askPathTrace.collect(() => ctx.ws.ask({ sessionId: ctx.sessionId, question: turn.q }));
      result = collected.value;
      events = collected.events;
    } catch (e) {
      error = e && e.message ? e.message : String(e);
      errorStack = e && e.stack ? String(e.stack) : null;
    }
    const session = ctx.ws._sessions.get(ctx.sessionId) || ctx.session;
    const statements = ctx.pool.statements.slice(before.sqlCount);
    const after = {
      missions: missionFingerprint(ctx.engine, ctx.tenant.id),
      bound: boundState(ctx.engine, ctx.tenant.id, ctx.boundMissionId),
      policy: policyFingerprint(session),
      routerAudit: amo.listExecutionRouterAudit().length,
    };
    const ownership = (result && result.workspaceOwnership) || {};
    for (const t of tablesRead(statements)) readSoFar.add(t);
    turns.push({
      q: turn.q,
      expect: turn.expect || null,
      latencyMs: Date.now() - started,
      error,
      prose: result ? String(result.prose || '') : '',
      owner: ownership.owner || null,
      ownerReason: ownership.reason || null,
      route: result ? result.route || null : null,
      action: result && result.resolution ? result.resolution.action || null : null,
      analyticalFrame: result && result.operationalEvidence ? result.operationalEvidence.analyticalFrame || null : null,
      sourcesRead: tablesRead(statements),
      // Evidence read earlier in the same conversation may legitimately be reused.
      sourcesAvailable: [...readSoFar],
      // An execution dispatch that threw (e.g. a state-consistency guard) is
      // still an attempted dispatch.
      attemptedDispatch: Boolean(errorStack && /routeExecutionRequest|executeMissionStage/.test(errorStack)),
      errorCode: errorStack ? (errorStack.match(/code: '([A-Z_]+)'/) || [])[1] || null : null,
      writes: statements.filter((s) => !/^(SELECT|WITH)\b/i.test(s)),
      missionsChanged: before.missions !== after.missions,
      boundBefore: before.bound,
      boundAfter: after.bound,
      policyChanged: before.policy !== after.policy,
      routerDispatches: after.routerAudit - before.routerAudit,
      trace: compactTrace(events),
    });
  }
  return { case: testCase, tenant: ctx.tenant, boundMissionId: ctx.boundMissionId, turns };
}

/**
 * Run a case through an alternative responder (negative/positive controls).
 * A responder never mutates state; it only returns prose.
 */
async function runCaseOnResponder(testCase, responder) {
  const tenant = TENANTS[testCase.tenant];
  const turns = [];
  for (const turn of testCase.turns) {
    const out = await responder({ testCase, turn, tenant });
    turns.push({
      q: turn.q,
      expect: turn.expect || null,
      latencyMs: 0,
      error: null,
      prose: String(out.prose || ''),
      owner: out.owner || 'control',
      ownerReason: out.ownerReason || 'control',
      route: null,
      action: null,
      analyticalFrame: null,
      sourcesRead: out.sourcesRead || [],
      sourcesAvailable: out.sourcesRead || [],
      attemptedDispatch: false,
      writes: [],
      missionsChanged: false,
      boundBefore: testCase.bindPending ? { pending: 'execution_approval' } : null,
      boundAfter: testCase.bindPending ? { pending: out.consumesDecision ? null : 'execution_approval' } : null,
      policyChanged: false,
      routerDispatches: out.consumesDecision ? 1 : 0,
      trace: [],
    });
  }
  return { case: testCase, tenant, boundMissionId: testCase.bindPending ? 'control' : null, turns };
}

module.exports = { runCaseOnEngine, runCaseOnResponder, openCase, readyMission };
