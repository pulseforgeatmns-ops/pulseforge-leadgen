'use strict';

/**
 * P0 Max System-Level Reliability Recovery — deterministic regression matrix.
 *
 * Every case is tenant-independent: tenants '10' and '11' are interchangeable
 * fixtures, and no assertion depends on a tenant-specific branch. No network,
 * no database (the PostgreSQL restart case lives in
 * test/maxReliabilityRecoveryPostgres.test.js).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { createWorkspaceEngine } = require('..');
const { resolveOperatorObjective } = require('../OperatorObjectiveResolutionEngine');
const { classifyMessageType } = require('../MessageTypeClassifier');
const {
  resolveWorkspaceOwner,
  resolveContinuationPrecedence,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { stripNegatedDirectives } = require('../NegatedDirective');
const { maybeHandleAcquisitionOwnershipTurn } = require('../AcquisitionOwnership');
const { applyExecutionTruth } = require('../AcquisitionMissionExecution');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const {
  createAcquisitionMissionRuntime,
  MISSION_PERSISTENCE_UNAVAILABLE,
} = require('../../../../services/acquisitionMissionRuntime');
const { getMissionEngine, getMissionPersistenceStatus } = require('../../../../utils/missionRuntime');

const {
  routeExecutionRequest,
  createExecutionRequest,
  classifyExecutionEvidence,
  EXECUTION_EVIDENCE_STATES: STATES,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  clearExecutionRouterAudit,
  listExecutionRouterAudit,
} = amo;

const NEW_MISSION =
  'Create a new acquisition mission. Objective: Acquire one recurring commercial cleaning client ' +
  'from a property management company in the Greater Manchester area.';

const REVIEW_ONLY_IMPORT =
  'Set up a review-only contact intake mission for an existing customer. ' +
  'No outreach, calls, content, or campaign activation.';

const PREPARATION_ONLY_WORK_ORDER = [
  'Continue the Campaign 001 preparation-only canary package.',
  '',
  'Use these 3 prospects:',
  '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
  '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
  '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
  '',
  'Turn the 3-prospect canary into a verification work order.',
  'Do not create a mission.',
  'Do not launch, execute, approve, print, or mail anything.',
].join('\n');

function sessionWithActiveWork() {
  return {
    id: 'sess_p0',
    context: {
      tenantId: '10',
      activeWorkContext: {
        workflow: 'campaign_canary',
        entities: [{ id: 'PM-001', companyName: 'Fixture Co' }],
        constraints: { preparationOnly: true },
      },
    },
  };
}

function missionCreationResolution() {
  return {
    primaryObjective: 'mission_creation',
    routingDecision: { owner: 'mission_creation', pipeline: 'MissionRuntime', reason: 'primary_objective:mission_creation' },
    confidence: 0.96,
  };
}

function workspaceFor(engine) {
  return createWorkspaceEngine({
    disableLlm: true,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => null },
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
  });
}

function seedMission(engine, tenantId = '10') {
  return engine.create({
    tenantId,
    clientId: Number(tenantId),
    objective: 'Find commercial property managers in the pilot area',
    createdBy: 'test',
  });
}

function routerContext(engine, handlers, tenantId = '10') {
  return { engine, tenantId, handlers, persist: false };
}

function request(mission, intent, extra = {}) {
  return createExecutionRequest({
    source: EXECUTION_SOURCES.CHAT,
    missionId: mission.id,
    mission,
    operatorId: 'operator_test',
    intent,
    ...extra,
  });
}

describe('P0 Max reliability recovery — routing and context integrity', () => {
  it('new mission with no active context creates a mission', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const ws = workspaceFor(engine);
    const opened = await ws.open({ tenantId: '10' });
    const result = await ws.ask({
      sessionId: opened.sessionId,
      question: NEW_MISSION,
    });
    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(engine.list('10').length, 1);
    assert.ok(result.mission && result.mission.id);
    // Mission-creation turns carry the routing envelope (was dropped before).
    assert.equal(result.messageClassification.type, 'mission_creation');
  });

  it('existing workflow + continuation request continues instead of creating a mission', async () => {
    const session = sessionWithActiveWork();
    const owner = await resolveWorkspaceOwner({
      question: 'Continue with PM-001.',
      session,
      context: session.context,
      objectiveResolution: missionCreationResolution(),
    });
    assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(owner.reason, 'active_work_context_precedence');
    assert.equal(owner.overriddenObjective, 'mission_creation');
  });

  it('ambiguous intent with an active workflow preserves context (no new mission)', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const ws = workspaceFor(engine);
    const first = await ws.ask({
      question: PREPARATION_ONLY_WORK_ORDER,
      context: { tenantId: '10', page: 'command-deck' },
    });
    const followUp = await ws.ask({ sessionId: first.sessionId, question: 'Continue.' });
    assert.equal(engine.list('10').length, 0, 'no mission manufactured');
    const awc = ws._sessions.get(followUp.sessionId).activeWorkContext;
    assert.ok(awc && awc.entities.length === 3, 'active work context preserved');
  });

  it('explicit new objective overrides active work context', () => {
    const session = sessionWithActiveWork();
    for (const question of [
      'Start a new mission for dental offices.',
      'Create a brand-new acquisition mission. Do not resume, reuse, or continue any existing mission.',
    ]) {
      const decision = resolveContinuationPrecedence(question, { session });
      assert.equal(decision.continuation, false, question);
      assert.equal(decision.reason, 'explicit_new_objective', question);
    }
  });

  it('continuation cues inside a prohibition do not claim continuation', () => {
    const session = sessionWithActiveWork();
    const decision = resolveContinuationPrecedence('Do not continue anything yet.', { session });
    assert.equal(decision.continuation, false);
  });

  it('structured mission_creation decision conflicting with a desk workflow applies documented precedence', async () => {
    const owner = await resolveWorkspaceOwner({
      question: 'Proceed with PM-001 packet-content review.',
      session: { id: 's', context: { tenantId: '10' } },
      context: { tenantId: '10' },
      objectiveResolution: missionCreationResolution(),
    });
    assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(owner.reason, 'active_desk_workflow_precedence');
  });

  it('negated directives are constraints, not mission-creation requests', () => {
    assert.equal(stripNegatedDirectives('Do not create a mission.'), '.');
    assert.notEqual(classifyMessageType('Do not create a mission.').type, 'mission_creation');
    assert.notEqual(
      resolveOperatorObjective({ question: 'Do not create a mission. Summarize the table.' }).primaryObjective,
      'mission_creation'
    );
    // Affirmative creation with a negated side constraint still creates.
    assert.equal(
      resolveOperatorObjective({ question: 'Create a new mission with no outreach.' }).primaryObjective,
      'mission_creation'
    );
  });

  it('routing precedence never branches on tenant', async () => {
    for (const tenantId of ['10', '11', '2']) {
      const session = sessionWithActiveWork();
      session.context.tenantId = tenantId;
      const owner = await resolveWorkspaceOwner({
        question: 'Continue with PM-001.',
        session,
        context: session.context,
        objectiveResolution: missionCreationResolution(),
      });
      assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION, `tenant ${tenantId}`);
    }
  });
});

describe('P0 Max reliability recovery — authorization and review-only safety', () => {
  beforeEach(() => clearExecutionRouterAudit());

  it('review-only import creates a mission without any execution dispatch', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const ws = workspaceFor(engine);
    const opened = await ws.open({ tenantId: '10' });
    const result = await ws.ask({ sessionId: opened.sessionId, question: REVIEW_ONLY_IMPORT });
    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION,
      'routes to mission creation, not generic reasoning or retrieval');
    const missions = engine.list('10');
    assert.equal(missions.length, 1);
    assert.equal(listExecutionRouterAudit().length, 0, 'no CER dispatched');
    assert.deepEqual(engine.store.listContributions(missions[0].id), [], 'no specialist contributed');
  });

  it('approval/permission missing blocks execution before any handler runs', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = seedMission(engine);
    let invoked = false;
    const handlers = { [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: () => { invoked = true; return {}; } };
    await assert.rejects(
      routeExecutionRequest(
        request(mission, EXECUTION_INTENTS.EXECUTE_OUTBOUND, { permissions: { canExecute: false } }),
        routerContext(engine, handlers)
      ),
      (err) => err.code === 'cer_permission_denied'
    );
    await assert.rejects(
      routeExecutionRequest(
        request(mission, EXECUTION_INTENTS.EXECUTE_OUTBOUND, { executionMode: 'read_only' }),
        routerContext(engine, handlers)
      ),
      (err) => err.code === 'cer_policy_blocked'
    );
    assert.equal(invoked, false);
  });

  it('unsupported action returns an explicit unsupported result', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = seedMission(engine);
    await assert.rejects(
      routeExecutionRequest(request(mission, EXECUTION_INTENTS.GENERATE_VARIANTS), routerContext(engine, {})),
      (err) => err.code === 'cer_unknown_intent'
    );
    assert.throws(
      () => createExecutionRequest({ source: EXECUTION_SOURCES.CHAT, intent: 'send_everything' }),
      (err) => err.code === 'cer_unknown_intent'
    );
  });

  it('tenant B cannot route execution against tenant A mission', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const missionA = seedMission(engine, '10');
    await assert.rejects(
      routeExecutionRequest(request(missionA, EXECUTION_INTENTS.GENERATE_VARIANTS), routerContext(engine, {
        [EXECUTION_INTENTS.GENERATE_VARIANTS]: () => ({ transactionId: 'tx' }),
      }, '11')),
      (err) => err.code === 'amo_tenant_mismatch'
    );
  });
});

describe('P0 Max reliability recovery — execution evidence', () => {
  beforeEach(() => clearExecutionRouterAudit());

  it('committed TME result is completed_verified with correlation identifiers', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = seedMission(engine);
    const routed = await routeExecutionRequest(
      request(mission, EXECUTION_INTENTS.GENERATE_VARIANTS),
      routerContext(engine, {
        [EXECUTION_INTENTS.GENERATE_VARIANTS]: () => ({ transactionId: 'tx_1', executionOutcome: 'completed', missionVersion: 3 }),
      })
    );
    assert.equal(routed.execution.state, STATES.COMPLETED_VERIFIED);
    assert.equal(routed.execution.verified, true);
    assert.equal(routed.execution.evidence.transactionId, 'tx_1');
    assert.equal(routed.execution.correlation.missionId, mission.id);
    assert.equal(routed.execution.correlation.tenantId, '10');
    assert.equal(routed.execution.correlation.specialist, 'paige');
    assert.equal(routed.execution.correlation.requestId, routed.request.id);
    assert.equal(routed.audit.executionState, STATES.COMPLETED_VERIFIED);
  });

  it('specialist returning no completion evidence stays unverified and Max says so', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = seedMission(engine);
    const routed = await routeExecutionRequest(
      request(mission, EXECUTION_INTENTS.START_DISCOVERY),
      routerContext(engine, { [EXECUTION_INTENTS.START_DISCOVERY]: () => ({ snapshot: null }) })
    );
    assert.equal(routed.execution.state, STATES.OUTCOME_UNKNOWN);
    assert.equal(routed.execution.verified, false);
    assert.equal(routed.audit.outcome, 'dispatched');
    assert.equal(routed.audit.executionState, STATES.OUTCOME_UNKNOWN);

    const response = applyExecutionTruth(
      { prose: 'Discovery Complete', structured: { answer: 'Discovery Complete', metadata: {} } },
      routed.execution
    );
    assert.match(response.prose, /^Scout was dispatched, but completion is not verified/);
    assert.equal(response.structured.metadata.executionVerified, false);
  });

  it('specialist dispatch failure (rollback) is failed, never success', () => {
    const execution = classifyExecutionEvidence({
      request: { id: 'cer_x', missionId: 'm', intent: 'start_discovery' },
      executionResult: { rolledBack: true, transactionId: 'tx_r', rollbackReason: 'provider_down' },
      specialist: 'scout',
    });
    assert.equal(execution.state, STATES.FAILED);
    assert.equal(execution.verified, false);
    assert.equal(execution.evidence.rollbackReason, 'provider_down');
  });

  it('blocked stage outcome is blocked, not completed', () => {
    const execution = classifyExecutionEvidence({
      request: { id: 'cer_b' },
      executionResult: { transactionId: 'tx_b', executionOutcome: 'blocked' },
      specialist: 'emmett',
    });
    assert.equal(execution.state, STATES.BLOCKED);
    const response = applyExecutionTruth({ prose: 'Sent', structured: { answer: 'Sent', metadata: {} } }, execution);
    assert.match(response.prose, /^Emmett did not complete this stage: it is blocked/);
  });

  it('retry after a completed stage is already_executed (idempotent, no duplicate)', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = seedMission(engine);
    let sends = 0;
    const handlers = {
      [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: () => {
        if (sends > 0) return { alreadyExecuted: true, transactionId: 'tx_send' };
        sends += 1;
        return { transactionId: 'tx_send', executionOutcome: 'completed' };
      },
    };
    const first = await routeExecutionRequest(request(mission, EXECUTION_INTENTS.EXECUTE_OUTBOUND), routerContext(engine, handlers));
    const retry = await routeExecutionRequest(request(mission, EXECUTION_INTENTS.EXECUTE_OUTBOUND), routerContext(engine, handlers));
    assert.equal(first.execution.state, STATES.COMPLETED_VERIFIED);
    assert.equal(retry.execution.state, STATES.ALREADY_EXECUTED);
    assert.equal(sends, 1);
  });
});

describe('P0 Max reliability recovery — durable mission state', () => {
  function failingPool(message = 'connect ECONNREFUSED 127.0.0.1:5432') {
    return { query: async () => { throw new Error(message); } };
  }

  it('Mission Engine fails closed when PostgreSQL is unavailable, then recovers', async () => {
    const previous = process.env.MISSION_STORE_ALLOW_EPHEMERAL;
    delete process.env.MISSION_STORE_ALLOW_EPHEMERAL;
    try {
      await assert.rejects(
        getMissionEngine({ reset: true, pool: failingPool() }),
        (err) => err.code === 'MISSION_PERSISTENCE_UNAVAILABLE'
      );
      const status = getMissionPersistenceStatus();
      assert.equal(status.durable, false);
      assert.equal(status.store, 'none');
      // Failed boot is not cached: an explicit isolated caller succeeds next.
      const engine = await getMissionEngine({ inMemory: true });
      assert.equal(engine.persistence.store, 'memory');
      assert.equal(engine.persistence.reason, 'explicit_in_memory');
    } finally {
      if (previous !== undefined) process.env.MISSION_STORE_ALLOW_EPHEMERAL = previous;
      await getMissionEngine({ reset: true, inMemory: true });
    }
  });

  it('AMO hydrate failure blocks mission creation with an explicit, truthful turn', async () => {
    const runtime = createAcquisitionMissionRuntime({ pool: failingPool(), persist: true, production: false });
    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: NEW_MISSION,
      context: { tenantId: '10' },
      objectiveResolution: missionCreationResolution(),
      acquisitionMissionRuntime: runtime,
      cieService: { getApprovedClientBlueprint: async () => null },
    });
    assert.equal(turn.reason, 'acquisition_mission_persistence_unavailable');
    assert.equal(turn.created, false);
    assert.equal(turn.mission, null);
    assert.match(turn.prose, /Nothing was created/);
    assert.equal(runtime.engine().list('10').length, 0, 'no phantom in-memory mission');
  });

  it('failed durable write rolls back creation; retry creates exactly one mission', async () => {
    const runtime = createAcquisitionMissionRuntime({ persist: false, pool: null, production: false });
    const realPersist = runtime.persistMissionState.bind(runtime);
    let failNext = true;
    runtime.persistMissionState = async (...args) => {
      if (failNext) { failNext = false; throw new Error('write failed'); }
      return realPersist(...args);
    };
    const input = { tenantId: '10', clientId: 10, objective: 'Find veterinary clinics', createdBy: 'test' };
    await assert.rejects(runtime.create(input), (err) => err.code === MISSION_PERSISTENCE_UNAVAILABLE);
    assert.equal(runtime.engine().list('10').length, 0);
    await runtime.create(input);
    assert.equal(runtime.engine().list('10').length, 1);
  });

  it('missing tenant on a routed mission-creation decision is blocked, not generic reasoning', async () => {
    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: 'Create a new mission.',
      context: {},
      objectiveResolution: missionCreationResolution(),
      acquisitionMissionRuntime: createTestAmoRuntime(),
    });
    assert.equal(turn.reason, 'acquisition_mission_tenant_required');
    assert.equal(turn.created, false);
  });

  it('tenants with similar workflows stay isolated', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const runtime = createTestAmoRuntime({ engine });
    const question = NEW_MISSION;
    for (const tenantId of ['10', '11']) {
      const turn = await maybeHandleAcquisitionOwnershipTurn({
        question,
        context: { tenantId },
        objectiveResolution: missionCreationResolution(),
        acquisitionMissionRuntime: runtime,
        cieService: { getApprovedClientBlueprint: async () => null },
      });
      assert.equal(turn.created, true, `tenant ${tenantId} gets its own mission`);
    }
    const a = engine.list('10');
    const b = engine.list('11');
    assert.equal(a.length, 1);
    assert.equal(b.length, 1);
    assert.notEqual(a[0].id, b[0].id);
    assert.throws(() => engine.get(a[0].id, '11'), (err) => err.code === 'amo_tenant_mismatch',
      'tenant 11 cannot read tenant 10 mission');
  });

  it('status request after creation reports the actual mission, without creating another', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const ws = workspaceFor(engine);
    const opened = await ws.open({ tenantId: '10' });
    await ws.ask({
      sessionId: opened.sessionId,
      question: NEW_MISSION,
    });
    const [mission] = engine.list('10');
    const status = await ws.ask({ sessionId: opened.sessionId, question: 'What is the status of the mission?' });
    assert.equal(engine.list('10').length, 1, 'status does not manufacture a mission');
    assert.equal(engine.list('10')[0].stage, mission.stage, 'state unchanged by a status read');
    assert.ok(!/completed/i.test(status.prose) || /not/i.test(status.prose),
      'status does not claim completion that has no evidence');
  });
});
