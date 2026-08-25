'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMissionEngine } = require('../../mission-engine');
const {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  createExecutionRequestFromChat,
  createExecutionRequestFromApprovalButton,
  createExecutionRequestFromVoice,
  createExecutionRequestFromApi,
  canonicalRequestShape,
  intentFromPendingDecision,
  routeExecutionRequest,
  replayExecutionRequest,
  clearExecutionRouterAudit,
  listExecutionRouterAudit,
  MISSION_RUNTIME_BOUNDARY_VIOLATION,
} = amo;

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Hooksett and Auburn.';

describe('ADR-090 / SPEC-171 — Canonical Execution Router', () => {
  beforeEach(() => {
    clearExecutionRouterAudit();
  });

  it('creates an immutable CER with a unique id', () => {
    const a = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: 'mission_c1b6003f-0000-4000-8000-000000000001',
      operatorId: 'operator',
      objective: STR_OBJECTIVE,
    });
    const b = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: 'mission_c1b6003f-0000-4000-8000-000000000001',
      operatorId: 'operator',
      objective: STR_OBJECTIVE,
    });

    assert.equal(a.spec, 'SPEC-171');
    assert.match(a.id, /^cer_/);
    assert.notEqual(a.id, b.id);
    assert.ok(Object.isFrozen(a));
    assert.throws(() => {
      a.intent = EXECUTION_INTENTS.APPROVE_PLAN;
    });
    assert.equal(a.intent, EXECUTION_INTENTS.APPROVE_DISCOVERY);
  });

  it('chat and approval button produce the same canonical intent', () => {
    const pending = { kind: 'discovery_approval', prompt: 'Approve discovery?' };
    const chat = createExecutionRequestFromChat({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: 'mission_demo',
      operatorId: 'jacob',
      objective: STR_OBJECTIVE,
      question: 'Approved. Begin Discovery.',
    });
    const button = createExecutionRequestFromApprovalButton({
      pendingOperatorDecision: pending,
      missionId: 'mission_demo',
      operatorId: 'jacob',
      objective: STR_OBJECTIVE,
    });

    assert.equal(intentFromPendingDecision(pending), EXECUTION_INTENTS.APPROVE_DISCOVERY);
    assert.equal(chat.intent, button.intent);
    assert.equal(chat.source, EXECUTION_SOURCES.CHAT);
    assert.equal(button.source, EXECUTION_SOURCES.APPROVAL_BUTTON);
    const chatShape = canonicalRequestShape(chat);
    const buttonShape = canonicalRequestShape(button);
    assert.equal(chatShape.intent, buttonShape.intent);
    assert.equal(chatShape.missionId, buttonShape.missionId);
    assert.equal(chatShape.operatorId, buttonShape.operatorId);
  });

  it('voice START_DISCOVERY is the same dispatch intent as APPROVE_DISCOVERY', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
      planApproved: true,
    });

    let discoveryHandlerCalls = 0;
    const handlers = {
      [EXECUTION_INTENTS.APPROVE_DISCOVERY]: async () => {
        discoveryHandlerCalls += 1;
        return { executionOutcome: 'completed', snapshot: engine.inspect(mission.id, { tenantId: '10' }) };
      },
      [EXECUTION_INTENTS.START_DISCOVERY]: async (ctx) => handlers[EXECUTION_INTENTS.APPROVE_DISCOVERY](ctx),
    };

    const voice = createExecutionRequestFromVoice({
      intent: EXECUTION_INTENTS.START_DISCOVERY,
      missionId: mission.id,
      operatorId: 'voice-operator',
      objective: STR_OBJECTIVE,
    });
    const button = createExecutionRequestFromApprovalButton({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'voice-operator',
      objective: STR_OBJECTIVE,
    });

    const voiceRouted = await routeExecutionRequest(voice, { engine, tenantId: '10', handlers });
    const buttonRouted = await routeExecutionRequest(button, { engine, tenantId: '10', handlers });

    assert.equal(voice.intent, EXECUTION_INTENTS.START_DISCOVERY);
    assert.equal(button.intent, EXECUTION_INTENTS.APPROVE_DISCOVERY);
    assert.equal(voiceRouted.specialist, 'scout');
    assert.equal(buttonRouted.specialist, 'scout');
    assert.equal(voiceRouted.action, 'discovery_approved');
    assert.equal(buttonRouted.action, 'discovery_approved');
    assert.equal(discoveryHandlerCalls, 2);
  });

  it('rejects unknown intents and unfrozen objects', async () => {
    assert.throws(
      () => createExecutionRequest({ source: 'chat', intent: 'WAVE_AT_SCOUT' }),
      (err) => err.code === 'cer_unknown_intent'
    );
    await assert.rejects(
      () => routeExecutionRequest({ id: 'cer_x', intent: EXECUTION_INTENTS.APPROVE_DISCOVERY, source: 'chat' }),
      (err) => err.code === 'cer_invalid'
    );
  });

  it('validates permissions and execution policy before dispatch', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      planApproved: true,
    });
    const denied = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      permissions: { canExecute: false },
    });
    await assert.rejects(
      () => routeExecutionRequest(denied, { engine, tenantId: '10', handlers: {} }),
      (err) => err.code === 'cer_permission_denied'
    );

    const blocked = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      executionMode: 'read_only',
    });
    await assert.rejects(
      () => routeExecutionRequest(blocked, { engine, tenantId: '10', handlers: {} }),
      (err) => err.code === 'cer_policy_blocked'
    );
  });

  it('validates runtime ownership and strips Mission Engine for AMO dispatch', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      planApproved: true,
    });
    const legacyEngine = createMissionEngine();
    let seenMissionEngine = 'unset';
    const request = createExecutionRequestFromApi({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      runtimeOwner: 'amo',
    });

    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      missionEngine: legacyEngine,
      handlers: {
        [EXECUTION_INTENTS.APPROVE_DISCOVERY]: async (ctx) => {
          seenMissionEngine = ctx.missionEngine;
          return { executionOutcome: 'completed', snapshot: engine.inspect(mission.id, { tenantId: '10' }) };
        },
      },
    });

    assert.equal(routed.runtimeOwner, 'amo');
    assert.equal(seenMissionEngine, null);

    const mismatched = createExecutionRequestFromApi({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      runtimeOwner: 'mission_engine',
    });
    await assert.rejects(
      () => routeExecutionRequest(mismatched, {
        engine,
        tenantId: '10',
        handlers: { [EXECUTION_INTENTS.APPROVE_DISCOVERY]: async () => ({}) },
      }),
      (err) => err.code === MISSION_RUNTIME_BOUNDARY_VIOLATION
    );
  });

  it('dispatches discovery through the router and audits once', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
      planApproved: true,
    });

    const request = createExecutionRequestFromChat({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      objective: STR_OBJECTIVE,
      question: 'Approved. Begin Discovery.',
    });

    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: false,
      runScout: async () => ({
        status: 'completed',
        confidence: 0.81,
        payload: {
          opportunities: [
            {
              companyId: 'co-str-1',
              name: 'Summit STR Management',
              fit: 0.84,
              signals: [{ type: 'hiring', label: 'Hiring coordinator', source: 'job_board' }],
              evidenceRefs: [{ label: 'Job board posting', snapshot: { source: 'job_board' } }],
            },
          ],
          qualifiedCount: 1,
        },
      }),
    });

    assert.equal(routed.routed, true);
    assert.equal(routed.request.id, request.id);
    assert.equal(routed.specialist, 'scout');
    assert.equal(routed.executionResult.executionOutcome, 'completed');
    assert.equal(engine.get(mission.id, '10').stage, 'discover');

    const audit = listExecutionRouterAudit({ requestId: request.id });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].intent, EXECUTION_INTENTS.APPROVE_DISCOVERY);
    assert.equal(audit[0].outcome, 'dispatched');
  });

  it('replays the same request id', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
      planApproved: true,
    });
    const request = createExecutionRequestFromApprovalButton({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      objective: STR_OBJECTIVE,
    });
    const runScout = async () => ({
      status: 'completed',
      confidence: 0.8,
      payload: {
        opportunities: [{
          companyId: 'co-1',
          name: 'Replay Co',
          fit: 0.8,
          signals: [{ type: 'hiring', label: 'Hiring', source: 'job_board' }],
          evidenceRefs: [{ label: 'Job board', snapshot: { source: 'job_board' } }],
        }],
        qualifiedCount: 1,
      },
    });

    const first = await routeExecutionRequest(request, { engine, tenantId: '10', runScout });
    const replayed = await replayExecutionRequest(request, { engine, tenantId: '10', runScout });

    assert.equal(first.request.id, request.id);
    assert.equal(replayed.request.id, request.id);
    assert.equal(replayed.replay, true);
    assert.equal(replayed.executionResult.alreadyExecuted, true);
    const rows = listExecutionRouterAudit({ requestId: request.id });
    assert.equal(rows.length, 2);
    assert.equal(rows[1].replay, true);
  });
});
