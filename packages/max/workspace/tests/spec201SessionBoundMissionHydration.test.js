'use strict';

/**
 * SPEC-201 — Session-Bound Mission Hydration.
 * Production regression: session.context.missionId must resolve via engine.get(id)
 * before list()-based discovery heuristics.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS, EXECUTION_INTENTS } = amo;
const {
  resolveAcquisitionActiveMission,
  resolveActiveMissionLock,
  UNRESOLVED_BOUND_MISSION_REASON,
} = require('../ActiveMissionGuard');
const {
  resolvePendingOperatorDecision,
} = require('../PendingDecisionResolver');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const { MISSION_RUNTIMES } = require('../MissionRuntimeDispatch');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function createListEmptyGetByIdRuntime(sourceEngine, mission) {
  const runtime = createTestAmoRuntime({ engine: sourceEngine });
  const targetEngine = runtime.engine();
  targetEngine.list = () => [];
  return runtime;
}

function createListEmptyGetMissingRuntime() {
  const runtime = createTestAmoRuntime({ engine: amo.createAcquisitionMissionEngine() });
  const targetEngine = runtime.engine();
  targetEngine.list = () => [];
  targetEngine.get = () => null;
  return runtime;
}

function createBoundAWinsRuntime(sourceEngine, missionA, missionB) {
  const runtime = createTestAmoRuntime({ engine: sourceEngine });
  const targetEngine = runtime.engine();
  targetEngine.list = () => [missionB];
  return runtime;
}

describe('SPEC-201 — Session-Bound Mission Hydration', () => {
  describe('resolveAcquisitionActiveMission', () => {
    it('hydrates session-bound mission by ID when list() is empty', async () => {
      const sourceEngine = amo.createAcquisitionMissionEngine();
      const mission = sourceEngine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const runtime = createListEmptyGetByIdRuntime(sourceEngine, mission);

      const resolved = await resolveAcquisitionActiveMission({
        context: { tenantId: '10', missionId: mission.id },
        session: {
          id: 'spec-201-bound',
          context: { tenantId: '10', missionId: mission.id, acquisitionMissionId: mission.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(resolved.mission.id, mission.id);
      assert.equal(resolved.unresolvedBoundMissionId, null);
      assert.equal(runtime.engine().list('10').length, 0);
    });

    it('fail-closes when session-bound mission ID cannot be resolved', async () => {
      const runtime = createListEmptyGetMissingRuntime();
      const boundId = 'mission-missing';

      const resolved = await resolveAcquisitionActiveMission({
        context: { tenantId: '10', missionId: boundId },
        session: {
          id: 'spec-201-missing',
          context: { tenantId: '10', missionId: boundId, acquisitionMissionId: boundId },
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(resolved.mission, null);
      assert.equal(resolved.unresolvedBoundMissionId, boundId);
    });

    it('prefers session-bound mission A over list() mission B', async () => {
      const sourceEngine = amo.createAcquisitionMissionEngine();
      const missionA = sourceEngine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const missionB = sourceEngine.create({
        tenantId: '10',
        objective: 'Acquire restaurant clients in Boston MA.',
        targetSegment: 'Restaurants',
      });
      const runtime = createBoundAWinsRuntime(sourceEngine, missionA, missionB);

      const resolved = await resolveAcquisitionActiveMission({
        context: { tenantId: '10', missionId: missionA.id },
        session: {
          id: 'spec-201-a-wins',
          context: { tenantId: '10', missionId: missionA.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(resolved.mission.id, missionA.id);
      assert.notEqual(resolved.mission.id, missionB.id);
    });
  });

  describe('E2E — plan approval after mission creation', () => {
    let sourceEngine;
    let mission;
    let runtime;

    beforeEach(() => {
      sourceEngine = amo.createAcquisitionMissionEngine();
      mission = sourceEngine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      assert.equal(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);
      runtime = createListEmptyGetByIdRuntime(sourceEngine, mission);
    });

    it('Turn 2 "approved" hydrates bound mission by ID and approves plan', async () => {
      const workspace = createWorkspaceEngine({
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
        disableLlm: true,
      });

      const opened = workspace.open({ tenantId: '10' });
      const session = workspace._sessions.get(opened.sessionId);
      session.context.missionId = mission.id;
      session.context.acquisitionMissionId = mission.id;

      const pendingResolution = resolvePendingOperatorDecision('approved', mission);
      assert.equal(pendingResolution.resolved, true);
      assert.equal(pendingResolution.action, 'approve_plan');
      assert.equal(pendingResolution.executionIntent, EXECUTION_INTENTS.APPROVE_PLAN);

      const turn = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'approved',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(turn.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
      assert.equal(turn.mission.id, mission.id);
      assert.match(turn.prose, /Mission Plan Approved/i);
      assert.doesNotMatch(turn.prose, /today'?s briefing/i);
      assert.doesNotMatch(turn.prose, /I can investigate today'?s briefing/i);

      const after = sourceEngine.inspect(mission.id, { tenantId: '10' });
      assert.equal(after.mission.structuredMissionApproved, true);
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );
    });
  });

  describe('E2E — fail-closed unresolved bound mission', () => {
    it('"approved" does not fall back to briefing when bound mission is missing', async () => {
      const boundId = 'mission-unresolved';
      const runtime = createListEmptyGetMissingRuntime();
      const workspace = createWorkspaceEngine({
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
        disableLlm: true,
      });

      const opened = workspace.open({ tenantId: '10' });
      const session = workspace._sessions.get(opened.sessionId);
      session.context.missionId = boundId;
      session.context.acquisitionMissionId = boundId;

      const lock = await resolveActiveMissionLock({
        question: 'approved',
        session,
        context: session.context,
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
      });
      assert.equal(lock.unresolvedBoundMission, true);
      assert.equal(lock.boundMissionId, boundId);

      const turn = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'approved',
        context: { tenantId: '10', missionId: boundId },
      });

      assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(turn.resolution.reason, UNRESOLVED_BOUND_MISSION_REASON);
      assert.equal(turn.resolution.action, 'blocked');
      assert.equal(turn.mission, null);
      assert.doesNotMatch(turn.prose, /today'?s briefing/i);
      assert.doesNotMatch(turn.prose, /morning briefing/i);
      assert.doesNotMatch(turn.prose, /Continuing with the active Mission/i);

      const intent = await analyzeOperatorIntent({
        question: 'approved',
        session,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });
      assert.equal(intent.mission, null);
    });
  });
});
