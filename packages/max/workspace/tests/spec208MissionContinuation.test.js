'use strict';

/**
 * SPEC-208 — State-Aware Conversational Mission Continuation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  EXECUTION_INTENTS,
  resolveMissionContinuation,
  canAutoAdvanceOutreachToPaige,
} = amo;
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const {
  maybeHandleAcquisitionMissionExecution,
  detectExecutionAction,
} = require('../AcquisitionMissionExecution');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
} = require('../AmoOperatorApproval');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime, installTestAmoRuntime } = require('./amoTestRuntime');
const { MISSION_RUNTIMES } = require('../MissionActions');
const { specialistContext } = require('../../../acquisition-mission/Lifecycle');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function seedUnderstandMaxComplete(engine, mission) {
  await advancePlanAfterApproval({
    engine,
    mission,
    tenantId: '10',
    question: 'Approved.',
  });
  await advanceDiscoveryAfterApproval({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    question: 'Approved. Begin Discovery.',
    allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    question: 'Approved prioritization.',
  });
  await advanceMaxPrioritization({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    allowFixtureFallback: true,
  });
  return engine.inspect(mission.id, { tenantId: '10' });
}

describe('SPEC-208 — State-Aware Conversational Mission Continuation', () => {
  describe('MissionProgression.resolveMissionContinuation', () => {
    it('returns execute with GENERATE_VARIANTS when Paige is the sole eligible step', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const snapshot = await seedUnderstandMaxComplete(engine, mission);
      const ctx = specialistContext(snapshot.contributions || []);

      assert.equal(snapshot.mission.stage, STAGES.UNDERSTAND);
      assert.equal(ctx.maxComplete, true);
      assert.ok(!ctx.paigeComplete);
      assert.equal(canAutoAdvanceOutreachToPaige(snapshot), true);

      const resolution = resolveMissionContinuation(snapshot);
      assert.equal(resolution.kind, 'execute');
      assert.equal(resolution.progression.intent, EXECUTION_INTENTS.GENERATE_VARIANTS);
      assert.equal(resolution.progression.action, 'generate_variants');
    });

    it('returns pending_decision when a consumable pending decision exists', () => {
      const snapshot = {
        mission: {
          id: 'm-1',
          pendingOperatorDecision: {
            kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
            prompt: 'Continue investigation?',
          },
        },
        contributions: [],
      };
      assert.equal(resolveMissionContinuation(snapshot).kind, 'pending_decision');
    });

    it('returns inspect when progression graph has no eligible auto-advance', () => {
      const snapshot = {
        mission: {
          id: 'm-done',
          stage: STAGES.IMPROVE,
          planCancelled: false,
          pendingOperatorDecision: null,
        },
        contributions: [],
      };
      assert.equal(resolveMissionContinuation(snapshot).kind, 'inspect');
    });
  });

  describe('classifyOperatorCognition — bare continue', () => {
    it('executes mission continuation when exactly one canonical step is eligible', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const snapshot = await seedUnderstandMaxComplete(engine, mission);

      const intent = classifyOperatorCognition('continue', {
        mission: snapshot.mission,
        snapshot,
      });

      assert.equal(intent.intent, THINKING_MODES.EXECUTE);
      assert.equal(intent.via, 'mission_continuation');
      assert.equal(intent.missionContinuation.intent, EXECUTION_INTENTS.GENERATE_VARIANTS);
    });

    it('preserves read-only conversational continue without an active mission snapshot', () => {
      const intent = classifyOperatorCognition('Continue.');
      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.via, 'conversational_continue');
    });

    it('preserves read-only continue when mission has no eligible progression', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const snapshot = engine.inspect(mission.id, { tenantId: '10' });

      const intent = classifyOperatorCognition('continue', {
        mission: snapshot.mission,
        snapshot,
      });

      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.via, 'conversational_continue');
    });
  });

  describe('production regression — understand stage → Paige variants', () => {
    let engine;
    let mission;
    let runtime;
    let snapshot;

    beforeEach(async () => {
      engine = amo.createAcquisitionMissionEngine();
      mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      runtime = installTestAmoRuntime({ engine });
      snapshot = await seedUnderstandMaxComplete(engine, mission);
    });

    it('analyzeOperatorIntent requests execution for bare continue', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continue',
        mission: snapshot.mission,
        snapshot,
        resolveMission: false,
        session: {
          id: 'spec-208',
          context: { tenantId: '10', missionId: mission.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(intent.executionRequested, true);
      assert.equal(intent.conversationIntent.via, 'mission_continuation');
      assert.equal(
        intent.conversationIntent.missionContinuation.intent,
        EXECUTION_INTENTS.GENERATE_VARIANTS
      );
    });

    it('detectExecutionAction routes to generate_variants', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continue',
        mission: snapshot.mission,
        snapshot,
        resolveMission: false,
      });
      const action = detectExecutionAction('continue', snapshot, intent);
      assert.equal(action, 'generate_variants');
    });

    it('maybeHandleAcquisitionMissionExecution runs Paige via canonical CER', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continue',
        mission: snapshot.mission,
        snapshot,
        resolveMission: false,
        session: {
          id: 'spec-208-exec',
          context: { tenantId: '10', missionId: mission.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      const turn = await maybeHandleAcquisitionMissionExecution({
        question: 'continue',
        conversationIntent: intent.conversationIntent,
        operatorIntent: intent,
        context: { tenantId: '10', missionId: mission.id },
        acquisitionMissionRuntime: runtime,
        allowFixtureFallback: true,
      });

      assert.ok(turn);
      assert.equal(turn.action, 'generate_variants');
      assert.equal(turn.executionRequest.intent, EXECUTION_INTENTS.GENERATE_VARIANTS);

      const after = engine.inspect(mission.id, { tenantId: '10' });
      const ctx = specialistContext(after.contributions || []);
      assert.equal(ctx.paigeComplete, true);
    });

    it('WorkspaceEngine.ask does not fall back to today\'s briefing', async () => {
      const workspace = createWorkspaceEngine({
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
        disableLlm: true,
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => engine.get(mission.id, '10'),
            resolve: async () => ({ action: 'intelligence' }),
            clearActiveMission: async () => {},
          },
        },
      });

      const opened = workspace.open({ tenantId: '10', missionId: mission.id });
      const session = workspace._sessions.get(opened.sessionId);
      session.context.missionId = mission.id;
      session.context.acquisitionMissionId = mission.id;

      const result = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'continue',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(result.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
      assert.doesNotMatch(result.prose, /today'?s briefing/i);
      assert.doesNotMatch(result.prose, /command deck/i);

      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(specialistContext(after.contributions || []).paigeComplete, true);
    });

    it('active mission owns the turn through workspace ownership', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continue',
        mission: snapshot.mission,
        snapshot,
        resolveMission: false,
        session: {
          id: 'spec-208-owner',
          context: { tenantId: '10', missionId: mission.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      const owner = await resolveWorkspaceOwner({
        question: 'continue',
        session: {
          id: 'spec-208-owner',
          context: { tenantId: '10', missionId: mission.id },
        },
        conversationSubject: intent.conversationSubject,
        operatorIntent: intent,
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    });
  });

  describe('discovery regression — pending investigation continuation', () => {
    it('pending decision resolution remains authoritative over cognition', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const runtime = createTestAmoRuntime({ engine });

      await advancePlanAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved.',
      });
      await advanceDiscoveryAfterApproval({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        question: 'Approved. Begin Discovery.',
        allowFixtureFallback: false,
        runScout: async () => ({
          status: 'completed',
          summary: 'No qualified prospects yet.',
          payload: {
            opportunities: [],
            qualifiedCount: 0,
            candidateUniverseCount: 8,
            evidence: [{ label: 'Google Places search', source: 'google_places' }],
          },
          discoveryStatus: 'complete',
        }),
      });

      const snapshot = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        snapshot.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION
      );

      const intent = await analyzeOperatorIntent({
        question: 'continue',
        mission: snapshot.mission,
        snapshot,
        resolveMission: false,
        session: {
          id: 'spec-208-discovery',
          context: { tenantId: '10', missionId: mission.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(intent.executionRequested, true);
      assert.equal(intent.conversationIntent.via, 'pending_decision_resolved');
      assert.equal(intent.pendingDecisionResolution.action, 'continue_investigation');
      assert.equal(
        intent.pendingDecisionResolution.executionIntent,
        EXECUTION_INTENTS.CONTINUE_INVESTIGATION
      );
    });
  });

  describe('no-active-mission regression', () => {
    it('bare continue stays read-only without mission context', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continue',
        resolveMission: false,
        session: { id: 'spec-208-none', context: {} },
      });

      assert.equal(intent.executionRequested, false);
      assert.equal(intent.conversationIntent.via, 'conversational_continue');
      assert.equal(intent.mission, null);
    });
  });
});
