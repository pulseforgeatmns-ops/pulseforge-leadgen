'use strict';

/**
 * SPEC-197 — Pending Decision Conversational Resolution.
 * AUDIT-072 regression: pending discovery_investigation must resolve from natural language.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS, EXECUTION_INTENTS, EXECUTION_SOURCES } = amo;
const {
  resolvePendingOperatorDecision,
  RESOLUTION_OUTCOMES,
} = require('../PendingDecisionResolver');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
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
} = require('../AmoOperatorApproval');
const {
  hasPendingDiscoveryInvestigation,
} = require('../../../acquisition-mission/PendingOperatorDecision');
const {
  createExecutionRequestFromChat,
  createExecutionRequestFromApprovalButton,
  canonicalRequestShape,
} = require('../../../acquisition-mission/ExecutionRequest');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime, installTestAmoRuntime } = require('./amoTestRuntime');
const { MISSION_RUNTIMES } = require('../MissionActions');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function investigationMission(extra = {}) {
  return {
    id: 'm-investigation',
    stage: STAGES.DISCOVER,
    objective: OBJECTIVE,
    pendingOperatorDecision: {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
      prompt: 'Continue investigation?',
    },
    ...extra,
  };
}

async function seedInvestigationPending(engine, mission) {
  await advancePlanAfterApproval({
    engine,
    mission,
    tenantId: '10',
    question: 'Approved.',
  });

  const result = await advanceDiscoveryAfterApproval({
    engine,
    mission,
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

  assert.equal(
    result.snapshot.mission.pendingOperatorDecision.kind,
    OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION
  );
  assert.equal(hasPendingDiscoveryInvestigation(result.snapshot), true);
  return result.snapshot;
}

describe('SPEC-197 — Pending Decision Conversational Resolution', () => {
  describe('PendingDecisionResolver — discovery_investigation matrix', () => {
    const mission = investigationMission();

    const affirmCases = [
      'continue',
      'yes continue',
      'continue investigation',
      'continue the investigation please',
      'continue thee investigation please',
      'go ahead',
      'keep investigating',
      'proceed',
      'approved',
      'yep',
      'yeah',
      'yes',
      'sounds good',
      'do it',
    ];

    for (const utterance of affirmCases) {
      it(`affirms "${utterance}" as continue_investigation`, () => {
        const resolution = resolvePendingOperatorDecision(utterance, mission);
        assert.equal(resolution.resolved, true);
        assert.equal(resolution.resolvedFromPendingDecision, true);
        assert.equal(resolution.decisionKind, OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION);
        assert.equal(resolution.action, 'continue_investigation');
        assert.equal(resolution.executionIntent, EXECUTION_INTENTS.CONTINUE_INVESTIGATION);
        assert.ok(resolution.confidence >= 0.9);
      });
    }

    it('does not affirm unrelated briefing question', () => {
      const resolution = resolvePendingOperatorDecision(
        "tell me about today's briefing",
        mission
      );
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
    });

    it('does not affirm Scout rejection inspection question', () => {
      const resolution = resolvePendingOperatorDecision(
        'why did Scout reject Lot 202?',
        mission
      );
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
    });

    it('does not affirm decision clarification questions', () => {
      for (const utterance of ['why?', 'what happens if we continue?']) {
        const resolution = resolvePendingOperatorDecision(utterance, mission);
        assert.equal(resolution.resolved, false);
        assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.QUESTION);
      }
    });

    it('routes modification language to modify_mission', () => {
      const resolution = resolvePendingOperatorDecision('change this to Bedford', mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.MODIFY);
      assert.equal(resolution.action, 'modify_mission');
      assert.equal(resolution.resolvedFromPendingDecision, false);
    });

    it('routes cancellation language to cancel', () => {
      for (const utterance of ['cancel', 'stop the mission']) {
        const resolution = resolvePendingOperatorDecision(utterance, mission);
        assert.equal(resolution.resolved, true);
        assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.REJECT);
        assert.equal(resolution.action, 'cancel');
        assert.equal(resolution.executionIntent, EXECUTION_INTENTS.CANCEL_PLAN);
      }
    });
  });

  describe('OperatorIntent — pending decision beats cognition demotion', () => {
    it('AUDIT-072: continue thee investigation please requests execution', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const created = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const snapshot = await seedInvestigationPending(engine, created);
      const runtime = createTestAmoRuntime({ engine });

      const bareCognition = classifyOperatorCognition(
        'continue thee investigation please',
        { mission: snapshot.mission }
      );
      assert.equal(bareCognition.intent, THINKING_MODES.INSPECT);
      assert.equal(bareCognition.via, 'conversational_continue');

      const intent = await analyzeOperatorIntent({
        question: 'continue thee investigation please',
        mission: snapshot.mission,
        resolveMission: false,
        session: {
          id: 'audit-072',
          context: { tenantId: '10', missionId: created.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(intent.executionRequested, true);
      assert.equal(intent.mutatesMission, true);
      assert.equal(intent.conversationIntent.intent, THINKING_MODES.EXECUTE);
      assert.equal(intent.conversationIntent.via, 'pending_decision_resolved');
      assert.ok(intent.pendingDecisionResolution);
      assert.equal(intent.pendingDecisionResolution.resolvedFromPendingDecision, true);
      assert.equal(intent.pendingDecisionResolution.action, 'continue_investigation');
      assert.equal(
        intent.pendingDecisionResolution.executionIntent,
        EXECUTION_INTENTS.CONTINUE_INVESTIGATION
      );

      const owner = await resolveWorkspaceOwner({
        question: 'continue thee investigation please',
        session: {
          id: 'audit-072',
          context: { tenantId: '10', missionId: created.id },
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

  describe('PendingDecisionResolver — execution_approval matrix', () => {
    const mission = {
      id: 'm-execution-approval',
      stage: STAGES.READY,
      objective: OBJECTIVE,
      pendingOperatorDecision: {
        stage: STAGES.READY,
        kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
        prompt: 'Approve execution of the prepared outbound plan?',
      },
    };

    it('resolves the exact production utterance as request_revision', () => {
      const q =
        'Do not authorize this outreach. The prepared messaging needs to be regenerated before execution. Regenerate the outreach messaging and outbound plan for these prospects, then return the updated package for my approval. Do not send anything.';
      const resolution = resolvePendingOperatorDecision(q, mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.REQUEST_REVISION);
      assert.equal(resolution.action, 'request_revision');
      assert.equal(resolution.resolvedFromPendingDecision, false);
      assert.equal(resolution.decisionKind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
      assert.equal(resolution.executionIntent, null);
    });

    it('rejects no-send language without authorizing execution', () => {
      const resolution = resolvePendingOperatorDecision("No, don't send this.", mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.REJECT);
      assert.equal(resolution.action, 'cancel');
      assert.equal(resolution.executionIntent, EXECUTION_INTENTS.CANCEL_PLAN);
    });

    it('approves execution when the operator explicitly authorizes it', () => {
      const resolution = resolvePendingOperatorDecision('Yes, authorize it.', mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AFFIRM);
      assert.equal(resolution.action, 'approve_execution');
      assert.equal(resolution.executionIntent, EXECUTION_INTENTS.APPROVE_EXECUTION);
    });

    it('does not approve when the operator says do not authorize this', () => {
      const resolution = resolvePendingOperatorDecision('Do not authorize this.', mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.REJECT);
      assert.notEqual(resolution.action, 'approve_execution');
      assert.equal(resolution.executionIntent, EXECUTION_INTENTS.CANCEL_PLAN);
    });

    it('routes rewrite language to request_revision', () => {
      const resolution = resolvePendingOperatorDecision('Regenerate the message.', mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.REQUEST_REVISION);
      assert.equal(resolution.action, 'request_revision');
      assert.equal(resolution.executionIntent, null);
    });

    it('leaves clearly ambiguous execution_approval replies unresolved', () => {
      const resolution = resolvePendingOperatorDecision('Continuee', mission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AMBIGUOUS);
    });

    it('routes objective change language to modify_mission', () => {
      const resolution = resolvePendingOperatorDecision(
        'Change the mission objective instead.',
        mission
      );
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.MODIFY);
      assert.equal(resolution.action, 'modify_mission');
      assert.equal(resolution.resolvedFromPendingDecision, false);
    });
  });

  describe('AcquisitionMissionExecution — resolved pending decision executes Scout', () => {
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
      snapshot = await seedInvestigationPending(engine, mission);
    });

    it('maybeHandleAcquisitionMissionExecution advances investigation via canonical CER', async () => {
      let scoutRuns = 0;
      const intent = await analyzeOperatorIntent({
        question: 'continue thee investigation please',
        mission: snapshot.mission,
        resolveMission: false,
        session: {
          id: 'spec-197-exec',
          context: { tenantId: '10', missionId: mission.id },
        },
        acquisitionMissionRuntime: runtime,
      });

      const turn = await maybeHandleAcquisitionMissionExecution({
        question: 'continue thee investigation please',
        conversationIntent: intent.conversationIntent,
        operatorIntent: intent,
        context: { tenantId: '10', missionId: mission.id },
        acquisitionMissionRuntime: runtime,
        allowFixtureFallback: false,
        runScout: async () => {
          scoutRuns += 1;
          return {
            status: 'completed',
            summary: '1 prospect matches mission objective.',
            confidence: 0.81,
            discoveryStatus: 'complete',
            payload: {
              opportunities: [
                {
                  companyId: 'co-1',
                  name: 'Summit STR Management',
                  fit: 0.84,
                  timing: 0.72,
                  confidence: 0.81,
                  signals: [
                    {
                      type: 'hiring',
                      label: 'Hiring cleaning operations coordinator',
                      source: 'job_board',
                    },
                  ],
                  evidenceRefs: [
                    {
                      label: 'Job posting: cleaning operations coordinator',
                      snapshot: {
                        source: 'job_board',
                        companyName: 'Summit STR Management',
                      },
                    },
                  ],
                },
              ],
              qualifiedCount: 1,
            },
          };
        },
      });

      assert.ok(turn);
      assert.equal(scoutRuns, 1);
      assert.equal(turn.action, 'discovery_investigation_continued');
      assert.ok(turn.executionRequest);
      assert.equal(turn.executionRequest.intent, EXECUTION_INTENTS.CONTINUE_INVESTIGATION);
      assert.equal(turn.executionRequest.source, EXECUTION_SOURCES.CHAT);
      assert.equal(
        turn.executionResult.investigationContinuation,
        true
      );
    });

    it('detectExecutionAction consumes OperatorIntent pending resolution', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continue the investigation please',
        mission: snapshot.mission,
        resolveMission: false,
      });
      const action = detectExecutionAction(
        'continue the investigation please',
        snapshot,
        intent
      );
      assert.equal(action, 'discovery_investigation_continued');
    });
  });

  describe('chat and approval button share canonical execution intent', () => {
    it('CONTINUE INVESTIGATION button and natural language produce the same intent', () => {
      const mission = investigationMission();
      const chatIntent = resolvePendingOperatorDecision(
        'yeah keep investigating',
        mission
      );
      const chatCer = createExecutionRequestFromChat({
        intent: chatIntent.executionIntent,
        missionId: mission.id,
        operatorId: 'operator',
        objective: mission.objective,
        question: 'yeah keep investigating',
      });
      const buttonCer = createExecutionRequestFromApprovalButton({
        intent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
        missionId: mission.id,
        operatorId: 'operator',
        objective: mission.objective,
        pendingOperatorDecision: mission.pendingOperatorDecision,
      });

      assert.deepEqual(
        canonicalRequestShape(chatCer).intent,
        canonicalRequestShape(buttonCer).intent
      );
      assert.equal(chatCer.source, EXECUTION_SOURCES.CHAT);
      assert.equal(buttonCer.source, EXECUTION_SOURCES.APPROVAL_BUTTON);
    });
  });

  describe('AUDIT-072 regression — WorkspaceEngine.ask', () => {
    let engine;
    let mission;
    let runtime;

    beforeEach(async () => {
      engine = amo.createAcquisitionMissionEngine();
      mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      runtime = createTestAmoRuntime({ engine });
      await seedInvestigationPending(engine, mission);
    });

    it('does not fall back to today\'s briefing for continue investigation answer', async () => {
      const before = engine.inspect(mission.id, { tenantId: '10' });
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
        question: 'Continue the investigation please.',
        context: { tenantId: '10', missionId: mission.id },
      });

      const operatorIntent = session.context.operatorIntent;
      assert.ok(operatorIntent);
      assert.equal(operatorIntent.executionRequested, true);
      assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(result.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
      assert.match(result.prose, /Investigation Continued/i);
      assert.doesNotMatch(result.prose, /Continue in mission workspace/i);
      assert.doesNotMatch(result.prose, /I can investigate today'?s briefing/i);
      assert.equal(
        result.resolution.reason,
        'acquisition_mission_discovery_investigation_continued'
      );

      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.notEqual(after.mission.version, before.mission.version);
    });
  });
});
