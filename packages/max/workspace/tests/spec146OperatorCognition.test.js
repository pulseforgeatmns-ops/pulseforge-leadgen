'use strict';

/**
 * SPEC-146 — Operator Cognition Engine.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  THINKING_MODES,
  classifyOperatorCognition,
  mayMutateMission,
  isReadOnlyCognition,
  selectSpecialists,
} = require('../../operatorCognition');
const { maybeHandleOperatorCognitionTurn } = require('../CognitionRouting');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const { createTestAmoRuntime, installTestAmoRuntime } = require('./amoTestRuntime');
const {
  advancePlanAfterApproval,
} = require('../AmoOperatorApproval');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-146 — Operator Cognition Engine', () => {
  describe('classifyOperatorCognition', () => {
    it('classifies explain questions', () => {
      const intent = classifyOperatorCognition('Why did Scout stop?');
      assert.equal(intent.intent, THINKING_MODES.EXPLAIN);
      assert.equal(intent.mutatesMission, false);
      assert.equal(intent.thinkingMode, 'reasoning');
      assert.ok(intent.confidence >= 0.9);
    });

    it('classifies inspect questions', () => {
      const intent = classifyOperatorCognition('Where are we?');
      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.mutatesMission, false);
    });

    it('classifies execute commands', () => {
      const intent = classifyOperatorCognition('Approved. Begin Discovery.');
      assert.equal(intent.intent, THINKING_MODES.EXECUTE);
      assert.equal(intent.mutatesMission, true);
      assert.equal(intent.thinkingMode, 'execution');
    });

    it('classifies edit commands', () => {
      const intent = classifyOperatorCognition('Remove Bedford from the mission.');
      assert.equal(intent.intent, THINKING_MODES.EDIT);
      assert.equal(intent.mutatesMission, true);
    });

    it('classifies challenge, compare, strategy, brainstorm, teach, resume', () => {
      assert.equal(classifyOperatorCognition('I disagree.').intent, THINKING_MODES.CHALLENGE);
      assert.equal(classifyOperatorCognition('Compare Harbor and Granite.').intent, THINKING_MODES.COMPARE);
      assert.equal(classifyOperatorCognition('Should we pivot?').intent, THINKING_MODES.STRATEGY);
      assert.equal(classifyOperatorCognition('Give me ideas.').intent, THINKING_MODES.BRAINSTORM);
      assert.equal(classifyOperatorCognition('How does Scout work?').intent, THINKING_MODES.TEACH);
      assert.equal(classifyOperatorCognition('Resume mission.').intent, THINKING_MODES.RESUME);
    });
  });

  describe('execution guard', () => {
    it('only Execute and Edit may mutate mission state', () => {
      assert.equal(mayMutateMission({ intent: THINKING_MODES.EXECUTE, mutatesMission: true }), true);
      assert.equal(mayMutateMission({ intent: THINKING_MODES.EDIT, mutatesMission: true }), true);
      assert.equal(mayMutateMission({ intent: THINKING_MODES.EXPLAIN, mutatesMission: false }), false);
      assert.equal(mayMutateMission({ intent: THINKING_MODES.INSPECT, mutatesMission: false }), false);
      assert.equal(isReadOnlyCognition({ intent: THINKING_MODES.EXPLAIN, mutatesMission: false }), true);
    });
  });

  describe('specialist participation', () => {
    it('Explain includes Scout and Max', () => {
      const specialists = selectSpecialists({
        intent: THINKING_MODES.EXPLAIN,
      });
      assert.deepEqual(specialists, ['scout', 'max']);
    });

    it('Execute includes execution specialists', () => {
      const specialists = selectSpecialists({
        intent: THINKING_MODES.EXECUTE,
      });
      assert.ok(specialists.includes('scout'));
      assert.ok(specialists.includes('max'));
    });
  });

  describe('acceptance — Why did Scout stop?', () => {
    let engine;
    let mission;
    let runtime;

    beforeEach(() => {
      engine = amo.createAcquisitionMissionEngine();
      runtime = installTestAmoRuntime({ engine });
      mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
    });

    async function seedMissionAwaitingPrioritization() {
      await advancePlanAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved.',
      });

      const { SPECIALISTS, CONTRIBUTION_KINDS } = amo;
      engine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.SCOUT,
          kind: CONTRIBUTION_KINDS.DISCOVERY,
          payload: {
            companies: [{ name: 'Harbor Law Group', icpScore: 82 }],
            prospects: [{ name: 'Jordan Lee', company: 'Harbor Law Group' }],
            complete: true,
          },
        },
        { tenantId: '10' }
      );

      const updated = engine.get(mission.id, '10');
      updated.pendingOperatorDecision = {
        stage: STAGES.DISCOVER,
        kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
        prompt: 'Approve prioritization?',
      };
      engine.store.putMission(updated);
      return engine.inspect(mission.id, { tenantId: '10' });
    }

    it('explain turn does not invoke execution handler', async () => {
      await seedMissionAwaitingPrioritization();
      const intent = classifyOperatorCognition('Why did Scout stop?');
      const turn = await maybeHandleAcquisitionMissionExecution({
        question: 'Why did Scout stop?',
        conversationIntent: intent,
        context: { tenantId: '10', missionId: mission.id },
        acquisitionMissionRuntime: runtime,
      });
      assert.equal(turn, null);
    });

    it('explain turn returns inspection prose without mutating mission', async () => {
      const before = await seedMissionAwaitingPrioritization();
      const beforeMission = before.mission;
      const intent = classifyOperatorCognition('Why did Scout stop?');

      const turn = await maybeHandleOperatorCognitionTurn({
        question: 'Why did Scout stop?',
        conversationIntent: intent,
        context: { tenantId: '10', missionId: mission.id },
        acquisitionMissionRuntime: runtime,
        session: { id: 'spec-146', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.ok(turn);
      assert.equal(turn.conversationIntent.intent, THINKING_MODES.EXPLAIN);
      assert.match(turn.prose, /Scout|Approve prioritization|operator/i);

      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(after.mission.version, beforeMission.version);
      assert.equal(after.mission.stage, beforeMission.stage);
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        beforeMission.pendingOperatorDecision.kind
      );
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
      );
    });

    it('WorkspaceEngine.ask explains without mission mutation', async () => {
      await seedMissionAwaitingPrioritization();
      const before = engine.get(mission.id, '10');

      const workspace = createWorkspaceEngine({
        missionsEnabled: true,
        resolverEnabled: true,
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => null,
            resolve: async () => ({ action: 'intelligence' }),
            clearActiveMission: async () => {},
          },
        },
        acquisitionMissionRuntime: runtime,
      });

      const opened = await workspace.open({ tenantId: '10', missionId: mission.id });
      const result = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'Why did Scout stop?',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.ok(result.prose);
      assert.equal(result.conversationIntent.intent, THINKING_MODES.EXPLAIN);
      assert.equal(result.conversationIntent.mutatesMission, false);
      assert.match(result.prose, /Scout|prioritization|operator/i);

      const after = engine.get(mission.id, '10');
      assert.equal(after.version, before.version);
      assert.equal(after.stage, before.stage);
      assert.equal(after.pendingOperatorDecision.kind, before.pendingOperatorDecision.kind);
      assert.doesNotMatch(result.prose, /Mission Updated/i);
    });
  });
});
