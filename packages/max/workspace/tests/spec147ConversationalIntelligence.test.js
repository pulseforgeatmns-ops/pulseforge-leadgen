'use strict';

/**
 * SPEC-147 — Conversational Intelligence Layer.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS, SPECIALISTS, CONTRIBUTION_KINDS } = amo;
const {
  THINKING_MODES,
  classifyOperatorCognition,
  isReadOnlyCognition,
} = require('../../operatorCognition');
const { maybeHandleOperatorCognitionTurn } = require('../CognitionRouting');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const { installTestAmoRuntime } = require('./amoTestRuntime');
const { advancePlanAfterApproval } = require('../AmoOperatorApproval');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  composeConversationalResponse,
  extractMissionFacts,
} = require('../ConversationLayer');
const {
  ensureConversationMemory,
  hasExplained,
  deriveTopicKey,
} = require('../ConversationMemory');
const { PresentationEngine } = require('../PresentationEngine');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-147 — Conversational Intelligence Layer', () => {
  describe('ConversationLayer', () => {
    it('composes natural prose without status-card headlines', () => {
      const prose = composeConversationalResponse({
        question: 'Why did Scout stop?',
        conversationIntent: { intent: THINKING_MODES.EXPLAIN },
        snapshot: {
          mission: {
            stage: STAGES.UNDERSTAND,
            pendingOperatorDecision: {
              kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
              prompt: 'Approve prioritization?',
            },
          },
          workspace: { scout: { state: 'complete' } },
          contributions: [],
        },
        answered: {
          kind: 'blocker',
          mission: { stage: STAGES.UNDERSTAND },
          missionContext: { stageLabel: 'Understanding' },
          inspection: { property: 'blocker' },
        },
      }).prose;

      assert.ok(prose.length > 20);
      assert.doesNotMatch(prose, /Mission Updated/i);
      assert.doesNotMatch(prose, /^Status\s*$/m);
      assert.doesNotMatch(prose, /^Stage\s*$/m);
      assert.match(prose, /Scout|prioritization|waiting/i);
    });

    it('surfaces market coverage judgment when discovery found zero companies', () => {
      const facts = extractMissionFacts(
        {
          mission: { stage: STAGES.UNDERSTAND },
          workspace: { scout: { state: 'complete' } },
          contributions: [
            {
              specialist: SPECIALISTS.SCOUT,
              kind: CONTRIBUTION_KINDS.DISCOVERY,
              payload: { companies: [], complete: true, confidence: 0.42 },
            },
          ],
        },
        {}
      );
      assert.equal(facts.discoveryCount, 0);

      const { prose } = composeConversationalResponse({
        question: 'Why?',
        conversationIntent: { intent: THINKING_MODES.EXPLAIN },
        snapshot: {
          mission: { stage: STAGES.UNDERSTAND },
          workspace: { scout: { state: 'complete' } },
          contributions: [
            {
              specialist: SPECIALISTS.SCOUT,
              kind: CONTRIBUTION_KINDS.DISCOVERY,
              payload: { companies: [], complete: true, confidence: 0.42 },
            },
          ],
        },
        answered: { kind: 'inspection', missionContext: { stageLabel: 'Understanding' } },
        explicitReasoning: true,
      });

      assert.match(prose, /market coverage|evidence threshold/i);
      assert.doesNotMatch(prose, /Mission Updated/i);
    });

    it('records conversation memory and avoids repeating full status cards', () => {
      const session = { id: 'mem-1', messages: [] };
      const input = {
        question: 'Where are we?',
        conversationIntent: { intent: THINKING_MODES.INSPECT },
        snapshot: {
          mission: { stage: STAGES.UNDERSTAND, objective: OBJECTIVE },
          workspace: { scout: { state: 'complete' } },
          contributions: [],
        },
        answered: {
          kind: 'workspace',
          missionContext: { stageLabel: 'Understanding' },
        },
        session,
      };

      composeConversationalResponse(input);
      const memory = ensureConversationMemory(session);
      const topicKey = deriveTopicKey({
        intent: THINKING_MODES.INSPECT,
        stage: STAGES.UNDERSTAND,
      });
      assert.ok(hasExplained(memory, topicKey) || memory.explainedTopics.length > 0);

      const second = composeConversationalResponse(input);
      assert.match(second.prose, /As I mentioned|Building on what we already covered/i);
    });
  });

  describe('operator cognition — conversational continue', () => {
    it('treats bare Continue as read-only when no pending decision', () => {
      const intent = classifyOperatorCognition('Continue.');
      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.mutatesMission, false);
    });

    it('treats bare Continue as read-only even when mission has pending decision', () => {
      const intent = classifyOperatorCognition('Continue.', {
        mission: {
          pendingOperatorDecision: {
            kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
            prompt: 'Approve prioritization?',
          },
        },
      });
      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.mutatesMission, false);
    });

    it('treats Approved. Continue. as execute', () => {
      const intent = classifyOperatorCognition('Approved. Continue.');
      assert.equal(intent.intent, THINKING_MODES.EXECUTE);
      assert.equal(intent.mutatesMission, true);
    });
  });

  describe('presentation', () => {
    it('passes conversational responses through without status-card rewrite', async () => {
      const engine = new PresentationEngine({ disableLlm: true });
      const presented = await engine.present({
        answer: 'Scout finished discovery, but nothing met our evidence threshold.',
        reasoning: [],
        metadata: {
          conversationalIntelligence: true,
          strictOutputShape: true,
        },
      });
      assert.equal(presented.presentation, 'conversational_intelligence');
      assert.doesNotMatch(presented.prose, /Mission Updated/i);
    });
  });

  describe('acceptance — twenty-minute discussion without mutation', () => {
    let engine;
    let mission;
    let runtime;
    let workspace;

    beforeEach(() => {
      engine = amo.createAcquisitionMissionEngine();
      runtime = installTestAmoRuntime({ engine });
      mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      workspace = createWorkspaceEngine({
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
    });

    async function seedZeroDiscoveryMission() {
      await advancePlanAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved.',
      });

      engine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.SCOUT,
          kind: CONTRIBUTION_KINDS.DISCOVERY,
          payload: {
            companies: [],
            prospects: [],
            complete: true,
            confidence: 0.41,
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
      return engine.get(mission.id, '10');
    }

    async function askMission(question) {
      const opened = await workspace.open({ tenantId: '10', missionId: mission.id });
      return workspace.ask({
        sessionId: opened.sessionId,
        question,
        context: { tenantId: '10', missionId: mission.id },
      });
    }

    it('supports explain, challenge, strategy, teach, and inspect without mutation', async () => {
      const before = await seedZeroDiscoveryMission();
      const turns = [
        'Why did Scout stop?',
        'Why?',
        'I disagree — I think we should prioritize anyway.',
        'Should we pivot to a different market?',
        'How does Scout work?',
        'Where are we?',
        'Give me ideas.',
        'Continue.',
      ];

      let sessionId = null;
      for (const question of turns) {
        const intent = classifyOperatorCognition(question, { mission: before });
        assert.equal(
          isReadOnlyCognition(intent),
          true,
          `expected read-only cognition for: ${question} (got ${intent.intent})`
        );

        const exec = await maybeHandleAcquisitionMissionExecution({
          question,
          conversationIntent: intent,
          context: { tenantId: '10', missionId: mission.id },
          acquisitionMissionRuntime: runtime,
        });
        assert.equal(exec, null, `execution handler should not run for: ${question}`);

        const opened = sessionId
          ? { sessionId }
          : await workspace.open({ tenantId: '10', missionId: mission.id });
        sessionId = opened.sessionId;

        const result = await workspace.ask({
          sessionId,
          question,
          context: { tenantId: '10', missionId: mission.id },
        });

        assert.ok(result.prose, `missing prose for: ${question}`);
        assert.doesNotMatch(result.prose, /Mission Updated/i, question);
        if (result.conversationIntent) {
          assert.equal(result.conversationIntent.mutatesMission, false, question);
        }
      }

      const after = engine.get(mission.id, '10');
      assert.equal(after.version, before.version);
      assert.equal(after.stage, before.stage);
      assert.equal(
        after.pendingOperatorDecision.kind,
        before.pendingOperatorDecision.kind
      );
    });

    it('routes strategy and teach through conversational mission handler', async () => {
      await seedZeroDiscoveryMission();
      for (const question of ['Should we pivot?', 'Teach me how Scout works.']) {
        const intent = classifyOperatorCognition(question);
        const turn = await maybeHandleOperatorCognitionTurn({
          question,
          conversationIntent: intent,
          context: { tenantId: '10', missionId: mission.id },
          acquisitionMissionRuntime: runtime,
          session: { id: 'spec-147', context: { tenantId: '10', missionId: mission.id } },
        });
        assert.ok(turn, question);
        assert.match(turn.prose, /.+/);
        assert.doesNotMatch(turn.prose, /Mission Updated/i);
        assert.equal(turn.structured.metadata.conversationalIntelligence, true);
      }
    });
  });
});
