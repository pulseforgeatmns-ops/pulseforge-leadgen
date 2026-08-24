'use strict';

/**
 * SPEC-155 — Conversation Contract Engine acceptance tests.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  buildConversationContract,
  detectContractSignals,
  getConversationContract,
  setConversationContract,
  contractBlocksExecution,
} = require('../ConversationContract');
const { resolveConversationContract, missionOwnershipProhibited } = require('../ConversationContractEngine');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { missionMayOwnTurn } = require('../OperatorIntentContract');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { resolveMissionRuntime } = require('../MissionRuntimeDispatch');
const { PresentationEngine } = require('../PresentationEngine');
const {
  CONVERSATION_SUBJECTS,
} = require('../ConversationSubject');
const {
  setConversationalState,
  getConversationalState,
  CONVERSATIONAL_MODES,
} = require('../ConversationalStateMachine');
const { THINKING_MODES } = require('../../operatorCognition');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function seedAmoMission(extra = {}) {
  const amoEngine = amo.createAcquisitionMissionEngine();
  const created = amoEngine.create({
    tenantId: '10',
    objective: OBJECTIVE,
    targetSegment: 'Law Firms',
    ...extra,
  });
  return {
    amoEngine,
    mission: created,
    runtime: createTestAmoRuntime({ engine: amoEngine }),
    session: {
      id: 's155',
      context: { tenantId: '10', missionId: created.id, acquisitionMissionId: created.id },
    },
  };
}

describe('SPEC-155 — Conversation Contract Engine', () => {
  describe('contract detection', () => {
    it('detects execution-forbidden reflection contract from operator rules', () => {
      const text =
        "I'd like to understand how you think. Don't execute anything. Answer naturally. Maintain the conversation.";
      const signals = detectContractSignals(text);
      assert.equal(signals.executionForbidden, true);
      assert.equal(signals.executionAllowed, false);
      assert.equal(signals.naturalConversation, true);
      assert.equal(signals.maintainContext, true);
      assert.equal(signals.reflectionMode, true);
      assert.equal(signals.shouldLock, true);

      const built = buildConversationContract({ question: text });
      assert.equal(built.contract.executionAllowed, false);
      assert.equal(built.contract.reasoningMode, 'reflection');
      assert.equal(built.contract.maintainContext, true);
      assert.equal(built.contract.naturalConversation, true);
      assert.equal(built.contract.locked, true);
      assert.equal(built.contract.conversationGoal, "Understand Max's reasoning.");
    });

    it('updates contract when operator enables execution', () => {
      const prior = buildConversationContract({
        question: "Don't execute anything. Let's discuss your reasoning.",
      }).contract;

      const updated = buildConversationContract({
        question: 'Stop theorizing. Execute.',
        priorContract: prior,
      });

      assert.equal(updated.contract.executionAllowed, true);
      assert.equal(updated.changed, true);
      assert.equal(updated.reason, 'execution_enabled');
    });

    it('resets contract on explicit topic switch', () => {
      const prior = buildConversationContract({
        question: "Don't execute. Maintain context.",
      }).contract;

      const switched = buildConversationContract({
        question: "Let's switch topics.",
        priorContract: prior,
      });

      assert.equal(switched.reason, 'topic_switch');
      assert.equal(switched.contract.executionAllowed, true);
    });
  });

  describe('acceptance tests', () => {
    it('Test 1 — execution forbidden keeps conversation owner with active mission', async () => {
      const { runtime, session } = seedAmoMission();

      const contractResolution = resolveConversationContract({
        question: "Don't execute anything. Let's discuss your reasoning.",
        session,
      });

      assert.equal(contractResolution.contract.executionAllowed, false);
      assert.equal(missionOwnershipProhibited(contractResolution.contract), true);

      const operatorIntent = await analyzeOperatorIntent({
        question: "Don't execute anything. Let's discuss your reasoning.",
        session,
        conversationContract: contractResolution.contract,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(operatorIntent.executionRequested, false);
      assert.equal(operatorIntent.mutatesMission, false);
      assert.equal(missionMayOwnTurn(operatorIntent), false);

      const owner = await resolveWorkspaceOwner({
        question: "Don't execute anything. Let's discuss your reasoning.",
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        conversationContract: contractResolution.contract,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.notEqual(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    });

    it('Test 2 — Why? with active mission stays conversational under contract', async () => {
      const { runtime, session } = seedAmoMission();

      resolveConversationContract({
        question: "Don't execute anything. Let's discuss your reasoning.",
        session,
      });

      setConversationalState(session, {
        subject: CONVERSATION_SUBJECTS.IDENTITY,
        owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
        activeObject: 'max',
        mode: CONVERSATIONAL_MODES.EXPLANATION,
        depth: 1,
        lastQuestion: 'What is your role?',
        lastIntent: THINKING_MODES.EXPLAIN,
        contract: getConversationContract(session),
      });

      const contract = getConversationContract(session);
      const operatorIntent = await analyzeOperatorIntent({
        question: 'Why do you think that?',
        session,
        conversationContract: contract,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });

      const owner = await resolveWorkspaceOwner({
        question: 'Why do you think that?',
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        conversationContract: contract,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(missionMayOwnTurn(operatorIntent), false);
    });

    it('Test 3 — Actually approve discovery updates contract and allows mission', async () => {
      const { runtime, session, mission } = seedAmoMission({
        planApproved: true,
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
          prompt: 'Approve discovery?',
        },
      });

      setConversationContract(
        session,
        buildConversationContract({
          question: "Don't execute anything.",
        }).contract
      );

      const contractResolution = resolveConversationContract({
        question: 'Actually approve discovery.',
        session,
      });

      assert.equal(contractResolution.contract.executionAllowed, true);
      assert.equal(contractResolution.changed, true);

      const operatorIntent = await analyzeOperatorIntent({
        question: 'Actually approve discovery.',
        session,
        mission,
        conversationContract: contractResolution.contract,
        resolveMission: false,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(operatorIntent.executionRequested, true);
      assert.equal(missionMayOwnTurn(operatorIntent), true);

      const owner = await resolveWorkspaceOwner({
        question: 'Actually approve discovery.',
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        conversationContract: contractResolution.contract,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    });

    it('Test 4 — stay conversational maintains thread across follow-ups', async () => {
      const session = { id: 's155-4', context: { tenantId: '10' } };

      resolveConversationContract({
        question: "Don't execute anything. Stay conversational.",
        session,
      });

      setConversationalState(session, {
        subject: CONVERSATION_SUBJECTS.IDENTITY,
        owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
        activeObject: 'max',
        mode: CONVERSATIONAL_MODES.EXPLANATION,
        depth: 1,
        lastQuestion: 'What is your role?',
        lastIntent: THINKING_MODES.EXPLAIN,
        contract: getConversationContract(session),
      });

      const contract = getConversationContract(session);
      assert.equal(contract.maintainContext, true);
      assert.equal(contractBlocksExecution(contract), true);

      for (const followUp of ['Why?', 'How?', 'What assumption?', 'What if?']) {
        const operatorIntent = await analyzeOperatorIntent({
          question: followUp,
          session,
          conversationContract: contract,
          resolveMission: false,
        });

        const owner = await resolveWorkspaceOwner({
          question: followUp,
          session,
          conversationSubject: operatorIntent.conversationSubject,
          operatorIntent,
          conversationContract: contract,
        });

        assert.equal(
          owner.owner,
          WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
          `follow-up "${followUp}" should stay conversational`
        );
        assert.equal(missionMayOwnTurn(operatorIntent), false);
      }
    });

    it('Test 5 — Stop theorizing. Execute. enables mission runtime', async () => {
      const { runtime, session, mission } = seedAmoMission({
        planApproved: true,
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
          prompt: 'Approve discovery?',
        },
      });

      setConversationContract(
        session,
        buildConversationContract({
          question: "Don't execute anything. Let's discuss your reasoning.",
        }).contract
      );

      const contractResolution = resolveConversationContract({
        question: 'Stop theorizing. Execute.',
        session,
      });

      assert.equal(contractResolution.contract.executionAllowed, true);

      const operatorIntent = await analyzeOperatorIntent({
        question: 'Stop theorizing. Execute.',
        session,
        mission,
        conversationContract: contractResolution.contract,
        resolveMission: false,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(missionMayOwnTurn(operatorIntent), true);

      const runtimeDecision = await resolveMissionRuntime({
        question: 'Stop theorizing. Execute.',
        session,
        operatorIntent,
        conversationContract: contractResolution.contract,
        acquisitionMissionRuntime: runtime,
      });

      assert.notEqual(runtimeDecision.runtime, null);
      assert.notEqual(runtimeDecision.reason, 'conversation_contract_read_only');
    });
  });

  describe('presentation and mission runtime guards', () => {
    it('PresentationEngine suppresses mission communication under read-only contract', async () => {
      const engine = new PresentationEngine({ disableLlm: true });
      const contract = buildConversationContract({
        question: "Don't execute anything.",
      }).contract;

      const result = await engine.present({
        answer: 'Mission Updated: Discovery approved.',
        reasoning: [],
        metadata: {
          missionCommunication: true,
          conversationContract: contract,
        },
      });

      assert.equal(result.presentation, 'conversation_contract_read_only');
      assert.equal(result.metadata.readOnlyConversation, true);
      assert.equal(result.metadata.missionCommunication, false);
    });

    it('resolveMissionRuntime returns read-only when contract forbids execution', async () => {
      const { runtime, session } = seedAmoMission();
      const contract = buildConversationContract({
        question: "Don't execute anything.",
      }).contract;

      const runtimeDecision = await resolveMissionRuntime({
        question: 'Approve discovery.',
        session,
        conversationContract: contract,
        operatorIntent: {
          executionRequested: false,
          conversationContract: contract,
        },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(runtimeDecision.runtime, null);
      assert.equal(runtimeDecision.reason, 'conversation_contract_read_only');
      assert.equal(runtimeDecision.readOnly, true);
    });
  });

  describe('WorkspaceEngine multi-turn success criteria', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('full reflection conversation without mission mutation', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const establish = await workspace.ask({
        sessionId,
        question:
          "I'd like to understand how you think. Don't execute anything. Answer naturally.",
        context: { tenantId: '10' },
      });

      assert.equal(establish.conversationContract.executionAllowed, false);
      assert.equal(establish.conversationContract.locked, true);
      assert.equal(establish.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.notEqual(establish.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);

      const turns = [
        'What is your role?',
        'Why?',
        'How is that different from Scout?',
        'Why?',
        'What assumption is that based on?',
        'What if that assumption is wrong?',
        'Summarize how your thinking evolved.',
      ];

      let lastTurn = establish;
      for (const question of turns) {
        lastTurn = await workspace.ask({
          sessionId,
          question,
          context: { tenantId: '10' },
        });

        assert.equal(
          lastTurn.conversationContract.executionAllowed,
          false,
          `execution should stay forbidden for "${question}"`
        );
        assert.notEqual(lastTurn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
        assert.ok(
          !String(lastTurn.prose || '').includes('Mission Updated'),
          `should not present mission update for "${question}"`
        );
      }

      assert.ok(lastTurn.conversationContract.maintainContext || lastTurn.conversationalState.contract);
      assert.equal(getConversationalState({ id: sessionId, context: lastTurn.context }).contract.executionAllowed, false);
    });
  });
});
