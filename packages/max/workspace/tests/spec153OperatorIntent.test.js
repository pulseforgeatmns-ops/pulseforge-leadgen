'use strict';

/**
 * SPEC-153 — Single Source of Truth for Operator Intent.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  analyzeOperatorIntent,
} = require('../OperatorIntent');
const { missionMayOwnTurn } = require('../OperatorIntentContract');
const {
  resetOperatorIntentAudit,
  getOperatorIntentAuditViolations,
} = require('../audit/OperatorIntentAudit');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const {
  CONVERSATION_SUBJECTS,
} = require('../ConversationSubject');
const {
  THINKING_MODES,
  classifyOperatorCognition,
} = require('../../operatorCognition');
const {
  advanceConversationalState,
  setConversationalState,
  CONVERSATIONAL_MODES,
} = require('../ConversationalStateMachine');
const { isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { isMissionPlanningTurn } = require('../MissionPlanningTurn');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function seedIdentityState(session) {
  setConversationalState(session, {
    subject: CONVERSATION_SUBJECTS.IDENTITY,
    owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
    activeObject: 'max',
    mode: CONVERSATIONAL_MODES.EXPLANATION,
    depth: 1,
    objects: ['max'],
    lastQuestion: 'What is your role?',
    lastIntent: THINKING_MODES.EXPLAIN,
    lastResolvedQuestion: 'What is your role?',
    confidence: 0.97,
  });
}

describe('SPEC-153 — Single Source of Truth for Operator Intent', () => {
  afterEach(() => {
    resetOperatorIntentAudit();
  });

  describe('analyzeOperatorIntent — classify once', () => {
    it('maps meta reflection to locked identity intent without execution', async () => {
      const intent = await analyzeOperatorIntent({
        question: "I'd like to continue evaluating how you think",
        session: { id: 's1', context: { tenantId: '10' } },
        resolveMission: false,
      });
      assert.equal(intent.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(intent.conversationLocked, true);
      assert.equal(intent.executionRequested, false);
      assert.equal(intent.planningRequested, false);
      assert.equal(intent.mutatesMission, false);
      assert.equal(intent.intent, 'meta_conversation');
    });

    it('detects execution and planning flags from cognition without downstream re-parse', async () => {
      const mission = {
        id: 'm1',
        stage: STAGES.UNDERSTAND,
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
        },
      };
      const intent = await analyzeOperatorIntent({
        question: 'Approved. Begin Discovery.',
        session: { id: 's1', context: { tenantId: '10', missionId: 'm1' } },
        mission,
        resolveMission: false,
      });
      assert.equal(intent.executionRequested, true);
      assert.equal(intent.mutatesMission, true);
    });
  });

  describe('ownership consumes OperatorIntent — no duplicate parsing', () => {
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
          id: 's153',
          context: { tenantId: '10', missionId: created.id, acquisitionMissionId: created.id },
        },
      };
    }

    it('identity follow-ups stay in conversation when a mission exists', async () => {
      const { runtime, session } = seedAmoMission();
      seedIdentityState(session);

      const operatorIntent = await analyzeOperatorIntent({
        question: 'Why?',
        session,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });

      const owner = await resolveWorkspaceOwner({
        question: 'Why?',
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(getOperatorIntentAuditViolations().length, 0);
    });

    it('execution commands bind to active mission via structured intent', async () => {
      const { runtime, session, mission } = seedAmoMission({
        planApproved: true,
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
          prompt: 'Approve discovery?',
        },
      });

      const operatorIntent = await analyzeOperatorIntent({
        question: 'Approve discovery.',
        session,
        mission,
        resolveMission: false,
        acquisitionMissionRuntime: runtime,
      });

      const owner = await resolveWorkspaceOwner({
        question: 'Approve discovery.',
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(getOperatorIntentAuditViolations().length, 0);
    });

    it('meta cognition does not bind to mission when one is active', async () => {
      const { runtime, session } = seedAmoMission();

      const operatorIntent = await analyzeOperatorIntent({
        question: 'How do you think?',
        session,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(operatorIntent.conversationLocked, true);
      assert.equal(missionMayOwnTurn(operatorIntent), false);

      const owner = await resolveWorkspaceOwner({
        question: 'How do you think?',
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(getOperatorIntentAuditViolations().length, 0);
    });

    it('business strategy questions fall through to reasoning with active mission', async () => {
      const { runtime, session } = seedAmoMission();

      const operatorIntent = await analyzeOperatorIntent({
        question: "What's the best acquisition strategy?",
        session,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(missionMayOwnTurn(operatorIntent), false);

      const owner = await resolveWorkspaceOwner({
        question: "What's the best acquisition strategy?",
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.REASONING);
      assert.equal(getOperatorIntentAuditViolations().length, 0);
    });

    it('planning edits bind to mission via planningRequested', async () => {
      const { runtime, session, mission } = seedAmoMission({
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION,
          prompt: 'Which geography?',
        },
      });

      const operatorIntent = await analyzeOperatorIntent({
        question: "Let's revise the mission objective.",
        session,
        mission,
        resolveMission: false,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(operatorIntent.mutatesMission, true);

      const owner = await resolveWorkspaceOwner({
        question: "Let's revise the mission objective.",
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    });
  });

  describe('audit — parsers blocked after OperatorIntent sealed', () => {
    it('records violation when execution parser runs after seal', async () => {
      await analyzeOperatorIntent({
        question: 'Hello',
        session: { id: 's1', context: {} },
        resolveMission: false,
      });
      isMissionExecutionCommand('continue');
      assert.ok(getOperatorIntentAuditViolations().includes('isMissionExecutionCommand'));
    });

    it('records violation when planning parser runs after seal', async () => {
      await analyzeOperatorIntent({
        question: 'Hello',
        session: { id: 's1', context: {} },
        resolveMission: false,
      });
      isMissionPlanningTurn({ pendingOperatorDecision: { kind: 'PLAN_APPROVAL' } }, 'approved');
      assert.ok(getOperatorIntentAuditViolations().includes('isMissionPlanningTurn'));
    });
  });

  describe('acceptance — identity conversation chain', () => {
    it('routes role → why → compare → who decides without mission ownership', async () => {
      const session = { id: 's1', context: { tenantId: '10' } };
      const chain = [
        'What is your role?',
        'Why?',
        'How is that different from Scout?',
        'Who decides?',
        'Why?',
        'When should I ignore your advice?',
      ];

      for (const question of chain) {
        resetOperatorIntentAudit();
        const operatorIntent = await analyzeOperatorIntent({
          question,
          session,
          resolveMission: false,
        });
        advanceConversationalState(session, {
          question,
          conversationSubject: operatorIntent.conversationSubject,
          conversationIntent: operatorIntent.conversationIntent,
          workspaceOwnership: {
            owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
          },
          resolvedQuestion: operatorIntent.resolvedQuestion,
          continuityApplied: operatorIntent.continuityApplied,
        });
        const owner = await resolveWorkspaceOwner({
          question,
          session,
          conversationSubject: operatorIntent.conversationSubject,
          operatorIntent,
        });
        assert.equal(
          owner.owner,
          WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
          `expected identity owner for: ${question}`
        );
        assert.equal(getOperatorIntentAuditViolations().length, 0, question);
      }
    });
  });
});
