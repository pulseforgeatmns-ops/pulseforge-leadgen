'use strict';

/**
 * SPEC-148 — Session State Manager acceptance tests (ADR-068).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  OPERATING_MODES,
  EXECUTION_POLICIES,
  REASONING_MODES,
  CONVERSATION_STYLES,
  EVALUATION_MODES,
  getSessionState,
  getSessionStateHistory,
  createDefaultSessionState,
  sessionStateBlocksExecution,
} = require('../SessionState');
const {
  resolveSessionState,
  buildSessionState,
  detectSessionDirectiveSignals,
  isSessionInspectionQuestion,
  formatSessionInspection,
  applySessionStateToContract,
} = require('../SessionStateManager');
const { resolveConversationContract, missionOwnershipProhibited } = require('../ConversationContractEngine');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { missionMayOwnTurn } = require('../OperatorIntentContract');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { resolveMissionRuntime } = require('../MissionRuntimeDispatch');
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
      id: 's148ssm',
      context: { tenantId: '10', missionId: created.id, acquisitionMissionId: created.id },
    },
  };
}

describe('SPEC-148 — Session State Manager', () => {
  describe('directive detection', () => {
    it('detects persistent session directives from operator rules', () => {
      const text =
        'For the remainder of this conversation: Operate as the business operating system. ' +
        "Don't execute anything. Explain your reasoning naturally.";
      const signals = detectSessionDirectiveSignals(text);
      assert.equal(signals.persistent, true);
      assert.equal(signals.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(signals.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(signals.reasoningMode, REASONING_MODES.ANALYTICAL);
      assert.equal(signals.conversationStyle, CONVERSATION_STYLES.NATURAL);

      const built = buildSessionState({ question: text });
      assert.equal(built.state.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(built.state.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(built.state.reasoningMode, REASONING_MODES.ANALYTICAL);
      assert.equal(built.state.conversationStyle, CONVERSATION_STYLES.NATURAL);
      assert.equal(built.changed, true);
    });

    it('detects evaluation mode from operator phrase', () => {
      const built = buildSessionState({ question: "We're evaluating Max." });
      assert.equal(built.state.evaluationMode, EVALUATION_MODES.MAX);
      assert.equal(built.state.operatingMode, OPERATING_MODES.REASONING_EVALUATION);
    });

    it('records session history on mutation', () => {
      const session = { id: 'hist', context: {} };
      buildSessionState({
        question: "Don't execute anything.",
        session,
      });
      const history = getSessionStateHistory(session);
      assert.ok(history.length >= 1);
      assert.equal(history[0].field, 'executionPolicy');
      assert.equal(history[0].current, EXECUTION_POLICIES.READ_ONLY);
    });
  });

  describe('acceptance tests', () => {
    it('Test 1 — execution disabled persists five turns later', () => {
      const session = { id: 't1', context: {} };

      resolveSessionState({
        question: "For the rest of this conversation: Don't execute anything.",
        session,
      });

      const stateAfterTurn1 = getSessionState(session);
      assert.equal(stateAfterTurn1.executionPolicy, EXECUTION_POLICIES.READ_ONLY);

      for (let i = 0; i < 5; i += 1) {
        resolveSessionState({
          question: `Follow-up question ${i + 2}: Why did you choose that approach?`,
          session,
        });
      }

      const stateAfterTurn6 = getSessionState(session);
      assert.equal(stateAfterTurn6.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.ok(sessionStateBlocksExecution(stateAfterTurn6));
    });

    it('Test 2 — reasoning mode persists across subsequent responses', () => {
      const session = { id: 't2', context: {} };

      resolveSessionState({
        question: 'Explain your reasoning naturally.',
        session,
      });

      assert.equal(getSessionState(session).reasoningMode, REASONING_MODES.ANALYTICAL);

      resolveSessionState({
        question: 'What assumption are you making?',
        session,
      });

      assert.equal(getSessionState(session).reasoningMode, REASONING_MODES.ANALYTICAL);
    });

    it('Test 3 — session inspection returns stored state, not inference', () => {
      const session = { id: 't3', context: {} };

      resolveSessionState({
        question:
          'For the remainder of this conversation: Operate as the business operating system. ' +
          "Don't execute anything. Explain your reasoning naturally.",
        session,
      });

      assert.equal(isSessionInspectionQuestion('What operating mode are you using?'), true);

      const inspection = formatSessionInspection(getSessionState(session));
      assert.match(inspection, /Operating Mode: Business Operation/);
      assert.match(inspection, /Execution Policy: Read Only/);
      assert.match(inspection, /Reasoning Mode: Analytical/);
      assert.match(inspection, /Conversation Style: Natural/);
      assert.match(inspection, /remain active until you change or reset them/);
    });

    it('Test 4 — operator mode change updates immediately and records history', () => {
      const session = { id: 't4', context: {} };

      resolveSessionState({
        question: "Don't execute anything.",
        session,
      });

      const historyBefore = getSessionStateHistory(session).length;

      const updated = resolveSessionState({
        question: 'Enable execution. Autonomous execution.',
        session,
      });

      assert.equal(updated.state.executionPolicy, EXECUTION_POLICIES.AUTONOMOUS);
      assert.equal(updated.changed, true);
      assert.ok(getSessionStateHistory(session).length > historyBefore);
    });

    it('Test 5 — mission runtime blocked when session executionPolicy is read_only', async () => {
      const { runtime, session } = seedAmoMission();

      const sessionResolution = resolveSessionState({
        question: "For the rest of this conversation: Don't execute anything.",
        session,
      });

      const contractResolution = resolveConversationContract({
        question: 'Approve discovery and execute.',
        session,
        sessionState: sessionResolution.state,
      });

      assert.equal(contractResolution.contract.executionAllowed, false);

      const operatorIntent = await analyzeOperatorIntent({
        question: 'Approve discovery and execute.',
        session,
        conversationContract: contractResolution.contract,
        sessionState: sessionResolution.state,
        resolveMission: true,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(operatorIntent.executionRequested, false);
      assert.equal(operatorIntent.mutatesMission, false);
      assert.equal(missionMayOwnTurn(operatorIntent), false);

      const runtimeResult = await resolveMissionRuntime({
        question: 'Approve discovery and execute.',
        session,
        operatorIntent,
        conversationContract: contractResolution.contract,
        sessionState: sessionResolution.state,
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(runtimeResult.runtime, null);
      assert.equal(runtimeResult.reason, 'session_state_read_only');
      assert.equal(runtimeResult.readOnly, true);

      const owner = await resolveWorkspaceOwner({
        question: 'Approve discovery and execute.',
        session,
        conversationSubject: operatorIntent.conversationSubject,
        operatorIntent,
        conversationContract: contractResolution.contract,
        acquisitionMissionRuntime: runtime,
      });

      assert.notEqual(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    });
  });

  describe('workspace integration', () => {
    it('success criteria — full operator directive flow via WorkspaceEngine', async () => {
      const { runtime } = seedAmoMission();
      const engine = createWorkspaceEngine({
        disableLlm: true,
        acquisitionMissionRuntime: runtime,
      });

      const opened = await engine.open({ tenantId: '10' });
      const directive =
        'For the remainder of this conversation: Operate as the business operating system. ' +
        "Don't execute anything. Explain your reasoning naturally.";

      await engine.ask({
        sessionId: opened.sessionId,
        question: directive,
      });

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(state.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(state.reasoningMode, REASONING_MODES.ANALYTICAL);
      assert.equal(state.conversationStyle, CONVERSATION_STYLES.NATURAL);

      for (let i = 0; i < 4; i += 1) {
        await engine.ask({
          sessionId: opened.sessionId,
          question: `Follow-up ${i + 1}: tell me more.`,
        });
      }

      const inspection = await engine.ask({
        sessionId: opened.sessionId,
        question: 'What operating mode are you using?',
      });

      assert.match(inspection.structured.answer, /Operating Mode: Business Operation/);
      assert.match(inspection.structured.answer, /Execution Policy: Read Only/);
      assert.match(inspection.structured.answer, /Reasoning Mode: Analytical/);
      assert.match(inspection.structured.answer, /Conversation Style: Natural/);
    });
  });
});
