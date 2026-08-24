'use strict';

/**
 * SPEC-150 — Session State Inspection acceptance tests (ADR-070).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { MESSAGE_TYPES } = require('../MessageType');
const {
  classifyMessageType,
  messageTypeBypassesReasoning,
  messageTypeBypassesOwnership,
} = require('../MessageTypeClassifier');
const {
  OPERATING_MODES,
  EXECUTION_POLICIES,
  REASONING_MODES,
  CONVERSATION_STYLES,
  EVALUATION_MODES,
  getSessionState,
  setSessionState,
  createDefaultSessionState,
} = require('../SessionState');
const {
  resolveSessionState,
  getCurrentState,
  isSessionInspectionQuestion,
  formatOperatingModeLabel,
  formatExecutionPolicyLabel,
  formatReasoningModeLabel,
} = require('../SessionStateManager');
const {
  isSessionStateExplanationQuestion,
  inspectCurrentSession,
} = require('../SessionInspectionOperator');
const { THINKING_MODES } = require('../../operatorCognition');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';
const ACQUISITION_RE = /repeatable commercial acquisition|commercial acquisition recommendation/i;

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
      id: 's150inspect',
      context: { tenantId: '10', missionId: created.id, acquisitionMissionId: created.id },
    },
  };
}

async function openEngine(runtime) {
  const engine = createWorkspaceEngine({
    disableLlm: true,
    acquisitionMissionRuntime: runtime,
  });
  const opened = await engine.open({ tenantId: '10' });
  return { engine, opened };
}

describe('SPEC-150 — Session State Inspection', () => {
  describe('classifier and read API', () => {
    it('classifies inspection questions as SESSION_INSPECTION', () => {
      const samples = [
        'What operating mode are you using?',
        'What operating mode are you currently using?',
        'What mode are you currently in?',
        'What are your current session settings?',
        'How are you operating right now?',
        'What execution policy are you following?',
        'What execution policy is active?',
        'What conversation style is active?',
        'What reasoning mode is active?',
        'Summarize the current session.',
      ];
      for (const text of samples) {
        assert.equal(isSessionInspectionQuestion(text), true, text);
        const result = classifyMessageType(text);
        assert.equal(result.type, MESSAGE_TYPES.SESSION_INSPECTION, text);
        assert.equal(result.mutatesSession, false, text);
        assert.equal(result.mutatesMission, false, text);
        assert.ok(messageTypeBypassesReasoning(result.type), text);
        assert.ok(messageTypeBypassesOwnership(result.type), text);
      }
    });

    it('classifies why-about-session as QUESTION, not inspection', () => {
      const text = 'Why are you using that operating mode?';
      assert.equal(isSessionInspectionQuestion(text), false);
      assert.equal(isSessionStateExplanationQuestion(text), true);
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.QUESTION);
      assert.equal(messageTypeBypassesReasoning(result.type), false);
    });

    it('getCurrentState returns stored fields and never infers from mission context', () => {
      const stored = createDefaultSessionState();
      stored.operatingMode = OPERATING_MODES.BUSINESS_OPERATION;
      stored.executionPolicy = EXECUTION_POLICIES.READ_ONLY;
      stored.reasoningMode = REASONING_MODES.ANALYTICAL;
      stored.conversationStyle = CONVERSATION_STYLES.NATURAL;
      stored.evaluationMode = EVALUATION_MODES.MAX;

      const session = {
        context: {
          missionId: 'mission-inferred',
          acquisitionMissionId: 'mission-inferred',
          executionDomain: 'mission',
        },
      };
      setSessionState(session, stored);

      const current = getCurrentState(session);
      assert.equal(current.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(current.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(current.reasoningMode, REASONING_MODES.ANALYTICAL);
      assert.equal(current.conversationStyle, CONVERSATION_STYLES.NATURAL);
      assert.equal(current.evaluationMode, EVALUATION_MODES.MAX);
      assert.notEqual(current.operatingMode, OPERATING_MODES.MISSION_EXECUTION);
      assert.ok('sessionStarted' in current);
      assert.ok('lastUpdated' in current);
      assert.ok('activeObjective' in current);
    });
  });

  describe('acceptance tests', () => {
    it('Test 1 — operate according to role then inspect stored operating mode', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Operate according to your role.',
      });

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'What operating mode are you using?',
      });

      const session = engine._sessions.get(opened.sessionId);
      const stored = getSessionState(session);
      assert.equal(stored.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Current Session/);
      assert.match(result.prose, /Operating Mode\s+Business Operation/);
      assert.equal(result.workspaceOwnership.owner, 'session_state_manager');
      assert.equal(result.routingTrace.pipeline, 'SessionStateManager');
      assert.equal(result.mission, null);
    });

    it('Test 2 — execution policy inspection reads stored Read Only', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: "Don't execute anything.",
      });

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'What execution policy is active?',
      });

      const session = engine._sessions.get(opened.sessionId);
      assert.equal(getSessionState(session).executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Execution Policy\s+Read Only/);
    });

    it('Test 3 — reasoning mode inspection reads stored state', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Explain your reasoning naturally.',
      });

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'What reasoning mode is active?',
      });

      const session = engine._sessions.get(opened.sessionId);
      const stored = getSessionState(session);
      const expected = formatReasoningModeLabel(stored.reasoningMode);
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, new RegExp(`Reasoning Mode\\s+${expected}`));
      assert.match(result.prose, /Conversation Style\s+Natural/);
    });

    it('Test 4 — summarize the current session returns complete Session State', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Operate according to your role.',
      });

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Summarize the current session.',
      });

      const session = engine._sessions.get(opened.sessionId);
      const stored = getCurrentState(session);
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Current Session/);
      assert.match(
        result.prose,
        new RegExp(`Operating Mode\\s+${formatOperatingModeLabel(stored.operatingMode)}`)
      );
      assert.match(
        result.prose,
        new RegExp(`Execution Policy\\s+${formatExecutionPolicyLabel(stored.executionPolicy)}`)
      );
      assert.match(result.prose, /Reasoning Mode/);
      assert.match(result.prose, /Conversation Style/);
      assert.match(result.prose, /Evaluation Mode/);
    });

    it('Test 5 — inspection does not create a mission, invoke Scout, or advise acquisition', async () => {
      const { runtime, amoEngine } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);
      const beforeCount = amoEngine.list('10').length;

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'What operating mode are you currently using?',
      });

      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.equal(result.mission, null);
      assert.equal(result.workspaceOwnership.owner, 'session_state_manager');
      assert.equal(result.routingTrace.pipeline, 'SessionStateManager');
      assert.doesNotMatch(result.prose, ACQUISITION_RE);
      assert.doesNotMatch(result.prose, /business advisory/i);
      assert.doesNotMatch(result.prose, /\bScout\b/);
      assert.equal(result.structured.metadata.sessionInspection, true);
      assert.equal(result.structured.metadata.businessIntelligenceUsed, false);
      assert.equal(amoEngine.list('10').length, beforeCount);
    });

    it('Test 6 — why follow-up reasons over Session State and does not infer mode', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Operate according to your role.',
      });

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Why are you using that operating mode?',
      });

      const session = engine._sessions.get(opened.sessionId);
      const stored = getSessionState(session);
      assert.equal(stored.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.QUESTION);
      assert.equal(isSessionStateExplanationQuestion('Why are you using that operating mode?'), true);
      assert.ok(result.conversationIntent);
      assert.equal(result.conversationIntent.intent, THINKING_MODES.EXPLAIN);
      assert.match(result.prose, /Session State/i);
      assert.match(result.prose, /Business Operation/);
      assert.doesNotMatch(result.prose, /business advisory/i);
      assert.doesNotMatch(result.prose, ACQUISITION_RE);
      assert.equal(result.structured.metadata.sessionStateEvidence, true);
      assert.equal(result.structured.metadata.businessIntelligenceUsed, false);
      assert.ok(result.structured.reasoning.some((line) => /session state/i.test(line)));
      assert.ok(
        result.structured.supportingEvidence.some((ref) => ref.sourceType === 'session_state')
      );
    });
  });

  describe('inspection operator', () => {
    it('inspectCurrentSession formats stored state without reconstructing it', () => {
      const session = { id: 'inspect-unit', context: {} };
      resolveSessionState({
        question: "Don't execute anything.",
        session,
      });
      const turn = inspectCurrentSession({ session });
      assert.equal(turn.reason, 'session_inspection');
      assert.match(turn.prose, /Execution Policy\s+Read Only/);
      assert.equal(turn.structured.metadata.sessionInspection, true);
    });
  });
});
