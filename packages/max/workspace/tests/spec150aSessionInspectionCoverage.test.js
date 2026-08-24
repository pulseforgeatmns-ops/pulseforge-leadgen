'use strict';

/**
 * SPEC-150A — Complete Session Inspection Coverage (ADR-070).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { MESSAGE_TYPES } = require('../MessageType');
const {
  classifyMessageType,
  messageTypeBypassesReasoning,
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
  resolveSessionStateField,
  SESSION_STATE_FIELDS,
  formatSessionFieldInspection,
  formatExecutionPolicyLabel,
  formatReasoningModeLabel,
} = require('../SessionStateManager');
const {
  isSessionStateExplanationQuestion,
  inspectCurrentSession,
} = require('../SessionInspectionOperator');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');

function seedAmoMission(extra = {}) {
  const amoEngine = amo.createAcquisitionMissionEngine();
  const created = amoEngine.create({
    tenantId: '10',
    objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    targetSegment: 'Law Firms',
    ...extra,
  });
  return {
    amoEngine,
    mission: created,
    runtime: createTestAmoRuntime({ engine: amoEngine }),
    session: {
      id: 's150a',
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

describe('SPEC-150A — Complete Session Inspection Coverage', () => {
  describe('field registry', () => {
    it('registers every inspectable Session State field', () => {
      const keys = SESSION_STATE_FIELDS.map((field) => field.key);
      assert.deepEqual(keys, [
        'executionPolicy',
        'reasoningMode',
        'conversationStyle',
        'evaluationMode',
        'operatingMode',
        'summary',
      ]);
    });

    it('resolves alias phrases to the correct field', () => {
      const cases = [
        ['What operating mode are you using?', 'operatingMode'],
        ['What mode are you currently in?', 'operatingMode'],
        ['How are you operating right now?', 'operatingMode'],
        ['What execution policy are you following?', 'executionPolicy'],
        ['What execution policy is active?', 'executionPolicy'],
        ['Are you allowed to execute?', 'executionPolicy'],
        ['What reasoning mode are you using?', 'reasoningMode'],
        ['What reasoning mode is active?', 'reasoningMode'],
        ['What thinking mode is active?', 'reasoningMode'],
        ['What conversation style is active?', 'conversationStyle'],
        ['What response style is active?', 'conversationStyle'],
        ['What evaluation mode is active?', 'evaluationMode'],
        ['What are we evaluating?', 'evaluationMode'],
        ['Summarize your current session.', 'summary'],
        ['Summarize the current session.', 'summary'],
        ['How are you configured?', 'summary'],
      ];

      for (const [question, expectedKey] of cases) {
        const field = resolveSessionStateField(question);
        assert.ok(field, question);
        assert.equal(field.key, expectedKey, question);
        assert.equal(isSessionInspectionQuestion(question), true, question);
        assert.equal(classifyMessageType(question).type, MESSAGE_TYPES.SESSION_INSPECTION, question);
      }
    });

    it('does not classify configuration or why questions as inspection', () => {
      const samples = [
        'Operate according to your role.',
        "Don't execute anything.",
        'Explain your reasoning naturally.',
        'Why are you using that reasoning mode?',
      ];
      for (const text of samples) {
        assert.equal(isSessionInspectionQuestion(text), false, text);
      }
      assert.equal(isSessionStateExplanationQuestion('Why are you using that reasoning mode?'), true);
    });
  });

  describe('response contract', () => {
    it('returns only the requested field for single-field inspection', () => {
      const state = createDefaultSessionState();
      state.reasoningMode = REASONING_MODES.ANALYTICAL;
      state.executionPolicy = EXECUTION_POLICIES.READ_ONLY;

      const reasoning = formatSessionFieldInspection(
        state,
        resolveSessionStateField('What reasoning mode are you using?')
      );
      assert.equal(
        reasoning,
        ['Current Session', '', 'Reasoning Mode', '', 'Analytical'].join('\n')
      );

      const execution = formatSessionFieldInspection(
        state,
        resolveSessionStateField('What execution policy are you following?')
      );
      assert.equal(
        execution,
        ['Current Session', '', 'Execution Policy', '', 'Read Only'].join('\n')
      );
      assert.doesNotMatch(reasoning, /\nExecution Policy\n/);
      assert.doesNotMatch(execution, /\nReasoning Mode\n/);
    });

    it('inspectCurrentSession uses the operator question for field dispatch', () => {
      const session = { id: 'field-dispatch', context: {} };
      resolveSessionState({ question: "Don't execute anything.", session });
      const turn = inspectCurrentSession({
        session,
        question: 'What execution policy are you following?',
      });
      assert.match(turn.prose, /Execution Policy\s+Read Only/);
      assert.doesNotMatch(turn.prose, /\nReasoning Mode\n/);
      assert.doesNotMatch(turn.prose, /\nOperating Mode\n/);
    });
  });

  describe('acceptance tests', () => {
    const ACQUISITION_RE = /repeatable commercial acquisition|commercial acquisition recommendation/i;

    async function inspectAfterConfig(configQuestion, inspectQuestion) {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);
      if (configQuestion) {
        await engine.ask({ sessionId: opened.sessionId, question: configQuestion });
      }
      return engine.ask({ sessionId: opened.sessionId, question: inspectQuestion });
    }

    it('Test 1 — operating mode inspection', async () => {
      const result = await inspectAfterConfig(
        'Operate according to your role.',
        'What operating mode are you using?'
      );
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Operating Mode\s+Business Operation/);
      assert.doesNotMatch(result.prose, /\nExecution Policy\n/);
      assert.ok(messageTypeBypassesReasoning(result.messageClassification.type));
    });

    it('Test 2 — execution policy inspection', async () => {
      const result = await inspectAfterConfig(
        "Don't execute anything.",
        'What execution policy are you following?'
      );
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Execution Policy\s+Read Only/);
      assert.doesNotMatch(result.prose, /\nOperating Mode\n/);
    });

    it('Test 3 — reasoning mode inspection', async () => {
      const result = await inspectAfterConfig(
        'Explain your reasoning naturally.',
        'What reasoning mode are you using?'
      );
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      const expected = formatReasoningModeLabel(result.sessionState.reasoningMode);
      assert.match(result.prose, new RegExp(`Reasoning Mode\\s+${expected}`));
      assert.doesNotMatch(result.prose, /\nConversation Style\n/);
      assert.doesNotMatch(result.prose, /\nExecution Policy\n/);
    });

    it('Test 4 — conversation style inspection', async () => {
      const result = await inspectAfterConfig(
        'Explain your reasoning naturally.',
        'What conversation style is active?'
      );
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Conversation Style\s+Natural/);
      assert.doesNotMatch(result.prose, /\nReasoning Mode\n/);
    });

    it('Test 5 — evaluation mode inspection', async () => {
      const result = await inspectAfterConfig(
        'Evaluate how you operate.',
        'What evaluation mode is active?'
      );
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Evaluation Mode\s+Max Operating Model/);
      assert.doesNotMatch(result.prose, /\nOperating Mode\n/);
    });

    it('Test 6 — summarize current session returns complete Session State', async () => {
      const result = await inspectAfterConfig(
        'Operate according to your role.',
        'Summarize your current session.'
      );
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_INSPECTION);
      assert.match(result.prose, /Current Session/);
      assert.match(result.prose, /Operating Mode/);
      assert.match(result.prose, /Execution Policy/);
      assert.match(result.prose, /Reasoning Mode/);
      assert.match(result.prose, /Conversation Style/);
      assert.match(result.prose, /Evaluation Mode/);
    });

    it('Test 7 — why follow-up uses reasoning, not inspection', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);
      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Explain your reasoning naturally.',
      });
      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Why are you using that reasoning mode?',
      });
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.QUESTION);
      assert.equal(isSessionInspectionQuestion('Why are you using that reasoning mode?'), false);
      assert.match(result.prose, /Session State/i);
      assert.doesNotMatch(result.prose, ACQUISITION_RE);
      assert.doesNotMatch(result.prose, /business advisory/i);
    });

    it('inspection never invokes business advisory or mission ownership', async () => {
      const result = await inspectAfterConfig(null, 'What execution policy are you following?');
      assert.equal(result.mission, null);
      assert.equal(result.workspaceOwnership.owner, 'session_state_manager');
      assert.doesNotMatch(result.prose, ACQUISITION_RE);
      assert.doesNotMatch(result.prose, /business advisory/i);
      assert.equal(result.structured.metadata.sessionInspection, true);
      assert.equal(result.structured.metadata.businessIntelligenceUsed, false);
    });
  });
});
