'use strict';

/**
 * SPEC-149 — Message Type Classification acceptance tests (ADR-069).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { MESSAGE_TYPES } = require('../MessageType');
const {
  classifyMessageType,
  resolveMessageType,
  isSessionConfigurationMessage,
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
} = require('../SessionState');
const { resolveSessionState } = require('../SessionStateManager');
const { classifyOperatorCognition, THINKING_MODES } = require('../../operatorCognition');
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
      id: 's149mtc',
      context: { tenantId: '10', missionId: created.id, acquisitionMissionId: created.id },
    },
  };
}

describe('SPEC-149 — Message Type Classification', () => {
  describe('classifier unit tests', () => {
    it('Test 1 — persistent directive classifies as SESSION_CONFIGURATION', () => {
      const text =
        'For the remainder of this conversation: Don\'t execute anything.';
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.equal(result.mutatesSession, true);
      assert.equal(result.mutatesMission, false);
      assert.ok(result.confidence >= 0.88);
      assert.ok(messageTypeBypassesReasoning(result.type));
      assert.ok(messageTypeBypassesOwnership(result.type));
    });

    it('Test 2 — Why? classifies as QUESTION', () => {
      const result = classifyMessageType('Why?');
      assert.equal(result.type, MESSAGE_TYPES.QUESTION);
      assert.equal(result.mutatesSession, false);
      assert.equal(classifyOperatorCognition('Why?').intent, THINKING_MODES.EXPLAIN);
    });

    it('Test 3 — Create a mission classifies as MISSION_CREATION', () => {
      const result = classifyMessageType('Create a mission.');
      assert.equal(result.type, MESSAGE_TYPES.MISSION_CREATION);
      assert.equal(result.mutatesMission, true);
    });

    it('Test 4 — Approved classifies as APPROVAL', () => {
      const result = classifyMessageType('Approved.');
      assert.equal(result.type, MESSAGE_TYPES.APPROVAL);
      assert.equal(result.mutatesMission, true);
    });

    it('Test 5 — misunderstanding classifies as CORRECTION', () => {
      const result = classifyMessageType("You're misunderstanding me.");
      assert.equal(result.type, MESSAGE_TYPES.CORRECTION);
    });

    it('Test 6 — for today\'s session evaluation classifies as SESSION_CONFIGURATION', () => {
      const text =
        "For today's session, evaluate your reasoning instead of executing tasks.";
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.equal(result.mutatesSession, true);
    });

    it('Run Scout classifies as COMMAND', () => {
      const result = classifyMessageType('Run Scout.');
      assert.equal(result.type, MESSAGE_TYPES.COMMAND);
      assert.equal(result.mutatesMission, true);
    });

    it('session configuration is detected before reflection phrases', () => {
      const text =
        'For the remainder of this conversation: Explain your reasoning naturally.';
      assert.equal(isSessionConfigurationMessage(text), true);
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.notEqual(result.type, MESSAGE_TYPES.QUESTION);
    });
  });

  describe('acceptance tests — workspace integration', () => {
    it('success criteria — full operator directive acknowledges without reasoning', async () => {
      const { runtime } = seedAmoMission();
      const engine = createWorkspaceEngine({
        disableLlm: true,
        acquisitionMissionRuntime: runtime,
      });

      const opened = await engine.open({ tenantId: '10' });
      const directive =
        'I want to evaluate how you operate.\n' +
        'Treat Anchor Cleaning as a production business.\n' +
        "Don't execute anything.\n" +
        'Explain your reasoning naturally.\n' +
        'For the remainder of this conversation...';

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: directive,
      });

      assert.equal(result.messageClassification.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.match(result.prose, /^Acknowledged\./);
      assert.match(result.prose, /Operating Mode/);
      assert.match(result.prose, /Execution Policy\s+Read Only/);
      assert.doesNotMatch(result.prose, /misunderstand/i);
      assert.doesNotMatch(result.prose, /Anchor Cleaning customers/i);

      assert.equal(result.routingTrace.messageType, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.equal(result.routingTrace.pipeline, 'SessionStateManager');
      assert.equal(result.conversationIntent, null);

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.operatingMode, OPERATING_MODES.BUSINESS_OPERATION);
      assert.equal(state.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(state.reasoningMode, REASONING_MODES.ANALYTICAL);
      assert.equal(state.conversationStyle, CONVERSATION_STYLES.NATURAL);
    });

    it('SESSION_CONFIGURATION turn skips reasoning; follow-up QUESTION uses pipeline', async () => {
      const { runtime } = seedAmoMission();
      const engine = createWorkspaceEngine({
        disableLlm: true,
        acquisitionMissionRuntime: runtime,
      });

      const opened = await engine.open({ tenantId: '10' });

      await engine.ask({
        sessionId: opened.sessionId,
        question:
          "For the rest of this conversation: Don't execute anything.",
      });

      const followUp = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Why?',
      });

      assert.equal(followUp.messageClassification.type, MESSAGE_TYPES.QUESTION);
      assert.notEqual(followUp.prose, 'Acknowledged.');
      assert.ok(followUp.routingTrace);
      assert.equal(followUp.routingTrace.messageType, MESSAGE_TYPES.QUESTION);
      assert.equal(followUp.routingTrace.intent, THINKING_MODES.EXPLAIN);
    });

    it('session state persists after configuration acknowledgement', async () => {
      const { runtime } = seedAmoMission();
      const engine = createWorkspaceEngine({
        disableLlm: true,
        acquisitionMissionRuntime: runtime,
      });

      const opened = await engine.open({ tenantId: '10' });

      await engine.ask({
        sessionId: opened.sessionId,
        question:
          'For the remainder of this conversation: Operate as the business operating system. ' +
          "Don't execute anything.",
      });

      for (let i = 0; i < 3; i += 1) {
        await engine.ask({
          sessionId: opened.sessionId,
          question: `Follow-up ${i + 1}: what is the mission objective?`,
        });
      }

      const session = engine._sessions.get(opened.sessionId);
      assert.equal(getSessionState(session).executionPolicy, EXECUTION_POLICIES.READ_ONLY);
    });
  });

  describe('runtime guarantees', () => {
    it('every message has exactly one primary message type', () => {
      const samples = [
        'Why?',
        'Run Scout.',
        'Approved.',
        "Don't execute anything.",
        'Create a mission.',
        "You're misunderstanding me.",
        'We signed a new client.',
      ];
      for (const text of samples) {
        const { classification } = resolveMessageType({ question: text });
        assert.ok(classification.type);
        assert.ok(typeof classification.confidence === 'number');
        assert.ok(Array.isArray(classification.evidence));
      }
    });

    it('session mutations occur via Session State Manager before downstream pipelines', () => {
      const session = { id: 'hist', context: {} };
      const question =
        'For the remainder of this conversation: Don\'t execute anything.';
      const { classification } = resolveMessageType({ question, session });
      assert.equal(classification.mutatesSession, true);

      const resolution = resolveSessionState({ question, session });
      assert.equal(resolution.state.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(resolution.changed, true);
    });
  });
});
