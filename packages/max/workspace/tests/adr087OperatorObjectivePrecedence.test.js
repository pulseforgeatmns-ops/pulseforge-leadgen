'use strict';

/**
 * ADR-087 — Operator Objective Takes Precedence acceptance tests (AUDIT-046).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { MESSAGE_TYPES } = require('../MessageType');
const {
  classifyMessageType,
  detectPrimaryObjective,
  messageTypeBypassesReasoning,
  messageTypeBypassesOwnership,
} = require('../MessageTypeClassifier');
const { extractIntents, isCompoundMessage } = require('../IntentExtractor');
const {
  EXECUTION_POLICIES,
  REASONING_MODES,
  CONVERSATION_STYLES,
  getSessionState,
} = require('../SessionState');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const AUDIT_046_PROMPT = [
  'Create a production acquisition mission.',
  'Execute autonomously.',
  'Explain your reasoning naturally.',
].join('\n');

describe('ADR-087 — Operator Objective Takes Precedence', () => {
  describe('AUDIT-046 regression', () => {
    it('compound executive message routes to mission_creation, not session_configuration', () => {
      const result = classifyMessageType(AUDIT_046_PROMPT);
      assert.equal(result.type, MESSAGE_TYPES.MISSION_CREATION);
      assert.notEqual(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.equal(result.mutatesMission, true);
      assert.equal(result.mutatesSession, true);
      assert.ok(result.evidence.includes('session_modifiers_present'));
      assert.ok(result.evidence.includes('mission_creation'));
    });

    it('detectPrimaryObjective resolves mission creation from first segment', () => {
      const objective = detectPrimaryObjective(AUDIT_046_PROMPT);
      assert.ok(objective);
      assert.equal(objective.type, MESSAGE_TYPES.MISSION_CREATION);
    });

    it('Create a production acquisition mission matches mission creation pattern', () => {
      const result = classifyMessageType('Create a production acquisition mission.');
      assert.equal(result.type, MESSAGE_TYPES.MISSION_CREATION);
    });

    it('intent extraction finds mission_creation plus session modifiers', () => {
      const { intents } = extractIntents({ question: AUDIT_046_PROMPT });
      const types = intents.map((row) => row.type);
      assert.ok(types.includes('mission_creation'));
      assert.ok(types.includes('session_configuration'));
      assert.equal(isCompoundMessage(intents), true);
    });

    it('modifiers do not bypass ownership or reasoning when objective leads', () => {
      const result = classifyMessageType(AUDIT_046_PROMPT);
      assert.equal(messageTypeBypassesReasoning(result.type), false);
      assert.equal(messageTypeBypassesOwnership(result.type), false);
    });
  });

  describe('modifier-only messages remain session_configuration', () => {
    it('pure session directive still routes to session_configuration', () => {
      const text =
        'For the remainder of this conversation: Explain your reasoning naturally.';
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
      assert.ok(messageTypeBypassesReasoning(result.type));
    });

    it('execution and conversation modifiers without objective stay session_configuration', () => {
      const text = 'Execute autonomously.\nExplain your reasoning naturally.';
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
    });
  });

  describe('objective-first when modifiers precede objective', () => {
    it('mission creation after modifiers still routes to mission_creation', () => {
      const text = [
        'Execute autonomously.',
        'Explain your reasoning naturally.',
        'Create a production acquisition mission.',
      ].join('\n');
      const result = classifyMessageType(text);
      assert.equal(result.type, MESSAGE_TYPES.MISSION_CREATION);
      assert.ok(result.evidence.includes('session_modifiers_present'));
    });
  });

  describe('workspace integration', () => {
    it('compound AUDIT-046 prompt executes via MIEP, not session-only acknowledgement', async () => {
      const amoEngine = amo.createAcquisitionMissionEngine();
      const runtime = createTestAmoRuntime({ engine: amoEngine });
      const engine = createWorkspaceEngine({
        disableLlm: true,
        acquisitionMissionRuntime: runtime,
      });

      const opened = await engine.open({ tenantId: '10' });
      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: AUDIT_046_PROMPT,
      });

      assert.equal(result.messageClassification.type, MESSAGE_TYPES.MISSION_CREATION);
      assert.notEqual(result.resolution?.action, 'session_configured');
      assert.equal(result.metadata.miep, true);

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.executionPolicy, EXECUTION_POLICIES.AUTONOMOUS);
      assert.equal(state.conversationStyle, CONVERSATION_STYLES.NATURAL);
      assert.equal(state.reasoningMode, REASONING_MODES.ANALYTICAL);
    });
  });
});
