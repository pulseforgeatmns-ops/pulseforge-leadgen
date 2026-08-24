'use strict';

/**
 * SPEC-151 — Multi-Intent Execution Planner acceptance tests (ADR-072).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { EXECUTION_POLICIES, REASONING_MODES, getSessionState } = require('../SessionState');
const { extractIntents, splitMessageSegments, isCompoundMessage } = require('../IntentExtractor');
const { buildExecutionPlan } = require('../ExecutionPlanner');
const { INTENT_TYPES, STEP_KINDS } = require('../MultiIntentTypes');
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

describe('SPEC-151 — Multi-Intent Execution Planner', () => {
  describe('intent extraction and planning', () => {
    it('splits compound operator messages into independent segments', () => {
      const text =
        'Use autonomous execution.\n\nAcquire recurring client.';
      const segments = splitMessageSegments(text);
      assert.equal(segments.length, 2);

      const { intents } = extractIntents({ question: text });
      assert.equal(intents.length, 2);
      assert.equal(intents[0].type, INTENT_TYPES.SESSION_CONFIGURATION);
      assert.equal(intents[1].type, INTENT_TYPES.BUSINESS_OPERATION);
      assert.equal(isCompoundMessage(intents), true);
    });

    it('builds ordered plan: session configuration before business operation', () => {
      const text =
        'Use autonomous execution.\n\nAcquire recurring client.';
      const { intents } = extractIntents({ question: text });
      const plan = buildExecutionPlan({ intents });

      const kinds = plan.steps.map((row) => row.kind);
      const configIdx = kinds.indexOf(STEP_KINDS.APPLY_SESSION_CONFIGURATION);
      const businessIdx = kinds.indexOf(STEP_KINDS.BUSINESS_OPERATION);
      assert.ok(configIdx >= 0);
      assert.ok(businessIdx > configIdx);
    });

    it('orders inspection before mission execution when written that way', () => {
      const text =
        'Summarize current session.\n\nThen continue the acquisition mission.';
      const { intents } = extractIntents({ question: text, hasActiveMission: true });
      assert.equal(intents[0].type, INTENT_TYPES.INSPECTION);
      assert.equal(intents[1].type, INTENT_TYPES.MISSION_EXECUTION);

      const plan = buildExecutionPlan({ intents });
      const inspectionIdx = plan.steps.findIndex(
        (row) => row.kind === STEP_KINDS.SESSION_INSPECTION
      );
      const missionIdx = plan.steps.findIndex(
        (row) => row.kind === STEP_KINDS.MISSION_EXECUTION
      );
      assert.ok(inspectionIdx >= 0);
      assert.ok(missionIdx > inspectionIdx);
    });

    it('marks mission steps blocked when execution is disabled', () => {
      const text = 'Disable execution.\n\nCreate a mission.';
      const { intents } = extractIntents({ question: text });
      const plan = buildExecutionPlan({
        intents,
        sessionState: { executionPolicy: EXECUTION_POLICIES.READ_ONLY },
      });
      const missionStep = plan.steps.find(
        (row) => row.kind === STEP_KINDS.MISSION_CREATION
      );
      assert.ok(missionStep);
      assert.equal(missionStep.blocking, true);
    });
  });

  describe('acceptance tests', () => {
    it('Test 1 — session configuration then business operation', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Use autonomous execution.\n\nAcquire recurring client.',
      });

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.executionPolicy, EXECUTION_POLICIES.AUTONOMOUS);
      assert.equal(result.metadata.miep, true);
      assert.ok(result.executionPlan.steps.length >= 2);
      assert.notEqual(result.resolution?.action, 'session_configured');
    });

    it('Test 2 — session configuration then inspection reflects Teaching mode', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question:
          'Switch to Teaching mode.\n\nWhat operating mode are you using?',
      });

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.reasoningMode, REASONING_MODES.TEACHING);
      assert.equal(result.metadata.miep, true);
      assert.ok(result.prose.length > 10);
    });

    it('Test 3 — disable execution blocks mission creation with explanation', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Disable execution.\n\nCreate a mission.',
      });

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
      assert.equal(result.metadata.miep, true);
      assert.equal(result.executionPlan.blocked, true);
      assert.match(result.prose, /did not|blocked|read-only|disabled/i);
    });

    it('Test 4 — inspection then mission runtime', async () => {
      const { runtime, mission } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Summarize current session.',
        context: {
          tenantId: '10',
          missionId: mission.id,
          acquisitionMissionId: mission.id,
        },
      });

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question:
          'Summarize current session.\n\nThen continue the acquisition mission.',
        context: {
          tenantId: '10',
          missionId: mission.id,
          acquisitionMissionId: mission.id,
        },
      });

      assert.equal(result.metadata.miep, true);
      const kinds = result.executionPlan.steps.map((row) => row.kind);
      assert.ok(kinds.includes(STEP_KINDS.SESSION_INSPECTION));
      assert.ok(kinds.includes(STEP_KINDS.MISSION_EXECUTION));
    });

    it('Test 5 (regression) — compound directive continues through reasoning', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: [
          'Use autonomous execution.',
          'Explain your reasoning naturally.',
          'Acquire one recurring commercial cleaning client.',
          'Based on everything you currently know,',
          'what should happen next?',
        ].join('\n\n'),
      });

      const session = engine._sessions.get(opened.sessionId);
      const state = getSessionState(session);
      assert.equal(state.executionPolicy, EXECUTION_POLICIES.AUTONOMOUS);
      assert.equal(result.metadata.miep, true);
      assert.ok(result.prose.length > 20);
      assert.notEqual(result.resolution?.action, 'session_configured');

      const kinds = result.executionPlan.steps.map((row) => row.kind);
      assert.ok(kinds.includes(STEP_KINDS.APPLY_SESSION_CONFIGURATION));
      assert.ok(kinds.includes(STEP_KINDS.BUSINESS_REASONING));
    });
  });
});
