'use strict';

/**
 * SPEC-152 — Execution State & Planner Introspection acceptance tests (ADR-073).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { EXECUTION_POLICIES, getSessionState } = require('../SessionState');
const { STEP_KINDS } = require('../MultiIntentTypes');
const {
  createExecutionState,
  recordStepStarted,
  recordStepCompleted,
  recordExecutionPaused,
  EXECUTION_STATUSES,
  getExecutionState,
} = require('../ExecutionState');
const {
  isExecutionInspectionQuestion,
  inspectExecutionState,
  formatPauseExplanation,
} = require('../ExecutionInspectionOperator');
const { buildExecutionPlan } = require('../ExecutionPlanner');
const { extractIntents } = require('../IntentExtractor');
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

describe('SPEC-152 — Execution State & Planner Introspection', () => {
  describe('Execution State model', () => {
    it('projects state from event log', () => {
      const plan = buildExecutionPlan({
        intents: extractIntents({
          question: 'Use autonomous execution.\n\nAcquire recurring client.',
        }).intents,
      });
      let state = createExecutionState({ plan });
      assert.equal(state.status, EXECUTION_STATUSES.RUNNING);

      const step = plan.steps[0];
      state = recordStepStarted(state, step, 0);
      state = recordStepCompleted(state, step, 0);
      assert.equal(state.completedSteps.length, 1);
      assert.ok(state.events.length >= 3);
      assert.ok(state.eventLog || formatPauseExplanation(state));
    });

    it('records pause with blocking contract', () => {
      const plan = buildExecutionPlan({
        intents: extractIntents({ question: 'Acquire recurring client.' }).intents,
      });
      let state = createExecutionState({ plan });
      state = recordExecutionPaused(state, {
        pauseReason: 'Discovery Review requires operator approval.',
        blockingContract: 'SPEC-147 Discovery Review',
        nextStep: 'Approve findings',
      });
      assert.equal(state.status, EXECUTION_STATUSES.PAUSED);
      assert.equal(state.pauseReason, 'Discovery Review requires operator approval.');
      assert.equal(state.blockingContract, 'SPEC-147 Discovery Review');
      assert.equal(state.nextStep, 'Approve findings');
    });
  });

  describe('execution inspection operator', () => {
    it('detects execution inspection questions', () => {
      assert.equal(isExecutionInspectionQuestion('What are you doing?'), true);
      assert.equal(isExecutionInspectionQuestion('Why did you stop?'), true);
      assert.equal(isExecutionInspectionQuestion("What's next?"), true);
      assert.equal(isExecutionInspectionQuestion('Show me your execution state.'), true);
      assert.equal(isExecutionInspectionQuestion('Why didn\'t you continue autonomous execution?'), true);
    });

    it('Test 1 — What are you doing? returns current step, status, next step', () => {
      const plan = buildExecutionPlan({
        intents: extractIntents({
          question: 'Use autonomous execution.\n\nAcquire recurring client.',
        }).intents,
      });
      let state = createExecutionState({ plan });
      const businessStep = plan.steps.find((row) => row.kind === STEP_KINDS.BUSINESS_OPERATION);
      const businessIdx = plan.steps.indexOf(businessStep);
      plan.steps.forEach((step, idx) => {
        if (idx <= businessIdx) {
          state = recordStepStarted(state, step, idx);
          state = recordStepCompleted(state, step, idx);
        }
      });
      state = recordExecutionPaused(state, {
        pauseReason: 'Mission update completed.',
        blockingContract: 'SPEC-147 Autonomous Mission Progression',
        nextStep: 'Run autonomous progression',
      });

      const result = inspectExecutionState({
        question: 'What are you doing?',
        executionState: state,
      });
      assert.match(result.prose, /Execution Status:/i);
      assert.match(result.prose, /Current Step:/i);
      assert.match(result.prose, /Next Step:/i);
    });

    it('Test 2 — Why did you stop? returns pause reason, blocking contract, next step', () => {
      const plan = buildExecutionPlan({
        intents: extractIntents({ question: 'Acquire recurring client.' }).intents,
      });
      let state = createExecutionState({ plan });
      state = recordExecutionPaused(state, {
        pauseReason: 'Discovery Review requires operator approval.',
        blockingContract: 'SPEC-147 Discovery Review',
        nextStep: 'Approve findings',
      });

      const result = inspectExecutionState({
        question: 'Why did you stop?',
        executionState: state,
      });
      assert.match(result.prose, /Pause Reason:/i);
      assert.match(result.prose, /Discovery Review requires operator approval/i);
      assert.match(result.prose, /Blocking Contract:/i);
      assert.match(result.prose, /SPEC-147 Discovery Review/i);
      assert.match(result.prose, /Next Step:/i);
      assert.match(result.prose, /Approve findings/i);
    });

    it('Test 3 — What\'s next? returns stored next step', () => {
      const state = recordExecutionPaused(createExecutionState({ plan: { steps: [] } }), {
        nextStep: 'Approve findings',
      });
      const result = inspectExecutionState({
        question: "What's next?",
        executionState: state,
      });
      assert.match(result.prose, /Next Step: Approve findings/);
    });

    it('Test 4 — Show me your execution state returns complete state', () => {
      const plan = buildExecutionPlan({
        intents: extractIntents({
          question: 'Use autonomous execution.\n\nAcquire recurring client.',
        }).intents,
      });
      let state = createExecutionState({ plan });
      state = recordExecutionPaused(state, {
        pauseReason: 'Mission update completed.',
        blockingContract: 'SPEC-147',
        nextStep: 'Continue',
      });

      const result = inspectExecutionState({
        question: 'Show me your execution state.',
        executionState: state,
      });
      assert.match(result.prose, /Execution State/);
      assert.match(result.prose, /Status:/);
      assert.match(result.prose, /Event Log:/);
      assert.match(result.prose, /Pause Reason:/);
    });

    it('Test 5 — Why didn\'t you continue autonomous execution reads Execution State', () => {
      let state = createExecutionState({ plan: { steps: [] } });
      state = recordExecutionPaused(state, {
        pauseReason:
          'Mission update completed. Autonomous progression has not continued because the business operation step finished without issuing an autonomous progression command.',
        blockingContract: 'SPEC-147 Autonomous Mission Progression',
        nextStep: 'Run autonomous progression or approve the next mission stage.',
      });

      const result = inspectExecutionState({
        question: "Why didn't you continue autonomous execution?",
        executionState: state,
      });
      assert.match(result.prose, /Autonomous progression has not continued/i);
      assert.match(result.prose, /Blocking Contract:/i);
      assert.notEqual(result.prose.trim(), 'Acknowledged.');
    });
  });

  describe('acceptance tests — workspace integration', () => {
    it('Test 6 (regression) — after mission update, Why did you stop? never returns Acknowledged.', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Use autonomous execution.\n\nAcquire recurring client.',
      });

      const session = engine._sessions.get(opened.sessionId);
      const executionState = getExecutionState(session);
      assert.ok(executionState, 'Execution State should be persisted after MIEP run');
      assert.ok(
        executionState.completedSteps.some(
          (row) => row.kind === STEP_KINDS.BUSINESS_OPERATION || row.kind === STEP_KINDS.MISSION_CREATION
        ),
        'Mission-related step should be completed'
      );

      const followUp = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Why did you stop?',
      });

      assert.notEqual(followUp.prose.trim(), 'Acknowledged.');
      assert.match(followUp.prose, /Pause Reason:|Execution Status:/i);
      assert.equal(followUp.resolution?.action, 'execution_inspected');
      assert.ok(
        followUp.executionState ||
          followUp.metadata?.executionState ||
          followUp.structured?.metadata?.executionStateRead
      );
    });

    it('persists execution state on session after compound turn', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Use autonomous execution.\n\nAcquire recurring client.',
      });

      assert.equal(result.metadata.miep, true);
      assert.ok(result.executionState);
      assert.ok(result.executionState.executionId);

      const session = engine._sessions.get(opened.sessionId);
      const stored = getExecutionState(session);
      assert.ok(stored);
      assert.equal(stored.executionId, result.executionState.executionId);
    });

    it('records pause when autonomous policy set but progression does not continue', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Use autonomous execution.\n\nAcquire recurring client.',
      });

      const session = engine._sessions.get(opened.sessionId);
      const state = getExecutionState(session);
      assert.ok(state);
      assert.equal(getSessionState(session).executionPolicy, EXECUTION_POLICIES.AUTONOMOUS);

      if (state.status === EXECUTION_STATUSES.PAUSED) {
        assert.ok(state.pauseReason);
        assert.ok(state.blockingContract);
      } else {
        assert.equal(state.status, EXECUTION_STATUSES.COMPLETED);
      }
    });
  });
});
