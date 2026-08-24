'use strict';

/**
 * BUG-004 — Execution Inspection Ownership acceptance tests.
 *
 * Execution introspection must route to ExecutionInspectionOperator before
 * Business Intelligence, Daily Briefing, or operating-evidence retrieval.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  isExecutionInspectionQuestion,
  detectInspectionMode,
} = require('../ExecutionInspectionRegistry');
const {
  createExecutionState,
  recordExecutionPaused,
  recordStepStarted,
  recordStepCompleted,
  getExecutionState,
} = require('../ExecutionState');
const { buildExecutionPlan } = require('../ExecutionPlanner');
const { extractIntents } = require('../IntentExtractor');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { shouldRetrieveOperatingEvidence } = require('../OperatingEvidenceRetrieval');
const { maybeHandleRetrievalBeforeDelegationTurn } = require('../RetrievalBeforeDelegationContext');
const { classifyMorningBriefing } = require('../ExecutionDomain');
const { classifyCognitiveMode } = require('../../specialistDelegation/CognitiveMode');

const EXECUTION_INSPECTION_FIXTURES = [
  { question: 'Show me the execution state.', mode: 'full_state' },
  { question: 'Why did you stop?', mode: 'pause_explanation' },
  { question: 'What step are you on?', mode: 'current_activity' },
  { question: 'Show planner state.', mode: 'full_state' },
];

const BRIEFING_REGRESSION = "Show today's briefing.";

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

describe('BUG-004 — Execution Inspection Ownership', () => {
  describe('registry detection', () => {
    for (const [index, fixture] of EXECUTION_INSPECTION_FIXTURES.entries()) {
      it(`Test ${index + 1} — "${fixture.question}" routes to execution inspection`, () => {
        assert.equal(isExecutionInspectionQuestion(fixture.question), true);
        assert.equal(detectInspectionMode(fixture.question), fixture.mode);
      });
    }

    it('briefing regression — daily briefing is not execution inspection', () => {
      assert.equal(isExecutionInspectionQuestion(BRIEFING_REGRESSION), false);
      assert.ok(classifyMorningBriefing(BRIEFING_REGRESSION));
      assert.equal(shouldRetrieveOperatingEvidence(BRIEFING_REGRESSION), false);
    });
  });

  describe('precedence guards — retrieval must not claim execution inspection', () => {
    it('operating evidence retrieval defers to execution inspection', () => {
      for (const fixture of EXECUTION_INSPECTION_FIXTURES) {
        assert.equal(
          shouldRetrieveOperatingEvidence(fixture.question),
          false,
          fixture.question
        );
      }
    });

    it('retrieval-before-delegation returns null for execution inspection', async () => {
      for (const fixture of EXECUTION_INSPECTION_FIXTURES) {
        const turn = await maybeHandleRetrievalBeforeDelegationTurn({
          question: fixture.question,
          session: { context: { tenantId: '10', briefing: { headline: 'Today briefing' } } },
        });
        assert.equal(turn, null, fixture.question);
      }
    });

    it('cognitive mode does not classify execution inspection as retrieval', () => {
      for (const fixture of EXECUTION_INSPECTION_FIXTURES) {
        const mode = classifyCognitiveMode(fixture.question);
        assert.equal(mode.via, 'execution_inspection', fixture.question);
        assert.notEqual(mode.via, 'operating_evidence');
        assert.notEqual(mode.via, 'summary');
      }
    });
  });

  describe('workspace integration', () => {
    it('Show me the execution state returns Execution State — not briefing', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      await engine.ask({
        sessionId: opened.sessionId,
        question: 'Use autonomous execution.\n\nAcquire recurring client.',
      });

      const session = engine._sessions.get(opened.sessionId);
      const executionState = getExecutionState(session);
      assert.ok(executionState, 'Execution State should exist after MIEP run');

      const plan = buildExecutionPlan({
        intents: extractIntents({
          question: 'Use autonomous execution.\n\nAcquire recurring client.',
        }).intents,
      });
      let state = executionState;
      if (plan.steps.length) {
        const step = plan.steps[0];
        state = recordStepStarted(state, step, 0);
        state = recordStepCompleted(state, step, 0);
        state = recordExecutionPaused(state, {
          pauseReason: 'Mission update completed.',
          blockingContract: 'SPEC-147 Autonomous Mission Progression',
          nextStep: 'Run autonomous progression',
        });
        session.context.executionState = state;
      }

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: 'Show me the execution state.',
      });

      assert.equal(result.resolution?.action, 'execution_inspected');
      assert.match(result.prose, /Execution State|Execution Status:/i);
      assert.match(result.prose, /Pause Reason:|Current Step:|Event Log:/i);
      assert.doesNotMatch(result.prose, /You're reviewing today's briefing/i);
      assert.doesNotMatch(result.prose, /Today's briefing/i);
    });

    it('Show today\'s briefing still routes to briefing — no regression', async () => {
      const { runtime } = seedAmoMission();
      const { engine, opened } = await openEngine(runtime);

      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: BRIEFING_REGRESSION,
        context: {
          tenantId: '10',
          briefing: {
            headline: 'Pipeline steady.',
            summary: 'Two warm signals overnight.',
          },
        },
      });

      assert.notEqual(result.resolution?.action, 'execution_inspected');
      assert.match(result.prose, /Pipeline steady|briefing|warm signals/i);
    });
  });
});
