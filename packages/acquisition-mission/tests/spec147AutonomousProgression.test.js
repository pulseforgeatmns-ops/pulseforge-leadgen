'use strict';

/**
 * SPEC-147 — Autonomous Mission Progression (ADR-066).
 * Acceptance tests for automatic stage execution until operator judgment is required.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  OPERATOR_DECISION_KINDS,
  PROGRESSION_STAGES,
  runAutonomousProgression,
  deriveProgressionStage,
  deriveMissionPause,
  deriveExecutionBlock,
  isAutonomousProgressionCommand,
} = amo;
const { maybeHandleAcquisitionMissionExecution } = require('../../max/workspace/AcquisitionMissionExecution');
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-147 — Autonomous Mission Progression', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  it('Test 1: unambiguous mission automatically enters Discovery', async () => {
    assert.equal(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);

    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(result.outcome, 'paused');
    assert.equal(result.progressionStage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.ok(result.transitions.length >= 2);
    assert.equal(result.transitions[0].from, PROGRESSION_STAGES.MISSION_PLANNING);
    assert.equal(result.transitions[0].to, PROGRESSION_STAGES.DISCOVERY_RUNNING);
    assert.equal(result.transitions[0].automatic, true);
    assert.equal(result.transitions[1].from, PROGRESSION_STAGES.DISCOVERY_RUNNING);
    assert.equal(result.transitions[1].to, PROGRESSION_STAGES.DISCOVERY_REVIEW);

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.DISCOVER);
    assert.equal(snapshot.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
    assert.match(result.presentation, /Beginning Scout Investigation|Mission Intelligence Report Ready/i);
  });

  it('Test 2: Scout completion automatically enters Discovery Review', async () => {
    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(deriveProgressionStage(result.snapshot), PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.equal(result.pause.stage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.match(result.pause.reason, /Scout investigation completed/i);
    assert.equal(result.snapshot.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
  });

  it('Test 3: Discovery Review pauses with explicit decision request', async () => {
    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(result.outcome, 'paused');
    assert.ok(result.pause);
    assert.equal(result.pause.stage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.match(result.pause.requiredDecision, /priorit/i);
    assert.ok(result.pause.availableOptions.includes('Approve findings'));
    assert.match(result.presentation, /Mission Paused|Mission Intelligence Report Ready/i);
    assert.match(result.presentation, /Operator judgment required|Decision Needed/i);
  });

  it('Test 4: blocked transition surfaces blocking component and precondition', async () => {
    const blockedEngine = amo.createAcquisitionMissionEngine();
    const blockedMission = blockedEngine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const result = await runAutonomousProgression({
      engine: blockedEngine,
      missionId: blockedMission.id,
      tenantId: '10',
      allowFixtureFallback: false,
      deps: {
        advancePlanAfterApproval: require('../../max/workspace/AmoOperatorApproval').advancePlanAfterApproval,
        advanceDiscoveryAfterApproval: async () => {
          throw Object.assign(new Error('Investigation planner unavailable.'), {
            code: 'tme_scout_unavailable',
            blockingComponent: 'Scout Investigation Runtime',
            recommendedAction: 'Retry planning.',
          });
        },
      },
    });

    assert.equal(result.outcome, 'blocked');
    assert.ok(result.block);
    assert.equal(result.block.stage, PROGRESSION_STAGES.DISCOVERY_RUNNING);
    assert.match(result.block.unmetPrecondition, /Investigation planner unavailable/i);
    assert.equal(result.block.blockingComponent, 'Scout Investigation Runtime');
    assert.match(result.block.recommendedAction, /Retry/i);
    assert.match(result.presentation, /Mission Blocked|Component/i);
  });

  it('Test 5: execute end-to-end progresses until first human decision point', async () => {
    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Execute all autonomous stages for this mission.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      allowFixtureFallback: true,
    });

    assert.equal(turn.action, 'autonomous_progression');
    assert.match(turn.prose, /Mission Intelligence Report Ready|Mission Paused/i);
    assert.match(turn.prose, /Operator judgment required|Decision Needed/i);
    assert.equal(engine.get(mission.id, '10').stage, STAGES.DISCOVER);
    assert.equal(
      engine.get(mission.id, '10').pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
    );
  });

  it('ambiguous mission pauses at Understanding without auto-advancing', async () => {
    const ambiguousEngine = amo.createAcquisitionMissionEngine();
    const ambiguous = ambiguousEngine.create({
      tenantId: '10',
      objective: 'Find STR operators around Manchester.',
    });

    const result = await runAutonomousProgression({
      engine: ambiguousEngine,
      missionId: ambiguous.id,
      tenantId: '10',
    });

    assert.equal(result.outcome, 'paused');
    assert.equal(result.progressionStage, PROGRESSION_STAGES.MISSION_PLANNING);
    assert.equal(result.transitions.length, 0);
    assert.equal(
      ambiguousEngine.get(ambiguous.id, '10').pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION
    );
    assert.match(result.pause.reason, /ambigu/i);
  });

  it('isAutonomousProgressionCommand detects operator phrasing', () => {
    assert.equal(isAutonomousProgressionCommand('Execute all autonomous stages.'), true);
    assert.equal(isAutonomousProgressionCommand('Run end-to-end until human decision.'), true);
    assert.equal(isAutonomousProgressionCommand('What is the mission status?'), false);
  });

  it('inspect snapshot includes progression metadata', () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.ok(snapshot.progression);
    assert.equal(snapshot.progression.stage, PROGRESSION_STAGES.MISSION_PLANNING);
    assert.ok(snapshot.progression.pause);
  });

  it('deriveExecutionBlock from discovery payload blocked state', async () => {
    engine.contribute(mission.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        blocked: true,
        summary: 'No attributable signals found.',
        qualifiedCount: 0,
        confidence: 0.4,
        companies: [],
        prospects: [],
      },
    }, { tenantId: '10' });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const block = deriveExecutionBlock(snapshot);
    assert.ok(block);
    assert.match(block.unmetPrecondition, /No attributable signals/i);
  });
});
