'use strict';

/**
 * ADR-067 — Stage Contracts Are Authoritative.
 * Execution pauses when the contract requires human judgment, even if MissionPause cannot be built.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  PROGRESSION_STAGES,
  MISSION_STAGE_CONTRACTS,
  resolveHumanDecisionGate,
  resolveProgressionState,
  validateStageTransition,
  runAutonomousProgression,
  deriveMissionPause,
} = amo;

describe('ADR-067 — Stage Contracts Are Authoritative', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });
  });

  it('contract requires human decision even when MissionPause is unavailable', () => {
    const snapshot = {
      mission: {
        ...mission,
        stage: STAGES.EXECUTE,
        pendingOperatorDecision: null,
        blockers: [],
      },
      contributions: [],
    };

    assert.equal(deriveMissionPause(snapshot), null);
    assert.equal(MISSION_STAGE_CONTRACTS[PROGRESSION_STAGES.EXECUTION].requiresHumanDecision, true);

    const gate = resolveHumanDecisionGate(snapshot);
    assert.equal(gate.shouldPause, true);
    assert.equal(gate.pause, null);
    assert.ok(gate.block);
    assert.equal(gate.block.pauseFallback, true);
    assert.equal(gate.block.blockingComponent, 'Stage Contract');
  });

  it('resolveProgressionState returns paused with ExecutionBlock fallback', () => {
    const snapshot = {
      mission: {
        ...mission,
        stage: STAGES.EXECUTE,
        pendingOperatorDecision: null,
        blockers: [],
      },
      contributions: [],
    };

    const state = resolveProgressionState(snapshot);
    assert.equal(state.outcome, 'paused');
    assert.equal(state.pause, null);
    assert.ok(state.block);
    assert.equal(state.block.pauseFallback, true);
  });

  it('validateStageTransition rejects automatic transition when contract requires human decision', () => {
    const result = validateStageTransition(
      PROGRESSION_STAGES.DISCOVERY_REVIEW,
      PROGRESSION_STAGES.OUTREACH_PLANNING
    );
    assert.equal(result.ok, false);
    assert.match(result.reason, /operator judgment/i);
  });

  it('validateStageTransition permits contracted automatic transitions', () => {
    const result = validateStageTransition(
      PROGRESSION_STAGES.UNDERSTANDING,
      PROGRESSION_STAGES.DISCOVERY
    );
    assert.equal(result.ok, true);
  });

  it('validateStageTransition rejects non-contracted transitions', () => {
    const result = validateStageTransition(
      PROGRESSION_STAGES.UNDERSTANDING,
      PROGRESSION_STAGES.EXECUTION
    );
    assert.equal(result.ok, false);
    assert.match(result.reason, /not permitted/i);
  });

  it('runAutonomousProgression never continues past contract human-decision boundary', async () => {
    const execEngine = amo.createAcquisitionMissionEngine();
    const execMission = execEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });

    const updated = execEngine.get(execMission.id, '10');
    execEngine.store.putMission({
      ...updated,
      stage: STAGES.EXECUTE,
      pendingOperatorDecision: null,
      structuredMissionApproved: true,
      blockers: [],
    });

    const result = await runAutonomousProgression({
      engine: execEngine,
      missionId: execMission.id,
      tenantId: '10',
    });

    assert.equal(result.outcome, 'paused');
    assert.equal(result.progressionStage, PROGRESSION_STAGES.EXECUTION);
    assert.equal(result.pause, null);
    assert.ok(result.block);
    assert.equal(result.block.pauseFallback, true);
    assert.match(result.presentation, /Mission Paused/i);
  });

  it('inspect snapshot uses contract-authoritative progression state', () => {
    const updated = engine.get(mission.id, '10');
    engine.store.putMission({
      ...updated,
      stage: STAGES.EXECUTE,
      pendingOperatorDecision: null,
      structuredMissionApproved: true,
      blockers: [],
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.progression.outcome, 'paused');
    assert.ok(snapshot.progression.contract);
    assert.equal(snapshot.progression.contract.requiresHumanDecision, true);
    assert.ok(snapshot.progression.block);
    assert.equal(snapshot.progression.block.pauseFallback, true);
  });

  it('existing Discovery Review pause still uses MissionPause when available', async () => {
    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(result.outcome, 'paused');
    assert.ok(result.pause);
    assert.equal(result.pause.stage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.equal(result.block, null);
  });
});
