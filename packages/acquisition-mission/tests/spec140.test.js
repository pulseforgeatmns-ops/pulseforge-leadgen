'use strict';

/**
 * SPEC-140 — Unified Acquisition Mission Runtime.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const { STAGES } = amo;
const {
  MULTIPLE_ACQUISITION_RUNTIMES,
  createAcquisitionMissionRuntime,
  bootAcquisitionMissionRuntime,
  getAcquisitionMissionRuntime,
  resetAcquisitionMissionRuntime,
  setAcquisitionMissionRuntimeForTests,
  assertSingleRuntime,
  assertRuntimeEngine,
  resolveAcquisitionMissionRuntime,
} = require('../../../services/acquisitionMissionRuntime');
const {
  resetEngine,
  createMission,
  listMissions,
} = require('../../../services/acquisitionMission');
const { maybeHandleAcquisitionOwnershipTurn } = require('../../max/workspace/AcquisitionOwnership');
const { advancePlanAfterApproval } = require('../../max/workspace/AmoOperatorApproval');
const { maybeHandleAcquisitionMissionExecution } = require('../../max/workspace/AcquisitionMissionExecution');
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-140 — Unified Acquisition Mission Runtime', () => {
  beforeEach(() => {
    resetAcquisitionMissionRuntime();
  });

  it('bootAcquisitionMissionRuntime creates exactly one production runtime', () => {
    const runtime = bootAcquisitionMissionRuntime({ persist: false });
    assert.ok(runtime.runtimeId);
    assert.ok(runtime.engineId);
    assert.ok(runtime.storeId);
    assertSingleRuntime();
    assert.throws(
      () => createAcquisitionMissionRuntime({ production: true, persist: false }),
      (err) => err.code === MULTIPLE_ACQUISITION_RUNTIMES
    );
  });

  it('createMission and workspace execution share the same engine identity', async () => {
    resetEngine();
    const runtime = getAcquisitionMissionRuntime({ production: false, persist: false });
    const engineId = runtime.engineId;

    const mission = runtime.engine().create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    assert.equal(runtime.engineId, engineId);

    const execution = await maybeHandleAcquisitionMissionExecution({
      question: 'approved',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: runtime,
      persist: false,
    });

    assert.ok(execution);
    assert.equal(runtime.engine().get(mission.id, '10').stage, STAGES.UNDERSTAND);
    assert.equal(runtime.engineId, engineId);
  });

  it('assertRuntimeEngine rejects a foreign engine before execution', async () => {
    const runtime = createTestAmoRuntime();
    const foreignEngine = amo.createAcquisitionMissionEngine();

    assert.throws(
      () => assertRuntimeEngine(foreignEngine),
      (err) => err.code === MULTIPLE_ACQUISITION_RUNTIMES
    );

    setAcquisitionMissionRuntimeForTests(runtime);
    assert.doesNotThrow(() => assertRuntimeEngine(runtime.engine()));
  });

  it('resolveAcquisitionMissionRuntime returns the same runtime for creation and inspection', async () => {
    const runtime = createTestAmoRuntime();
    const input = { acquisitionMissionRuntime: runtime, persist: false };

    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10' },
      ...input,
    });

    assert.ok(turn);
    assert.equal(turn.created, true);

    const resolved = resolveAcquisitionMissionRuntime(input);
    assert.equal(resolved.runtimeId, runtime.runtimeId);
    assert.equal(resolved.engineId, runtime.engineId);
    assert.equal(resolved.storeId, runtime.storeId);

    const missions = resolved.engine().list('10');
    assert.equal(missions.length, 1);
    assert.equal(missions[0].id, turn.mission.id);
  });

  it('plan approval advances through the same runtime engine', async () => {
    const runtime = createTestAmoRuntime();
    const engine = runtime.engine();
    const mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const advanced = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
      acquisitionMissionRuntime: runtime,
      persist: false,
    });

    assert.equal(advanced.snapshot.mission.structuredMissionApproved, true);
    assert.equal(engine.get(mission.id, '10').pendingOperatorDecision.kind, 'discovery_approval');
    assert.equal(runtime.engineId, runtime.inspect().engineId);
  });

  it('runtime.inspect reports stable engineId and storeId', () => {
    const runtime = createTestAmoRuntime();
    const first = runtime.inspect();
    const second = runtime.inspect();
    assert.equal(first.engineId, second.engineId);
    assert.equal(first.storeId, second.storeId);
    assert.equal(first.runtimeId, second.runtimeId);
  });
});
