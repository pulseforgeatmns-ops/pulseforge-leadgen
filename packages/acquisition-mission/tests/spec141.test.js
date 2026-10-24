'use strict';

/**
 * SPEC-141 — Discovery Review Gate.
 * Scout completion does not advance to Understanding; operator prioritization approval does.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  hasPendingPrioritizationApproval,
  hasPendingDiscoveryApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { maybeHandleAcquisitionMissionExecution } = require('../../max/workspace/AcquisitionMissionExecution');
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-141 — Discovery Review Gate', () => {
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

  async function throughDiscovery() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    return advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
  }

  it('commitDiscoveryStage leaves mission in discover with prioritization_approval pending', async () => {
    const result = await throughDiscovery();
    assert.equal(result.executionOutcome, 'completed');

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.DISCOVER);
    assert.equal(snapshot.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
    assert.equal(hasPendingPrioritizationApproval(snapshot), true);
    assert.equal(hasPendingDiscoveryApproval(snapshot), false);
  });

  it('first response after Scout presents discovery evidence, not Understanding status', async () => {
    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      allowFixtureFallback: true,
    });

    assert.equal(turn.action, 'discovery_approved');
    assert.match(turn.prose, /Harbor Law Group|Discovery Complete/i);
    assert.doesNotMatch(turn.prose, /Status[\s\S]*Understanding/i);
    assert.match(turn.prose, /Approve prioritization\?/i);
    assert.equal(engine.get(mission.id, '10').stage, STAGES.DISCOVER);
  });

  it('advancePrioritizationAfterApproval is the only path discover → understand', async () => {
    await throughDiscovery();

    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(before.mission.stage, STAGES.DISCOVER);

    const advanced = await advancePrioritizationAfterApproval({
      engine,
      mission: before.mission,
      tenantId: '10',
      question: 'Approved prioritization.',
    });

    assert.equal(advanced.alreadyExecuted, false);
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.stage, STAGES.UNDERSTAND);
    assert.equal(after.mission.pendingOperatorDecision, null);
    assert.equal(hasPendingPrioritizationApproval(after), false);
  });

  it('AUDIT-027: no stage transition before prioritization approval', async () => {
    const discoveryResult = await throughDiscovery();
    assert.equal(discoveryResult.snapshot.mission.stage, STAGES.DISCOVER);

    const prioritizationTurn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved prioritization.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });

    assert.equal(prioritizationTurn.action, 'prioritization_approved');
    assert.equal(engine.get(mission.id, '10').stage, STAGES.UNDERSTAND);
  });

  it('rejects direct engine.progress discover → understand when prioritization is pending', async () => {
    await throughDiscovery();

    assert.throws(
      () => engine.progress(mission.id, { role: 'operator' }, {
        tenantId: '10',
        stage: STAGES.UNDERSTAND,
      }),
      (err) => err.code === 'amo_prioritization_pending'
    );
  });
});
