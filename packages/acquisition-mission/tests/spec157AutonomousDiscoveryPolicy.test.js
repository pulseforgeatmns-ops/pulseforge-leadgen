'use strict';

/**
 * SPEC-157 — Autonomous Discovery Approval Policy (ADR-074 companion).
 * When executionPolicy = autonomous, discovery approval is auto-consumed unless
 * the mission contract explicitly requires operator judgment for Discovery.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  OPERATOR_DECISION_KINDS,
  PROGRESSION_STAGES,
  WORKSPACE_MODES,
  deriveWorkspaceMode,
  shouldAutoConsumeDiscoveryApproval,
  discoveryRequiresOperatorJudgment,
} = amo;
const {
  advancePlanAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  maybeAutoAdvanceDiscoveryAfterPlan,
  maybeHandleAcquisitionMissionExecution,
} = require('../../max/workspace/AcquisitionMissionExecution');
const { EXECUTION_POLICIES } = require('../../max/workspace/SessionState');
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-157 — Autonomous Discovery Approval Policy', () => {
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

  it('Test 1: plan approved under autonomous policy auto-consumes discovery approval', async () => {
    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approve mission plan.',
      operatorId: 'operator',
    });

    const chained = await maybeAutoAdvanceDiscoveryAfterPlan(
      {
        engine,
        mission,
        tenantId: '10',
        executionPolicy: EXECUTION_POLICIES.AUTONOMOUS,
        allowFixtureFallback: true,
      },
      planResult
    );

    assert.equal(chained.action, 'discovery_approved');
    assert.equal(chained.autoConsumedDiscoveryApproval, true);
    assert.equal(
      chained.snapshot.mission.pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
    );
    assert.equal(deriveWorkspaceMode({ missionId: mission.id, snapshot: chained.snapshot }), WORKSPACE_MODES.DISCOVERY_REVIEW);
  });

  it('Test 2: normal execution policy pauses at discovery approval', async () => {
    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approve mission plan.',
      operatorId: 'operator',
    });

    const chained = await maybeAutoAdvanceDiscoveryAfterPlan(
      {
        engine,
        mission,
        tenantId: '10',
        executionPolicy: EXECUTION_POLICIES.NORMAL,
        allowFixtureFallback: true,
      },
      planResult
    );

    assert.equal(chained.action, 'plan_approved');
    assert.equal(chained.discoveryResult, null);
    assert.equal(
      chained.snapshot.mission.pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
    );
    assert.equal(deriveWorkspaceMode({ missionId: mission.id, snapshot: chained.snapshot }), WORKSPACE_MODES.DISCOVERY_APPROVAL);
  });

  it('Test 3: mission contract requireDiscoveryApproval blocks auto-consume', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approve mission plan.',
      operatorId: 'operator',
    });

    const locked = engine.get(mission.id, '10');
    locked.structuredMission = {
      ...locked.structuredMission,
      execution: { state: 'approved', requireDiscoveryApproval: true },
    };
    engine.store.putMission(locked);

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(discoveryRequiresOperatorJudgment(snapshot), true);
    assert.equal(shouldAutoConsumeDiscoveryApproval(snapshot, EXECUTION_POLICIES.AUTONOMOUS), false);
  });

  it('Test 4: workspace mode reflects discovery approval vs review execution state', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approve mission plan.',
      operatorId: 'operator',
    });

    const waiting = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(
      deriveWorkspaceMode({ missionId: mission.id, snapshot: waiting }),
      WORKSPACE_MODES.DISCOVERY_APPROVAL
    );
    assert.equal(waiting.progression.stage, PROGRESSION_STAGES.DISCOVERY_APPROVAL);

    const chained = await maybeAutoAdvanceDiscoveryAfterPlan(
      {
        engine,
        mission,
        tenantId: '10',
        executionPolicy: EXECUTION_POLICIES.AUTONOMOUS,
        allowFixtureFallback: true,
      },
      { snapshot: waiting }
    );

    assert.equal(
      deriveWorkspaceMode({ missionId: mission.id, snapshot: chained.snapshot }),
      WORKSPACE_MODES.DISCOVERY_REVIEW
    );
    assert.equal(chained.snapshot.progression.stage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
  });

  it('Test 5: approve plan via ask path with autonomous session chains to Scout', async () => {
    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approve mission plan.',
      session: {
        sessionState: { executionPolicy: EXECUTION_POLICIES.AUTONOMOUS },
      },
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      allowFixtureFallback: true,
    });

    assert.equal(turn.action, 'discovery_approved');
    assert.equal(
      engine.get(mission.id, '10').pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
    );
  });
});
