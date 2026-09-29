'use strict';

/**
 * SPEC-250 — Legacy acquisition approach reconciliation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  ACQUISITION_APPROACHES,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  OPERATOR_DECISION_KINDS,
  createExecutionRequest,
  routeExecutionRequest,
  createAcquisitionMissionEngine,
  createMemoryAmoStore,
  resolveMissionContinuation,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
} = require('../../max/workspace/AmoOperatorApproval');

const TENANT_ID = '10';
const OBJECTIVE = 'Acquire one recurring commercial cleaning client from a short-term-rental operator in Greater Manchester.';

describe('SPEC-250 — Legacy acquisition approach reconciliation', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: TENANT_ID,
      objective: OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });
  });

  async function throughPrioritization() {
    await advancePlanAfterApproval({ engine, mission, tenantId: TENANT_ID, question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      question: 'Approved discovery.',
      allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      question: 'Approved prioritization.',
    });
  }

  async function decideModernApproach(approach = ACQUISITION_APPROACHES.OUTBOUND) {
    await throughPrioritization();
    return advanceAcquisitionApproach({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      question: `Decide ${approach}.`,
      selectedApproach: approach,
    });
  }

  function restoreWithoutApproach(targetStage = STAGES.READY) {
    const snap = engine.store.snapshot();
    snap.contributions = snap.contributions.filter(
      (row) => row.kind !== CONTRIBUTION_KINDS.ACQUISITION_APPROACH
    );
    snap.events = snap.events.filter(
      (row) => !(row.label || '').includes('acquisition approach')
    );
    snap.missions = snap.missions.map(([id, row]) => [
      id,
      row.id === mission.id
        ? {
          ...row,
          stage: targetStage,
          status: targetStage === STAGES.READY ? 'Ready' : 'Preparing',
          pendingOperatorDecision: targetStage === STAGES.READY
            ? row.pendingOperatorDecision
            : null,
        }
        : row,
    ]);
    const store = createMemoryAmoStore();
    store.restore(snap);
    engine = createAcquisitionMissionEngine({ store });
    mission = engine.get(mission.id, TENANT_ID);
    return engine.inspect(mission.id, { tenantId: TENANT_ID });
  }

  async function makeLegacyPrepareMission() {
    await decideModernApproach(ACQUISITION_APPROACHES.OUTBOUND);
    engine.progress(mission.id, { role: 'max' }, { tenantId: TENANT_ID, stage: STAGES.PREPARE });
    return restoreWithoutApproach(STAGES.PREPARE);
  }

  async function makeLegacyReadyMission() {
    await decideModernApproach(ACQUISITION_APPROACHES.OUTBOUND);
    await advancePaigeVariants({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      allowFixtureFallback: true,
    });
    await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      allowFixtureFallback: true,
    });
    return restoreWithoutApproach(STAGES.READY);
  }

  async function reconcile(approach = ACQUISITION_APPROACHES.OUTBOUND, extra = {}) {
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.RECONCILE_ACQUISITION_APPROACH,
      payload: {
        reconciliation: true,
        approach,
        question: `Reconcile ${approach}.`,
        ...extra.payload,
      },
    });
    return routeExecutionRequest(request, {
      engine,
      tenantId: TENANT_ID,
      operatorId: 'operator-test',
      allowFixtureFallback: true,
      ...extra.context,
    });
  }

  it('keeps normal PLAN-stage SPEC-248 behavior unchanged', async () => {
    const routed = await decideModernApproach(ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.approach.payload.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(engine.inspect(mission.id, { tenantId: TENANT_ID }).mission.stage, STAGES.PLAN);
  });

  it('rejects normal approach selection on an eligible legacy READY mission', async () => {
    await makeLegacyReadyMission();
    await assert.rejects(
      () => advanceAcquisitionApproach({
        engine,
        mission: engine.get(mission.id, TENANT_ID),
        tenantId: TENANT_ID,
        selectedApproach: ACQUISITION_APPROACHES.OUTBOUND,
      }),
      /requires stage plan/i
    );
  });

  it('reconciles eligible legacy PREPARE and READY missions without rewinding stage', async () => {
    let snapshot = await makeLegacyPrepareMission();
    assert.equal(snapshot.acquisitionApproach, null);
    let routed = await reconcile(ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.snapshot.mission.stage, STAGES.PREPARE);
    assert.equal(routed.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.executionResult.approach.payload.legacyReconciliation.originalStage, STAGES.PREPARE);

    engine = createAcquisitionMissionEngine();
    mission = engine.create({ tenantId: TENANT_ID, objective: OBJECTIVE, targetSegment: 'Short-Term Rental Operators' });
    snapshot = await makeLegacyReadyMission();
    assert.equal(snapshot.acquisitionApproach, null);
    routed = await reconcile(ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.snapshot.mission.stage, STAGES.READY);
    assert.equal(routed.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.executionResult.reconciliation.lifecycleRewound, false);
  });

  it('preserves historical Scout, Max, Paige, Emmett, and operator approval contributions', async () => {
    const before = await makeLegacyReadyMission();
    const beforeCounts = {
      scout: before.contributions.filter((row) => row.specialist === SPECIALISTS.SCOUT).length,
      maxPrioritization: before.contributions.filter(
        (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
      ).length,
      paige: before.contributions.filter((row) => row.specialist === SPECIALISTS.PAIGE).length,
      emmett: before.contributions.filter((row) => row.specialist === SPECIALISTS.EMMETT).length,
      approvals: before.contributions.filter((row) => row.specialist === SPECIALISTS.OPERATOR).length,
    };

    const routed = await reconcile(ACQUISITION_APPROACHES.BOTH);
    const after = routed.snapshot;

    assert.equal(after.contributions.filter((row) => row.specialist === SPECIALISTS.SCOUT).length, beforeCounts.scout);
    assert.equal(after.contributions.filter(
      (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
    ).length, beforeCounts.maxPrioritization);
    assert.equal(after.contributions.filter((row) => row.specialist === SPECIALISTS.PAIGE).length, beforeCounts.paige);
    assert.equal(after.contributions.filter((row) => row.specialist === SPECIALISTS.EMMETT).length, beforeCounts.emmett);
    assert.equal(after.contributions.filter((row) => row.specialist === SPECIALISTS.OPERATOR).length, beforeCounts.approvals);
  });

  it('runs Max approach reasoning instead of inferring OUTBOUND from historical preparation', async () => {
    await makeLegacyReadyMission();
    let invoked = false;

    const routed = await reconcile(ACQUISITION_APPROACHES.OUTBOUND, {
      context: {
        runMaxApproach: async (current, opts) => {
          invoked = true;
          const { runMaxAcquisitionApproach } = require('../../max/workspace/MaxAcquisitionApproachExecutor');
          const contributions = opts.engine.inspect(current.id, { tenantId: TENANT_ID }).contributions || [];
          const input = amo.buildExecutionInput({
            mission: current,
            specialist: SPECIALISTS.MAX,
            contributions,
            transactionId: opts.transactionId,
            executionContext: { requestedApproach: ACQUISITION_APPROACHES.PAID },
            store: opts.engine.store,
          });
          return runMaxAcquisitionApproach({
            ...input,
            mission: current,
            contributions,
            requestedApproach: ACQUISITION_APPROACHES.PAID,
          });
        },
      },
    });

    assert.equal(invoked, true);
    assert.equal(routed.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.PAID);
    assert.equal(routed.snapshot.workspace.penny.state, 'waiting');
    assert.equal(routed.snapshot.executionRecords.length, 0);
  });

  it('preserves outbound semantics for OUTBOUND and BOTH while leaving paid work outstanding for BOTH', async () => {
    await makeLegacyReadyMission();
    let routed = await reconcile(ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.snapshot.workspace.penny.label, 'Not Required');

    engine = createAcquisitionMissionEngine();
    mission = engine.create({ tenantId: TENANT_ID, objective: OBJECTIVE, targetSegment: 'Short-Term Rental Operators' });
    await makeLegacyReadyMission();
    routed = await reconcile(ACQUISITION_APPROACHES.BOTH);
    assert.equal(routed.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.BOTH);
    assert.equal(routed.snapshot.workspace.penny.state, 'waiting');
  });

  it('PAID, DEFER, and BLOCKED preserve historical artifacts but fail closed for outbound continuation', async () => {
    await makeLegacyReadyMission();
    let routed = await reconcile(ACQUISITION_APPROACHES.PAID);
    assert.equal(routed.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.PAID);
    assert.equal(routed.snapshot.contributions.some((row) => row.specialist === SPECIALISTS.PAIGE), true);
    let continuation = resolveMissionContinuation(routed.snapshot);
    assert.notEqual(continuation.kind, 'pending_decision');
    assert.notEqual(continuation.progression?.intent, EXECUTION_INTENTS.EXECUTE_OUTBOUND);

    engine = createAcquisitionMissionEngine();
    mission = engine.create({ tenantId: TENANT_ID, objective: OBJECTIVE, targetSegment: 'Short-Term Rental Operators' });
    await makeLegacyReadyMission();
    routed = await reconcile(ACQUISITION_APPROACHES.DEFER);
    assert.equal(routed.snapshot.blocker.kind, 'acquisition_approach_deferred');
    continuation = resolveMissionContinuation(routed.snapshot);
    assert.notEqual(continuation.kind, 'pending_decision');
    assert.notEqual(continuation.progression?.intent, EXECUTION_INTENTS.EXECUTE_OUTBOUND);

    engine = createAcquisitionMissionEngine();
    mission = engine.create({ tenantId: TENANT_ID, objective: OBJECTIVE, targetSegment: 'Short-Term Rental Operators' });
    await makeLegacyReadyMission();
    routed = await reconcile(ACQUISITION_APPROACHES.BLOCKED);
    assert.equal(routed.snapshot.blocker.kind, 'acquisition_approach_blocked');
    continuation = resolveMissionContinuation(routed.snapshot);
    assert.notEqual(continuation.kind, 'pending_decision');
    assert.notEqual(continuation.progression?.intent, EXECUTION_INTENTS.EXECUTE_OUTBOUND);
  });

  it('fails closed for malformed downstream missions and does not bypass validators', async () => {
    await throughPrioritization();
    const snap = engine.store.snapshot();
    snap.missions = snap.missions.map(([id, row]) => [
      id,
      row.id === mission.id
        ? { ...row, stage: STAGES.READY, status: 'Ready', pendingOperatorDecision: null }
        : row,
    ]);
    const store = createMemoryAmoStore();
    store.restore(snap);
    engine = createAcquisitionMissionEngine({ store });
    mission = engine.get(mission.id, TENANT_ID);

    const routed = await reconcile(ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.executionResult.rolledBack, true);
    assert.match(routed.executionResult.error.message, /canonical downstream mission work/i);
  });

  it('is idempotent once a canonical approach exists', async () => {
    await makeLegacyReadyMission();
    const first = await reconcile(ACQUISITION_APPROACHES.OUTBOUND);
    const second = await reconcile(ACQUISITION_APPROACHES.PAID);
    const approaches = second.snapshot.contributions.filter(
      (row) => row.kind === CONTRIBUTION_KINDS.ACQUISITION_APPROACH
    );

    assert.equal(first.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(second.executionResult.alreadyExecuted, true);
    assert.equal(second.snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(approaches.length, 1);
  });

  it('survives reload and Max inspection explains reconciled historical state', async () => {
    await makeLegacyReadyMission();
    await reconcile(ACQUISITION_APPROACHES.BOTH);

    const restoredStore = createMemoryAmoStore();
    restoredStore.restore(engine.store.snapshot());
    const restored = createAcquisitionMissionEngine({ store: restoredStore });
    const snapshot = restored.inspect(mission.id, { tenantId: TENANT_ID });
    const answer = restored.answerOperator('why is this mission here?', {
      tenantId: TENANT_ID,
      missionId: mission.id,
      silentInspection: true,
    });

    assert.equal(snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.BOTH);
    assert.equal(snapshot.acquisitionApproach.requiresSpecialistEvidence, true);
    assert.match(answer.prose, /originated before canonical acquisition-approach selection/i);
    assert.match(answer.prose, /Historical preparation remains evidence/i);
  });

  it('does not create external acquisition action during reconciliation', async () => {
    await makeLegacyReadyMission();
    const routed = await reconcile(ACQUISITION_APPROACHES.BOTH);

    assert.equal(routed.snapshot.outcomes.length, 0);
    assert.equal(routed.snapshot.executionRecords.length, 0);
    assert.equal(routed.snapshot.timeline.includes('Queued'), false);
    assert.equal(routed.snapshot.timeline.includes('Launched'), false);
    assert.equal(routed.snapshot.contributions.some(
      (row) => row.payload?.decisionKind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    ), false);
  });
});
