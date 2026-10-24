'use strict';

/**
 * Regression: READY missions awaiting execution_approval must hydrate.
 * putMission runs SPEC-136 checks that require Paige + Emmett contributions.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  OPERATOR_DECISION_KINDS,
  EXECUTION_INTENTS,
  hasPendingExecutionApproval,
  MISSION_STATE_INCONSISTENT,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  createAcquisitionMissionRuntime,
  resetAcquisitionMissionRuntime,
} = require('../../../services/acquisitionMissionRuntime');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function readySnapshot(engine, mission) {
  await advancePlanAfterApproval({
    engine, mission, tenantId: '10', question: 'Approved.',
  });
  await advanceDiscoveryAfterApproval({
    engine, mission, tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.',
  });
  await advanceMaxPrioritization({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceAcquisitionApproach({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advancePaigeVariants({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceEmmettCapacity({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  return engine.inspect(mission.id, { tenantId: '10' });
}

function hydrateLoadedSnapshot(loaded) {
  resetAcquisitionMissionRuntime();
  const runtime = createAcquisitionMissionRuntime({
    persist: true,
    pool: { query: async () => ({ rows: [] }) },
    production: false,
  });
  const store = runtime.engine().store;
  for (const row of loaded.contributions) if (row) store.addContribution(row);
  for (const mission of loaded.missions) store.putMission(mission);
  return runtime.engine();
}

describe('SPEC-136 — execution_approval hydration', () => {
  beforeEach(() => {
    resetAcquisitionMissionRuntime();
  });

  it('putMission rejects execution_approval when contributions are not yet in store', () => {
    const store = amo.createMemoryAmoStore();
    const mission = {
      id: 'mission_hydrate_exec_approval',
      tenantId: '10',
      stage: STAGES.READY,
      structuredMissionApproved: true,
      pendingOperatorDecision: {
        stage: STAGES.READY,
        kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
        prompt: 'Authorize external execution of prepared outreach?',
      },
    };
    store.addContribution({
      id: 'contrib_paige',
      missionId: mission.id,
      specialist: 'paige',
      kind: 'variants',
      payload: { variants: [{ candidateId: 'c1', label: 'Primary' }] },
    });
    store.addContribution({
      id: 'contrib_emmett',
      missionId: mission.id,
      specialist: 'emmett',
      kind: 'capacity',
      payload: {
        queue: {
          items: [{
            id: 'c1',
            paige: {
              candidateId: 'c1',
              bindingScope: 'prospect',
              attributableIntelligence: {},
            },
          }],
        },
      },
    });

    const emptyStore = amo.createMemoryAmoStore();
    assert.throws(
      () => emptyStore.putMission(mission),
      (err) => err.code === MISSION_STATE_INCONSISTENT
        && /execution approval that the execution engine cannot consume/.test(err.message)
    );

    assert.doesNotThrow(() => store.putMission(mission));
  });

  it('hydrates READY + pending execution approval when contributions load before missions', async () => {
    const sourceEngine = amo.createAcquisitionMissionEngine();
    const mission = sourceEngine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    const before = await readySnapshot(sourceEngine, mission);
    assert.equal(before.mission.stage, STAGES.READY);
    assert.equal(before.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
    assert.ok(hasPendingExecutionApproval(before));

    const loaded = {
      missions: [structuredClone(before.mission)],
      contributions: before.contributions.map((row) => structuredClone(row)),
    };

    const emptyStore = amo.createMemoryAmoStore();
    assert.throws(
      () => emptyStore.putMission(loaded.missions[0]),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );

    const engine = hydrateLoadedSnapshot(loaded);
    const hydrated = engine.get(mission.id, '10');
    assert.ok(hydrated, 'mission must hydrate when contributions precede putMission');
    assert.equal(hydrated.stage, STAGES.READY);
    assert.equal(hydrated.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);

    await advanceExecutionAfterApproval({
      engine,
      mission: hydrated,
      tenantId: '10',
      operatorId: 'operator-1',
      question: 'Authorize prepared bundle for revision preflight.',
    });

    const paigePayload = before.contributions
      .filter((row) => row.specialist === 'paige' && row.kind === 'variants')
      .at(-1).payload;
    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate capacity.',
    });
    const routed = await amo.routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });
    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.equal(routed.snapshot.mission.stage, STAGES.READY);
  });
});
