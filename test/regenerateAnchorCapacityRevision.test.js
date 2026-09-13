'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../packages/acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_INTENTS,
  validateProspectMessageBindings,
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
} = require('../packages/max/workspace/AmoOperatorApproval');
const { sanitizeQueueItem } = require('../packages/max/workspace/EmmettCapacityExecution');
const {
  inspectCapacitySpec212,
  activePaigePayload,
} = require('../scripts/regenerateAnchorCapacityRevision');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function preparedReadyMission() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: OBJECTIVE, targetSegment: 'Law Firms' });
  await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
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
  await advanceExecutionAfterApproval({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    operatorId: 'operator-1',
    question: 'Authorize prepared bundle.',
  });
  return { engine, mission: engine.get(mission.id, '10') };
}

function simulatePreSpec212Persist(engine, missionId, tenantId) {
  const snapshot = engine.inspect(missionId, { tenantId });
  const emmett = snapshot.contributions.find(
    (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
  );
  assert.ok(emmett, 'expected emmett capacity');
  const body = { ...(emmett.payload || {}) };
  const items = Array.isArray(body.queue?.items) ? body.queue.items : [];
  body.queue = {
    ...(body.queue || {}),
    items: items.map((item) => sanitizeQueueItem({
      ...item,
      paige: item.paige
        ? {
          ...item.paige,
          candidateId: item.paige.candidateId,
          bindingScope: item.paige.bindingScope,
          attributableIntelligence: item.paige.attributableIntelligence,
          variantId: item.paige.variantId,
        }
        : item.paige,
    })),
  };
  // Pre-fix bug dropped bindings — simulate by stripping after sanitize with old behavior
  body.queue.items = body.queue.items.map((item) => {
    if (!item.paige) return item;
    return {
      ...item,
      paige: {
        author: item.paige.author,
        source: item.paige.source,
        ready: item.paige.ready,
        variantLabel: item.paige.variantLabel,
        sendable: item.paige.sendable,
      },
    };
  });
  engine.store.updateContribution(emmett.id, (row) => ({
    ...row,
    payload: body,
  }));
}

describe('regenerateAnchorCapacityRevision — canonical path', () => {
  it('REVISE_PREPARED_OUTREACH clears pre-SPEC-212 contamination when Paige variants are reused', async () => {
    const { engine, mission } = await preparedReadyMission();
    simulatePreSpec212Persist(engine, mission.id, '10');

    const before = engine.inspect(mission.id, { tenantId: '10' });
    const oldCapacity = before.contributions.find(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const beforeSpec212 = inspectCapacitySpec212(oldCapacity.payload);
    assert.equal(beforeSpec212.valid, false);
    assert.ok(beforeSpec212.violationCount > 0);

    const paigePayload = activePaigePayload(before.contributions);
    assert.ok(paigePayload?.variants?.length);

    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate capacity only.',
    });

    const routed = await amo.routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });

    assert.equal(routed.action, 'revise_prepared_outreach');
    const after = routed.snapshot;
    assert.equal(after.mission.stage, STAGES.READY);

    const capacities = after.contributions.filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const newCapacity = capacities.at(-1);
    const supersededOld = after.contributions.find((row) => row.id === oldCapacity.id);
    assert.notEqual(newCapacity.id, oldCapacity.id);
    assert.equal(supersededOld.payload.superseded, true);

    const afterSpec212 = inspectCapacitySpec212(newCapacity.payload);
    assert.equal(afterSpec212.valid, true, afterSpec212.blocker);
    assert.equal(afterSpec212.violationCount, 0);

    const validation = validateProspectMessageBindings(
      (newCapacity.payload.payload && newCapacity.payload.specialist)
        ? newCapacity.payload.payload
        : newCapacity.payload
    );
    assert.equal(validation.valid, true);
  });
});
