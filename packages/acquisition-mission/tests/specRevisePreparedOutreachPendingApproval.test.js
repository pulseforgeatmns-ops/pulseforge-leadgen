'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  routeExecutionRequest,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
} = require('../../max/workspace/AmoOperatorApproval');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function readyWithoutConsumedApproval() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: OBJECTIVE, targetSegment: 'Law Firms' });
  await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
  await advanceDiscoveryAfterApproval({
    engine, mission, tenantId: '10', question: 'Approved. Begin Discovery.', allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved prioritization.',
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
  return { engine, mission: engine.get(mission.id, '10') };
}

describe('REVISE_PREPARED_OUTREACH with pending execution approval', () => {
  it('succeeds at READY before execution approval is consumed', async () => {
    const { engine, mission } = await readyWithoutConsumedApproval();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(before.mission.stage, STAGES.READY);
    assert.equal(before.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);

    const oldPaige = before.contributions.filter(
      (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
    ).at(-1);
    const oldEmmett = before.contributions.filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    ).at(-1);
    const paigePayload = oldPaige.payload;

    const routed = await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Revise prepared outreach with live CRM emails.',
    }), {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });

    assert.notEqual(routed.executionResult?.rolledBack, true, routed.executionResult?.rollbackReason);
    assert.equal(routed.action, 'revise_prepared_outreach');
    const after = routed.snapshot;
    const newPaige = after.contributions.filter(
      (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
    ).at(-1);
    const newEmmett = after.contributions.filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    ).at(-1);
    assert.notEqual(newPaige.id, oldPaige.id);
    assert.notEqual(newEmmett.id, oldEmmett.id);
    assert.equal(after.mission.revisionState.maxContributionId || before.executionReview?.artifactBinding?.maxContributionId,
      before.executionReview?.artifactBinding?.maxContributionId);
    assert.equal(after.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
  });

  it('surfaces exact rollbackReason when revision fails', async () => {
    const { engine, mission } = await readyWithoutConsumedApproval();
    const routed = await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Revise prepared outreach.',
    }), {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runEmmett: async () => { throw new Error('forced Emmett failure'); },
    });

    assert.equal(routed.executionResult.rolledBack, true);
    assert.match(String(routed.executionResult.rollbackReason), /forced Emmett failure/);
  });
});
