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
  findValidExecutionApproval,
  computePreparedArtifactRevision,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function preparedRuntime() {
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
    question: 'Authorize the prepared bundle.',
  });
  return { engine, mission: engine.get(mission.id, '10') };
}

function revisionRequest(mission) {
  return createExecutionRequest({
    source: EXECUTION_SOURCES.CHAT,
    intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
    missionId: mission.id,
    mission,
    operatorId: 'operator-1',
    stage: STAGES.READY,
    question: 'Regenerate the outreach messaging and outbound plan. Do not send anything.',
  });
}

describe('SPEC-215 — transactional prepared-outreach revision', () => {
  it('replaces Paige and Emmett together and creates a new approval', async () => {
    const { engine, mission } = await preparedRuntime();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    const oldPaige = before.contributions.find((row) => row.specialist === SPECIALISTS.PAIGE);
    const oldEmmett = before.contributions.find((row) => row.specialist === SPECIALISTS.EMMETT);
    const oldApproval = findValidExecutionApproval(before.contributions, mission.id);

    const result = await routeExecutionRequest(revisionRequest(mission), {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
    });
    const after = result.snapshot;
    const paige = after.contributions.filter((row) => row.specialist === SPECIALISTS.PAIGE).at(-1);
    const emmett = after.contributions.filter((row) => row.specialist === SPECIALISTS.EMMETT).at(-1);
    const approval = after.mission.pendingOperatorDecision;

    assert.equal(result.action, 'revise_prepared_outreach');
    assert.equal(after.mission.stage, STAGES.READY);
    assert.notEqual(paige.id, oldPaige.id);
    assert.notEqual(emmett.id, oldEmmett.id);
    assert.equal(after.executionReview.artifactBinding.maxContributionId,
      before.executionReview.artifactBinding.maxContributionId);
    assert.equal(after.executionReview.artifactBinding.paigeContributionId, paige.id);
    assert.equal(after.executionReview.artifactBinding.emmettContributionId, emmett.id);
    assert.equal(after.executionReview.preparedArtifactRevision,
      computePreparedArtifactRevision(mission.id, after.contributions));
    assert.equal(approval.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
    assert.equal(approval.executionReview.preparedArtifactRevision,
      after.executionReview.preparedArtifactRevision);
    const historicalApproval = after.contributions.find((row) => row.id === oldApproval.id);
    assert.equal(historicalApproval.payload.invalidated, true);
    assert.equal(findValidExecutionApproval(after.contributions, mission.id), null);
  });

  it('rolls back both replacements when Emmett fails and stays retryable', async () => {
    const { engine, mission } = await preparedRuntime();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    const oldPaige = before.contributions.find((row) => row.specialist === SPECIALISTS.PAIGE);
    const oldEmmett = before.contributions.find((row) => row.specialist === SPECIALISTS.EMMETT);

    const routed = await routeExecutionRequest(revisionRequest(mission), {
        engine,
        tenantId: '10',
        allowFixtureFallback: true,
        runEmmett: async () => { throw new Error('forced Emmett failure'); },
      });
    assert.equal(routed.executionResult.rolledBack, true);
    assert.match(routed.executionResult.error.message, /forced Emmett failure/);

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.stage, STAGES.PREPARE);
    assert.equal(after.mission.revisionState.status, 'failed');
    assert.equal(after.mission.revisionState.retryable, true);
    assert.equal(after.contributions.filter((row) => row.specialist === SPECIALISTS.PAIGE).at(-1).id, oldPaige.id);
    assert.equal(after.contributions.filter((row) => row.specialist === SPECIALISTS.EMMETT).at(-1).id, oldEmmett.id);
    assert.equal(findValidExecutionApproval(after.contributions, mission.id), null);
    assert.equal(after.executionReview, null);
  });
});
