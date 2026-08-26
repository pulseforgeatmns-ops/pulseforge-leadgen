'use strict';

/**
 * Canonical READY transition & artifact-bound execution approval.
 */

const { describe, it, beforeEach } = require('node:test');
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
  clearExecutionRouterAudit,
  specialistContext,
  canEnter,
  computePreparedArtifactRevision,
  isExecutionApproved,
  buildExecutionReview,
  hasPendingExecutionApproval,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
  findPrioritizationApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('Canonical READY transition & execution approval', () => {
  let engine;
  let mission;

  beforeEach(() => {
    clearExecutionRouterAudit();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  async function throughPrepareComplete() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      question: 'Approved prioritization.',
    });
    await advanceMaxPrioritization({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });
    await advancePaigeVariants({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
      question: 'Generate variants.',
    });
    await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
      question: 'Plan capacity.',
    });
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  it('automatic PREPARE → READY when Paige + Emmett complete and deliverability healthy', async () => {
    const snapshot = await throughPrepareComplete();
    assert.equal(snapshot.mission.stage, STAGES.READY);
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(ctx.paigeComplete, true);
    assert.equal(ctx.emmettComplete, true);
    assert.equal(ctx.deliverabilityPaused, false);
    assert.equal(canEnter(STAGES.READY, ctx).ok, true);
  });

  it('READY produces pending execution approval decision', async () => {
    const snapshot = await throughPrepareComplete();
    assert.equal(snapshot.mission.pendingOperatorDecision?.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
    assert.match(
      snapshot.mission.pendingOperatorDecision?.prompt || '',
      /authorize external execution/i
    );
    assert.ok(hasPendingExecutionApproval(snapshot));
    assert.ok(snapshot.executionReview);
    assert.ok(snapshot.executionReview.communication.subject);
    assert.ok(snapshot.executionReview.infrastructure.queue != null);
  });

  it('historical plan/discovery/prioritization approvals do not authorize execution', async () => {
    const snapshot = await throughPrepareComplete();
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(ctx.operatorApproved, true);
    assert.equal(ctx.executionApproved, false);
    assert.equal(canEnter(STAGES.EXECUTE, ctx).ok, false);
  });

  it('APPROVE_EXECUTION commits approval, consumes pending decision, and enables EXECUTE', async () => {
    await throughPrepareComplete();

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.APPROVAL_BUTTON,
      intent: EXECUTION_INTENTS.APPROVE_EXECUTION,
      missionId: mission.id,
      operatorId: 'operator-1',
      stage: STAGES.READY,
    });

    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
    });

    assert.equal(routed.specialist, 'operator');
    assert.equal(routed.action, 'execution_approved');
    assert.equal(routed.executionResult.alreadyExecuted, false);

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(snapshot.mission.stage, STAGES.READY);
    assert.equal(snapshot.mission.pendingOperatorDecision, null);
    assert.equal(ctx.executionApproved, true);
    assert.equal(canEnter(STAGES.EXECUTE, { ...ctx, deliverabilityPaused: false }).ok, true);

    const approval = snapshot.contributions.find(
      (row) => row.specialist === SPECIALISTS.OPERATOR
        && row.kind === CONTRIBUTION_KINDS.APPROVAL
        && row.payload?.decisionKind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    );
    assert.ok(approval);
    assert.equal(
      approval.payload.preparedArtifactRevision,
      computePreparedArtifactRevision(mission.id, snapshot.contributions)
    );
    assert.ok(approval.payload.paigeContributionId);
    assert.ok(approval.payload.emmettContributionId);
    assert.ok(approval.payload.maxContributionId);
  });

  it('approval payload binds to current prepared artifacts', async () => {
    const snapshot = await throughPrepareComplete();
    const review = buildExecutionReview(snapshot.mission, snapshot.contributions);
    const revision = computePreparedArtifactRevision(mission.id, snapshot.contributions);

    assert.equal(review.preparedArtifactRevision, revision);
    assert.ok(review.artifactBinding.paigeContributionId);
    assert.ok(review.artifactBinding.emmettContributionId);
    assert.ok(review.artifactBinding.maxContributionId);
    assert.ok(review.communication.subject);
    assert.ok(review.decision.plannedSendCount >= 0);
  });

  it('stale execution approval after Paige contribution changes', async () => {
    await throughPrepareComplete();
    await advanceExecutionAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      operatorId: 'operator-1',
      question: 'Authorize execution.',
    });

    let snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(isExecutionApproved(snapshot.contributions, mission.id), true);

    engine.contribute(mission.id, {
      specialist: SPECIALISTS.PAIGE,
      kind: CONTRIBUTION_KINDS.VARIANTS,
      payload: {
        variants: [{
          label: 'Revised',
          subject: 'Updated subject',
          body: 'Updated body',
          cta: 'Reply now',
        }],
      },
    }, { tenantId: '10' });

    snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(isExecutionApproved(snapshot.contributions, mission.id), false);
    assert.equal(ctx.executionApproved, false);
    assert.equal(canEnter(STAGES.EXECUTE, ctx).ok, false);
  });

  it('deliverability pause blocks PREPARE → READY', async () => {
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
    await advancePaigeVariants({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });

    engine.contribute(mission.id, {
      specialist: SPECIALISTS.EMMETT,
      kind: CONTRIBUTION_KINDS.CAPACITY,
      payload: {
        capacity: { recommended: 0, remaining: 0 },
        queue: { items: [] },
        deliverability: { status: 'paused' },
        governor: { outcome: 'pause', reason: 'Reputation risk detected.' },
      },
    }, { tenantId: '10' });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.PREPARE);
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(ctx.deliverabilityPaused, true);
    assert.equal(canEnter(STAGES.READY, ctx).ok, false);
    assert.equal(hasPendingExecutionApproval(snapshot), false);
  });

  it('prioritization approval remains distinct from execution approval', async () => {
    const snapshot = await throughPrepareComplete();
    const prioritizationApproval = findPrioritizationApproval(snapshot.contributions);
    assert.ok(prioritizationApproval);
    assert.notEqual(prioritizationApproval.payload.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
  });
});
