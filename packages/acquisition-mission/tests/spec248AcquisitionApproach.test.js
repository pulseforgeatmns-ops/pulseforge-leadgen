'use strict';

/**
 * SPEC-248 — Canonical Acquisition Approach Decision.
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
  EVENT_KINDS,
  createExecutionRequest,
  routeExecutionRequest,
  createAcquisitionMissionEngine,
  createMemoryAmoStore,
  specialistContext,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  runMaxAcquisitionApproach,
} = require('../../max/workspace/MaxAcquisitionApproachExecutor');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-248 — Canonical acquisition approach decision', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  async function throughPrioritization() {
    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      question: 'Approved discovery.',
      allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      question: 'Approved prioritization.',
    });
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  async function decideApproach(approach) {
    await throughPrioritization();
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      tenantId: '10',
      intent: EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH,
      payload: { approach, question: `Decide ${approach}.` },
    });
    return routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      operatorId: 'operator-test',
      allowFixtureFallback: true,
    });
  }

  it('target prioritization no longer emits fixed outbound delegation', async () => {
    const snapshot = await throughPrioritization();
    const max = snapshot.contributions.find(
      (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
    );

    assert.ok(max);
    assert.ok(Array.isArray(max.payload.priorities));
    assert.equal(Object.prototype.hasOwnProperty.call(max.payload, 'delegation'), false);
    assert.equal(snapshot.acquisitionApproach, null);
  });

  it('PLAN cannot complete into PREPARE without a canonical acquisition approach decision', async () => {
    await throughPrioritization();
    engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage: STAGES.PLAN });

    assert.throws(
      () => engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage: STAGES.PREPARE }),
      (err) => err.code === 'amo_stage_blocked' && /Acquisition approach decision/i.test(err.message)
    );
  });

  it('OUTBOUND approach permits existing Paige and Emmett preparation', async () => {
    const routed = await decideApproach(ACQUISITION_APPROACHES.OUTBOUND);
    assert.equal(routed.specialist, SPECIALISTS.MAX);
    assert.equal(routed.executionResult.approach.kind, CONTRIBUTION_KINDS.ACQUISITION_APPROACH);

    const paige = await advancePaigeVariants({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });
    assert.equal(paige.executionOutcome, 'completed');

    const emmett = await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });
    assert.equal(emmett.executionOutcome, 'completed');
    assert.equal(engine.inspect(mission.id, { tenantId: '10' }).mission.stage, STAGES.READY);
  });

  it('BOTH permits outbound preparation and records paid preparation as unsupported', async () => {
    await decideApproach(ACQUISITION_APPROACHES.BOTH);
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });

    assert.equal(snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.BOTH);
    assert.deepEqual(snapshot.acquisitionApproach.unsupportedCapabilities, ['paid_preparation']);
    assert.equal(specialistContext(snapshot.contributions).acquisitionApproachPermitsOutbound, true);

    const paige = await advancePaigeVariants({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      allowFixtureFallback: true,
    });
    assert.equal(paige.executionOutcome, 'completed');
  });

  it('PAID does not invoke Paige or Emmett and exposes unsupported paid preparation', async () => {
    const routed = await decideApproach(ACQUISITION_APPROACHES.PAID);
    const snapshot = routed.snapshot;
    const ctx = specialistContext(snapshot.contributions);

    assert.equal(snapshot.mission.stage, STAGES.PLAN);
    assert.equal(snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.PAID);
    assert.equal(ctx.acquisitionApproachPermitsOutbound, false);
    assert.ok(snapshot.blocker);
    assert.match(snapshot.blocker.reason, /Paid acquisition preparation is not implemented/i);
    assert.equal(snapshot.contributions.some((row) => row.specialist === SPECIALISTS.PAIGE), false);
    assert.equal(snapshot.contributions.some((row) => row.specialist === SPECIALISTS.EMMETT), false);
  });

  it('DEFER stops channel-specific progression', async () => {
    await decideApproach(ACQUISITION_APPROACHES.DEFER);

    await assert.rejects(
      () => advancePaigeVariants({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        allowFixtureFallback: true,
      }),
      /Selected acquisition approach does not permit outbound preparation|Paige execution requires stage prepare/i
    );
  });

  it('BLOCKED approach is durable and fails closed', async () => {
    await throughPrioritization();
    const result = await advanceAcquisitionApproach({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      runMaxApproach: async () => runMaxAcquisitionApproach({
        transactionId: 'sec_blocked_approach',
        specialistInput: {
          structuredMission: { objective: OBJECTIVE },
          maxPrioritization: { priorities: [] },
        },
        contributions: [],
        mission: engine.get(mission.id, '10'),
      }),
    });

    assert.equal(result.approach.payload.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.BLOCKED);
    assert.equal(engine.inspect(mission.id, { tenantId: '10' }).blocker.kind, 'acquisition_approach_blocked');
  });

  it('acquisition approach contribution survives store snapshot and reload', async () => {
    await decideApproach(ACQUISITION_APPROACHES.OUTBOUND);
    const restoredStore = createMemoryAmoStore();
    restoredStore.restore(engine.store.snapshot());
    const restored = createAcquisitionMissionEngine({ store: restoredStore });
    const snapshot = restored.inspect(mission.id, { tenantId: '10' });

    assert.equal(snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.OUTBOUND);
    assert.ok(snapshot.contributions.some(
      (row) => row.kind === CONTRIBUTION_KINDS.ACQUISITION_APPROACH
    ));
  });

  it('mission inspection explains approach, rationale, confidence, evidence, blockers, unknowns, and next step', async () => {
    await decideApproach(ACQUISITION_APPROACHES.PAID);
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });

    assert.equal(snapshot.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.PAID);
    assert.ok(snapshot.acquisitionApproach.rationale);
    assert.ok(Number.isFinite(snapshot.acquisitionApproach.confidence));
    assert.ok(Array.isArray(snapshot.acquisitionApproach.evidence));
    assert.ok(Array.isArray(snapshot.acquisitionApproach.blockers));
    assert.ok(Array.isArray(snapshot.acquisitionApproach.unknowns));
    assert.match(snapshot.acquisitionApproach.nextStep, /paid preparation is not implemented/i);
    assert.ok(snapshot.why.reasons.some((row) => /Max selected paid/i.test(row)));
    assert.ok(snapshot.blocker);
  });

  it('existing mission without approach remains readable but cannot silently execute outbound', async () => {
    const snapshot = await throughPrioritization();
    assert.equal(snapshot.mission.stage, STAGES.UNDERSTAND);
    assert.equal(engine.inspect(mission.id, { tenantId: '10' }).acquisitionApproach, null);

    assert.throws(
      () => engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage: STAGES.PREPARE }),
      (err) => err.code === 'amo_stage_blocked' && /Acquisition approach decision/i.test(err.message)
    );
  });

  it('approach selection performs no external action', async () => {
    await decideApproach(ACQUISITION_APPROACHES.OUTBOUND);
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });

    assert.equal(snapshot.outcomes.length, 0);
    assert.equal(snapshot.executionRecords.length, 0);
    assert.equal(snapshot.contributions.some((row) => row.specialist === SPECIALISTS.PAIGE), false);
    assert.equal(snapshot.contributions.some((row) => row.specialist === SPECIALISTS.EMMETT), false);
    assert.equal(snapshot.timeline.includes(EVENT_KINDS.QUEUED), false);
    assert.equal(snapshot.timeline.includes(EVENT_KINDS.LAUNCHED), false);
  });
});
