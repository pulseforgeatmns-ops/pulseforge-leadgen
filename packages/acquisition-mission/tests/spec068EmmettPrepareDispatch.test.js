'use strict';

/**
 * SPEC-068 — Canonical Emmett PREPARE Dispatch.
 * PREPARE + paigeComplete → CER → TME → SEC → SPEC-117 → CAPACITY → emmettComplete.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  BLOCKER_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  EXECUTION_STATUSES,
  createExecutionRequest,
  routeExecutionRequest,
  clearExecutionRouterAudit,
  runAutonomousProgression,
  buildExecutionInput,
  assertContract,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  advanceEmmettCapacity,
} = require('../../max/workspace/AmoOperatorApproval');
const { specialistContext } = require('../Lifecycle');
const {
  buildMissionBoundCandidates,
} = require('../../max/workspace/EmmettMissionCandidates');
const { fixtureInfrastructureSnapshot } = require('../../max/workspace/EmmettCapacityExecution');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-068 — Canonical Emmett PREPARE Dispatch', () => {
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

  async function throughPaigeComplete() {
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
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  it('happy path: PREPARE + Paige → GENERATE_CAPACITY → CAPACITY → emmettComplete', async () => {
    const atPrepare = await throughPaigeComplete();
    assert.equal(atPrepare.mission.stage, STAGES.PREPARE);
    const ctxBefore = specialistContext(atPrepare.contributions || []);
    assert.equal(ctxBefore.paigeComplete, true);
    assert.ok(!ctxBefore.emmettComplete);

    const emmettResult = await advanceEmmettCapacity({
      engine,
      mission: atPrepare.mission,
      tenantId: '10',
      allowFixtureFallback: true,
      question: 'Plan capacity.',
    });
    assert.equal(emmettResult.alreadyExecuted, false);
    assert.equal(emmettResult.executionOutcome, 'completed');
    assert.ok(emmettResult.capacity);

    const final = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(final.contributions || []);
    assert.equal(ctx.emmettComplete, true);
    assert.equal(final.mission.stage, STAGES.PREPARE);

    const capacity = final.contributions.find(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    assert.ok(capacity);
    assert.ok(capacity.payload.capacity?.recommended != null);
    assert.ok(Array.isArray(capacity.payload.queue?.items));
    assert.ok(capacity.payload.sendRecommendations?.length);
    assert.ok(capacity.payload.deliverability);
    assert.ok(capacity.payload.governor);
  });

  it('GENERATE_CAPACITY CER routes through ExecutionRouter to Emmett', async () => {
    await throughPaigeComplete();

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.GENERATE_CAPACITY,
      missionId: mission.id,
      operatorId: 'operator',
      stage: STAGES.PREPARE,
    });

    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(routed.specialist, 'emmett');
    assert.equal(routed.action, 'generate_capacity');
    assert.equal(routed.executionResult.executionOutcome, 'completed');

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || []);
    assert.equal(ctx.emmettComplete, true);
  });

  it('autonomous progression completes Emmett after Paige', async () => {
    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine, mission, tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.',
    });

    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
      maxSteps: 16,
    });

    const snapshot = result.snapshot || engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || []);
    assert.equal(ctx.maxComplete, true);
    assert.equal(ctx.paigeComplete, true);
    assert.equal(ctx.emmettComplete, true);
    assert.equal(snapshot.mission.stage, STAGES.PREPARE);
    assert.equal(result.outcome, 'complete');
  });

  it('mission-bound queue candidates originate from Scout/Max not client CRM', async () => {
    const snapshot = await throughPaigeComplete();
    const candidates = buildMissionBoundCandidates(snapshot.mission, snapshot.contributions);
    assert.ok(candidates.length >= 2);
    for (const row of candidates) {
      assert.equal(row.missionBound, true);
      assert.equal(row.source, 'mission_intelligence');
      assert.ok(row.company);
      assert.ok(row.maxPriority != null);
    }
    const maxRow = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.MAX && r.kind === CONTRIBUTION_KINDS.PRIORITIZATION
    );
    const scoutRow = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.SCOUT && r.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
    const topMax = maxRow.payload.rankedTargets?.[0]?.name
      || scoutRow.payload.rankedProspects?.[0]?.name
      || maxRow.payload.priorities?.find((row) => row.name && row.name !== 'Law firms')?.name;
    assert.equal(candidates[0].company, topMax);
  });

  it('Emmett SEC input receives Scout, Max, Paige, and structured mission', async () => {
    const snapshot = await throughPaigeComplete();
    const secInput = buildExecutionInput({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      specialist: SPECIALISTS.EMMETT,
      transactionId: 'sec_emmett_test',
      infrastructureSnapshot: fixtureInfrastructureSnapshot('10'),
    });
    const emmett = secInput.specialistInput;
    assert.ok(emmett.structuredMission);
    assert.ok(emmett.scoutDiscovery);
    assert.ok(emmett.maxPrioritization);
    assert.ok(emmett.paigeReadiness?.ready);
    assert.ok(emmett.missionCandidates?.length);
    assert.ok(emmett.rankedTargets?.length || emmett.priorities?.length);
    assert.ok(emmett.deliverabilityPolicy);
    assert.equal(emmett.missionBound, true);
  });

  it('Emmett output cannot contain forbidden messaging or business fields', async () => {
    await throughPaigeComplete();
    const result = await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });
    const payload = result.capacity.payload;
    assert.throws(
      () => assertContract(SPECIALISTS.EMMETT, { ...payload, subject: 'hi' }),
      (err) => err.code === 'amo_contract_violation'
    );
    assert.throws(
      () => assertContract(SPECIALISTS.EMMETT, { ...payload, body: 'hello' }),
      (err) => err.code === 'amo_contract_violation'
    );
    assert.throws(
      () => assertContract(SPECIALISTS.EMMETT, { ...payload, variants: [{ label: 'x' }] }),
      (err) => err.code === 'amo_contract_violation'
    );
    for (const item of payload.queue?.items || []) {
      assert.equal(item.subject, undefined);
      assert.equal(item.body, undefined);
      assert.equal(item.cta, undefined);
    }
  });

  it('SEC BLOCKED: no CAPACITY, emmettComplete false, stage PREPARE', async () => {
    await throughPaigeComplete();
    await assert.rejects(
      () => advanceEmmettCapacity({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        allowFixtureFallback: true,
        runEmmett: async () => {
          throw new Error('Infrastructure unavailable.');
        },
      }),
      (err) => err.tmeClass === 'validation' || err.tmeClass === 'specialist' || err.code === 'tme_emmett_blocked'
    );
    const after = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(after.contributions || []);
    assert.ok(!ctx.emmettComplete);
    assert.equal(after.mission.stage, STAGES.PREPARE);
  });

  it('contract failure: invalid contribution fails closed', async () => {
    await throughPaigeComplete();
    await assert.rejects(
      () => advanceEmmettCapacity({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        runEmmett: async () => ({
          capacityPayload: { capacity: { recommended: 5 }, cta: 'book now' },
          executionInput: {},
        }),
      }),
      (err) => err.code === 'amo_contract_violation' || err.tmeClass === 'validation'
    );
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.ok(!specialistContext(after.contributions).emmettComplete);
  });

  it('deliverability risk commits canonically and surfaces pause blocker', async () => {
    await throughPaigeComplete();
    const riskyInfra = {
      ...fixtureInfrastructureSnapshot('10'),
      bounceRate: 0.05,
      recentSends: 25,
      complaintRate: 0.002,
      blacklist: { listed: true, sources: ['test'] },
    };
    const result = await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      infrastructureSnapshot: riskyInfra,
    });
    assert.equal(result.executionOutcome, 'completed');
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(specialistContext(snapshot.contributions).emmettComplete, true);
    const capacity = snapshot.contributions.find((r) => r.specialist === SPECIALISTS.EMMETT);
    assert.equal(capacity.payload.capacity.recommended, 0);
    assert.equal(snapshot.blocker?.kind, BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK);
  });

  it('READY eligibility after emmettComplete', async () => {
    await throughPaigeComplete();
    await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || []);
    assert.equal(ctx.emmettComplete, true);
    assert.equal(snapshot.mission.stage, STAGES.PREPARE);
    const readyGate = amo.canEnter(STAGES.READY, ctx);
    assert.equal(readyGate.ok, true);
  });

  it('regression: Scout, Max, and Paige flows unchanged', async () => {
    const atPrepare = await throughPaigeComplete();
    const ctx = specialistContext(atPrepare.contributions || []);
    assert.equal(ctx.maxComplete, true);
    assert.equal(ctx.paigeComplete, true);
    assert.ok(!ctx.emmettComplete);
    const variants = atPrepare.contributions.find(
      (r) => r.specialist === SPECIALISTS.PAIGE && r.kind === CONTRIBUTION_KINDS.VARIANTS
    );
    assert.ok(variants.payload.variants?.length);
  });
});
