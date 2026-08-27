'use strict';

/**
 * AUDIT-067 / Canonical Paige Post-Max Dispatch.
 * Max PRIORITIZATION → PLAN → PREPARE → Paige SEC → VARIANTS → paigeComplete.
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
  runAutonomousProgression,
  buildExecutionInput,
  EXECUTION_STATUSES,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  fixturePaigeVariantsResult,
} = require('../../max/workspace/AmoOperatorApproval');
const { specialistContext } = require('../Lifecycle');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('AUDIT-067 — Canonical Paige Post-Max Dispatch', () => {
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

  async function throughUnderstand() {
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
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  it('happy path: Max PRIORITIZATION → PLAN → PREPARE → Paige → VARIANTS → paigeComplete', async () => {
    const atUnderstand = await throughUnderstand();
    assert.equal(atUnderstand.mission.stage, STAGES.UNDERSTAND);

    const ctxAtUnderstand = specialistContext(atUnderstand.contributions || []);
    assert.equal(ctxAtUnderstand.maxComplete, true);

    const maxResult = await advanceMaxPrioritization({
      engine,
      mission: atUnderstand.mission,
      tenantId: '10',
      allowFixtureFallback: true,
    });
    assert.equal(maxResult.alreadyExecuted, true);
    assert.ok(maxResult.prioritization);

    const afterMax = engine.inspect(mission.id, { tenantId: '10' });
    const ctxAfterMax = specialistContext(afterMax.contributions || []);
    assert.equal(ctxAfterMax.maxComplete, true);
    assert.ok(!ctxAfterMax.paigeComplete);

    const paigeResult = await advancePaigeVariants({
      engine,
      mission: afterMax.mission,
      tenantId: '10',
      allowFixtureFallback: true,
      question: 'Generate variants.',
    });
    assert.equal(paigeResult.alreadyExecuted, false);
    assert.equal(paigeResult.executionOutcome, 'completed');
    assert.ok(paigeResult.variants);

    const final = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(final.mission.stage, STAGES.PREPARE);
    const ctx = specialistContext(final.contributions || []);
    assert.equal(ctx.paigeComplete, true);
    assert.ok(!ctx.emmettComplete);

    const variants = final.contributions.find(
      (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
    );
    assert.ok(variants);
    assert.ok(Array.isArray(variants.payload.variants));
    assert.ok(variants.payload.variants.length > 0);
    assert.ok(variants.payload.subjects?.length);
    assert.ok(variants.payload.cta);
  });

  it('autonomous progression completes Max + Paige after prioritization approval', async () => {
    await throughUnderstand();

    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
      maxSteps: 12,
    });

    const snapshot = result.snapshot || engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || []);
    assert.equal(ctx.maxComplete, true);
    assert.equal(ctx.paigeComplete, true);
    assert.equal(snapshot.mission.stage, STAGES.READY);
    assert.equal(result.outcome, 'paused');
    assert.equal(result.pause?.requiredDecision, 'Authorize external execution of prepared outreach?');
  });

  it('Paige SEC input receives Max prioritization and Scout intelligence', async () => {
    const snapshot = await throughUnderstand();
    await advanceMaxPrioritization({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    const afterMax = engine.inspect(mission.id, { tenantId: '10' });
    const secInput = buildExecutionInput({
      mission: afterMax.mission,
      contributions: afterMax.contributions,
      specialist: SPECIALISTS.PAIGE,
      transactionId: 'sec_test',
    });

    const paige = secInput.specialistInput;
    assert.ok(paige.structuredMission);
    assert.ok(paige.scoutDiscovery);
    assert.ok(paige.maxPrioritization);
    assert.ok(paige.priorities?.length || paige.rankedTargets?.length);
    assert.ok(paige.objectives?.length);
    assert.ok(paige.objectiveReason);
    assert.ok(paige.recommendations?.length);
    assert.ok(paige.missionBound);
    assert.equal(paige.structuredOnly, true);
    assert.ok(secInput.workspaceContext?.scout);
    assert.ok(secInput.workspaceContext?.max);
  });

  it('Paige cannot emit forbidden ownership fields', async () => {
    const snapshot = await throughUnderstand();
    await advanceMaxPrioritization({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    const invalidPayload = fixturePaigeVariantsResult(snapshot.mission, snapshot.contributions);
    invalidPayload.recipients = ['a@example.com'];
    assert.throws(
      () => amo.assertContract(SPECIALISTS.PAIGE, invalidPayload),
      (err) => err.code === 'amo_contract_violation'
    );

    invalidPayload.recipients = undefined;
    invalidPayload.queue = [{ id: 1 }];
    assert.throws(
      () => amo.assertContract(SPECIALISTS.PAIGE, invalidPayload),
      (err) => err.code === 'amo_contract_violation'
    );
  });

  it('invalid Paige output: no VARIANTS, paigeComplete false, Emmett not invoked', async () => {
    const snapshot = await throughUnderstand();
    await advanceMaxPrioritization({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      allowFixtureFallback: true,
    });
    const afterMax = engine.inspect(mission.id, { tenantId: '10' });
    engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage: STAGES.PLAN });
    engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage: STAGES.PREPARE });

    await assert.rejects(
      () => advancePaigeVariants({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        allowFixtureFallback: true,
        runPaige: async () => ({
          variants: [],
          subjects: [],
          cta: null,
        }),
      }),
      (err) => err.tmeClass === 'validation' || err.code === 'amo_contract_empty'
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(after.contributions || []);
    assert.ok(!ctx.paigeComplete);
    assert.ok(!ctx.emmettComplete);
    assert.throws(
      () => engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage: STAGES.READY }),
      (err) => err.code === 'amo_stage_blocked'
    );
  });

  it('SEC BLOCKED: mission recoverable, no false completion', async () => {
    const snapshot = await throughUnderstand();
    await advanceMaxPrioritization({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    await assert.rejects(
      () => advancePaigeVariants({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        allowFixtureFallback: true,
        runPaige: async () => {
          const err = new Error('Paige blocked.');
          err.code = 'paige_blocked';
          throw err;
        },
      }),
      (err) => err.tmeClass === 'validation' || err.tmeClass === 'specialist'
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(after.contributions || []);
    assert.ok(!ctx.paigeComplete);
    assert.equal(ctx.maxComplete, true);
    assert.ok([STAGES.UNDERSTAND, STAGES.PLAN, STAGES.PREPARE].includes(after.mission.stage));
  });

  it('GENERATE_VARIANTS CER routes through ExecutionRouter to Paige', async () => {
    await throughUnderstand();
    await advanceMaxPrioritization({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.GENERATE_VARIANTS,
      missionId: mission.id,
      operatorId: 'operator',
      stage: STAGES.PREPARE,
    });

    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(routed.specialist, 'paige');
    assert.equal(routed.action, 'generate_variants');
    assert.equal(routed.executionResult.executionOutcome, 'completed');

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = specialistContext(snapshot.contributions || []);
    assert.equal(ctx.paigeComplete, true);
  });

  it('regression: Scout, operator approval, and Max post-discovery dispatch unchanged', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    const discovery = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
    assert.equal(discovery.executionOutcome, 'completed');

    const afterDiscovery = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(afterDiscovery.mission.stage, STAGES.DISCOVER);
    assert.equal(
      afterDiscovery.mission.pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
    );

    const prioritization = await advancePrioritizationAfterApproval({
      engine,
      mission: afterDiscovery.mission,
      tenantId: '10',
      question: 'Approved prioritization.',
    });
    assert.equal(prioritization.alreadyExecuted, false);

    const afterPrioritization = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(afterPrioritization.mission.stage, STAGES.UNDERSTAND);
    assert.equal(afterPrioritization.mission.pendingOperatorDecision, null);
    assert.ok(afterPrioritization.contributions.some((row) => row.specialist === SPECIALISTS.SCOUT));
    assert.ok(
      afterPrioritization.contributions.some(
        (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
      )
    );
    const ctx = specialistContext(afterPrioritization.contributions || []);
    assert.equal(ctx.maxComplete, true);
  });
});
