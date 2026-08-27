'use strict';

/**
 * AUDIT-066 — Canonical Max Post-Discovery Dispatch.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_STATUSES,
} = amo;
const {
  EXECUTION_INTENTS,
  specialistForIntent,
} = require('../ExecutionRequest');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  findLatestMaxPrioritization,
} = require('../../max/workspace/AmoOperatorApproval');
const { specialistContext, canEnter } = amo;

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('AUDIT-066 — Canonical Max Post-Discovery Dispatch', () => {
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

  async function throughDiscovery() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    return advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
  }

  it('happy path: Scout discovery → prioritization approval → Max SEC → PRIORITIZATION contribution → UNDERSTAND', async () => {
    await throughDiscovery();

    const result = await advancePrioritizationAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved prioritization.',
    });

    assert.equal(result.alreadyExecuted, false);
    assert.ok(result.prioritization);
    assert.equal(result.prioritization.specialist, SPECIALISTS.MAX);
    assert.equal(result.prioritization.kind, CONTRIBUTION_KINDS.PRIORITIZATION);
    assert.ok(Array.isArray(result.prioritization.payload.priorities));
    assert.ok(Array.isArray(result.prioritization.payload.objectives));
    assert.ok(result.prioritization.payload.objectiveReason);
    assert.ok(result.prioritization.payload.timing);
    assert.ok(Array.isArray(result.prioritization.payload.recommendations));
    assert.ok(Array.isArray(result.prioritization.payload.constraints));
    assert.deepEqual(result.prioritization.payload.delegation, {
      paige: 'variants',
      emmett: 'capacity',
    });
    assert.equal(result.maxResult.status, EXECUTION_STATUSES.SUCCESS);

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.UNDERSTAND);
    assert.equal(findLatestMaxPrioritization(snapshot.contributions).id, result.prioritization.id);

    const ctx = specialistContext(snapshot.contributions || []);
    assert.equal(ctx.maxComplete, true);
    const planGate = canEnter(STAGES.PLAN, ctx);
    assert.equal(planGate.ok, true);
  });

  it('contract failure: invalid Max contribution fails closed with no false maxComplete', async () => {
    await throughDiscovery();

    await assert.rejects(
      () => advancePrioritizationAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved prioritization.',
        runMax: async () => ({
          spec: 'SPEC-132',
          status: EXECUTION_STATUSES.SUCCESS,
          confidence: { overall: 0.5, evidence: 0.5, fit: 0.5, completeness: 0.5 },
          evidence: [{
            label: 'Fixture evidence',
            source: 'test',
            confidence: 0.5,
            timestamp: '2026-08-01T00:00:00.000Z',
            provenance: { kind: 'observed', source: 'test' },
          }],
          contributions: { priorities: [] },
          recommendations: [],
          unknowns: [],
          nextActions: [],
          audit: { transactionId: 'tme_invalid', specialist: SPECIALISTS.MAX },
          explainability: {
            whyRecommended: [],
            whyNotRecommended: [],
            evidenceConfidenceChanges: [],
            remainsUnknown: [],
          },
        }),
      }),
      (err) => err.code === 'tme_contract' || err.code === 'tme_validation' || err.code === 'amo_contract_empty'
    );

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.DISCOVER);
    assert.equal(findLatestMaxPrioritization(snapshot.contributions), undefined);
    const ctx = specialistContext(snapshot.contributions || []);
    assert.ok(!ctx.maxComplete);
    const prepareGate = canEnter(STAGES.PREPARE, ctx);
    assert.equal(prepareGate.ok, false);
  });

  it('execution failure: Max BLOCKED does not commit completion and mission stays recoverable', async () => {
    await throughDiscovery();

    await assert.rejects(
      () => advancePrioritizationAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved prioritization.',
        runMax: async () => ({
          spec: 'SPEC-132',
          status: EXECUTION_STATUSES.BLOCKED,
          blocked: {
            reason: 'Missing discovery intelligence.',
            requiredPrecondition: 'discovery_contribution',
          },
          confidence: { overall: 0, evidence: 0, fit: 0, completeness: 0 },
          evidence: [],
          contributions: {},
          recommendations: [],
          unknowns: [],
          nextActions: [],
          audit: { transactionId: 'tme_blocked', specialist: SPECIALISTS.MAX },
          explainability: {
            whyRecommended: [],
            whyNotRecommended: ['Missing discovery intelligence.'],
            evidenceConfidenceChanges: [],
            remainsUnknown: [],
          },
        }),
      }),
      (err) => err.code === 'tme_max_blocked' || err.code === 'tme_validation'
    );

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.DISCOVER);
    assert.equal(findLatestMaxPrioritization(snapshot.contributions), undefined);
  });

  it('execution failure: Max throw rolls back without PRIORITIZATION contribution', async () => {
    await throughDiscovery();

    await assert.rejects(
      () => advancePrioritizationAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved prioritization.',
        runMax: async () => {
          throw new Error('Max runtime unavailable.');
        },
      }),
      /Max runtime unavailable|tme_max_blocked|sec_validation/
    );

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.DISCOVER);
    assert.equal(findLatestMaxPrioritization(snapshot.contributions), undefined);
  });

  it('ownership: APPROVE_PRIORITIZATION maps to Max and advancePrioritizationAfterApproval does not invoke Emmett', async () => {
    assert.equal(specialistForIntent(EXECUTION_INTENTS.APPROVE_PRIORITIZATION), 'max');

    await throughDiscovery();

    const result = await advancePrioritizationAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved prioritization.',
    });

    assert.equal(result.prioritization.specialist, SPECIALISTS.MAX);
    const emmettRows = (result.snapshot.contributions || []).filter(
      (row) => row.specialist === SPECIALISTS.EMMETT
    );
    assert.equal(emmettRows.length, 0);
  });

  it('regression: Scout discovery/review behavior remains unchanged', async () => {
    const discoveryResult = await throughDiscovery();

    assert.equal(discoveryResult.executionOutcome, 'completed');
    assert.ok(discoveryResult.discovery);
    assert.equal(discoveryResult.discovery.specialist, SPECIALISTS.SCOUT);
    assert.equal(discoveryResult.discovery.kind, CONTRIBUTION_KINDS.DISCOVERY);

    const afterDiscovery = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(afterDiscovery.mission.stage, STAGES.DISCOVER);
    assert.equal(afterDiscovery.mission.pendingOperatorDecision.kind, 'prioritization_approval');
    assert.equal(findLatestMaxPrioritization(afterDiscovery.contributions), undefined);
  });
});
