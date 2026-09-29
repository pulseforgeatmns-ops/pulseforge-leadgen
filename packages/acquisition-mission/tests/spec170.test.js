'use strict';

/**
 * SPEC-170 — Native Acquisition Mission Specialist Execution.
 * Specialists execute against runtime contracts, not runtime implementations.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const { createMissionEngine } = require('../../mission-engine');
const { createBuiltinRegistry } = require('../../capabilities');
const {
  resetEngine,
} = require('../../../services/acquisitionMission');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  buildMissionExecutionContext,
  isNativeAmoExecution,
  RUNTIME_KINDS,
} = amo;

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

function fixtureScoutResult() {
  return {
    status: 'completed',
    confidence: 0.74,
    summary: '2 prospects ranked against the mission objective.',
    payload: {
      opportunities: [
        {
          companyId: 'co-harbor',
          name: 'Harbor Law Group',
          fit: 0.78,
          timing: 0.65,
          confidence: 0.74,
          signals: [
            {
              type: 'hiring',
              label: 'Hiring operations manager',
              source: 'job_board',
            },
            {
              type: 'decision_maker',
              label: 'Alex Morgan, Office Manager',
              source: 'existing_repository',
            },
          ],
          evidenceRefs: [
            {
              label: 'Operations manager job posting',
              snapshot: { source: 'job_board', companyName: 'Harbor Law Group' },
            },
          ],
          unknowns: [{ text: 'Current cleaning vendor unknown.' }],
        },
        {
          companyId: 'co-granite',
          name: 'Granite Legal Partners',
          fit: 0.71,
          timing: 0.58,
          confidence: 0.68,
          signals: [
            {
              type: 'hiring',
              label: 'Hiring office coordinator',
              source: 'linkedin',
            },
          ],
          evidenceRefs: [
            {
              label: 'LinkedIn hiring post',
              snapshot: { source: 'linkedin', companyName: 'Granite Legal Partners' },
            },
          ],
          unknowns: [],
        },
      ],
      qualifiedCount: 2,
    },
    missionIntelligenceReport: {
      recommendation: {
        summary: 'Prioritize Harbor Law Group for outreach.',
        confidence: 0.74,
      },
      strategicDecision: {
        expectedBusinessOutcome: 'Book discovery call with top-ranked firm.',
      },
    },
  };
}

function legacyEngineWithUpdateSpy() {
  const engine = createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
  let updateCount = 0;
  const originalUpdate = engine.store.update.bind(engine.store);
  engine.store.update = async (...args) => {
    updateCount += 1;
    return originalUpdate(...args);
  };
  return { engine, getUpdateCount: () => updateCount };
}

describe('SPEC-170 — Native Acquisition Mission Specialist Execution', () => {
  beforeEach(() => {
    resetEngine();
  });

  it('Scenario 1: plan approval → discovery approval executes successfully', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Proceed with this plan.',
    });
    assert.equal(planResult.alreadyExecuted || planResult.rolledBack, undefined);

    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: planResult.snapshot.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      runScout: async () => fixtureScoutResult(),
    });

    assert.equal(discoveryResult.executionOutcome, 'completed');
    assert.ok(discoveryResult.discovery);
    assert.equal(discoveryResult.discovery.specialist, 'scout');
    assert.equal(discoveryResult.discovery.kind, 'discovery');
    assert.ok(discoveryResult.discovery.payload.companies.length >= 1);
  });

  it('Scenario 2: no MissionStore.update() during Acquisition Mission discovery execution', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const { engine: legacyEngine, getUpdateCount } = legacyEngineWithUpdateSpy();

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      runScout: async () => fixtureScoutResult(),
    });

    assert.equal(getUpdateCount(), 0);
    assert.ok(legacyEngine);
  });

  it('Scenario 3: AMO mission id is never resolved through the SPEC-022 mission store', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const { engine: legacyEngine } = legacyEngineWithUpdateSpy();
    let legacyGetCount = 0;
    const originalGet = legacyEngine.store.get?.bind(legacyEngine.store);
    if (originalGet) {
      legacyEngine.store.get = async (id) => {
        if (id === mission.id) legacyGetCount += 1;
        return originalGet(id);
      };
    }

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      runScout: async () => fixtureScoutResult(),
    });

    assert.equal(legacyGetCount, 0);
  });

  it('Scenario 4: discovery contribution is written only through TME commit (single contribution)', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      runScout: async () => fixtureScoutResult(),
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const scoutContributions = (snapshot.contributions || []).filter(
      (row) => row.specialist === 'scout' && row.kind === 'discovery'
    );
    assert.equal(scoutContributions.length, 1);
    assert.equal(scoutContributions[0].payload.approvalConsumed, true);
  });

  it('Scenario 5: Mission Intelligence Report is stored on the AMO discovery contribution', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const scoutFixture = fixtureScoutResult();

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      runScout: async () => scoutFixture,
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const discovery = (snapshot.contributions || []).find(
      (row) => row.specialist === 'scout' && row.kind === 'discovery'
    );
    assert.ok(discovery);
    assert.ok(discovery.payload.missionIntelligenceReport);
    assert.match(
      discovery.payload.missionIntelligenceReport.recommendation.summary,
      /Harbor Law Group/i
    );
  });

  it('Scenario 6: removing Mission Engine from path does not change discovery behavior', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const scoutFixture = fixtureScoutResult();

    const result = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      runScout: async () => scoutFixture,
    });

    assert.equal(result.executionOutcome, 'completed');
    assert.equal(result.discovery.payload.qualifiedCount, 2);
    assert.equal(result.discovery.payload.companies.length, 2);
    assert.ok(result.discovery.payload.rankedProspects.length >= 2);
    assert.ok(result.discovery.payload.summary);
    assert.ok(result.discovery.payload.confidence > 0);
  });

  it('Scenario 7: buildMissionExecutionContext exposes runtime contract for future specialists', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const ctx = buildMissionExecutionContext({
      engine,
      mission,
      tenantId: '10',
      transactionId: 'txn-spec170',
    });

    assert.equal(ctx.runtime, RUNTIME_KINDS.ACQUISITION_MISSION);
    assert.equal(ctx.mission.id, mission.id);
    assert.equal(ctx.transaction.id, 'txn-spec170');
    assert.equal(ctx.transaction.tenantId, '10');
    assert.ok(ctx.workspace);
    assert.ok(ctx.intelligence);
    assert.equal(ctx.persistence.runtime, 'amo');
    assert.equal(ctx.persistence.suppressSideEffects, true);
    assert.equal(ctx.persistence.pool, undefined);
    assert.equal(ctx.persistence.engine, undefined);
    assert.doesNotThrow(() => JSON.stringify(ctx));
    assert.ok(isNativeAmoExecution(ctx));
  });
});
