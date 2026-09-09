'use strict';

/**
 * SPEC-249 — Canonical Penny V1: Paid Acquisition Intelligence.
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
  createExecutionRequest,
  routeExecutionRequest,
  createAcquisitionMissionEngine,
  createMemoryAmoStore,
  buildExecutionInput,
  resolveMissionContinuation,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  VIABILITY,
  buildPaidAcquisitionRecommendationPayload,
} = require('../../max/workspace/PennyPaidAcquisitionExecutor');

const TENANT_ID = '10';
const OBJECTIVE = 'Acquire recurring commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-249 — Canonical Penny paid acquisition intelligence', () => {
  let engine;
  let mission;
  let legacyRequired;

  beforeEach(() => {
    engine = createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: TENANT_ID,
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    legacyRequired = false;
    const legacyPath = require.resolve('../../../pennyAgent');
    delete require.cache[legacyPath];
  });

  async function throughPrioritization() {
    await advancePlanAfterApproval({ engine, mission, tenantId: TENANT_ID, question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      question: 'Approved discovery.',
      allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine,
      mission: engine.get(mission.id, TENANT_ID),
      tenantId: TENANT_ID,
      question: 'Approved prioritization.',
    });
  }

  async function decideApproach(approach = ACQUISITION_APPROACHES.PAID) {
    await throughPrioritization();
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH,
      payload: { approach, question: `Decide ${approach}.` },
    });
    return routeExecutionRequest(request, {
      engine,
      tenantId: TENANT_ID,
      operatorId: 'operator-test',
      allowFixtureFallback: true,
    });
  }

  async function assessPaid(payload = {}, approach = ACQUISITION_APPROACHES.PAID) {
    await decideApproach(approach);
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.ASSESS_PAID_ACQUISITION,
      payload: {
        question: 'Should we use paid acquisition now?',
        ...payload,
      },
    });
    return routeExecutionRequest(request, {
      engine,
      tenantId: TENANT_ID,
      operatorId: 'operator-test',
      allowFixtureFallback: true,
    });
  }

  const readyContext = {
    availableBudget: { amount: 600, currency: 'USD', source: 'operator_constraint' },
    conversionReadiness: {
      ready: true,
      source: 'landing_page_readiness',
      evidence: ['Dedicated page and phone/form CTA available.'],
    },
    measurementReadiness: {
      ready: true,
      source: 'tracking_readiness',
      evidence: ['Paid source can be tracked to qualified lead and walkthrough.'],
    },
    candidatePaidChannels: [
      { name: 'Google Search', fit: 'moderate', evidence: ['Existing search intent evidence.'] },
      { name: 'ChatGPT Ads', evidence: ['Operator wants to evaluate emerging answer-channel demand.'] },
      { name: 'Yelp', fit: 'strong', evidence: ['Review-channel buyer research is relevant.'] },
    ],
    acquisitionEvidence: [{
      label: 'Prior outbound evidence shows recurring commercial target is the business objective',
      source: 'scout_max_evidence',
      confidence: 0.72,
    }],
    platformEvidence: [{
      channel: 'Google Search',
      metrics: { impressions: 1200, clicks: 60, conversions: 0 },
      source: 'read_only_google_ads',
    }],
  };

  it('is callable through canonical AMO execution and persists a Penny contribution', async () => {
    const routed = await assessPaid(readyContext);
    const snapshot = routed.snapshot;
    const contribution = snapshot.contributions.find(
      (row) =>
        row.specialist === SPECIALISTS.PENNY &&
        row.kind === CONTRIBUTION_KINDS.PAID_ACQUISITION_RECOMMENDATION
    );

    assert.equal(routed.specialist, SPECIALISTS.PENNY);
    assert.equal(routed.action, 'assess_paid_acquisition');
    assert.ok(contribution);
    assert.equal(snapshot.paidAcquisitionRecommendation.viability, VIABILITY.RECOMMEND_PAID);
    assert.equal(snapshot.workspace.penny.state, 'complete');
    assert.equal(snapshot.executionRecords.length, 0);
  });

  it('receives mission-bound structured context through SEC', async () => {
    await decideApproach(ACQUISITION_APPROACHES.PAID);
    const snapshot = engine.inspect(mission.id, { tenantId: TENANT_ID });
    const input = buildExecutionInput({
      mission: snapshot.mission,
      specialist: SPECIALISTS.PENNY,
      contributions: snapshot.contributions,
      ...readyContext,
    });

    assert.equal(input.spec, 'SPEC-132');
    assert.equal(input.missionBound, true);
    assert.equal(input.structuredOnly, true);
    assert.equal(input.specialistInput.missionId, mission.id);
    assert.equal(input.specialistInput.tenantId, TENANT_ID);
    assert.equal(input.specialistInput.objective, OBJECTIVE);
    assert.equal(input.specialistInput.acquisitionApproach.selectedApproach, ACQUISITION_APPROACHES.PAID);
    assert.equal(input.specialistInput.availableBudget.amount, 600);
    assert.equal(input.specialistInput.candidatePaidChannels.length, 3);
  });

  it('is the canonical mission continuation step after a paid approach decision', async () => {
    await decideApproach(ACQUISITION_APPROACHES.BOTH);
    const snapshot = engine.inspect(mission.id, { tenantId: TENANT_ID });
    const continuation = resolveMissionContinuation(snapshot);

    assert.equal(continuation.kind, 'execute');
    assert.equal(continuation.progression.intent, EXECUTION_INTENTS.ASSESS_PAID_ACQUISITION);
    assert.equal(continuation.progression.action, 'assess_paid_acquisition');
  });

  it('produces a valid SEC result with evidence, confidence, unknowns, and stop/continue/scale conditions', async () => {
    const routed = await assessPaid(readyContext);
    const result = routed.executionResult.executionResult;
    const payload = result.contributions.paidAcquisitionRecommendation;

    assert.equal(result.spec, 'SPEC-132');
    assert.equal(result.status, 'SUCCESS');
    assert.equal(payload.noExternalMutation, true);
    assert.equal(payload.platformMetricsAreEvidenceOnly, true);
    assert.ok(result.evidence.length >= 4);
    assert.ok(Number.isFinite(result.confidence.overall));
    assert.ok(Array.isArray(payload.stopConditions));
    assert.ok(Array.isArray(payload.continueConditions));
    assert.ok(Array.isArray(payload.scaleConditions));
    assert.ok(payload.businessOutcomeChain.includes('recurring_client'));
  });

  it('can recommend no paid when Max selected outbound', async () => {
    await decideApproach(ACQUISITION_APPROACHES.OUTBOUND);
    const snapshot = engine.inspect(mission.id, { tenantId: TENANT_ID });
    const payload = buildPaidAcquisitionRecommendationPayload({
      specialistInput: {
        ...readyContext,
        objective: OBJECTIVE,
        acquisitionApproach: snapshot.acquisitionApproach,
      },
    });

    assert.equal(payload.viability, VIABILITY.RECOMMEND_NO_PAID);
    assert.match(payload.paidAcquisitionRecommendation.rationale, /does not override/i);
  });

  it('defers when budget or readiness evidence is missing and does not invent economics', async () => {
    const routed = await assessPaid({
      candidatePaidChannels: ['Google Search', 'ChatGPT Ads', 'Yelp'],
    });
    const recommendation = routed.snapshot.paidAcquisitionRecommendation;

    assert.equal(recommendation.viability, VIABILITY.DEFER);
    assert.equal(recommendation.budgetConstraints.known, false);
    assert.equal(recommendation.budgetConstraints.invented, false);
    assert.ok(recommendation.unknowns.some((row) => /budget/i.test(row.unknown)));
  });

  it('blocks paid when conversion or measurement infrastructure is inadequate', async () => {
    const routed = await assessPaid({
      availableBudget: { amount: 500, currency: 'USD' },
      conversionReadiness: { ready: false, source: 'landing_page_audit' },
      measurementReadiness: { ready: false, source: 'tracking_audit' },
      candidatePaidChannels: ['Google Search', 'Yelp'],
    });
    const recommendation = routed.snapshot.paidAcquisitionRecommendation;

    assert.equal(recommendation.viability, VIABILITY.BLOCKED);
    assert.ok(recommendation.blockers.some((row) => row.kind === 'conversion_infrastructure_inadequate'));
    assert.ok(recommendation.blockers.some((row) => row.kind === 'measurement_infrastructure_inadequate'));
  });

  it('compares multiple channels and does not default to Google merely because Google evidence exists', async () => {
    const routed = await assessPaid(readyContext);
    const recommendation = routed.snapshot.paidAcquisitionRecommendation;

    assert.equal(recommendation.preferredChannel, 'Yelp');
    assert.deepEqual(
      recommendation.channelAssessments.map((row) => row.channel).sort(),
      ['ChatGPT Ads', 'Google Search', 'Yelp']
    );
  });

  it('missing platform credentials do not crash canonical reasoning', async () => {
    const routed = await assessPaid({
      ...readyContext,
      platformEvidence: [],
    });

    assert.equal(routed.executionResult.executionResult.status, 'SUCCESS');
    assert.equal(routed.snapshot.paidAcquisitionRecommendation.viability, VIABILITY.RECOMMEND_PAID);
  });

  it('Max inspection can explain Penny contribution from durable mission state', async () => {
    await assessPaid(readyContext);
    const restoredStore = createMemoryAmoStore();
    restoredStore.restore(engine.store.snapshot());
    const restored = createAcquisitionMissionEngine({ store: restoredStore });
    const snapshot = restored.inspect(mission.id, { tenantId: TENANT_ID });

    assert.equal(snapshot.paidAcquisitionRecommendation.viability, VIABILITY.RECOMMEND_PAID);
    assert.ok(snapshot.why.reasons.some((row) => /Penny assessed paid acquisition/i.test(row)));
    assert.ok(snapshot.timeline.some((row) => /Penny paid acquisition recommendation committed/i.test(row.label)));
  });

  it('canonical Penny execution does not invoke legacy pennyAgent.js', async () => {
    const legacyPath = require.resolve('../../../pennyAgent');
    require.cache[legacyPath] = {
      id: legacyPath,
      filename: legacyPath,
      loaded: true,
      exports: {
        run: async () => {
          legacyRequired = true;
          throw new Error('legacy pennyAgent.js must not be invoked');
        },
      },
    };

    const routed = await assessPaid(readyContext);

    assert.equal(routed.executionResult.executionResult.status, 'SUCCESS');
    assert.equal(legacyRequired, false);
  });
});
