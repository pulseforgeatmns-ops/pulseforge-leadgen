'use strict';

/**
 * SPEC-173 — Internal Reasoning Boundary.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const { assertContract } = require('../Contracts');
const { normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const {
  buildScoutDiscoveryArtifact,
  resolveScoutInternalReasoning,
} = require('../../scout/adapters/ScoutDiscoveryArtifact');
const {
  buildPublicMissionIntelligenceReport,
  containsForbiddenReasoningKeys,
} = require('../../scout/investigation/MissionIntelligenceReport');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { resetEngine } = require('../../../services/acquisitionMission');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

function internalMirWithHypothesisMetric(hypothesisCount = 17) {
  return {
    kind: 'mission_intelligence_report',
    spec: 'SPEC-159',
    summary: 'Market understood through professional-services terminology.',
    recommendation: {
      summary: 'Prioritize single-tenant law firms in Manchester pilot cluster.',
      confidence: 0.74,
      basedOnUnderstanding: true,
    },
    currentConfidence: 0.74,
    evidenceGraphSummary: {
      nodeCount: 42,
      edgeCount: 18,
      byType: { EVIDENCE: 12, CLAIM: 8, HYPOTHESIS: hypothesisCount },
    },
    businessUnderstanding: {
      items: [{ entity: 'Harbor Law Group', assertions: ['Single-tenant office'], confidence: 0.8 }],
      synthesizedNotRaw: true,
    },
    businessJudgment: {
      overallJudgment: { summary: 'Strong fit for commercial cleaning outreach.', confidence: 0.74 },
    },
    judgmentResult: {
      activatedHeuristics: [{ id: 'h-1', heuristicId: 'single_tenant', name: 'Single Tenant', score: 0.82 }],
      overallJudgment: { summary: 'Strong fit for commercial cleaning outreach.', confidence: 0.74 },
      basedOnHeuristics: true,
    },
    coverage: { coveragePct: 0.68 },
    investigationState: {
      activeHypotheses: [{ id: 'hyp-1', text: 'Law firm terminology', lifecycle: 'supported' }],
      evidenceGraph: {
        nodes: [],
        edges: [],
        summary: {
          candidates: 5,
          claims: 8,
          evidence: 12,
          hypotheses: hypothesisCount,
        },
      },
    },
    hypothesisHistory: [{ id: 'hyp-1', text: 'Law firm terminology', lifecycle: 'supported' }],
    investigativeStrategy: {
      hypotheses: [{ id: 'hyp-1', text: 'Law firm terminology' }],
    },
  };
}

function scoutResultWithInternalReasoning(hypothesisCount = 17) {
  return {
    status: 'completed',
    confidence: 0.74,
    payload: {
      opportunities: [
        {
          companyId: 'co-1',
          name: 'Harbor Law Group',
          fit: 0.78,
          signals: [{ type: 'hiring', label: 'Hiring ops manager', source: 'job_board' }],
          evidenceRefs: [
            {
              id: 'ev-1',
              label: 'Job posting',
              snapshot: { source: 'job_board', companyName: 'Harbor Law Group' },
            },
          ],
        },
      ],
      qualifiedCount: 1,
    },
    pipeline: {
      missionIntelligenceReport: internalMirWithHypothesisMetric(hypothesisCount),
      investigationState: {
        activeHypotheses: [{ id: 'hyp-1', text: 'Law firm terminology', lifecycle: 'supported' }],
        evidenceGraph: {
          nodes: [
            {
              id: 'ev-graph-1',
              type: 'EVIDENCE',
              data: { source: 'website', label: 'Careers page', observedAt: '2026-08-01T00:00:00.000Z' },
            },
          ],
          edges: [],
          summary: { hypotheses: hypothesisCount },
        },
      },
    },
  };
}

describe('SPEC-173 — Internal Reasoning Boundary', () => {
  beforeEach(() => {
    resetEngine();
  });

  it('Scenario 1 — graph summary.hypotheses metric does not leak into contribution', () => {
    const scoutResult = scoutResultWithInternalReasoning(17);
    const artifact = buildScoutDiscoveryArtifact(scoutResult);

    assert.ok(artifact.investigationState?.evidenceGraph?.summary?.hypotheses === 17);
    assert.ok(containsForbiddenReasoningKeys(artifact.missionIntelligenceReport));

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(containsForbiddenReasoningKeys(payload), false);
    assert.doesNotThrow(() => assertContract('scout', payload));
  });

  it('Scenario 2 — public MIR contains conclusions without investigation graph', () => {
    const internal = internalMirWithHypothesisMetric(17);
    const publicMir = buildPublicMissionIntelligenceReport(internal);

    assert.ok(publicMir.businessUnderstanding);
    assert.ok(publicMir.businessJudgment);
    assert.ok(publicMir.recommendation);
    assert.ok(publicMir.evidenceGraphSummary);
    assert.ok(publicMir.coverage);
    assert.equal(publicMir.investigationState, undefined);
    assert.equal(publicMir.hypothesisHistory, undefined);
    assert.equal(publicMir.investigativeStrategy, undefined);
    assert.equal(containsForbiddenReasoningKeys(publicMir), false);
  });

  it('Scenario 3 — replay/debug resolves complete internal investigation graph', () => {
    const scoutResult = scoutResultWithInternalReasoning(17);
    const internal = resolveScoutInternalReasoning(scoutResult);

    assert.ok(internal.investigationState);
    assert.ok(internal.investigationState.evidenceGraph);
    assert.equal(internal.investigationState.evidenceGraph.summary.hypotheses, 17);
    assert.ok(Array.isArray(internal.investigationState.activeHypotheses));
    assert.ok(internal.missionIntelligenceReport.investigationState);
  });

  it('Scenario 4 — Scout internal artifact retains search hypotheses', () => {
    const scoutResult = scoutResultWithInternalReasoning(9);
    const artifact = buildScoutDiscoveryArtifact(scoutResult);

    assert.ok(Array.isArray(artifact.investigationState?.activeHypotheses));
    assert.ok(artifact.investigationState.activeHypotheses.length >= 1);

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.discoveryArtifact.investigationState, undefined);
    assert.equal(containsForbiddenReasoningKeys(payload), false);
  });

  it('Scenario 5 — discovery contribution passes assertContract without sanitization hacks', () => {
    const payload = normalizeScoutDiscoveryPayload(scoutResultWithInternalReasoning(17));
    const result = assertContract('scout', payload);
    assert.equal(result.ok, true);
  });

  it('Scenario 6 — projection preserves evidence, confidence, recommendations, and judgment', () => {
    const scoutResult = scoutResultWithInternalReasoning(17);
    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    const internal = artifact.missionIntelligenceReport;
    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    const publicMir = payload.missionIntelligenceReport;

    assert.equal(payload.evidence.length, artifact.evidence.length);
    assert.equal(payload.confidence, artifact.confidence);
    assert.equal(publicMir.recommendation.summary, internal.recommendation.summary);
    assert.equal(
      publicMir.businessJudgment.overallJudgment.summary,
      internal.businessJudgment.overallJudgment.summary
    );
    assert.deepEqual(publicMir.evidenceGraphSummary, internal.evidenceGraphSummary);
  });

  it('Scenario 7 — end-to-end Anchor mission advances past discovery without contract violation', async () => {
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

    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: planResult.snapshot.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      runScout: async () => scoutResultWithInternalReasoning(17),
    });

    assert.equal(discoveryResult.executionOutcome, 'completed');
    assert.equal(discoveryResult.discovery.specialist, 'scout');
    assert.doesNotThrow(() => assertContract('scout', discoveryResult.discovery.payload));
    assert.equal(
      containsForbiddenReasoningKeys(discoveryResult.discovery.payload),
      false
    );
    assert.ok(discoveryResult.discovery.payload.missionIntelligenceReport.recommendation);
    assert.equal(
      discoveryResult.snapshot.mission.pendingOperatorDecision.kind,
      'prioritization_approval'
    );
  });
});
