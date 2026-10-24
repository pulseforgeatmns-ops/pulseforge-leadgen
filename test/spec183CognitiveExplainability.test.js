'use strict';

/**
 * SPEC-183 — Cognitive Explainability acceptance tests (ADR-098).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildExplainabilityGraph,
  traceRecommendation,
  serializeForOperator,
  serializeForAmo,
  deserializeGraph,
  answerOperatorQuestion,
  NODE_KINDS,
  validateExplainabilityGraph,
} = require('../packages/scout/explainability/ExplainabilityGraph');
const {
  buildScoutDiscoveryArtifact,
  resolveScoutInternalReasoning,
} = require('../packages/scout/adapters/ScoutDiscoveryArtifact');
const {
  containsForbiddenReasoningKeys,
} = require('../packages/scout/investigation/MissionIntelligenceReport');
const { createHypothesisInvestigationPlan } = require('../packages/scout/coverage/HypothesisInvestigationPlanner');
const { buildSemanticMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const { explainRecommendation } = require('../packages/acquisition-mission/Inspection');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const { formatCognitiveExplainabilityAnswer } = require('../packages/max/scoutAcquisition/Explainability');

const MISSION = {
  id: 'mission-spec183',
  objectiveText: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
};

function buildFixture() {
  const marketDefinition = buildSemanticMarketDefinition({
    mission: MISSION,
    segments: ['law_firm'],
    geography: 'Manchester NH',
    terminology: ['law firm', 'legal office'],
  });

  const plan = createHypothesisInvestigationPlan({
    mission: MISSION,
    marketDefinition,
    opts: { estimatedMarket: 120 },
  });

  const investigationState = {
    missionId: MISSION.id,
    marketDefinition,
    activeHypotheses: [
      {
        id: 'hyp-terminology',
        text: 'Law firms call themselves legal offices in Manchester.',
        kind: 'terminology',
        lifecycle: 'supported',
        confidence: 0.72,
      },
    ],
    rejectedHypotheses: [
      {
        id: 'hyp-rejected',
        text: 'Multi-tenant towers dominate the market.',
        lifecycle: 'rejected',
      },
    ],
    evidenceGraph: {
      nodes: [
        {
          id: 'ev-1',
          type: 'EVIDENCE',
          data: {
            source: 'google_maps',
            label: 'Harbor Law Group listing',
            providerId: 'google_maps',
            evidenceType: 'identity',
            weight: 0.82,
            observedAt: '2026-08-20T00:00:00.000Z',
          },
        },
      ],
      edges: [],
    },
    investigationPlan: plan,
    evidenceRequirements: plan.evidenceRequirements,
    assignedProviders: plan.assignedProviders,
  };

  const missionIntelligenceReport = {
    kind: 'mission_intelligence_report',
    spec: 'SPEC-159',
    summary: 'Single-tenant law firms show strong commercial cleaning fit.',
    recommendation: {
      kind: 'heuristic_priority',
      summary: 'Prioritize single-tenant law firms in Manchester pilot cluster.',
      confidence: 0.74,
      basedOnUnderstanding: true,
      basedOnHeuristics: true,
      notDirectFromEvidence: true,
    },
    businessUnderstanding: {
      items: [
        {
          entity: 'Harbor Law Group',
          assertions: ['Single-tenant professional office'],
          confidence: 0.8,
        },
      ],
      synthesizedNotRaw: true,
    },
    businessJudgment: {
      overallJudgment: {
        summary: 'Strong fit for commercial cleaning outreach.',
        confidence: 0.74,
      },
    },
    judgmentResult: {
      activatedHeuristics: [
        {
          id: 'h-1',
          heuristicId: 'single_tenant',
          name: 'Single Tenant Office',
          score: 0.82,
          triggeringEvidence: [{ source: 'synthesis', observation: 'Single-tenant professional office' }],
          contradictoryEvidence: [],
        },
      ],
      overallJudgment: {
        summary: 'Strong fit for commercial cleaning outreach.',
        confidence: 0.74,
      },
      basedOnHeuristics: true,
    },
  };

  return { marketDefinition, plan, investigationState, missionIntelligenceReport };
}

function pipelineResultFromFixture(fixture) {
  const { serializeGraph } = require('../packages/scout/explainability/ExplainabilityGraph');
  const graph = buildExplainabilityGraph({
    mission: MISSION,
    investigationState: fixture.investigationState,
    plan: fixture.plan,
    missionIntelligenceReport: fixture.missionIntelligenceReport,
  });

  return {
    status: 'completed',
    confidence: 0.74,
    payload: {
      opportunities: [{ companyId: 'co-1', name: 'Harbor Law Group', fit: 0.78 }],
      qualifiedCount: 1,
      missionObjective: MISSION.objectiveText,
    },
    pipeline: {
      investigationState: fixture.investigationState,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
      investigationPlan: fixture.plan,
      explainabilityGraph: serializeGraph(graph),
    },
  };
}

describe('SPEC-183 — Cognitive Explainability', () => {
  it('buildExplainabilityGraph composes the full cognitive chain', () => {
    const fixture = buildFixture();
    const graph = buildExplainabilityGraph({
      mission: MISSION,
      investigationState: fixture.investigationState,
      plan: fixture.plan,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
    });

    assert.equal(graph.spec, 'SPEC-183');
    assert.ok(graph.recommendationId);

    const trace = traceRecommendation(graph);
    assert.equal(trace.terminatesAtObjective, true);

    const kinds = trace.path.map((node) => node.kind);
    assert.ok(kinds.includes(NODE_KINDS.OBJECTIVE));
    assert.ok(kinds.includes(NODE_KINDS.MARKET_DEFINITION));
    assert.ok(kinds.includes(NODE_KINDS.HYPOTHESIS));
    assert.ok(kinds.includes(NODE_KINDS.PLAN));
    assert.ok(kinds.includes(NODE_KINDS.EVIDENCE));
    assert.ok(kinds.includes(NODE_KINDS.UNDERSTANDING));
    assert.ok(kinds.includes(NODE_KINDS.JUDGMENT));
    assert.ok(kinds.includes(NODE_KINDS.RECOMMENDATION));
  });

  it('every recommendation traces to mission objective — not directly to a provider', () => {
    const fixture = buildFixture();
    const graph = buildExplainabilityGraph({
      mission: MISSION,
      investigationState: fixture.investigationState,
      plan: fixture.plan,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
    });

    const rec = traceRecommendation(graph);
    const recommendationNode = rec.path[rec.path.length - 1];
    assert.equal(recommendationNode.kind, NODE_KINDS.RECOMMENDATION);

    const directParentKinds = recommendationNode.parentIds
      .map((id) => graph.nodes.get(id))
      .filter(Boolean)
      .map((node) => node.kind);
    assert.deepEqual(directParentKinds, [NODE_KINDS.JUDGMENT]);
    assert.ok(rec.path[0].kind === NODE_KINDS.OBJECTIVE);
  });

  it('serializeForOperator produces human-readable cognitive chain', () => {
    const fixture = buildFixture();
    const graph = buildExplainabilityGraph({
      mission: MISSION,
      investigationState: fixture.investigationState,
      plan: fixture.plan,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
    });

    const lines = serializeForOperator(graph);
    assert.ok(lines.some((line) => /Mission Objective/i.test(line)));
    assert.ok(lines.some((line) => /Recommendation/i.test(line)));
    assert.ok(lines.join('\n').includes(MISSION.objectiveText));
  });

  it('serializeForAmo respects SPEC-173 boundary', () => {
    const fixture = buildFixture();
    const graph = buildExplainabilityGraph({
      mission: MISSION,
      investigationState: fixture.investigationState,
      plan: fixture.plan,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
    });

    const amoProjection = serializeForAmo(graph);
    assert.equal(amoProjection.boundaryProjected, true);
    assert.equal(containsForbiddenReasoningKeys(amoProjection), false);
    assert.ok(amoProjection.summary.includes('Mission Objective'));
    assert.ok(amoProjection.terminatesAtObjective);
  });

  it('ScoutDiscoveryArtifact projects cognitive trace from explainability graph', () => {
    const scoutResult = pipelineResultFromFixture(buildFixture());
    const artifact = buildScoutDiscoveryArtifact(scoutResult, { mission: MISSION });

    assert.equal(artifact.explainabilitySpec, 'SPEC-183');
    assert.ok(artifact.cognitiveTrace);
    assert.equal(artifact.cognitiveTrace.terminatesAtObjective, true);
    assert.ok(Array.isArray(artifact.cognitiveTrace.chain));

    const internal = resolveScoutInternalReasoning(scoutResult);
    assert.ok(internal.explainabilityGraph);
    assert.ok(internal.cognitiveTrace);
  });

  it('discovery contribution carries cognitive trace without forbidden keys', () => {
    const scoutResult = pipelineResultFromFixture(buildFixture());
    const contribution = normalizeScoutDiscoveryPayload(scoutResult, {
      missionObjective: MISSION.objectiveText,
    });

    assert.ok(contribution.cognitiveTrace);
    assert.equal(containsForbiddenReasoningKeys(contribution), false);
    assert.ok(contribution.explainabilityGraph);
  });

  it('operator queries traverse the graph — not provider terminals', () => {
    const fixture = buildFixture();
    const graph = buildExplainabilityGraph({
      mission: MISSION,
      investigationState: fixture.investigationState,
      plan: fixture.plan,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
    });

    const linkedinAnswer = answerOperatorQuestion(graph, 'Why are we searching LinkedIn?');
    assert.match(linkedinAnswer, /hypothesis|plan|evidence requirement/i);
    assert.doesNotMatch(linkedinAnswer, /^LinkedIn\.$/);

    const terminologyAnswer = answerOperatorQuestion(graph, 'What terminology did we test?');
    assert.match(terminologyAnswer, /terminolog|law firm|legal office/i);

    const changeAnswer = answerOperatorQuestion(graph, 'What would change the recommendation?');
    assert.match(changeAnswer, /recommendation would change/i);
  });

  it('Inspection explainRecommendation uses cognitive trace derivedFrom chain', () => {
    const scoutResult = pipelineResultFromFixture(buildFixture());
    const contribution = normalizeScoutDiscoveryPayload(scoutResult);
    const explanation = explainRecommendation(
      {
        contributions: [
          {
            specialist: 'scout',
            kind: 'discovery',
            payload: contribution,
          },
        ],
      },
      'Why this recommendation?'
    );

    assert.equal(explanation.terminatesAtObjective, true);
    assert.ok(explanation.derivedFrom.length >= 5);
    assert.match(explanation.summary, /recommendation|judgment|objective/i);
  });

  it('Max cognitive explainability delegates to graph traversal', () => {
    const scoutResult = pipelineResultFromFixture(buildFixture());
    const answer = formatCognitiveExplainabilityAnswer({
      question: 'Why are we searching LinkedIn?',
      explainabilityGraph: scoutResult.pipeline.explainabilityGraph,
    });

    assert.ok(answer);
    assert.equal(answer.spec, 'SPEC-183');
    assert.match(answer.narrative, /hypothesis|plan|evidence/i);
  });

  it('round-trips serialized graph', () => {
    const fixture = buildFixture();
    const graph = buildExplainabilityGraph({
      mission: MISSION,
      investigationState: fixture.investigationState,
      plan: fixture.plan,
      missionIntelligenceReport: fixture.missionIntelligenceReport,
    });
    const { serializeGraph } = require('../packages/scout/explainability/ExplainabilityGraph');
    const restored = deserializeGraph(serializeGraph(graph));
    validateExplainabilityGraph(restored);
    assert.equal(traceRecommendation(restored).terminatesAtObjective, true);
  });
});
