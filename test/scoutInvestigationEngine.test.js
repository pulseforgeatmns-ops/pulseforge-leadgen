'use strict';

/**
 * SPEC-142 — Evidence-Driven Investigation Engine tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { Scout, investigation } = require('../packages/scout');

const {
  INVESTIGATION_EVENTS,
  GRAPH_NODE_TYPES,
  HYPOTHESIS_STATUS,
  generateHypotheses,
  generateCandidateHypotheses,
  createInvestigationGraph,
  serializeGraph,
  determineMissingEvidence,
  selectNextInvestigation,
  planInvestigationChain,
  computeClaimConfidence,
  detectContradictions,
  fuseAndUpdateClaims,
  buildInvestigationReport,
  buildSixQuestions,
  runInvestigationEngine,
  listInvestigationLog,
  clearInvestigationLog,
} = investigation;

const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');

function sampleMission(overrides = {}) {
  return {
    id: 'mission-spec142-1',
    tenantId: '10',
    clientId: 10,
    objectiveText: 'Acquire one recurring commercial cleaning client in Manchester NH',
    constraints: {
      vertical: 'property_management',
      locationHint: 'Manchester NH',
      industry: 'commercial_cleaning',
    },
    ...overrides,
  };
}

function sampleCandidates() {
  return [
    {
      id: 'c1',
      name: 'Granite State Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://granitepm.example',
      email: 'info@granitepm.example',
      icpScore: 82,
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-12T00:00:00.000Z',
          source: 'company_website',
          label: '37 managed properties listed on website.',
        },
      ],
      people: [{ name: 'Jane Owner', jobTitle: 'Owner', email: 'jane@granitepm.example' }],
    },
    {
      id: 'c2',
      name: 'Queen City Residences',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://queencity.example',
      icpScore: 75,
      signals: [
        {
          type: 'expansion',
          observedAt: '2026-07-20T00:00:00.000Z',
          source: 'news',
          label: 'Added three buildings to downtown portfolio.',
        },
      ],
      people: [{ name: 'Bob Manager', jobTitle: 'Operations Manager' }],
    },
    {
      id: 'c3',
      name: 'Conflict Co',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://conflict.example',
      icpScore: 70,
      metadata: { ownership: 'Family owned and operated since 1982' },
      evidence: [
        { source: 'website', label: 'Family owned and operated since 1982', kind: 'ownership' },
        { source: 'linkedin', label: '350 employees on LinkedIn', kind: 'company_size' },
      ],
      conflictHints: [
        {
          fieldA: 'ownership',
          valueA: 'Family owned',
          fieldB: 'company_size',
          valueB: '350 employees',
          description: 'Website says family owned; LinkedIn shows 350 employees.',
        },
      ],
      people: [{ name: 'Pat Owner', jobTitle: 'Owner' }],
    },
  ];
}

describe('SPEC-142 — Evidence-Driven Investigation Engine', () => {
  beforeEach(() => {
    clearInvestigationLog();
  });

  it('generates hypotheses before evidence collection', () => {
    const market = buildMarketDefinition({ mission: sampleMission() });
    const hypotheses = generateHypotheses(market, sampleMission());

    assert.ok(hypotheses.length >= 3);
    assert.ok(hypotheses.every((h) => h.text));
    assert.ok(hypotheses.every((h) => h.requiredEvidence.length > 0));
    assert.ok(hypotheses.every((h) => h.confidence === null));
    assert.ok(hypotheses.every((h) => h.status === HYPOTHESIS_STATUS.OPEN));
  });

  it('builds investigation graph with connected nodes', () => {
    const mission = sampleMission();
    const market = buildMarketDefinition({ mission });
    const candidates = sampleCandidates();
    const graph = createInvestigationGraph({ mission, market, candidates });
    const serialized = serializeGraph(graph);

    assert.ok(serialized.nodes.some((n) => n.type === GRAPH_NODE_TYPES.MISSION));
    assert.ok(serialized.nodes.some((n) => n.type === GRAPH_NODE_TYPES.MARKET));
    assert.equal(serialized.summary.candidates, 3);
    assert.ok(serialized.edges.length > 0);
  });

  it('tracks missing evidence with purpose', () => {
    const market = buildMarketDefinition({ mission: sampleMission() });
    const hypotheses = generateHypotheses(market, sampleMission());
    const missing = determineMissingEvidence({ hypotheses, claims: [] });

    assert.ok(missing.missing.length > 0);
    assert.ok(missing.gapCount > 0);
  });

  it('selects lowest-cost investigation for missing evidence', () => {
    const step = selectNextInvestigation({
      missing: ['decision_maker', 'portfolio_size'],
      attempted: [],
      resolvedGaps: [],
    });

    assert.ok(step);
    assert.ok(step.providerId);
    assert.ok(step.costScore <= 8);
    assert.ok(['free', 'cached'].includes(step.costTier) || step.costTier === 'local');
  });

  it('plans investigation chain website → linkedin → paid providers', () => {
    const chain = planInvestigationChain('decision_maker');
    assert.ok(chain.length >= 2);
    assert.equal(chain[0].costScore, Math.min(...chain.map((s) => s.costScore)));
  });

  it('detects contradictions and lowers confidence', () => {
    const candidate = sampleCandidates()[2];
    const conflicts = detectContradictions(candidate, candidate.evidence);
    assert.ok(conflicts.length >= 1);

    const highConf = computeClaimConfidence(
      [{ weight: 0.9, source: 'website' }, { weight: 0.85, source: 'linkedin' }],
      []
    );
    const lowConf = computeClaimConfidence(
      [{ weight: 0.9, source: 'website' }, { weight: 0.85, source: 'linkedin' }],
      conflicts
    );
    assert.ok(lowConf < highConf);
  });

  it('derives claims with confidence from fused evidence', () => {
    const candidate = sampleCandidates()[0];
    const market = buildMarketDefinition({ mission: sampleMission() });
    const hyps = generateCandidateHypotheses(generateHypotheses(market), candidate);
    const { claims } = fuseAndUpdateClaims(candidate, hyps);

    assert.ok(claims.length >= 1);
    assert.ok(claims.every((c) => c.confidence >= 0));
    assert.ok(claims.some((c) => (c.supportedBy || []).length > 0));
  });

  it('answers six questions for every recommendation', () => {
    const six = buildSixQuestions(
      {
        name: 'ABC Property',
        claim: 'ABC manages multiple STR properties.',
        confidence: 0.91,
        supportedBy: [{ source: 'website' }, { source: 'linkedin' }],
        missingEvidence: ['county_records'],
      },
      { nextStep: { providerLabel: 'County Records', gap: 'portfolio_size' } }
    );

    assert.ok(six.whatDoIBelieve);
    assert.ok(six.whyDoIBelieveIt);
    assert.equal(six.howConfidentAmI, 0.91);
    assert.ok(six.whatEvidenceSupportsIt.length >= 2);
    assert.deepEqual(six.whatEvidenceIsStillMissing, ['county_records']);
    assert.match(six.whatIsTheNextBestInvestigation, /County Records/i);
  });

  it('builds investigation report deliverable', () => {
    const report = buildInvestigationReport({
      mission: sampleMission(),
      marketDefinition: buildMarketDefinition({ mission: sampleMission() }),
      graph: { summary: { conflicts: 1 } },
      claims: [
        { id: 'c1', confidence: 0.91, missingEvidence: [], supportedBy: [{ source: 'website' }] },
        { id: 'c2', confidence: 0.56, missingEvidence: ['decision_maker'], supportedBy: [] },
      ],
      hypotheses: [],
      missingEvidence: { missing: ['decision_maker'], currentConfidence: 0.56 },
      overallConfidence: 0.735,
      conflicts: [{ id: 'conflict-1' }],
      ranking: {
        rankedOpportunities: [
          { rank: 1, name: 'Granite State PM', companyId: 'c1', rankScore: 90, tier: 'strong', reasons: ['High fit'] },
        ],
      },
      qualification: { qualifiedCount: 2, watchCount: 1, rejectedCount: 0 },
      candidateUniverse: { candidates: sampleCandidates(), estimatedMarket: 94 },
    });

    assert.equal(report.kind, 'investigation_report');
    assert.equal(report.missionIntelligence.claims, 2);
    assert.equal(report.missionIntelligence.highConfidence, 1);
    assert.equal(report.missionIntelligence.needsInvestigation, 1);
    assert.equal(report.missionIntelligence.conflicts, 1);
    assert.ok(report.recommendations.length >= 1);
    assert.ok(report.recommendations[0].sixQuestions);
    assert.equal(report.acceptanceCriteria.allRecommendationsAnswered, true);
  });

  it('runs investigation loop until confidence or evidence exhausted', async () => {
    const mission = sampleMission();
    const candidates = sampleCandidates();

    const result = await runInvestigationEngine({
      mission,
      opts: {
        discover: async () => candidates,
        companies: candidates,
        estimatedMarket: 4,
        maxIterations: 6,
        confidenceThreshold: 0.5,
      },
    });

    assert.ok(['completed', 'partial'].includes(result.outcome));
    assert.ok(result.iterations.length >= 1);
    assert.ok(result.hypotheses.length >= 3);
    assert.ok(result.claims.length >= 1);
    assert.ok(result.graph);
    assert.equal(result.report.kind, 'investigation_report');
    assert.ok(result.overallConfidence > 0);

    const events = listInvestigationLog();
    assert.ok(events.some((e) => e.event === INVESTIGATION_EVENTS.STARTED));
    assert.ok(events.some((e) => e.event === INVESTIGATION_EVENTS.COMPLETED));
  });

  it('Scout.investigate returns investigation report and graph', async () => {
    const mission = sampleMission();
    const candidates = sampleCandidates();

    const result = await Scout.investigate({
      mission,
      opts: {
        discover: async () => candidates,
        companies: candidates,
        estimatedMarket: 4,
        maxIterations: 6,
        confidenceThreshold: 0.5,
      },
    });

    assert.ok(result.investigationReport);
    assert.equal(result.investigationReport.kind, 'investigation_report');
    assert.ok(result.investigationGraph);
    assert.ok(result.hypotheses.length >= 1);
    assert.ok(result.claims.length >= 1);
    assert.ok(result.missingEvidence);
    assert.ok(['DISCOVERY_COMPLETED', 'DISCOVERY_PARTIAL'].includes(result.outcome));
  });

  it('blocks investigation when market definition is invalid', async () => {
    const result = await runInvestigationEngine({
      mission: sampleMission({ constraints: {}, objectiveText: '' }),
      scoutPayload: {},
      opts: {},
    });

    assert.equal(result.outcome, 'blocked');
    assert.equal(result.completionReason, 'blocked');
  });
});
