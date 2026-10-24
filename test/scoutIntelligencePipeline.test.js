'use strict';

/**
 * SPEC-141 — Scout Intelligence Pipeline tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  Scout,
  investigate,
  intelligence,
} = require('../packages/scout');

const {
  INTELLIGENCE_STAGES,
  SCOUT_INTELLIGENCE_EVENTS,
  buildMarketDefinition,
  buildEvidencePlan,
  buildProviderStrategy,
  fuseCandidateEvidence,
  buildIntelligenceReport,
  runIntelligencePipeline,
  createDefaultProviderRegistry,
  clearIntelligenceLog,
  listIntelligenceLog,
} = intelligence;

function sampleMission(overrides = {}) {
  return {
    id: 'mission-test-1',
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
      name: 'Low Fit LLC',
      industry: 'restaurant',
      location: 'Boston, MA',
      icpScore: 40,
      signals: [],
    },
  ];
}

describe('SPEC-141 — Scout Intelligence Pipeline', () => {
  beforeEach(() => {
    clearIntelligenceLog();
  });

  it('exports Scout.discover as the sole public discovery contract (SPEC-154)', () => {
    assert.equal(typeof Scout.discover, 'function');
    assert.equal(Scout.investigate, undefined);
    assert.equal(typeof investigate, 'function');
  });

  it('Stage 1 — builds market definition from mission', () => {
    const market = buildMarketDefinition({ mission: sampleMission() });
    assert.equal(market.geography, 'Manchester NH');
    assert.equal(market.segment, 'property management');
    assert.match(market.missionGoal, /commercial cleaning/i);
    assert.equal(market.valid, true);
  });

  it('Stage 2 — evidence plan lists requirements before any search', () => {
    const market = buildMarketDefinition({ mission: sampleMission() });
    const plan = buildEvidencePlan(market);
    assert.ok(plan.required.includes('candidate_universe'));
    assert.ok(plan.required.includes('decision_makers'));
    assert.ok(plan.required.includes('property_count'));
    assert.equal(plan.missing.length, plan.required.length);
    assert.match(plan.rationale, /No search executed/i);
  });

  it('Stage 3 — provider strategy prefers lower cost tiers', () => {
    const market = buildMarketDefinition({ mission: sampleMission() });
    const plan = buildEvidencePlan(market);
    const strategy = buildProviderStrategy(plan);
    assert.ok(strategy.assignments.length > 0);
    assert.ok(strategy.providers.includes('existing_pf'));
    assert.deepEqual(strategy.optimizationOrder, ['free', 'cached', 'local', 'paid']);
    const firstAssignment = strategy.assignments[0];
    assert.ok(['free', 'cached'].includes(firstAssignment.costTier));
  });

  it('evidence fusion combines sources with provenance and confidence', () => {
    const candidate = sampleCandidates()[0];
    const fused = fuseCandidateEvidence(candidate);
    assert.ok(fused.evidence.length >= 2);
    assert.ok(fused.confidence >= 0.5);
    assert.ok(fused.sources.length >= 1);
    assert.ok(fused.provenance.every((p) => p.source));
  });

  it('provider registry selects by capability', () => {
    const registry = createDefaultProviderRegistry();
    const emailProviders = registry.findByCapability('emails');
    assert.ok(emailProviders.some((p) => p.id === 'hunter'));
    const selected = registry.selectForCapabilities(['businesses', 'emails']);
    assert.ok(selected.length >= 2);
  });

  it('runs full pipeline with injected candidates and produces intelligence report', async () => {
    const mission = sampleMission();
    const candidates = sampleCandidates();

    const result = await runIntelligencePipeline({
      mission,
      opts: {
        runAcquisitionIntelligence: false,
        discover: async () => candidates,
        companies: candidates,
        estimatedMarket: 4,
      },
    });

    assert.equal(result.stages.length, 10);
    assert.equal(result.stages[0].stage, INTELLIGENCE_STAGES.MARKET_UNDERSTANDING);
    assert.equal(result.stages[1].stage, INTELLIGENCE_STAGES.INVESTIGATION_PLANNING);
    assert.equal(result.stages[6].stage, INTELLIGENCE_STAGES.EVIDENCE_CONFLICT_RESOLUTION);
    assert.equal(result.stages[9].stage, INTELLIGENCE_STAGES.MARKET_COVERAGE);
    assert.ok(result.conflictResolution);
    assert.ok(result.report.evidenceConflicts);
    assert.ok(result.investigationPlan);
    assert.equal(result.investigationPlan.version, 'SPEC-180');
    assert.ok(result.report);
    assert.equal(result.report.kind, 'mission_intelligence_report');
    assert.ok(result.report.investigationStrategy);
    assert.ok(result.report.qualified >= 1);
    assert.ok(result.report.confidence > 0);
    assert.ok(Array.isArray(result.report.evidenceSources));

    const events = listIntelligenceLog();
    assert.ok(events.some((e) => e.event === SCOUT_INTELLIGENCE_EVENTS.STARTED));
    assert.ok(events.some((e) => e.event === SCOUT_INTELLIGENCE_EVENTS.COMPLETED));
  });

  it('Scout.discover executes unified discovery pipeline (SPEC-154)', async () => {
    const mission = sampleMission();
    const candidates = sampleCandidates();

    const result = await Scout.discover({
      mission,
      scoutPayload: {},
      opts: {
        discover: async () => candidates,
        companies: candidates,
        estimatedMarket: 4,
      },
    });

    assert.ok(result.pipeline);
    assert.ok(result.marketDefinition);
    assert.ok(result.coveragePlan);
    assert.ok(result.intelligenceReport);
    assert.match(result.outcome, /^DISCOVERY_/);
    assert.equal(result.pipeline.coverageEngineUsed, true);
  });

  it('intelligence report includes coverage metrics and immediate opportunities', () => {
    const report = buildIntelligenceReport({
      marketDefinition: buildMarketDefinition({ mission: sampleMission() }),
      coverage: {
        estimatedUniverse: 94,
        investigated: 81,
        qualified: 18,
        strong: 7,
        immediate: 2,
        coveragePct: 0.86,
        confidence: 0.92,
        finished: true,
        sourcesUsed: ['google_maps', 'linkedin', 'website'],
      },
      ranking: {
        rankedOpportunities: [
          {
            rank: 1,
            name: 'ABC Property',
            immediate: true,
            reasons: ['Buying signals present'],
          },
        ],
      },
      evidenceCollection: { sourcesUsed: ['google_maps'] },
      providerStrategy: { providers: ['existing_pf', 'google_maps'] },
    });

    assert.equal(report.estimatedUniverse, 94);
    assert.equal(report.coverage, 0.86);
    assert.equal(report.qualified, 18);
    assert.equal(report.strong, 7);
    assert.equal(report.immediate, 2);
    assert.equal(report.confidence, 0.92);
    assert.ok(report.summary.includes('Market:'));
    assert.ok(report.evidenceSources.length >= 2);
  });

  it('blocks pipeline when market definition is invalid', async () => {
    const mission = sampleMission({
      constraints: {},
      objectiveText: '',
    });
    mission.constraints = {};

    const result = await runIntelligencePipeline({
      mission: {
        ...mission,
        constraints: {},
      },
      scoutPayload: {},
      opts: { runAcquisitionIntelligence: false },
    });

    assert.equal(result.outcome, 'blocked');
    assert.equal(result.stages.length, 1);
  });
});
