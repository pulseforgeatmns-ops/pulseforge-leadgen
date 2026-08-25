'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  estimateCandidateUniverse,
  reviseCandidateUniverseEstimate,
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
} = require('../packages/scout/universe/CandidateUniverseEstimate');
const {
  runDiscoveryPipeline,
  DISCOVERY_PIPELINE_STAGES,
} = require('../packages/scout');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { analyzeMarketCoverage } = require('../packages/scout/intelligence/MarketCoverage');
const { buildDiscoveryReport } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { buildIntelligenceReport } = require('../packages/scout/intelligence/IntelligenceReport');

function samplePlan() {
  return {
    totals: { searches: 12, cities: 3, concepts: 2, sources: 1 },
  };
}

function amoMission() {
  return {
    id: 'amo-spec155',
    tenantId: '10',
    clientId: 10,
    objective: 'Find law firms in Greater Manchester for commercial cleaning.',
    structuredMission: {
      objective: 'Find law firms in Greater Manchester for commercial cleaning.',
      market: {
        segment: 'law_firm',
        industry: 'legal',
        buyer: 'office manager',
        label: 'Law firms',
      },
      geography: {
        region: 'Greater Manchester',
        cities: ['Manchester', 'Bedford', 'Hooksett'],
      },
    },
    constraints: { vertical: 'law_firm', locationHint: 'Greater Manchester' },
  };
}

function injectedAdapter() {
  return createInjectedDiscoverAdapter(async ({ searchDefinition }) => {
    const workload = searchDefinition._coverageWorkload || {};
    const city = workload.city || 'Manchester NH';
    return [
      {
        id: `co-${city.replace(/\s+/g, '-').toLowerCase()}`,
        name: `${city} Legal Group`,
        location: city,
        industry: 'law_firm',
      },
    ];
  });
}

describe('SPEC-155 — Candidate Universe Estimation', () => {
  it('Scenario 1: generates universe estimate before external discovery', async () => {
    const mission = amoMission();
    const delegation = {
      tenantId: '10',
      targetContext: { geography: 'Greater Manchester', segments: ['law_firm'] },
      businessContext: { serviceGeography: 'Greater Manchester', commercialCapability: 'commercial_cleaning' },
    };

    const result = await runDiscoveryPipeline({
      mission,
      delegation,
      opts: { discover: injectedAdapter(), enablePlaces: false },
    });

    const estimateStage = result.stages.find((s) => s.stage === DISCOVERY_PIPELINE_STAGES.ESTIMATE_UNIVERSE);
    assert.ok(estimateStage, 'ESTIMATE_UNIVERSE stage must run before execution');
    assert.ok(estimateStage.output);
    assert.ok(estimateStage.output.expected > 0);
    assert.ok(estimateStage.output.minimum > 0);
    assert.ok(estimateStage.output.maximum >= estimateStage.output.expected);
    assert.ok(estimateStage.output.confidence > 0);
    assert.ok(Array.isArray(estimateStage.output.reasoning));
    assert.ok(estimateStage.output.reasoning.length > 0);

    const executeIdx = result.stages.findIndex((s) => s.stage === DISCOVERY_PIPELINE_STAGES.EXECUTE_COVERAGE_PLAN);
    const estimateIdx = result.stages.findIndex((s) => s.stage === DISCOVERY_PIPELINE_STAGES.ESTIMATE_UNIVERSE);
    assert.ok(estimateIdx < executeIdx);
  });

  it('Scenario 2: produces estimate without external sources using geographic and CRM evidence', () => {
    const estimate = estimateCandidateUniverse({
      discoveryPlan: samplePlan(),
      existingIntelligence: { companyCount: 8 },
      gapAnalysis: { relevantCount: 6, freshCount: 4 },
      marketDefinition: { segment: 'law_firm' },
    });

    assert.ok(estimate.expected > 0);
    assert.ok(estimate.minimum <= estimate.expected);
    assert.ok(estimate.maximum >= estimate.expected);
    assert.ok(estimate.confidence > 0);
    assert.ok(estimate.reasoning.some((line) => /CRM|geographic|coverage plan/i.test(line)));
    assert.equal(estimate.revisionHistory.length, 0);
  });

  it('Scenario 2b: missing external signals reduce confidence', () => {
    const rich = estimateCandidateUniverse({
      discoveryPlan: samplePlan(),
      existingIntelligence: { companyCount: 12 },
      gapAnalysis: { relevantCount: 10 },
      marketDefinition: { segment: 'law_firm' },
      memory: { marketSize: 38 },
    });
    const sparse = estimateCandidateUniverse({
      discoveryPlan: { totals: { searches: 2, cities: 1, concepts: 1 } },
      existingIntelligence: { companyCount: 0 },
      marketDefinition: { segment: 'law_firm' },
    });

    assert.ok(rich.confidence > sparse.confidence);
    assert.ok(sparse.reasoning.some((line) => /Missing signals/i.test(line)));
  });

  it('Scenario 3: revises estimate when new evidence materially changes understanding', () => {
    const initial = estimateCandidateUniverse({
      discoveryPlan: samplePlan(),
      marketDefinition: { segment: 'law_firm' },
    });

    const revised = reviseCandidateUniverseEstimate(initial, {
      investigated: initial.maximum + 20,
      coverageComplete: true,
    });

    assert.ok(revised.revisionHistory.length === 1);
    assert.ok(revised.expected > initial.expected);
    assert.match(revised.revisionHistory[0].reason, /exceeds prior maximum/i);
    assert.ok(revised.reasoning.some((line) => /Revision:/i.test(line)));
  });

  it('Scenario 4: Mission Intelligence Report displays estimate fields and coverage', () => {
    const universeEstimate = estimateCandidateUniverse({
      discoveryPlan: samplePlan(),
      marketDefinition: { segment: 'law_firm' },
    });
    const coverage = analyzeMarketCoverage({
      candidateUniverse: { discovered: 21, universeEstimate },
      qualification: { qualifiedCount: 2 },
      ranking: { strong: 1, immediate: 0 },
      evidenceCollection: { avgConfidence: 0.72, sourcesUsed: ['google_places'] },
      universeEstimate,
    });

    const report = buildIntelligenceReport({
      marketDefinition: { geography: 'Greater Manchester', segment: 'law firms' },
      coverage,
      ranking: { rankedOpportunities: [] },
      evidenceCollection: { withEvidence: 2, avgConfidence: 0.72 },
    });

    assert.equal(report.kind, 'mission_intelligence_report');
    assert.ok(report.estimatedMarket);
    assert.equal(report.estimatedMarket.expected, universeEstimate.expected);
    assert.equal(report.estimatedMarket.minimum, universeEstimate.minimum);
    assert.equal(report.estimatedMarket.maximum, universeEstimate.maximum);
    assert.equal(report.estimatedMarket.confidence, universeEstimate.confidence);
    assert.equal(report.coveragePct, computeCoverageFromEstimate(21, universeEstimate));
    assert.match(report.summary, /Estimated universe/);
    assert.match(report.summary, /Coverage/);
  });

  it('Scenario 5: coverage cannot be reported without an estimated universe', () => {
    assert.equal(computeCoverageFromEstimate(21, null), null);
    assert.equal(computeCoverageFromEstimate(21, { expected: 0 }), null);
    assert.equal(computeCoverageFromEstimate(21, undefined), null);

    const coverage = analyzeMarketCoverage({
      candidateUniverse: { discovered: 21 },
      qualification: {},
      ranking: {},
      evidenceCollection: {},
    });
    assert.equal(coverage.coveragePct, null);

    const report = buildDiscoveryReport({
      coverage: {},
      candidateUniverse: [{ candidate_id: 'a', dedupeStatus: 'primary' }],
      qualifiedCount: 0,
    });
    assert.equal(report.marketCoveragePct, undefined);
    assert.equal(report.estimatedMarket, undefined);
  });

  it('normalizeCandidateUniverseEstimate converts legacy scalar values', () => {
    const normalized = normalizeCandidateUniverseEstimate(48);
    assert.equal(normalized.expected, 48);
    assert.ok(normalized.minimum < normalized.expected);
    assert.ok(normalized.maximum > normalized.expected);
    assert.equal(extractExpectedValue(48), 48);
  });
});
