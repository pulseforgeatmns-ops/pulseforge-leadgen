'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  Scout,
  runDiscoveryPipeline,
  DISCOVERY_PIPELINE_STAGES,
  DISCOVERY_OUTCOMES,
} = require('../packages/scout');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');

function amoMission() {
  return {
    id: 'amo-spec154',
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
      constraints: ['commercial_only'],
      evidence: { requireDecisionMaker: true },
      successMetric: { qualifiedLeads: 5 },
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

describe('SPEC-154 — Unified Discovery Pipeline', () => {
  it('Scout exposes only discover() as public capability', () => {
    assert.equal(typeof Scout.discover, 'function');
    assert.equal(Scout.investigate, undefined);
  });

  it('runDiscoveryPipeline executes all seven stages in order', async () => {
    const mission = amoMission();
    const delegation = {
      tenantId: '10',
      targetContext: {
        geography: 'Greater Manchester',
        segments: ['law_firm'],
        businessType: 'law_firm',
      },
      businessContext: {
        serviceGeography: 'Greater Manchester',
        preferredSegments: ['law_firm'],
        commercialCapability: 'commercial_cleaning',
      },
    };

    const result = await runDiscoveryPipeline({
      mission,
      delegation,
      opts: {
        discover: injectedAdapter(),
        enablePlaces: false,
      },
    });

    const stageNames = result.stages.map((s) => s.stage);
    assert.deepEqual(stageNames, [
      DISCOVERY_PIPELINE_STAGES.UNDERSTAND_MARKET,
      DISCOVERY_PIPELINE_STAGES.ESTIMATE_UNIVERSE,
      DISCOVERY_PIPELINE_STAGES.BUILD_INVESTIGATION_PLAN,
      DISCOVERY_PIPELINE_STAGES.EXECUTE_COVERAGE_PLAN,
      DISCOVERY_PIPELINE_STAGES.MEASURE_COVERAGE,
      DISCOVERY_PIPELINE_STAGES.DETERMINE_SUFFICIENCY,
      DISCOVERY_PIPELINE_STAGES.PRODUCE_INTELLIGENCE_REPORT,
    ]);

    assert.ok(result.marketDefinition);
    assert.ok(result.marketDefinition.valid);
    assert.ok(result.universeEstimate != null);
    assert.ok(result.universeEstimate.expected > 0);
    assert.ok(result.universeEstimate.minimum <= result.universeEstimate.expected);
    assert.ok(result.universeEstimate.maximum >= result.universeEstimate.expected);
    assert.ok(result.coveragePlan);
    assert.ok(result.coveragePlan.workloads);
    assert.equal(result.coverageEngineUsed, true);
    assert.ok(result.intelligenceReport);
    assert.match(result.outcome, /^DISCOVERY_/);
  });

  it('pipeline produces identical artifacts: market, universe, plan, coverage, report', async () => {
    const mission = amoMission();
    const delegation = {
      tenantId: '10',
      targetContext: { geography: 'Greater Manchester', segments: ['law_firm'] },
      businessContext: { serviceGeography: 'Greater Manchester', commercialCapability: 'commercial_cleaning' },
    };
    const opts = { discover: injectedAdapter(), enablePlaces: false };

    const viaPipeline = await runDiscoveryPipeline({ mission, delegation, opts });
    const viaDiscover = await Scout.discover({ mission, scoutPayload: {}, opts: { ...opts, delegation } });

    assert.equal(viaDiscover.pipeline.outcome, viaPipeline.outcome);
    assert.equal(viaDiscover.coveragePlan.totals.searches, viaPipeline.coveragePlan.totals.searches);
    assert.equal(viaDiscover.universeEstimate.expected, viaPipeline.universeEstimate.expected);
    assert.equal(viaDiscover.coveragePct, viaPipeline.coveragePct);
    assert.equal(viaDiscover.emptyMarketDecision, viaPipeline.emptyMarketDecision);
    assert.equal(viaDiscover.marketDefinition.valid, viaPipeline.marketDefinition.valid);
  });

  it('constructCandidateUniverse rejects CoverageEngine bypass', async () => {
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      targetContext: { geography: 'Greater Manchester', segments: ['law_firm'] },
      businessContext: { serviceGeography: 'Greater Manchester' },
    });

    await assert.rejects(
      () =>
        constructCandidateUniverse({
          searchDefinition,
          existing: { companies: [] },
          adapters: [injectedAdapter()],
          useCoverageEngine: false,
          forceDiscover: true,
        }),
      /CoverageEngine bypass is not permitted/
    );
  });
});

describe('SPEC-154 — entry point parity', () => {
  it('Scout.discover and runDiscoveryPipeline share confidence and empty-market semantics', async () => {
    const mission = amoMission();
    const delegation = {
      tenantId: '10',
      targetContext: { geography: 'Greater Manchester', segments: ['law_firm'] },
      businessContext: { serviceGeography: 'Greater Manchester', commercialCapability: 'commercial_cleaning' },
    };
    const opts = { discover: injectedAdapter(), enablePlaces: false };

    const pipeline = await runDiscoveryPipeline({ mission, delegation, opts });
    const discover = await Scout.discover({ mission, scoutPayload: {}, opts: { ...opts, delegation } });

    assert.equal(typeof pipeline.confidence, 'number');
    assert.equal(typeof discover.confidence, 'number');
    assert.equal(typeof pipeline.emptyMarketDecision, 'boolean');
    assert.equal(typeof discover.emptyMarketDecision, 'boolean');
    assert.equal(pipeline.coverageEngineUsed, true);
    assert.equal(discover.pipeline.coverageEngineUsed, true);
  });
});
