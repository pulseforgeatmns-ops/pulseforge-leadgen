'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { expandConcepts } = require('../../scout/coverage/ConceptLibrary');
const {
  buildDiscoveryPlan,
  executeCoveragePlan,
  expandCitiesFromSearchDefinition,
  buildCandidateUniverseRecords,
  computeDiscoveryConfidence,
  buildDiscoveryReport,
  canConcludeEmptyUniverse,
  discoveryStatusFromCoverage,
} = require('../../scout/coverage/DiscoveryCoverageEngine');
const { createInjectedDiscoverAdapter } = require('../../max/scoutAcquisition/DiscoveryAdapters');
const { constructCandidateUniverse } = require('../../max/scoutAcquisition/CandidateUniverse');
const {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
} = require('../../acquisition-mission/DiscoveryPayload');
const { presentationFromDiscoveryPayload } = require('../../acquisition-mission/DiscoveryPresentation');
const { runScoutAcquisitionIntelligence } = require('../../max/scoutAcquisition/ScoutAdapter');
const { buildAcquisitionSearchDefinition } = require('../../max/scoutAcquisition/SearchDefinition');

function makeDiscoverFn(recordsByKey) {
  return async ({ searchDefinition }) => {
    const workload = searchDefinition._coverageWorkload || {};
    const key = `${workload.city || searchDefinition.geography?.label}|${workload.concept || (searchDefinition.segments || [])[0]}`;
    return recordsByKey[key] || [];
  };
}

describe('SPEC-153 — Discovery Coverage Engine', () => {
  it('Scenario 1: Greater Manchester expands to 6 cities', () => {
    const definition = buildAcquisitionSearchDefinition({
      tenantId: '1',
      targetContext: { geography: 'Greater Manchester', segments: ['property_management'] },
      businessContext: { serviceGeography: 'Greater Manchester', commercialCapability: 'commercial_cleaning' },
    });
    const cities = expandCitiesFromSearchDefinition(definition);
    assert.equal(cities.length, 6);
    assert.ok(cities.some((c) => /Manchester/i.test(c)));
    assert.ok(cities.some((c) => /Bedford/i.test(c)));
    assert.ok(cities.some((c) => /Hooksett/i.test(c)));
    assert.ok(cities.some((c) => /Londonderry/i.test(c)));
    assert.ok(cities.some((c) => /Auburn/i.test(c)));
    assert.ok(cities.some((c) => /Goffstown/i.test(c)));

    const plan = buildDiscoveryPlan(definition, {
      adapters: [createInjectedDiscoverAdapter(async () => [])],
    });
    assert.equal(plan.totals.cities, 6);
  });

  it('Scenario 2: short-term rental operators expands all STR terminology', () => {
    const definition = buildAcquisitionSearchDefinition({
      tenantId: '1',
      targetContext: {
        geography: 'Greater Manchester',
        segments: ['short_term_rental'],
        businessType: 'short_term_rental',
      },
      businessContext: { serviceGeography: 'Greater Manchester' },
    });
    const concepts = expandConcepts(definition);
    assert.ok(concepts.includes('STR'));
    assert.ok(concepts.includes('Vacation Rental'));
    assert.ok(concepts.includes('Airbnb Host'));
    assert.ok(concepts.includes('Vacation Property Manager'));
    assert.ok(concepts.includes('Property Manager'));
    assert.ok(concepts.includes('Hospitality Operator'));
    assert.equal(concepts.length, 6);
  });

  it('Scenario 3: CRM qualifying operators seed candidate universe before external discovery', async () => {
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      targetContext: { geography: 'Greater Manchester', segments: ['law_firm'] },
      businessContext: { serviceGeography: 'Greater Manchester', commercialCapability: 'commercial_cleaning' },
    });
    const existingCompanies = [
      {
        id: 'crm-1',
        name: 'Harbor Law Group',
        location: 'Manchester, NH',
        icpScore: 82,
        source: 'existing_pf',
      },
    ];
    const universe = await constructCandidateUniverse({
      searchDefinition,
      existing: {
        companies: existingCompanies,
        people: [],
        criteria: { geography: 'Greater Manchester', segments: ['law_firm'] },
        rejectedCandidates: [],
      },
      adapters: [],
      forceDiscover: false,
    });
    assert.ok(universe.candidateUniverse.length >= 1);
    assert.equal(universe.candidateUniverse[0].origin, 'existing_intelligence');
    assert.equal(universe.candidateUniverse[0].candidate_id, 'crm-1');
  });

  it('Scenario 4: zero Google Places results still executes alternate injected source workloads', async () => {
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '1',
      targetContext: { geography: 'Manchester, NH', segments: ['property_management'] },
      businessContext: {
        serviceGeography: 'Manchester, NH',
        commercialCapability: 'commercial_cleaning',
        preferredSegments: ['property_management'],
      },
    });
    const adapter = createInjectedDiscoverAdapter(
      makeDiscoverFn({
        'Manchester NH|Property Management': [
          { id: 'disc-1', name: 'Granite PM', location: 'Manchester, NH' },
        ],
      })
    );
    const plan = buildDiscoveryPlan(searchDefinition, { adapters: [adapter] });
    const result = await executeCoveragePlan(plan, searchDefinition, [adapter]);
    assert.ok(result.candidates.length >= 1);
    assert.equal(result.candidates[0].name, 'Granite PM');
  });

  it('Scenario 5: incomplete coverage blocks prioritization', () => {
    const presentation = presentationFromDiscoveryPayload({
      rankedProspects: [{ rank: 1, name: 'Test Co' }],
      qualifiedCount: 1,
      summary: 'Found one prospect.',
      buyingSignals: [{ label: 'Hiring manager', type: 'hiring' }],
      evidence: [{ label: 'Website', source: 'Company website' }],
      discoveryStatus: 'incomplete',
      coverage: {
        cities: { searched: 1, planned: 6 },
        warnings: ['Only 1 / 6 cities searched.', 'Discovery incomplete.'],
      },
    });
    assert.equal(presentation.discoveryStatus, 'incomplete');
    assert.equal(hasSufficientEvidenceForPrioritization(presentation), false);
  });

  it('Scenario 6: complete coverage with zero qualified candidates yields empty universe conclusion', () => {
    const coverage = {
      cities: { searched: 6, planned: 6, ratio: 1 },
      concepts: { searched: 6, planned: 6, ratio: 1 },
      sources: { searched: 1, planned: 1, ratio: 1 },
      searches: { executed: 36, addressed: 36, planned: 36, ratio: 1 },
      complete: true,
      warnings: [],
    };
    assert.equal(discoveryStatusFromCoverage(coverage), 'complete');
    assert.equal(canConcludeEmptyUniverse(coverage, 0), true);

    const confidence = computeDiscoveryConfidence({
      coverage,
      candidateUniverse: [],
      searchSuccess: 0.4,
      evidenceQuality: 0.5,
    });
    assert.ok(confidence.overall >= 0.6);
    assert.equal(confidence.investigationComplete, true);

    const report = buildDiscoveryReport({
      coverage,
      candidateUniverse: [],
      qualifiedCount: 0,
      discoveryConfidence: confidence,
    });
    assert.equal(report.candidateUniverse, 0);
    assert.equal(report.status, 'complete');
    assert.match(report.recommendation, /operator review/i);
  });

  it('normalizes discovery payload with coverage report fields', () => {
    const normalized = normalizeScoutDiscoveryPayload(
      {
        status: 'partial',
        summary: 'Discovery incomplete.',
        confidence: 0.42,
        payload: {
          opportunities: [],
          discoveryStatus: 'incomplete',
          coverage: {
            cities: { searched: 1, planned: 6 },
            concepts: { searched: 2, planned: 6 },
            sources: { searched: 1, planned: 1 },
            complete: false,
            warnings: ['Only 1 / 6 cities searched.', 'Discovery incomplete.'],
          },
          candidateUniverse: [{ candidate_id: 'seed-1', dedupeStatus: 'primary' }],
          discoveryReport: {
            coverage: { cities: '1/6', concepts: '2/6', sources: '1/1' },
            candidateUniverse: 1,
            qualified: 0,
            status: 'incomplete',
          },
        },
      },
      { missionObjective: 'Find STR operators in Greater Manchester.' }
    );
    assert.equal(normalized.discoveryStatus, 'incomplete');
    assert.equal(normalized.candidateUniverseCount, 1);
    assert.equal(normalized.blocked, true);
    assert.ok(normalized.discoveryReport);
  });

  it('runScoutAcquisitionIntelligence attaches coverage plan to payload', async () => {
    const delegation = {
      tenantId: '10',
      authority: 'observe',
      targetContext: {
        geography: 'Greater Manchester',
        segments: ['property_management'],
        businessType: 'commercial_cleaning',
      },
      businessContext: {
        serviceGeography: 'Greater Manchester',
        commercialCapability: 'commercial_cleaning',
        preferredSegments: ['property_management'],
      },
    };
    const result = await runScoutAcquisitionIntelligence(delegation, {
      companies: [
        {
          id: 'co-1',
          name: 'Existing PM Co',
          industry: 'property_management',
          location: 'Manchester, NH',
          icpScore: 75,
          signals: [{ type: 'portfolio_growth', observedAt: '2026-07-01T00:00:00.000Z' }],
        },
      ],
      discover: async () => [],
    });
    assert.ok(result.payload.discoveryPlan);
    assert.equal(result.payload.discoveryPlan.totals.cities, 6);
    assert.ok(Array.isArray(result.payload.candidateUniverse));
    assert.ok(result.payload.discoveryReport);
    assert.ok(result.payload.discoveryStatus);
  });
});
