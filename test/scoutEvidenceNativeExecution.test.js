'use strict';

/**
 * SPEC-181 — Evidence-Native Execution acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildEvidenceRequest,
  scopeSearchDefinitionForTask,
  geographyFromSearchDefinition,
  isEvidenceRequest,
} = require('../packages/scout/coverage/EvidenceRequest');
const {
  scopedSearchForTask,
  runHypothesisDrivenDiscovery,
} = require('../packages/scout/coverage/HypothesisDrivenDiscoveryEngine');
const {
  createPlacesDiscoveryAdapter,
  createInjectedDiscoverAdapter,
} = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const {
  createPlacesProvider,
  buildQueriesForEvidence,
} = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
const { INVESTIGATIVE_EVIDENCE } = require('../packages/scout/coverage/EvidenceRequirements');
const { createHypothesisInvestigationPlan } = require('../packages/scout/coverage/HypothesisInvestigationPlanner');

function propertyManagerMission() {
  return {
    id: 'mission-pm-1',
    tenantId: '10',
    objectiveText: 'Find property managers who outsource cleaning in Greater Manchester.',
    constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
  };
}

describe('SPEC-181 — Evidence-Native Execution', () => {
  it('builds evidence requests with segment, evidenceType, and geography — not search strings', () => {
    const searchDefinition = {
      tenantId: '10',
      geography: {
        label: 'Greater Manchester NH',
        state: 'NH',
        cities: ['Manchester', 'Bedford', 'Hooksett'],
      },
      segments: ['property_management'],
    };
    const task = {
      id: 'task:identity',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      providers: [{ providerId: 'google_maps' }],
    };

    const request = buildEvidenceRequest(task, searchDefinition, { segments: ['property_management'] });

    assert.equal(request.segment, 'property_management');
    assert.equal(request.evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.ok(Array.isArray(request.geography.cities));
    assert.ok(request.geography.cities.includes('Manchester'));
    assert.ok(request.geography.cities.includes('Bedford'));
    assert.equal(request.geography.state, 'NH');
    assert.equal(request.investigationTaskId, 'task:identity');
    assert.deepEqual(request.providerIds, ['google_maps']);
    assert.ok(isEvidenceRequest(request));

    const serialized = JSON.stringify(request);
    assert.ok(!serialized.includes('Property Manager Bedford NH'));
    assert.ok(!serialized.includes('property management company'));
  });

  it('scopedSearchForTask attaches evidenceRequest instead of segment keyword payloads', () => {
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      geography: { label: 'Manchester NH', state: 'NH' },
      segments: ['short_term_rental'],
    });
    const task = {
      id: 'task:identity',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      providers: [{ providerId: 'google_maps' }],
    };

    const scoped = scopedSearchForTask(searchDefinition, task, { segments: ['short_term_rental'] });

    assert.ok(scoped.evidenceRequest);
    assert.equal(scoped.evidenceRequest.segment, 'short_term_rental');
    assert.equal(scoped.evidenceRequest.evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.equal(scoped._evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.ok(!Array.isArray(scoped.segments) || scoped.segments.length <= 3);
  });

  it('PlacesProvider generates queries from evidence request — Scout never emits search strings', () => {
    const queries = buildQueriesForEvidence({
      segment: 'short_term_rental',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      cities: ['Manchester', 'Bedford', 'Hooksett'],
      state: 'NH',
    });

    assert.ok(queries.length >= 3);
    assert.ok(queries.some((q) => /manchester/i.test(q)));
    assert.ok(queries.some((q) => /bedford/i.test(q)));
    assert.ok(queries.every((q) => typeof q === 'string' && q.length > 0));
    assert.ok(!queries.some((q) => q.includes('evidenceType')));
  });

  it('PlacesProvider collectEvidence accepts identity rows without websites (ADR-092)', async () => {
    const fetchImpl = async (url) => {
      const u = String(url);
      if (u.includes('textsearch')) {
        return {
          ok: true,
          async json() {
            return {
              status: 'OK',
              results: [{ place_id: 'place-1', name: 'River City PM', formatted_address: '100 Main St, Manchester NH' }],
            };
          },
        };
      }
      if (u.includes('details')) {
        return {
          ok: true,
          async json() {
            return {
              result: {
                place_id: 'place-1',
                name: 'River City PM',
                formatted_address: '100 Main St, Manchester NH',
                formatted_phone_number: '603-555-0100',
                website: null,
              },
            };
          },
        };
      }
      return { ok: false, async json() { return {}; } };
    };

    const provider = createPlacesProvider({ apiKey: 'test-key', fetchImpl });
    const hits = await provider.collectEvidence({
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: ['Manchester'], state: 'NH' },
    });

    assert.equal(hits.length, 1);
    assert.equal(hits[0].companyName, 'River City PM');
    assert.equal(hits[0].website, null);
    assert.ok(hits[0].placeId);
  });

  it('Places discovery adapter dispatches evidenceRequest to provider.collectEvidence', async () => {
    const collected = [];
    const provider = {
      available: () => true,
      collectEvidence: async (request) => {
        collected.push(request);
        return [{ companyName: 'Test PM', placeId: 'p1', address: '1 Main St' }];
      },
      search: async () => [],
    };

    const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });
    const evidenceRequest = {
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: ['Manchester'], state: 'NH' },
    };

    const result = await adapter.discover({ evidenceRequest, tenantId: '10' });

    assert.equal(collected.length, 1);
    assert.equal(collected[0].segment, 'property_management');
    assert.equal(collected[0].evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.equal(result.candidates.length, 1);
    assert.equal(result.coverage.evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.ok(!result.coverage.legacy);
  });

  it('end-to-end: mission discovery passes evidence requests to injected adapter', async () => {
    const receivedRequests = [];
    const adapter = createInjectedDiscoverAdapter(async (input) => {
      if (input.evidenceRequest) receivedRequests.push(input.evidenceRequest);
      return [
        {
          id: 'pm-1',
          name: 'Granite Property Management',
          address: '100 Main St, Manchester NH',
          phone: '603-555-0100',
          placeId: 'place-1',
        },
      ];
    });

    const mission = propertyManagerMission();
    const market = buildMarketDefinition({ mission });
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      geography: { label: 'Manchester NH', state: 'NH' },
      segments: ['property_management'],
    });

    const result = await runHypothesisDrivenDiscovery({
      mission,
      marketDefinition: { ...market, searchDefinition },
      searchDefinition,
      adapters: [adapter],
      opts: { maxIterations: 2, requireEstablishedIdentity: true },
    });

    assert.ok(receivedRequests.length >= 1);
    assert.equal(receivedRequests[0].evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.ok(receivedRequests[0].segment);
    assert.ok(Array.isArray(receivedRequests[0].geography.cities));
    assert.ok(result.executedTasks.length > 0);
    assert.equal(result.investigationPlan.version, 'SPEC-180');
  });

  it('investigation plan tasks contain no search keywords (SPEC-180 + SPEC-181)', () => {
    const mission = propertyManagerMission();
    const market = buildMarketDefinition({ mission });
    const plan = createHypothesisInvestigationPlan({
      mission,
      marketDefinition: market,
    });

    const planJson = JSON.stringify(plan);
    assert.ok(!planJson.includes('Property Manager Bedford NH'));
    assert.ok(!planJson.includes('Google Places'));
    assert.ok(plan.tasks.every((t) => t.evidenceType && !t.query));
  });

  it('geographyFromSearchDefinition expands multi-city missions', () => {
    const searchDefinition = {
      geography: { label: 'Greater Manchester NH', state: 'NH' },
      segments: ['property_management'],
    };
    const geo = geographyFromSearchDefinition(searchDefinition);
    assert.ok(geo.cities.length >= 3);
    assert.equal(geo.state, 'NH');
  });
});
