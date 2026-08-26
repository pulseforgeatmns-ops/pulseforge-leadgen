'use strict';

/**
 * SPEC-185 — Blocked Discovery Telemetry Continuity (ADR-100).
 *
 * Every Discovery exit path — success, blocked, partial, provider failure —
 * must include providerExecution in the canonical payload shape.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const { runScoutAcquisitionIntelligence } = require('../../max/scoutAcquisition/ScoutAdapter');
const { createPlacesProvider } = require('../../capabilities/discovery/providers/PlacesProvider');
const { createPlacesDiscoveryAdapter } = require('../../max/scoutAcquisition/DiscoveryAdapters');
const { buildScoutDiscoveryArtifact } = require('../../scout/adapters/ScoutDiscoveryArtifact');
const { normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const {
  presentationFromDiscoveryPayload,
  formatDiscoveryResultsProse,
} = require('../DiscoveryPresentation');

function fetchMock(handler) {
  return async (url) => handler(String(url));
}

function jsonResponse(body, httpStatus = 200) {
  return {
    ok: httpStatus >= 200 && httpStatus < 300,
    status: httpStatus,
    json: async () => body,
  };
}

function propertyManagerDelegation() {
  return {
    tenantId: '10',
    authority: 'observe',
    targetContext: {
      geography: 'Manchester, NH',
      segments: ['property_management'],
      businessType: 'commercial_cleaning',
    },
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management'],
    },
    mission: {
      id: 'mission-spec-185',
      tenantId: '10',
      objectiveText: 'Find property managers who outsource cleaning in Greater Manchester.',
      constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
    },
  };
}

function marketDefinition() {
  return {
    segments: ['property_management'],
    geography: 'Manchester NH',
  };
}

function assertProviderExecutionShape(payload) {
  assert.ok(payload, 'payload must exist');
  assert.ok(Array.isArray(payload.providerExecution), 'providerExecution must be an array');
}

describe('SPEC-185 — Blocked Discovery Telemetry Continuity', () => {
  let savedPlacesKey;

  beforeEach(() => {
    savedPlacesKey = process.env.GOOGLE_PLACES_KEY;
    process.env.GOOGLE_PLACES_KEY = 'test-key';
  });

  afterEach(() => {
    if (savedPlacesKey === undefined) {
      delete process.env.GOOGLE_PLACES_KEY;
    } else {
      process.env.GOOGLE_PLACES_KEY = savedPlacesKey;
    }
  });

  it('blocked provider failure preserves providerExecution from universe.providerReports', async () => {
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        if (url.includes('textsearch')) {
          return jsonResponse(
            {
              status: 'REQUEST_DENIED',
              error_message: 'The provided API key is invalid.',
              results: [],
            },
            403
          );
        }
        return jsonResponse({ result: {} });
      }),
    });
    const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });

    const result = await runScoutAcquisitionIntelligence(propertyManagerDelegation(), {
      companies: [],
      people: [],
      discoveryAdapters: [adapter],
      enablePlaces: false,
      marketDefinition: marketDefinition(),
    });

    assert.ok(['blocked', 'partial'].includes(result.status));
    assertProviderExecutionShape(result.payload);
    assert.equal(result.payload.opportunities.length, 0);
    assert.ok(result.payload.providerExecution.length >= 1);
    const failedReport = result.payload.providerExecution.find((row) => row.status === 'failed');
    assert.ok(failedReport, 'expected at least one failed provider execution record');

    const artifact = buildScoutDiscoveryArtifact(result);
    assert.ok(Array.isArray(artifact.providerExecution));
    assert.ok(artifact.providerExecution.length >= 1);

    const contribution = normalizeScoutDiscoveryPayload(result);
    assert.ok(Array.isArray(contribution.providerExecution));
    assert.ok(contribution.providerExecution.length >= 1);
    const mapsFailure = contribution.providerExecution.find(
      (row) => /google maps/i.test(String(row.provider || ''))
    );
    assert.ok(mapsFailure, 'expected Google Maps provider execution record');
    assert.equal(mapsFailure.status, 'failed');

    const presentation = presentationFromDiscoveryPayload(contribution);
    assert.ok(presentation.providerExecution.length >= 1);

    const prose = formatDiscoveryResultsProse(contribution);
    assert.match(prose, /Provider Execution/i);
    assert.match(prose, /Google Maps/);
    assert.match(prose, /403|http_403/i);
  });

  it('early blocked paths include empty providerExecution array', async () => {
    const invalid = await runScoutAcquisitionIntelligence(
      {
        tenantId: '10',
        authority: 'observe',
        targetContext: {},
        businessContext: {},
      },
      { companies: [], people: [] }
    );
    assert.equal(invalid.status, 'blocked');
    assertProviderExecutionShape(invalid.payload);
    assert.deepEqual(invalid.payload.providerExecution, []);

    const fixtureFailure = await runScoutAcquisitionIntelligence(propertyManagerDelegation(), {
      mode: 'provider_failure',
      companies: [],
      people: [],
    });
    assert.equal(fixtureFailure.status, 'blocked');
    assertProviderExecutionShape(fixtureFailure.payload);
    assert.deepEqual(fixtureFailure.payload.providerExecution, []);
  });

  it('success and blocked payloads share the same providerExecution contract', async () => {
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        if (url.includes('textsearch')) {
          return jsonResponse({ status: 'ZERO_RESULTS', results: [] });
        }
        return jsonResponse({ result: {} });
      }),
    });
    const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });

    const blocked = await runScoutAcquisitionIntelligence(propertyManagerDelegation(), {
      companies: [],
      people: [],
      discoveryAdapters: [adapter],
      enablePlaces: false,
      marketDefinition: marketDefinition(),
    });

    const success = await runScoutAcquisitionIntelligence(propertyManagerDelegation(), {
      companies: [],
      people: [],
      discoveryAdapters: [
        {
          id: 'public_business_places',
          sourceType: 'public_business_data',
          available: () => true,
          discover: async (searchDefinition) => ({
            candidates: [
              {
                id: 'co-harbor',
                name: 'Harbor Property Mgmt',
                industry: 'property_management',
                location: searchDefinition.geography?.label || 'Manchester NH',
                website: 'https://harbor.example',
                signals: [
                  {
                    type: 'portfolio_growth',
                    observedAt: '2026-07-12T00:00:00.000Z',
                    source: 'company_website',
                    label: 'Portfolio page lists managed units.',
                  },
                ],
              },
            ],
            sourceTypesChecked: ['public_business_data'],
            errors: [],
          }),
        },
      ],
      enablePlaces: false,
    });

    assertProviderExecutionShape(blocked.payload);
    assertProviderExecutionShape(success.payload);
    assert.notEqual(blocked.status, 'completed');
    assert.ok(['completed', 'partial'].includes(success.status));
  });
});
