'use strict';

/**
 * AUDIT-059 — External Discovery Provider Failure.
 *
 * Trace: Mission → Investigation Plan → Provider Assignment → Provider Execution → 0 Prospects.
 * Stop at the first divergence: provider execution must record the actual Places request.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildQueriesForEvidence,
  createPlacesProvider,
} = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { INVESTIGATIVE_EVIDENCE } = require('../packages/scout/coverage/EvidenceRequirements');
const { buildEvidenceRequest } = require('../packages/scout/coverage/EvidenceRequest');
const { createHypothesisInvestigationPlan } = require('../packages/scout/coverage/HypothesisInvestigationPlanner');
const {
  executeProviderAssignment,
  runHypothesisDrivenDiscovery,
} = require('../packages/scout/coverage/HypothesisDrivenDiscoveryEngine');
const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { buildScoutDiscoveryArtifact } = require('../packages/scout/adapters/ScoutDiscoveryArtifact');

function propertyManagerMission() {
  return {
    id: 'mission-audit-059',
    tenantId: '10',
    objectiveText: 'Find property managers who outsource cleaning in Greater Manchester.',
    constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
  };
}

function identityTask() {
  return {
    id: 'task:identity',
    evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
    providers: [{ providerId: 'google_maps', providerLabel: 'Google Maps' }],
  };
}

function fetchMock(handler) {
  return async (url) => handler(String(url));
}

function jsonResponse(body, httpStatus = 200) {
  return {
    ok: httpStatus >= 200 && httpStatus < 300,
    status: httpStatus,
    async json() {
      return body;
    },
  };
}

describe('AUDIT-059 — External Discovery Provider Failure', () => {
  it('Q2/Q3: Google Places is assigned; actual query is registry template, not "Property Manager"', () => {
    const mission = propertyManagerMission();
    const marketDefinition = {
      segments: ['property_management'],
      geography: 'Manchester NH',
      searchDefinition: {
        tenantId: '10',
        geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
        segments: ['property_management'],
      },
    };
    const plan = createHypothesisInvestigationPlan({ mission, marketDefinition });
    const identityAssignments = (plan.assignedProviders || []).filter(
      (row) => row.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY
    );

    assert.ok(identityAssignments.some((row) => row.providerId === 'google_maps'));
    assert.ok(
      !identityAssignments.some((row) => row.providerId === 'linkedin'),
      'LinkedIn is not the identity provider'
    );

    const request = buildEvidenceRequest(identityTask(), marketDefinition.searchDefinition, marketDefinition);
    assert.equal(request.segment, 'property_management');
    assert.equal(request.evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.deepEqual(request.geography.cities, ['Manchester']);
    assert.equal(request.geography.state, 'NH');
    assert.ok(!JSON.stringify(request).includes('Property Manager'));

    const queries = buildQueriesForEvidence({
      segment: request.segment,
      evidenceType: request.evidenceType,
      cities: request.geography.cities,
      state: request.geography.state,
    });

    assert.ok(queries.includes('property management company Manchester NH'));
    assert.ok(queries.includes('commercial property management Manchester NH'));
    assert.ok(!queries.some((q) => q === 'Property Manager'));
    assert.ok(!queries.some((q) => /^Property Manager\b/i.test(q)));
  });

  it('Q1: Scout aborts before HTTP when Places is unavailable', async () => {
    const provider = createPlacesProvider({ apiKey: '', fetchImpl: fetchMock(() => jsonResponse({})) });
    const hits = await provider.collectEvidence({
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: ['Manchester'], state: 'NH' },
    });

    assert.equal(hits.length, 0);
    assert.equal(provider.lastExecution.executed, false);
    assert.equal(provider.lastExecution.abortReason, 'provider_unavailable');
    assert.equal(provider.lastExecution.queries.length, 0);
  });

  it('Q1/Q4: empty geography aborts before Places HTTP', async () => {
    const called = [];
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        called.push(url);
        return jsonResponse({ status: 'OK', results: [] });
      }),
    });
    const hits = await provider.collectEvidence({
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: [], state: 'NH' },
    });

    assert.equal(hits.length, 0);
    assert.equal(called.length, 0);
    assert.equal(provider.lastExecution.executed, false);
    assert.equal(provider.lastExecution.abortReason, 'empty_geography');
  });

  it('Q1/Q3/Q4: ZERO_RESULTS is a real Places execution with query, status, latency, retries', async () => {
    const requested = [];
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        requested.push(url);
        if (url.includes('textsearch')) {
          return jsonResponse({
            status: 'ZERO_RESULTS',
            results: [],
          }, 200);
        }
        return jsonResponse({ result: {} });
      }),
    });

    const hits = await provider.collectEvidence({
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: ['Manchester'], state: 'NH' },
    });

    assert.equal(hits.length, 0);
    assert.equal(provider.lastExecution.executed, true);
    assert.equal(provider.lastExecution.abortReason, null);
    assert.ok(provider.lastExecution.queries.length >= 2);
    assert.ok(
      provider.lastExecution.queries.every((row) => row.query !== 'Property Manager')
    );
    assert.ok(
      provider.lastExecution.queries.some((row) => row.query === 'property management company Manchester NH')
    );
    assert.ok(
      requested.some((url) => new URL(url).searchParams.get('query') === 'property management company Manchester NH')
    );
    for (const row of provider.lastExecution.queries) {
      assert.equal(row.googleStatus, 'ZERO_RESULTS');
      assert.equal(row.httpStatus, 200);
      assert.equal(row.resultCount, 0);
      assert.ok(row.latencyMs >= 0);
    }
    assert.equal(provider.lastExecution.totals.results, 0);
    assert.equal(provider.lastExecution.totals.retries, 0);
    assert.equal(provider.lastExecution.errors.length, 0);
  });

  it('Q4 first divergence: REQUEST_DENIED is a provider failure, not an empty market', async () => {
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        if (url.includes('textsearch')) {
          return jsonResponse({
            status: 'REQUEST_DENIED',
            error_message: 'This API project is not authorized to use this API.',
            results: [],
          });
        }
        return jsonResponse({ result: {} });
      }),
    });

    const hits = await provider.collectEvidence({
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: ['Manchester'], state: 'NH' },
    });

    assert.equal(hits.length, 0);
    assert.equal(provider.lastExecution.executed, true);
    assert.ok(provider.lastExecution.errors.length >= 1);
    assert.equal(provider.lastExecution.errors[0].code, 'google_places_status_REQUEST_DENIED');
    assert.equal(provider.lastExecution.errors[0].googleStatus, 'REQUEST_DENIED');
    assert.equal(provider.lastExecution.quota, null);

    const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });
    const report = await adapter.discover({
      evidenceRequest: {
        segment: 'property_management',
        evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
        geography: { cities: ['Manchester'], state: 'NH' },
      },
    });

    assert.equal(report.candidates.length, 0);
    assert.ok(report.errors.length >= 1);
    assert.equal(report.execution.errors[0].googleStatus, 'REQUEST_DENIED');

    const normalized = require('../packages/scout/coverage/ProviderEvidenceContract').normalizeProviderReport(
      report,
      { providerId: 'google_maps', evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY }
    );
    assert.equal(normalized.status, 'failed');
    assert.notEqual(normalized.status, 'empty');
    assert.equal(normalized.rawResultCount, 0);
  });

  it('Q4: OVER_QUERY_LIMIT records quota and retries', async () => {
    let textSearchCalls = 0;
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        if (url.includes('textsearch')) {
          textSearchCalls += 1;
          return jsonResponse({
            status: 'OVER_QUERY_LIMIT',
            error_message: 'You have exceeded your daily request quota for this API.',
            results: [],
          });
        }
        return jsonResponse({ result: {} });
      }),
    });

    await provider.collectEvidence({
      segment: 'property_management',
      evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
      geography: { cities: ['Manchester'], state: 'NH' },
    });

    assert.ok(textSearchCalls > 2, 'retryable quota errors retry per query');
    assert.equal(provider.lastExecution.quota.status, 'OVER_QUERY_LIMIT');
    assert.ok(provider.lastExecution.totals.retries >= 1);
    assert.ok(provider.lastExecution.errors.some((e) => e.googleStatus === 'OVER_QUERY_LIMIT'));
  });

  it('Q5 stop: candidates disappear at provider response, not identity/qualification', async () => {
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
    const result = await runHypothesisDrivenDiscovery({
      mission: propertyManagerMission(),
      marketDefinition: {
        segments: ['property_management'],
        geography: 'Manchester NH',
      },
      searchDefinition: {
        tenantId: '10',
        geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
        segments: ['property_management'],
      },
      adapters: [adapter],
      opts: { maxIterations: 2 },
    });

    const placesReports = (result.providerReports || []).filter((r) => r.providerId === 'google_maps');
    assert.ok(placesReports.length >= 1);
    const identity = placesReports.find((r) => r.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY) || placesReports[0];
    assert.equal(identity.execution.totals.results, 0);
    assert.equal(identity.mappedCandidateCount, 0);
    assert.equal(result.allCandidates.length, 0);
    assert.equal(result.candidates.length, 0);
  });

  it('Q6: Discovery Blocked is incomplete coverage with zero qualified — provider failure is the why', async () => {
    const blocked = buildScoutDiscoveryArtifact({
      status: 'partial',
      payload: { qualifiedCount: 0, discoveryStatus: 'incomplete' },
    });
    assert.equal(blocked.blocked, true);

    const emptyMarketComplete = buildScoutDiscoveryArtifact({
      status: 'completed',
      payload: { qualifiedCount: 0, discoveryStatus: 'complete' },
    });
    assert.equal(emptyMarketComplete.blocked, false);
  });

  it('Q1 execute path: executeProviderAssignment records Places HTTP against the evidence request', async () => {
    const requestedQueries = [];
    const provider = createPlacesProvider({
      apiKey: 'test-key',
      fetchImpl: fetchMock((url) => {
        if (url.includes('textsearch')) {
          const parsed = new URL(url);
          requestedQueries.push(parsed.searchParams.get('query'));
          return jsonResponse({ status: 'ZERO_RESULTS', results: [] });
        }
        return jsonResponse({ result: {} });
      }),
    });
    const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });
    const report = await executeProviderAssignment(
      { providerId: 'google_maps', providerLabel: 'Google Maps', evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY },
      {
        tenantId: '10',
        geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
        segments: ['property_management'],
      },
      [adapter],
      { segments: ['property_management'] }
    );

    assert.equal(report.execution.executed, true);
    assert.ok(requestedQueries.includes('property management company Manchester NH'));
    assert.ok(!requestedQueries.includes('Property Manager'));
    assert.equal(report.status, 'empty');
  });
});
