'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

// Keep the actual Scout/provider/HTTP-guard stack. Replace only database-backed
// metering/attribution and the final HTTP transport with synthetic fixtures.
const spend = require('../utils/apiSpendControl');
const attribution = require('../utils/placesCostAttribution');
const originalReserve = spend.reserveApiCall;
const originalSettle = spend.settleApiCall;
const originalRecord = attribution.recordPlacesRequest;
let reservations = [];
spend.reserveApiCall = async metadata => { reservations.push(metadata); return 'fixture-reservation'; };
spend.settleApiCall = async () => {};
attribution.recordPlacesRequest = async () => {};

const { Scout } = require('../packages/scout');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createPlacesDiscoveryAdapter, discoverCandidates } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
test.after(() => {
  spend.reserveApiCall = originalReserve;
  spend.settleApiCall = originalSettle;
  attribution.recordPlacesRequest = originalRecord;
});

function mission(tenantId) {
  return {
    id: 'offline-pause-discovery', tenantId: String(tenantId), clientId: tenantId,
    objective: 'Find law firms in Manchester for commercial cleaning.',
    structuredMission: {
      objective: 'Find law firms in Manchester for commercial cleaning.',
      market: { segment: 'law_firm', industry: 'legal', buyer: 'office manager', label: 'Law firms' },
      geography: { region: 'Manchester NH', cities: ['Manchester'] },
      constraints: ['commercial_only'], evidence: { requireDecisionMaker: true },
      successMetric: { qualifiedLeads: 1 },
    },
    constraints: { vertical: 'law_firm', locationHint: 'Manchester NH' },
  };
}

async function withFlag(value, fn) {
  const prior = process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  if (value == null) delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  else process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = value;
  try { return await fn(); }
  finally {
    if (prior == null) delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
    else process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = prior;
  }
}

test('actual Scout discovery propagates a mid-flight Places pause instead of a failed/empty intelligence result', async () => withFlag(undefined, async () => {
  reservations = [];
  const http = [];
  const provider = createPlacesProvider({
    tenantId: 10, apiKey: 'offline-scout-pause-fixture',
    fetchImpl: async url => {
      http.push(String(url));
      assert.equal(http.length, 1, 'a pause must prevent details and all later paid searches');
      assert.match(String(url), /textsearch/);
      process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
      return { ok: true, status: 200, json: async () => ({ status: 'OK', results: [{
        place_id: 'offline-place', name: 'Fixture Law Firm', formatted_address: 'Manchester NH',
      }] }) };
    },
  });
  const adapter = createPlacesDiscoveryAdapter({ placesProvider: provider });
  await assert.rejects(() => Scout.discover({
    mission: mission(10),
    opts: { companies: [], people: [], persistMemory: false, discoveryAdapters: [adapter], placesProvider: provider, enablePlaces: true },
  }), { code: 'anchor_research_provider_paused' });
  assert.equal(http.length, 1);
  assert.equal(reservations.length, 1, 'only completed search reservation is retained');
}));

test('legacy Places discovery and adapter aggregation propagate the same real control error', async () => withFlag(undefined, async () => {
  let calls = 0;
  const provider = createPlacesProvider({
    tenantId: 10, apiKey: 'offline-legacy-pause-fixture',
    fetchImpl: async () => {
      calls++;
      process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
      return { ok: true, status: 200, json: async () => ({ status: 'OK', results: [{ place_id: 'offline-legacy-place', name: 'Fixture Law' }] }) };
    },
  });
  await assert.rejects(() => discoverCandidates({ tenantId: '10', geography: { label: 'Manchester NH' }, segments: ['law_firm'] }, [
    createPlacesDiscoveryAdapter({ placesProvider: provider }),
  ]), { code: 'anchor_research_provider_paused' });
  assert.equal(calls, 1);
}));

test('ordinary provider errors retain their adapter error-result behavior', async () => withFlag('true', async () => {
  const result = await discoverCandidates({ tenantId: '13', geography: { label: 'Manchester NH' }, segments: ['law_firm'] }, [
    createPlacesDiscoveryAdapter({ placesProvider: {
      available: () => true,
      collectEvidence: async () => { throw new Error('synthetic ordinary provider failure'); },
      search: async () => { throw new Error('synthetic ordinary provider failure'); },
    } }),
  ]);
  assert.equal(result.candidates.length, 0);
  assert.ok(result.errors.some(error => error.code === 'provider_error'));
}));

test('real discovery provider retains armed non-Anchor and default-off Anchor HTTP behavior', async () => {
  for (const [flag, tenantId] of [['true', 13], [undefined, 10]]) {
    await withFlag(flag, async () => {
      let calls = 0;
      const provider = createPlacesProvider({ tenantId, apiKey: `offline-compatibility-${tenantId}`, fetchImpl: async () => {
        calls++;
        return { ok: true, status: 200, json: async () => ({ status: 'ZERO_RESULTS', results: [] }) };
      } });
      const result = await discoverCandidates({ tenantId: String(tenantId), geography: { label: 'Manchester NH' }, segments: ['law_firm'] }, [
        createPlacesDiscoveryAdapter({ placesProvider: provider }),
      ]);
      assert.equal(calls, 1);
      assert.equal(result.candidates.length, 0);
      assert.equal(result.errors.length, 0);
    });
  }
});

function seamProvider(label, { interrupt = false } = {}) {
  let calls = 0;
  const queries = [];
  const provider = createPlacesProvider({ apiKey: `offline-seam-${label}`, fetchImpl: async () => {
    calls++;
    if (interrupt) {
      assert.equal(calls, 1);
      process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED = 'true';
    }
    return { ok: true, status: 200, json: async () => interrupt
      ? { status: 'OK', results: [{ place_id: `offline-${label}`, name: 'Fixture Company' }] }
      : { status: 'ZERO_RESULTS', results: [] } };
  } });
  const search = provider.search.bind(provider);
  provider.search = (query, profile) => {
    queries.push(query);
    return search(query, profile);
  };
  return { provider, calls: () => calls, queries };
}

test('ProspectDiscovery preserves context tenant through the real provider and propagates interruption', async () => {
  const { createProspectDiscoveryCapability } = require('../packages/capabilities/discovery/ProspectDiscovery');
  for (const [flag, tenantId, interrupt] of [['true', 13, false], [undefined, 10, false], [undefined, 10, true]]) {
    await withFlag(flag, async () => {
      const fixture = seamProvider(`capability-${tenantId}-${interrupt}`, { interrupt });
      const capability = createProspectDiscoveryCapability({ searchProvider: fixture.provider, profileSelector: { select: () => ({
        selection: 'pinned', alternatives: [], profile: { id: 'offline-profile', name: 'Fixture', version: 1, targetCount: 1,
          industryTargets: ['law_firm'], geography: { label: 'Manchester NH' }, rankingSignals: [] },
      }) } });
      const invoke = () => capability.execute({ tenantId, clientId: tenantId, constraints: { targetCount: 1 } });
      if (interrupt) await assert.rejects(invoke, { code: 'anchor_research_provider_paused' });
      else await invoke();
      assert.equal(fixture.calls(), 1);
      assert.deepEqual(fixture.queries, [{ industry: 'law_firm', location: 'Manchester NH', limit: 5 }],
        'tenant authority travels through scope without changing provider query options');
    });
  }
});

test('acquisition adapter preserves request tenant, rejects nested conflicts, and escapes enclosing fallback', async () => {
  const { adaptSearchProvider } = require('../packages/capabilities/acquisition/providerContract');
  const { createProspectAcquisition } = require('../packages/capabilities/acquisition/ProspectAcquisition');
  for (const [flag, tenantId, interrupt] of [['true', 13, false], [undefined, 10, false], [undefined, 10, true]]) {
    await withFlag(flag, async () => {
      const fixture = seamProvider(`acquisition-${tenantId}-${interrupt}`, { interrupt });
      const adapter = adaptSearchProvider(fixture.provider);
      const acquisition = createProspectAcquisition({ registry: { get: () => adapter } });
      const invoke = () => acquisition.acquire({ tenantId, industry: 'law_firm', location: 'Manchester NH', targetCount: 1 });
      if (interrupt) await assert.rejects(invoke, { code: 'anchor_research_provider_paused' });
      else assert.deepEqual((await invoke()).errors, []);
      assert.equal(fixture.calls(), 1);
      assert.deepEqual(fixture.queries, [{ industry: 'law_firm', location: 'Manchester NH', limit: 1 }],
        'legacy search keeps its original query options');
    });
  }
  await withFlag('true', async () => {
    const fixture = seamProvider('query-conflict');
    const adapter = adaptSearchProvider(fixture.provider);
    await assert.rejects(() => adapter.acquire({ tenantId: 13, query: { tenantId: 10, industry: 'law_firm' } }), { code: 'research_provider_tenant_mismatch' });
    await assert.rejects(() => adapter.acquire({ tenantId: 10, query: { tenantId: 13, industry: 'law_firm' } }), { code: 'anchor_research_provider_paused' });
    assert.equal(fixture.calls(), 0);
  });
});

test('coverage engine propagates interruption from actual provider without failed-workload conversion', async () => withFlag(undefined, async () => {
  const { executeCoveragePlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
  const fixture = seamProvider('coverage-interruption', { interrupt: true });
  const adapter = createPlacesDiscoveryAdapter({ placesProvider: fixture.provider });
  await assert.rejects(() => executeCoveragePlan({
    workloads: [{ id: 'offline-work', city: 'Manchester NH', concept: 'Property Manager', source: 'public_business_data' }],
    totals: { cities: 1, concepts: 1, sources: 1, searches: 1 },
  }, { tenantId: '10', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] }, segments: ['property_management'] }, [adapter]), { code: 'anchor_research_provider_paused' });
  assert.equal(fixture.calls(), 1);
}));

test('candidate continuation propagates real provider pause instead of exhausted/failed task', async () => withFlag(undefined, async () => {
  const { runCandidateInvestigationLoop } = require('../packages/scout/investigation/CandidateInvestigation');
  const { executeInvestigationTask } = require('../packages/scout/coverage/HypothesisDrivenDiscoveryEngine');
  const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
  const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
  const company = { id: 'offline-company', name: 'Fixture Property Management', industry: 'property_management', location: 'Manchester NH', website: 'https://firm.example', icpScore: 91 };
  const searchDefinition = buildAcquisitionSearchDefinition({ tenantId: '10',
    targetContext: { geography: 'Manchester NH', segments: ['property_management'], businessType: 'commercial_cleaning' },
    businessContext: { serviceGeography: 'Manchester NH', commercialCapability: 'commercial_cleaning', preferredSegments: ['property_management'] },
  });
  const attached = attachFitToClassified({ name: company.name, fit: 0.91, signals: [], unknowns: ['No identifiable operations decision-maker'],
    observations: [{ text: 'Property management operator in Manchester.' }], evidenceRefs: [{ id: 'offline-evidence', label: 'Places listing', sourceKind: 'observed_fact' }],
  }, company, searchDefinition, Date.now());
  const fixture = seamProvider('candidate-interruption', { interrupt: true });
  const adapter = createPlacesDiscoveryAdapter({ placesProvider: fixture.provider });
  await assert.rejects(() => runCandidateInvestigationLoop({
    companies: [company], classified: [attached.classified], searchDefinition,
    marketDefinition: { segments: ['property_management'] }, mission: { id: 'offline-candidate-mission' }, adapters: [adapter],
    opts: { maxCandidateInvestigationIterations: 1, executeInvestigationTask: (task, search, adapters, market, opts) =>
      executeInvestigationTask({ ...task, evidenceType: 'identity', providers: [{ providerId: 'google_maps', evidenceType: 'identity', status: 'assigned' }] }, search, adapters, market, opts) },
  }), { code: 'anchor_research_provider_paused' });
  assert.equal(fixture.calls(), 1);
}));
