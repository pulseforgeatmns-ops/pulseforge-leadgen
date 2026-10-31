'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createFacilityResearchCoverage } = require('../services/scoutFacilityResearchCoverage');
const scope = { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'],
  cities: ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'] };

test('hypothesis discovery preserves typed unavailable-adapter causes after collecting a facility', async () => {
  const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
  const { providerReceiptDiagnostic } = require('../services/outboundControlDiagnostics');
  const searchDefinition = { tenantId: '10', segments: scope.eligibleSubsegments,
    businessNeed: 'commercial_cleaning', geography: { cities: scope.cities, state: 'NH' } };
  const result = await constructCandidateUniverse({ searchDefinition, existing: { companies: [], people: [] },
    adapters: [{ id: 'public_business_places', sourceType: 'public_business_data', available: () => true,
      discover: async () => ({ candidates: [{ name: 'Synthetic Manufacturing', placeId: 'synthetic-factory',
        address: '101 Fixture Rd, Manchester, NH 03101, USA', website: 'https://fixture.example' }],
      errors: [], execution: { executed: true, errors: [] } }) }],
    marketDefinition: { segments: scope.eligibleSubsegments, geography: 'NH', searchDefinition } });
  assert.ok(result.companies.length > 0);
  const unavailable = result.providerReceipts.filter(row => ['existing_pf', 'website'].includes(row.providerId));
  assert.ok(unavailable.length > 0);
  assert.ok(unavailable.every(row => row.errors[0].code === 'adapter_unavailable'));
  const receipt = providerReceiptDiagnostic('10', 'moc_10_1234567890abcdef',
    { kind: 'investigated', result: { status: 'completed', payload: { providerReceipts: result.providerReceipts } } },
    { researchOnly: true });
  assert.equal(receipt.discovery.blocker, 'none');
  assert.equal(receipt.discovery.nonBlockingProviderIssue, 'provider_unavailable');
  assert.ok(receipt.providers.some(row => row.errors.some(error => error.code === 'adapter_unavailable')));
});

test('native Scout result keeps incomplete coverage distinct from optional adapter unavailability', async () => {
  const { runScoutAcquisitionIntelligence } = require('../packages/max/scoutAcquisition/ScoutAdapter');
  const { providerReceiptDiagnostic } = require('../services/outboundControlDiagnostics');
  const segments = scope.eligibleSubsegments;
  const searchDefinition = { tenantId: '10', segments, businessNeed: 'commercial_cleaning',
    geography: { label: 'Greater Manchester NH', state: 'NH', cities: scope.cities } };
  const result = await runScoutAcquisitionIntelligence({ tenantId: '10', authority: 'observe',
    objective: 'Find approved commercial cleaning facilities.',
    businessContext: { serviceGeography: 'Greater Manchester NH', preferredSegments: segments },
    targetContext: { geography: 'Greater Manchester NH', cities: scope.cities, segments, businessType: 'commercial_cleaning' } },
  { companies: [], people: [], marketDefinition: { segments, geography: 'Greater Manchester NH', searchDefinition },
    discoveryAdapters: [{ id: 'public_business_places', sourceType: 'public_business_data', available: () => true,
      discover: async () => ({ candidates: [{ name: 'Synthetic Private School', placeId: 'synthetic-school',
        website: 'https://fixture.example', address: '1 Fixture Rd, Manchester, NH 03101, USA',
        location: 'Manchester NH', types: ['school'] }], errors: [], execution: { executed: true, errors: [] } }) }] });
  assert.equal(result.status, 'partial');
  const receipt = providerReceiptDiagnostic('10', 'moc_10_1234567890abcdef', result, { researchOnly: true });
  assert.equal(receipt.discovery.blocker, 'insufficient_coverage');
  assert.equal(receipt.discovery.nonBlockingProviderIssue, null);
  assert.ok(receipt.providers.some(row => row.errors.some(error => error.code === 'adapter_unavailable')));
});

test('real provider/checkpoint boundary keeps one page and diversifies across eight restarted synthetic passes', async () => {
  const checkpoints = new Map(), pages = new Map(), history = [], selected = [], searches = [];
  const store = { tenantId: '10', load: async key => checkpoints.get(key) || null,
    stage: async (key, candidates) => { const row = { state: 'fetched', candidates }; checkpoints.set(key, row); return row; },
    acknowledge: async keys => keys.forEach(key => { checkpoints.get(key).state = 'handled'; }) };
  const pageStore = { tenantId: '10',
    plan: async rootKey => pages.has(rootKey) ? { status: 'finite_query_exhausted', terminal: true }
      : { key: rootKey, rootKey, index: 0, pageToken: null },
    reserve: async plan => { pages.set(plan.rootKey, { state: 'reserved' }); return 'synthetic-reservation'; },
    stage: async (plan, candidates) => { pages.get(plan.rootKey).state = 'fetched'; return store.stage(plan.key, candidates); },
    fail: async () => assert.fail('synthetic successful search should not fail') };
  for (let n = 0; n < 8; n++) {
    const checkpoint = createCheckpointSession({ store, pageStore });
    const scheduler = createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => history });
    const provider = createPlacesProvider({ apiKey: 'synthetic-key', queryCheckpointSession: checkpoint,
      facilityResearchCoverage: scheduler, useCache: false,
      placesApiDeps: { db: { query: async () => ({ rows: [] }) }, reserveApiCall: async () => 'synthetic-usage', settleApiCall: async () => {} },
      fetchImpl: async url => {
        const parsed = new URL(url); assert.ok(parsed.pathname.endsWith('/textsearch/json'));
        const query = parsed.searchParams.get('query'); searches.push(query); history.push({ query_text: query, attempts: 1 });
        return { status: 200, ok: true, json: async () => ({ status: 'ZERO_RESULTS', results: [] }) };
      } });
    await provider.collectEvidence({ tenantId: '10', scope: 'market', segment: 'managed_facilities', evidenceType: 'identity',
      geography: { cities: scope.cities, state: 'NH' } });
    await checkpoint.acknowledge();
    const receipt = scheduler.snapshot(), chosen = receipt.decisions.filter(x => x.status === 'selected');
    assert.equal(chosen.length, 1); selected.push(chosen[0]);
    assert.equal(checkpoint.snapshot().continuation.newPagesReserved, 1);
    assert.ok(receipt.decisions.some(x => x.reason === 'cycle_page_budget_reached'));
  }
  assert.equal(searches.length, 8); assert.equal(new Set(searches).size, 8);
  assert.equal(new Set(selected.map(x => x.category)).size, 8); assert.equal(new Set(selected.map(x => x.city)).size, 8);
});
