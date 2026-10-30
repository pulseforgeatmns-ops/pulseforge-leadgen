'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createFacilityResearchCoverage } = require('../services/scoutFacilityResearchCoverage');
const scope = { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'],
  cities: ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'] };

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
