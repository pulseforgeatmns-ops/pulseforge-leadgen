'use strict';

const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
// Exercise the real provider/cache, coverage and Scout paths using only synthetic
// HTTP and persistence. No provider credentials, network or database are used.
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  async query(sql) {
    assert.match(sql, /places_api_requests|CREATE TABLE|CREATE INDEX|ALTER TABLE/);
    return { rows: [] };
  },
} };
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true, exports: {
  reserveApiCall: async () => 'synthetic-reservation', settleApiCall: async () => {},
} };
const cache = require('../utils/placesQueryCache');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createPlacesDiscoveryAdapter, discoverCandidates } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { executeCoveragePlan, executeLegacyDiscovery } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const { runScoutAcquisitionIntelligence } = require('../packages/max/scoutAcquisition/ScoutAdapter');
const { providerReceiptDiagnostic } = require('../services/outboundControlDiagnostics');
const { runAcquisitionIntelligenceLoop, createMemoryAcquisitionState } = require('../services/scoutAcquisitionIntelligence');
const { createMemoryStore, createSpecialistDelegationService } = require('../services/specialistDelegation');

const cycleId = 'moc_13_1234567890abcdef';
const sourceType = 'public_business_data';
const searchDefinition = {
  tenantId: '13', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
  segments: ['property_management'], businessNeed: 'commercial_cleaning',
};
const delegation = {
  tenantId: '13', authority: 'observe', objective: 'Find property management companies.',
  targetContext: { geography: 'Manchester NH', segments: ['property_management'], businessType: 'commercial_cleaning' },
  businessContext: { serviceGeography: 'Manchester NH', preferredSegments: ['property_management'], commercialCapability: 'commercial_cleaning' },
};
function plan(concepts = ['Property Manager']) {
  return { workloads: concepts.map((concept, i) => ({ id: `work-${i}`, city: 'Manchester NH', concept, source: sourceType })),
    totals: { cities: 1, concepts: concepts.length, sources: 1, searches: concepts.length } };
}
function diagnostic(providerReceipts, errors = []) {
  return providerReceiptDiagnostic('13', cycleId, { result: { payload: { providerReceipts }, errors } });
}
function fixtureAdapter(discover, extra = {}) {
  return { id: 'public_business_places', sourceType, available: () => true, discover, ...extra };
}
function fixtureProvider(status) {
  let fetches = 0;
  const provider = createPlacesProvider({ apiKey: 'synthetic-fixture-key', fetchImpl: async () => {
    fetches++;
    return { status: 200, ok: true, json: async () => ({ status, results: [], error_message: 'private-provider-message' }) };
  } });
  return { provider, fetches: () => fetches };
}
beforeEach(() => cache.clearPlacesQueryCache());

for (const [status, blocker] of [['REQUEST_DENIED', 'provider_denied'], ['OVER_QUERY_LIMIT', 'provider_quota'], ['ZERO_RESULTS', 'unknown']]) {
  test(`ordinary Scout coverage carries ${status} through the real facade into bounded receipts`, async () => {
    const fixture = fixtureProvider(status);
    const options = {
      delegationService: createSpecialistDelegationService({ store: createMemoryStore() }),
      aoStore: createMemoryAcquisitionState(), companies: [], people: [],
      discoveryAdapters: [createPlacesDiscoveryAdapter({ placesProvider: fixture.provider })],
    };
    const result = await runAcquisitionIntelligenceLoop({
      ...delegation, authorizedTenantId: '13', force: true,
      workflow: 'outbound_inventory_replenishment', inventoryDeficit: 1,
      question: 'Find property management companies in Manchester NH.',
    }, options);
    assert.equal(result.delegated, true);
    const receipt = providerReceiptDiagnostic('13', cycleId, result);
    assert.equal(receipt.reportsAvailable, true);
    assert.ok(receipt.providers.length > 0);
    assert.equal(receipt.discovery.blocker, blocker === 'unknown' ? 'insufficient_coverage' : blocker);
    assert.ok(fixture.fetches() > 0);
    for (const row of receipt.providers) {
      assert.equal(row.provider, 'google_places');
      assert.equal(row.status, status === 'ZERO_RESULTS' ? 'empty' : 'failed');
      assert.equal(row.executed, true);
      assert.equal(row.rawResultCount, 0);
      assert.equal(row.mappedCandidateCount, 0);
      assert.ok(row.queries.some(query => query.status === status));
    }
    assert.deepEqual(result.outboundInvoked, []);
    assert.ok(!JSON.stringify(receipt).includes('private-provider-message'));
    const fetches = fixture.fetches();
    const repeat = await runScoutAcquisitionIntelligence(delegation, { companies: [], people: [], discoveryAdapters: options.discoveryAdapters });
    const repeatedReceipt = providerReceiptDiagnostic('13', cycleId, repeat);
    assert.equal(fixture.fetches(), fetches, 'warm cache must not cause additional HTTP');
    assert.ok(repeatedReceipt.providers.every(row => row.queries.every(query => query.cacheHit === true && query.origin === 'cache')));
    assert.equal(repeatedReceipt.providers[0].queries[0].cacheStrategy, 'memory_ttl');
  });
}

test('coverage reuse records one actual attempt and preserves candidate filtering/coverage', async () => {
  let calls = 0;
  const adapter = fixtureAdapter(async () => {
    calls++;
    return { candidates: [{ name: 'Valid Company', website: 'https://valid.example', location: 'Manchester NH' }, {}],
      execution: { executed: true, totals: { queries: 1, results: 3 }, queries: [] } };
  });
  const result = await executeCoveragePlan(plan(['Property Manager', 'Property Manager']), searchDefinition, [adapter]);
  assert.equal(calls, 1);
  assert.equal(result.providerReceipts.length, 1);
  assert.equal(result.providerReceipts[0].rawResultCount, 3);
  assert.equal(result.providerReceipts[0].mappedCandidateCount, 2, 'receipt is pre-contract adapter output');
  assert.equal(result.candidates.length, 2, 'one accepted candidate per addressed workload is unchanged');
  assert.equal(result.errors.length, 2);
  assert.deepEqual(result.executed.map(row => row.reused), [false, true]);
  assert.equal(result.coverage.searches.executed, 2);
});

test('non-reused calls with the same evidence key retain every receipt', async () => {
  let calls = 0;
  const adapter = fixtureAdapter(async () => ({ candidates: [], execution: { totals: { queries: ++calls, results: 0 } } }), { id: 'custom_provider' });
  const result = await executeCoveragePlan(plan(['Property Manager', 'Property Manager']), searchDefinition, [adapter]);
  assert.equal(calls, 2);
  assert.deepEqual(result.providerReceipts.map(row => row.execution.totals.queries), [1, 2]);
  assert.equal(diagnostic(result.providerReceipts).providers[0].provider, 'unknown', 'do not label an injected/custom provider as Google');
});

for (const execute of [executeCoveragePlan, executeLegacyDiscovery]) {
  test(`${execute.name} distinguishes skipped and thrown outcomes from measured empty results`, async () => {
    for (const kind of ['skipped', 'failed', 'empty']) {
      let calls = 0;
      const adapter = fixtureAdapter(async () => { calls++; if (kind === 'failed') throw Error('private exception'); return { candidates: [] }; },
        { available: () => kind !== 'skipped' });
      const result = execute === executeCoveragePlan
        ? await execute(plan(), searchDefinition, [adapter])
        : await execute(searchDefinition, [adapter]);
      const row = diagnostic(result.providerReceipts, result.errors).providers[0];
      assert.equal(calls, kind === 'skipped' ? 0 : 1);
      assert.equal(row.status, kind);
      assert.equal(row.executed, null, 'never infer network activity from an adapter result or exception');
      assert.equal(row.rawResultCount, null);
      assert.equal(row.mappedCandidateCount, kind === 'empty' ? 0 : null);
      assert.equal(row.queryCount, null);
      assert.deepEqual(row.errors.map(error => error.code), kind === 'empty' ? []
        : [kind === 'skipped' ? 'adapter_unavailable' : 'provider_error']);
      assert.equal(diagnostic(result.providerReceipts).discovery.blocker, kind === 'empty' ? 'unknown'
        : kind === 'skipped' ? 'provider_unavailable' : 'provider_failed');
      assert.ok(!JSON.stringify(diagnostic(result.providerReceipts, result.errors)).includes('private exception'));
    }
  });
}

test('legacy discovery retains returned report identity and real execution metadata', async () => {
  const report = { candidates: [], errors: [{ code: 'google_places_status_REQUEST_DENIED' }],
    execution: { providerId: 'google_places', executed: true, totals: { queries: 1, results: 0 },
      errors: [{ code: 'google_places_status_REQUEST_DENIED', googleStatus: 'REQUEST_DENIED' }],
      queries: [{ googleStatus: 'REQUEST_DENIED', cacheHit: true, cacheStrategy: 'memory_ttl' }] } };
  const result = await discoverCandidates(searchDefinition, [fixtureAdapter(async () => report)]);
  assert.equal(result.sourceReports[0], report);
  assert.equal(result.providerReceipts[0].execution, report.execution);
  const receipt = diagnostic(result.providerReceipts);
  assert.equal(receipt.discovery.blocker, 'provider_denied');
  assert.equal(receipt.providers[0].queries[0].origin, 'cache');
});

test('unavailable receipt collection stays null, while completed empty collection stays []', async () => {
  assert.equal(diagnostic(undefined).reportsAvailable, false);
  assert.equal(diagnostic(null).reportsAvailable, false);
  assert.equal(diagnostic([]).reportsAvailable, true);
  const historical = { result: { payload: { providerExecution: [] } } };
  assert.equal(providerReceiptDiagnostic('13', cycleId, historical).reportsAvailable, true);
  historical.result.payload.providerReceipts = null;
  assert.equal(providerReceiptDiagnostic('13', cycleId, historical).reportsAvailable, false, 'explicit unknown must not fall back to legacy []');
  const empty = await executeCoveragePlan({ workloads: [] }, searchDefinition, []);
  assert.deepEqual(empty.providerReceipts, []);
  assert.equal(diagnostic(empty.providerReceipts).reportsAvailable, true);
  assert.deepEqual((await discoverCandidates(searchDefinition, [])).providerReceipts, []);
  for (const opts of [
    { discoveryAdapters: [] },
    { mode: 'provider_failure' },
    { loadCompanies: async () => { throw Error('fixture retrieval failure'); }, companies: undefined },
    { discoveryAdapters: [fixtureAdapter(async () => ({ candidates: [] }), { available() { throw Error('fixture gate failure'); } })] },
  ]) {
    const result = await runScoutAcquisitionIntelligence(delegation, { companies: [], people: [], ...opts });
    assert.deepEqual(result.payload.providerExecution, []);
    assert.equal(result.payload.providerReceipts, null);
    assert.equal(providerReceiptDiagnostic('13', cycleId, result).reportsAvailable, false);
  }
  const invalid = await runScoutAcquisitionIntelligence({ tenantId: '13', authority: 'observe' }, { companies: [] });
  assert.deepEqual(invalid.payload.providerExecution, []);
  assert.equal(invalid.payload.providerReceipts, null);
});

test('Scout retains thrown adapter attempt without claiming an executed HTTP request', async () => {
  const result = await runScoutAcquisitionIntelligence(delegation, { companies: [], people: [],
    discoveryAdapters: [fixtureAdapter(async () => { throw Error('private failure'); })] });
  const receipt = providerReceiptDiagnostic('13', cycleId, result);
  assert.equal(result.status, 'blocked');
  assert.equal(receipt.discovery.blocker, 'provider_failed');
  assert.equal(receipt.reportsAvailable, true);
  assert.ok(receipt.providers.length);
  assert.ok(receipt.providers.every(row => row.status === 'failed' && row.executed === null && row.rawResultCount === null));
});

test('hypothesis discovery continues to forward its existing evidence reports', async () => {
  const universe = await constructCandidateUniverse({ searchDefinition, existing: { companies: [], people: [] },
    adapters: [fixtureAdapter(async () => ({ candidates: [] }))],
    marketDefinition: { segments: ['property_management'], geography: 'Manchester NH', searchDefinition },
  });
  assert.ok(universe.providerReceipts.length > 0);
  const report = universe.providerReceipts.find(row => row.providerId === 'google_maps');
  assert.ok(report);
  assert.ok(Array.isArray(report.evidenceProduced));
  assert.equal(universe.providerReceipts, universe.providerReports, 'hypothesis evidence reports are preserved verbatim');
});

test('coverage receipts remain bounded and sanitized after propagation', async () => {
  const secret = 'secret@example.com https://private.test/token';
  const adapter = fixtureAdapter(async () => ({ candidates: [], errors: Array(25).fill({ code: secret, message: secret }), execution: { executed: true,
    queries: Array(25).fill({ googleStatus: secret, query: secret, httpStatus: 900, googleError: secret, cacheHit: true, cacheStrategy: secret }),
    errors: [{ code: secret, message: secret }] } }), { id: 'custom_provider' });
  const result = await executeCoveragePlan(plan(Array(25).fill('Property Manager')), searchDefinition, [adapter]);
  const receipt = diagnostic(result.providerReceipts);
  assert.equal(receipt.reportsTruncated, true);
  assert.equal(receipt.providers.length, 20);
  assert.equal(receipt.providers[0].queriesTruncated, true);
  assert.equal(receipt.providers[0].queries.length, 20);
  assert.ok(!JSON.stringify(receipt).includes('secret'));
});

// These consumers historically use providerExecution as evidence. Operational
// receipts must not change attachment, qualification or candidate production.
test('new observational receipts leave artifact and evidence decisions unchanged', async () => {
  const { buildScoutDiscoveryArtifact } = require('../packages/scout/adapters/ScoutDiscoveryArtifact');
  const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
  for (const report of [
    { candidates: [], execution: { executed: true, totals: { queries: 1, results: 3 }, errors: [], queries: [] } },
    { candidates: [], errors: [{ code: 'google_places_status_REQUEST_DENIED' }],
      execution: { executed: true, totals: { queries: 1, results: 0 }, errors: [{ code: 'google_places_status_REQUEST_DENIED' }], queries: [] } },
    { candidates: [], execution: null },
  ]) {
    const result = await runScoutAcquisitionIntelligence(delegation, { companies: [], people: [],
      discoveryAdapters: [fixtureAdapter(async () => report)] });
    assert.deepEqual(result.payload.providerExecution, [], 'ordinary evidence contract remains baseline');
    assert.ok(result.payload.providerReceipts.length > 0);
    const baseline = structuredClone(result);
    delete baseline.payload.providerReceipts;
    const artifact = buildScoutDiscoveryArtifact(result);
    const { sourceResult, ...decisionArtifact } = artifact;
    const { sourceResult: baselineSource, ...baselineArtifact } = buildScoutDiscoveryArtifact(baseline);
    assert.deepEqual(decisionArtifact, baselineArtifact);
    assert.equal(artifact.blocked, true);
    assert.equal(artifact.blockedDecisionReason, 'no_attachable_evidence');
    assert.deepEqual(normalizeScoutDiscoveryPayload(result), normalizeScoutDiscoveryPayload(baseline));
  }
});


test('post-processing failure and coverage reuse never invent additional provider attempts', async () => {
  let calls = 0;
  const report = { candidates: [null], execution: { executed: true, totals: { queries: 1, results: 1 } } };
  const result = await executeCoveragePlan(plan(['Property Manager', 'Property Manager']), searchDefinition,
    [fixtureAdapter(async () => { calls++; return report; })]);
  assert.equal(calls, 1);
  assert.equal(result.providerReceipts.length, 1);
  assert.equal(result.providerReceipts[0].status, 'completed');
  assert.equal(result.providerReceipts[0].execution, report.execution);
  assert.equal(result.errors.length, 2, 'existing processing errors remain unchanged');
  const malformed = await discoverCandidates(searchDefinition, [fixtureAdapter(async () => ({ candidates: {}, execution: report.execution }))]);
  assert.equal(malformed.providerReceipts.length, 1);
  assert.equal(malformed.errors.length, 1);
});

test('malformed diagnostic envelopes preserve unavailable state without throwing', () => {
  for (const payload of [null, undefined, false, 1, 'invalid']) {
    const receipt = providerReceiptDiagnostic('13', cycleId, { payload });
    assert.equal(receipt.reportsAvailable, false);
    assert.deepEqual(receipt.providers, []);
  }
});
