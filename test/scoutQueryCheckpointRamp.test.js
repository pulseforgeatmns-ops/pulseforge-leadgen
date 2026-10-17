'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { defaultScoutRamp, _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const { createCheckpointSession } = require('../services/scoutQueryCheckpoints');

test('governed ramp wires mission-bound checkpoints and emits only bounded progress counts', async () => {
  const events = [], logs = [];
  let acknowledged = 0;
  const snapshot = { status: 'pending_query_work', observedQueries: 2, handledQueries: 1,
    fetchedQueries: 0, replayedQueries: 0, pendingAcknowledgements: 0, unresolvedQueries: 1,
    privateQuery: 'private@example.com' };
  const pool = { query: async () => { throw Error('unexpected database access'); } };
  const result = await defaultScoutRamp({
    cycleId: 'moc_13_fixture', pool,
    program: { id: 'fixture', tenant_id: '13', policy: {} },
    source: { payload: { structuredMission: { market: { segment: 'small_business_owner' },
      geography: { region: 'United States', scope: 'nationwide', cities: [] } } } },
    store: { tenantId: '13', clientId: 13, event: async (...args) => events.push(args) },
    plan: { deficit: 1 }, skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0 }) },
    recoverExistingInventory: async () => ({}),
    loadDiscoveryBackoff: async () => ({ diversify: true, searchGeneration: 1 }),
    createQueryCheckpointSession: input => {
      assert.equal(input.pool, pool); assert.equal(input.tenantId, '13');
      assert.ok(input.fingerprint);
      return { acknowledge: async () => { acknowledged++; }, snapshot: () => snapshot };
    },
    runDiscovery: async (_input, opts) => {
      assert.equal(opts.placesProvider.id, 'google_places');
      assert.equal(typeof opts.acknowledgeDiscovery, 'function');
      assert.equal(acknowledged, 0, 'ramp must not acknowledge before ingestion');
      await opts.persistCompanies({ companies: [] });
      await opts.acknowledgeDiscovery();
      return { kind: 'investigated' };
    },
    logger: { log: (...args) => logs.push(args) },
  });
  assert.equal(acknowledged, 1);
  assert.equal(result.queryProgression.status, 'pending_query_work');
  assert.equal(result.queryProgression.unresolvedQueries, 1);
  assert.equal(events[0][0], 'scout_replenishment_plan_reconsidered');
  assert.ok(events.some(e => e[0] === 'scout_query_progression'));
  assert.ok(logs.some(e => e[0] === '[max-outbound-control] Scout query progression'));
  assert.ok(!JSON.stringify({ events, logs, progression: result.queryProgression }).includes('private@example.com'));
});

test('unresolved website evidence and the investigation cap retain staged candidates for retry', async t => {
  const websitePath = require.resolve('../services/scoutWebsiteBusinessEvidence');
  const originalWebsite = require.cache[websitePath];
  let siteCalls = 0;
  require.cache[websitePath] = { id: websitePath, filename: websitePath, loaded: true,
    exports: { acquireBusinessEvidence: async () => { siteCalls++; return null; } } };
  t.after(() => {
    if (originalWebsite) require.cache[websitePath] = originalWebsite;
    else delete require.cache[websitePath];
  });
  let payload = [];
  let state = null;
  const checkpointStore = {
    tenantId: '10',
    load: async () => state ? { state, candidates: payload } : null,
    stage: async (_key, candidates) => { state = 'fetched'; payload = candidates; return { state, candidates: payload }; },
    acknowledge: async () => { state = 'handled'; payload = []; },
  };
  const checkpoint = createCheckpointSession({ store: checkpointStore });
  const candidates = Array.from({ length: 21 }, (_, i) => ({
    id: `p${i}`, placeId: `p${i}`, name: `Fixture ${i}`, website: `https://fixture${i}.example`,
    address: 'Manchester, NH', location: 'Manchester, NH',
    industry: 'Property Management Manchester NH', discoveryQuery: 'Property Management Manchester NH',
    discoveryConcept: 'Property Management',
  }));
  const prior = await checkpoint.lookup('Property Management Manchester NH', false);
  await checkpoint.stage(prior.key, candidates);
  let receipt;
  let acknowledged = 0;
  const universe = await constructCandidateUniverse({
    searchDefinition: { tenantId: '10', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
      segments: ['property_management'], businessNeed: 'commercial_cleaning' },
    forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [{ id: 'public_business_places', sourceType: 'public_business_data', available: () => true,
      discover: async () => ({ candidates, errors: [] }) }],
    persistCompanies: async input => {
      receipt = await persistDiscoveredCompanies({ query: async sql => {
        assert.match(sql, /FROM prospects p/);
        return { rows: [] };
      } }, { tenantId: '10', clientId: 10, candidateOwnership: async () => null }, {
        ...input, scoutContext: { scope: { eligibleSubsegments: ['property_manager'] },
          allowedCities: ['manchester'], investigateBusinessEvidence: true },
      });
      return receipt;
    },
    acknowledgeDiscovery: async () => { acknowledged++; await checkpoint.acknowledge(); },
  });
  assert.equal(siteCalls, 20, 'the existing per-run investigation cap is unchanged');
  assert.equal(receipt.inserted, 0);
  assert.equal(receipt.admission.rejected.unclassifiable_vertical, 21);
  assert.equal(receipt.checkpointComplete, false);
  assert.deepEqual(universe.discoveryErrors, []);
  assert.equal(acknowledged, 0);
  assert.equal(state, 'fetched');
  assert.equal(payload.length, 21);
  assert.equal(checkpoint.snapshot().status, 'pending_query_work');
  const restarted = createCheckpointSession({ store: checkpointStore });
  assert.deepEqual((await restarted.lookup('Property Management Manchester NH', false)).candidates, candidates);
});

test('empty ingestion explicitly completes without database or provider calls', async () => {
  const result = await persistDiscoveredCompanies({ query: async () => { throw Error('unexpected database call'); } },
    { tenantId: '13', clientId: 13 }, { tenantId: '13', companies: [] });
  assert.equal(result.inserted, 0);
  assert.equal(result.checkpointComplete, true);
});

test('existing queue admission explicitly completes without resetting retry controls', async () => {
  const queries = [];
  const result = await persistDiscoveredCompanies({ query: async (sql, params) => {
    queries.push(sql);
    if (/FROM prospects p/.test(sql)) return { rows: [] };
    if (/INSERT INTO scout_unenriched/.test(sql)) {
      assert.equal(params[7], 13);
      return { rowCount: 0, rows: [] };
    }
    assert.match(sql, /WHERE u.client_id=\$1/);
    return { rows: [{ id: 'existing-queue-row', client_id: 13, eligibility_bucket: 'attempt_limit_hold' }] };
  } }, { tenantId: '13', clientId: 13, candidateOwnership: async () => null }, {
    tenantId: '13', companies: [{ name: 'Fixture Painting', website: 'https://fixture.example',
      placeTypes: ['painter'], description: 'Painting contractor', location: 'Denver, Colorado' }],
    scoutContext: { scope: { segment: 'small_business_owners', region: 'United States' } },
  });
  assert.equal(result.inserted, 0);
  assert.equal(result.admission.alreadyQueued, 1);
  assert.equal(result.checkpointComplete, true);
  assert.ok(queries.every(sql => !/UPDATE|DELETE/.test(sql)));
});
