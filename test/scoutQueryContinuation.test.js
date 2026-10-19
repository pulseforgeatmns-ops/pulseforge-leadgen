'use strict';
// Entirely synthetic HTTP and persistence. PostgreSQL durability has a separate required gate.
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
let clearFailedCacheAfterAttribution = false;
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  async query(sql) {
    assert.match(sql, /places_api_requests|CREATE TABLE|CREATE INDEX|ALTER TABLE/);
    if (clearFailedCacheAfterAttribution && /INSERT INTO places_api_requests/.test(sql)) {
      clearFailedCacheAfterAttribution = false; require('../utils/placesQueryCache').clearPlacesQueryCache();
    }
    return { rows: [] };
  },
} };
const reservations = [];
let denyBudget = false;
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true, exports: {
  reserveApiCall: async input => { if (denyBudget) throw Error('budget denied'); reservations.push(input); return 'fake'; },
  settleApiCall: async () => {},
} };
const { createCheckpointSession, queryIdentity } = require('../services/scoutQueryCheckpoints');
const { pageIdentity, selectPage, MAX_PAGES } = require('../services/scoutQueryContinuation');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const cache = require('../utils/placesQueryCache');
const request = { tenantId: '13', scope: 'market', segment: 'small_business_owners', evidenceType: 'identity',
  discoveryConcept: 'Painting Contractor', geography: { cities: ['United States'], state: null } };

function fixture() {
  const checkpoints = new Map(); const pages = new Map(); const tickets = new Map(); const queue = new Set();
  let now = Date.parse('2026-10-19T12:00:00Z');
  let failTransfer = false; let failStage = false;
  const store = { tenantId: '13', load: async key => checkpoints.get(key) || null,
    stage: async (key, candidates) => {
      if (!checkpoints.has(key)) checkpoints.set(key, { state: 'fetched', candidates: structuredClone(candidates) });
      return checkpoints.get(key);
    },
    acknowledge: async keys => keys.forEach(key => { checkpoints.get(key).state = 'handled'; }),
  };
  const pageStore = { tenantId: '13',
    plan: async rootKey => selectPage(rootKey, [...pages.values()].filter(row => row.root_query_key === rootKey).map(row => ({
      ...row, checkpoint_state: checkpoints.get(row.query_key)?.state, candidates: checkpoints.get(row.query_key)?.candidates,
    })), checkpoints.get(rootKey), now),
    reserve: async plan => {
      if (pages.has(plan.key)) return null;
      const id = randomUUID();
      pages.set(plan.key, { root_query_key: plan.rootKey, query_key: plan.key, page_index: plan.index,
        request_token: plan.pageToken, state: 'reserved', reservation_id: id, created_at: new Date(now).toISOString() });
      return id;
    },
    stage: async (plan, candidates, metadata) => {
      if (failStage) throw Error('synthetic failed staging');
      const row = pages.get(plan.key);
      assert.equal(row.reservation_id, plan.reservationId);
      assert.equal(row.state, 'reserved');
      assert.equal(checkpoints.has(plan.key), false);
      const snapshot = { state: 'fetched', candidates: structuredClone(candidates) };
      checkpoints.set(plan.key, snapshot);
      Object.assign(row, { state: 'fetched', next_page_token: metadata.nextPageToken,
        raw_results: metadata.rawResults, available_at: new Date(now + 5000).toISOString() });
      return snapshot;
    },
    fail: async (plan, failure) => {
      if (failure.noNetwork === true) { pages.delete(plan.key); return; }
      Object.assign(pages.get(plan.key), { state: 'blocked', blocked_reason:
        failure.googleStatus === 'INVALID_REQUEST' ? 'continuation_token_invalid' : 'page_provider_failure' });
    },
  };
  const ticketStore = { tenantId: '13', transferQuery: async key => {
    if (failTransfer) throw Error('synthetic transfer crash');
    const row = checkpoints.get(key);
    assert.ok(row);
    const candidates = row.candidates.map((hit, index) => ({ ...hit,
      _scoutTicketRefs: [{ tenantId: '13', queryKey: key, memberKey: String(index) }] }));
    for (const candidate of candidates) tickets.set(`${key}/${candidate.placeId}`, candidate);
    row.state = 'handled';
    return { transferred: true, queryKey: key, candidates };
  } };
  const providerCalls = [];
  const makeProvider = (session, options = {}) => createPlacesProvider({ apiKey: 'synthetic-key', queryCheckpointSession: session,
    fetchImpl: async input => {
      const url = new URL(input); providerCalls.push(url.pathname);
      if (url.pathname.includes('details')) {
        const id = url.searchParams.get('place_id');
        if (options.partialDetailsFailure && id === 'p1') throw Error('synthetic detail failure');
        return { status: 200, ok: true, json: async () => ({ status: 'OK', result: {
          place_id: id, name: options.unknownBusiness ? `Fixture Company ${id}` : `Fixture Painting Contractor ${id}`, website: `https://${id}.example`,
          formatted_address: 'Denver, Colorado, United States', types: options.unknownBusiness ? [] : ['painter'],
        } }) };
      }
      const page = url.searchParams.has('pagetoken') ? Number(url.searchParams.get('pagetoken').split('-').at(-1)) : 0;
      const status = options.transientFailure && providerCalls.filter(value => value.includes('textsearch')).length === 1
        ? 'OVER_QUERY_LIMIT' : options.status || (options.invalid && page > 0 ? 'INVALID_REQUEST' : 'OK');
      return { status: 200, ok: true, json: async () => ({ status,
        results: status === 'OK' ? Array.from({ length: options.oversized ? 21 : 20 }, (_, i) => ({
          place_id: `p${page * 20 + i}`, name: options.unknownBusiness ? `Fixture Company ${page * 20 + i}` : `Fixture Painting Contractor ${page * 20 + i}`, types: options.unknownBusiness ? [] : ['painter'],
          formatted_address: 'Denver, Colorado, United States',
        })) : [], next_page_token: page < 2 && status === 'OK' ? `opaque-next-${page + 1}` : null,
      }) };
    },
  });
  const newSession = (options = {}) => createCheckpointSession({ store, pageStore, ticketStore, ...options });
  const persist = async candidates => persistDiscoveredCompanies({ query: async (sql, params) => {
    if (/FROM prospects p/.test(sql)) return { rows: [] };
    if (/INSERT INTO scout_unenriched/.test(sql)) {
      const fresh = !queue.has(params[2]); queue.add(params[2]);
      return { rows: fresh ? [{ id: params[2] }] : [], rowCount: fresh ? 1 : 0 };
    }
    assert.match(sql, /eligibility_bucket/);
    return { rows: [{ id: 'existing', client_id: 13, eligibility_bucket: 'attempt_limit_hold' }] };
  } }, { tenantId: '13', clientId: 13, candidateOwnership: async () => null }, {
    tenantId: '13', companies: candidates.map(hit => ({ ...hit, name: hit.companyName, location: hit.address })),
    scoutContext: { scope: { segment: 'small_business_owners', region: 'United States', scope: 'nationwide' } },
  });
  return { checkpoints, pages, tickets, queue, store, pageStore, ticketStore, providerCalls, makeProvider, newSession, persist,
    advance: () => { now += 60000; cache.clearPlacesQueryCache(); },
    failTransfer: value => { failTransfer = value; }, failStage: value => { failStage = value; },
  };
}
test.beforeEach(() => { clearFailedCacheAfterAttribution = false; reservations.length = 0; denyBudget = false; cache.clearPlacesQueryCache(); });

test('three restarted cycles admit previously unseen eligible businesses, then stop without unchanged rework', async () => {
  const f = fixture();
  for (let cycle = 0; cycle < 3; cycle++) {
    const before = f.providerCalls.length;
    const s = f.newSession(); const provider = f.makeProvider(s);
    const hits = await provider.collectEvidence(request);
    assert.equal(hits.length, 20);
    assert.ok(hits.every(hit => !f.queue.has(hit.website)), 'each cycle produces unseen business identity');
    const admission = await f.persist(hits);
    assert.equal(admission.inserted, 20); assert.equal(admission.admission.admittedToEnrichment, 20);
    assert.equal(f.providerCalls.length - before, 21, 'one bounded search page plus at most twenty details');
    f.advance();
  }
  assert.equal(f.queue.size, 60); assert.equal(f.tickets.size, 60); assert.equal(f.pages.size, 3);
  const baselineCalls = f.providerCalls.length;
  for (let cycle = 0; cycle < 3; cycle++) {
    const s = f.newSession(); const provider = f.makeProvider(s);
    assert.deepEqual(await provider.collectEvidence(request), []);
    assert.equal(provider.lastExecution.abortReason, 'finite_query_exhausted');
    assert.equal(s.snapshot().continuation.statuses.finite_query_exhausted, 1);
    f.advance();
  }
  assert.equal(f.providerCalls.length, baselineCalls);
  assert.equal(reservations.length, 63);
  assert.equal(reservations.filter(x => x.operation === 'text_search').length, 3);
  assert.ok(Math.abs(reservations.reduce((sum, row) => sum + row.estimatedUsd, 0) - 1.62) < 1e-9);
  assert.ok(reservations.every(row => !JSON.stringify(row).includes('opaque-next')), 'tokens never enter cost attribution');
});

test('raw staged query labels never supply independent business-fit evidence', async () => {
  const f = fixture();
  const hits = await f.makeProvider(f.newSession(), { unknownBusiness: true }).collectEvidence(request);
  const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
  assert.equal(hits.length, 20);
  for (const hit of hits) {
    assert.equal(hit.discoveryQuery, hit.industry, 'actual provider query provenance survives staging');
    const admission = evaluateReplenishmentAdmission({ ...hit, name: hit.companyName, location: hit.address }, {
      tenantId: '13', missionSegment: 'small_business_owners', allowedCities: ['Denver'],
    });
    assert.equal(admission.admitted, false);
    assert.equal(admission.reason, 'unclassifiable_vertical');
  }
});

test('repeated same-session requests cannot fetch a second page even after durable transfer', async () => {
  const f = fixture(); const session = f.newSession(); const provider = f.makeProvider(session);
  await provider.collectEvidence(request);
  assert.equal(session.snapshot().pendingAcknowledgements, 0);
  f.advance();
  assert.deepEqual(await provider.collectEvidence(request), []);
  assert.equal(f.providerCalls.length, 21);
});

test('baseline proves fetched legacy payloads cannot discover page two even after acknowledgement', async () => {
  const f = fixture();
  for (let cycle = 0; cycle < 3; cycle++) {
    const s = createCheckpointSession({ store: f.store });
    const hits = await f.makeProvider(s).collectEvidence(request);
    await f.persist(hits); await s.acknowledge(); f.advance();
  }
  assert.equal(f.queue.size, 20, 'same acceptance fixture without opt-in pagination reaches only the original page');
  assert.equal(f.providerCalls.length, 21);
});

test('concurrent sessions reserve one page, coalesce no additional spend, and retain the winner after restart', async () => {
  const f = fixture();
  const results = await Promise.all([f.makeProvider(f.newSession()).collectEvidence(request), f.makeProvider(f.newSession()).collectEvidence(request)]);
  assert.deepEqual(results.map(rows => rows.length).sort(), [0,20]);
  assert.equal(f.providerCalls.length, 21); assert.equal(f.pages.size, 1);
  f.advance();
  const next = await f.makeProvider(f.newSession()).collectEvidence(request);
  assert.equal(next[0].placeId, 'p20'); assert.equal(f.providerCalls.length, 42);
});

test('transfer crash preserves raw first page; restart replays without spending before page two', async () => {
  const f = fixture(); f.failTransfer(true);
  await assert.rejects(f.makeProvider(f.newSession()).collectEvidence(request), /transfer crash/);
  assert.equal(f.checkpoints.values().next().value.state, 'fetched');
  assert.equal(f.pages.values().next().value.raw_results.length, 20);
  f.failTransfer(false); f.advance();
  const replay = await f.makeProvider(f.newSession()).collectEvidence(request);
  assert.equal(replay.length, 20); assert.equal(f.providerCalls.length, 21);
  f.advance();
  assert.equal((await f.makeProvider(f.newSession()).collectEvidence(request))[0].placeId, 'p20');
});

test('staging uncertainty retains reservation and blocks automatic refetch', async () => {
  const f = fixture(); f.failStage(true);
  await assert.rejects(f.makeProvider(f.newSession()).collectEvidence(request), /failed staging/);
  f.failStage(false); f.advance();
  const provider = f.makeProvider(f.newSession());
  assert.deepEqual(await provider.collectEvidence(request), []);
  assert.equal(provider.lastExecution.abortReason, 'page_fetch_in_progress');
  assert.equal(f.providerCalls.length, 21);
});

test('budget denial occurs before network and releases only a proven unused reservation', async () => {
  const f = fixture(); denyBudget = true;
  await f.makeProvider(f.newSession()).collectEvidence(request);
  assert.equal(f.providerCalls.length, 0); assert.equal(f.pages.size, 0); assert.equal(f.checkpoints.size, 0);
  denyBudget = false; cache.clearPlacesQueryCache();
  assert.equal((await f.makeProvider(f.newSession()).collectEvidence(request)).length, 20);
});

test('token activation delay, invalid token and legacy missing token all stop safely without reset', async () => {
  const f = fixture(); await f.makeProvider(f.newSession()).collectEvidence(request);
  const early = f.makeProvider(f.newSession());
  assert.deepEqual(await early.collectEvidence(request), []); assert.equal(early.lastExecution.abortReason, 'page_token_not_ready');
  f.advance();
  const invalid = f.makeProvider(f.newSession(), { invalid: true });
  await invalid.collectEvidence(request);
  assert.equal(f.providerCalls.length, 22);
  f.advance();
  const blocked = f.makeProvider(f.newSession());
  assert.deepEqual(await blocked.collectEvidence(request), []);
  assert.equal(blocked.lastExecution.abortReason, 'continuation_token_invalid'); assert.equal(f.providerCalls.length, 22);
  const old = fixture(); const key = queryIdentity('Painting Contractor United States', false);
  old.checkpoints.set(key, { state: 'handled', candidates: [] });
  const legacy = old.makeProvider(old.newSession());
  assert.deepEqual(await legacy.collectEvidence(request), []);
  assert.equal(legacy.lastExecution.abortReason, 'legacy_continuation_unknown'); assert.equal(old.providerCalls.length, 0);
});

test('page identity preserves original key and cannot exceed provider ceiling or cross a mismatched chain', () => {
  const key = queryIdentity('Painting Contractor United States', false);
  assert.equal(pageIdentity(key, 0), key); assert.notEqual(pageIdentity(key, 1), key);
  assert.throws(() => pageIdentity(key, MAX_PAGES), /Invalid/);
  assert.throws(() => selectPage(key, [{ query_key: pageIdentity(key, 1), root_query_key: key, page_index: 1 }], null), /Corrupt/);
});

test('one shared cycle allocation bounds all query workloads, including failed search retries', async () => {
  const f = fixture(); const s = f.newSession(); const provider = f.makeProvider(s);
  await provider.collectEvidence(request);
  assert.deepEqual(await provider.collectEvidence({ ...request, discoveryConcept: 'House Painting Company' }), []);
  assert.equal(f.providerCalls.length, 21);
  assert.equal(s.snapshot().continuation.statuses.cycle_page_budget_reached, 1);
  assert.equal(s.snapshot().continuation.newPagesReserved, 1);
  const bad = fixture(); cache.clearPlacesQueryCache(); reservations.length = 0;
  const failed = bad.makeProvider(bad.newSession(), { status: 'OVER_QUERY_LIMIT' });
  await failed.collectEvidence(request);
  await failed.collectEvidence({ ...request, discoveryConcept: 'House Painting Company' });
  assert.ok(bad.providerCalls.length <= 3, 'existing search retry ceiling is unchanged');
  assert.equal(reservations.length, bad.providerCalls.length, 'every actual failed attempt retains its spend reservation');
  assert.equal(bad.pages.size, 1); assert.equal(bad.pages.values().next().value.state, 'blocked');
});

test('mismatched tenant prevents page allocation or HTTP and missing page schema fails closed', async () => {
  const f = fixture(); const s = f.newSession();
  await assert.rejects(f.makeProvider(s).collectEvidence({ ...request, tenantId: '10' }), /tenant mismatch/);
  assert.equal(f.providerCalls.length, 0); assert.equal(f.pages.size, 0);
  const broken = f.newSession({ pageStore: { tenantId: '13', plan: async () => { throw Error('missing migration'); } } });
  await assert.rejects(f.makeProvider(broken).collectEvidence(request), /missing migration/);
  assert.equal(f.providerCalls.length, 0);
});

test('partial details failure cannot emit unstaged page candidates or bypass durable tickets', async () => {
  const f = fixture(); const provider = f.makeProvider(f.newSession(), { partialDetailsFailure: true });
  assert.deepEqual(await provider.collectEvidence(request), []);
  assert.equal(f.checkpoints.size, 0); assert.equal(f.tickets.size, 0);
  assert.equal(f.pages.values().next().value.state, 'blocked');
  assert.equal(f.providerCalls.length, 3);
});

test('concurrent workloads sharing one session cannot race past the one-page cycle budget', async () => {
  const f = fixture(); const session = f.newSession(); const a = f.makeProvider(session); const b = f.makeProvider(session);
  const hits = await Promise.all([a.collectEvidence(request), b.collectEvidence({...request, discoveryConcept:'House Painting Company'})]);
  assert.deepEqual(hits.map(rows => rows.length).sort(),[0,20]);
  assert.equal(f.providerCalls.length,21); assert.equal(session.snapshot().continuation.newPagesReserved,1);
});

test('oversized provider response fails before details and cannot silently drop a tail then advance', async () => {
  const f = fixture(); const provider = f.makeProvider(f.newSession(), { oversized: true });
  assert.deepEqual(await provider.collectEvidence(request), []);
  assert.equal(f.providerCalls.length, 1); assert.equal(f.checkpoints.size, 0); assert.equal(f.tickets.size, 0);
  assert.equal(f.pages.values().next().value.state, 'blocked');
  f.advance();
  assert.deepEqual(await f.makeProvider(f.newSession()).collectEvidence(request), []);
  assert.equal(f.providerCalls.length, 1);
});

test('a successful search retry stages the page and retains both real attempt reservations', async () => {
  const f = fixture(); clearFailedCacheAfterAttribution = true;
  const provider = f.makeProvider(f.newSession(), { transientFailure: true });
  const hits = await provider.collectEvidence(request);
  assert.equal(hits.length, 20); assert.equal(f.tickets.size, 20);
  assert.equal(f.pages.values().next().value.state, 'fetched');
  assert.equal(f.providerCalls.length, 22); assert.equal(reservations.length, 22);
  assert.equal(provider.lastExecution.totals.retries, 1); assert.deepEqual(provider.lastExecution.errors, []);
});

test('provider duplicate collapse retains every authoritative fetched membership without inventing one', async () => {
  const f = fixture();
  const payload = { placeId: 'same', companyName: 'Same Painting Company', website: 'same.example', address: 'Denver United States' };
  const a = queryIdentity('Painting Contractor Manchester NH',false);
  const b = queryIdentity('Painting Contractor Bedford NH',false);
  f.checkpoints.set(a,{state:'fetched',candidates:[payload]});
  f.checkpoints.set(b,{state:'fetched',candidates:[payload]});
  const provider = f.makeProvider(f.newSession());
  const hits = await provider.collectEvidence({...request,geography:{cities:['Manchester','Bedford'],state:'NH'}});
  assert.equal(hits.length,1); assert.equal(f.providerCalls.length,0);
  assert.deepEqual(new Set(hits[0]._scoutTicketRefs.map(ref=>ref.queryKey)),new Set([a,b]));
  assert.deepEqual(f.checkpoints.get(a).candidates,[payload]);
  assert.deepEqual(f.checkpoints.get(b).candidates,[payload]);
});

test('source fixtures expose finite approved-query counts; Babrun generations do not create new coverage', async () => {
  const { _test: { scoutInput } } = require('../services/maxOutboundControlLoop');
  const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
  const { buildDiscoveryPlan, executeCoveragePlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
  // Representative source fixtures, not a claim about the unobserved live mission rows.
  const source = (tenantId, segment, cities, region, eligibleSubsegments = []) => ({ objective: 'Replenish approved scope', payload: {
    structuredMission: { market: { segment, eligibleSubsegments,
      capability: tenantId === '13' ? 'business_transformation' : 'commercial_cleaning',
      businessType: tenantId === '13' ? 'founder_led_smb' : 'commercial_cleaning' },
    geography: { cities, region, scope: tenantId === '13' ? 'nationwide' : null } },
  } });
  async function sets(tenantId, mission) {
    const result = [];
    for (let generation = 0; generation <= 3; generation++) {
      const sd = buildAcquisitionSearchDefinition(scoutInput({tenant_id:tenantId},mission,{deficit:20},null,generation));
      const keys = new Set();
      const provider = createPlacesProvider({ apiKey:'fixture', fetchImpl:async()=>{throw Error('No HTTP permitted');},
        queryCheckpointSession:{ assertTenant: id => assert.equal(id,tenantId), lookup: async(query,requireWebsite)=>{
          keys.add(queryIdentity(query,requireWebsite)); return { handled:true,candidates:[] };
        } },
      });
      const adapter = createPlacesDiscoveryAdapter({placesProvider:provider});
      const plan = buildDiscoveryPlan(sd,{adapters:[adapter]});
      await executeCoveragePlan(plan,sd,[adapter]); result.push(keys);
    }
    return result;
  }
  const babrun = await sets('13',source('13','small_business_owners',[],'United States'));
  assert.deepEqual(babrun.map(keys=>keys.size),[21,21,21,21]);
  assert.equal(new Set(babrun.flatMap(keys=>[...keys])).size,21);
  const anchor = await sets('10',source('10','commercial_cleaning',
    ['Manchester','Hooksett','Bedford','Auburn','Goffstown','Londonderry'],'Greater Manchester',
    ['property_manager','str_manager','realtor','commercial_office']));
  assert.deepEqual(anchor.map(keys=>keys.size),[54,54,54,54]);
  assert.equal(new Set(anchor.flatMap(keys=>[...keys])).size,216);
});
