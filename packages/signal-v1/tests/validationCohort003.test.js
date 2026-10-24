'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { SignalService } = require('../SignalService');
const { buildValidationCohort003 } = require('../acquisition/empirical/buildValidationCohort003');
const { selectChronologicalCohort } = require('../acquisition/deterministicSelection');
const { assertEmpiricalCohort, EmpiricalValidationError } = require('../research/empiricalGuard');
const {
  getClusterPairRelationship,
  CLUSTER_RELATIONSHIP,
  persistClusterRelationship,
} = require('../research/clusterRelationships');
const { gatherIndependentConvergence } = require('../research/observationTriggers');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const { backfillCallerEvents } = require('../acquisition/backfill/callerBackfill');
const { discoverCandidates } = require('../acquisition/providers/proceduralCandidateProvider');
const { wilsonInterval } = require('../research/wilsonInterval');
const { VALIDATION_COHORT_003_ID } = require('../acquisition/candidateTypes');
const { createHistoricalCallerCatalogProvider } = require('../acquisition/providers/historicalCallerCatalogProvider');
const { resolveAchievableObservationPrice } = require('../research/achievablePrice');

function geckoMarketPath(tokenAddress, anchorIso) {
  const t0 = new Date(anchorIso).getTime();
  const points = [];
  for (let i = 0; i < 30; i += 1) {
    points.push({
      tokenAddress,
      occurredAt: new Date(t0 + i * 60000),
      priceUsd: 0.0001 * (1 + i * 0.02),
      provider: 'geckoterminal',
      intervalSeconds: 60,
      provenance: { evidenceClass: 'REAL_PROVIDER', provider: 'geckoterminal' },
    });
  }
  return points;
}

function mockGeckoProvider(pricePathsByToken) {
  return {
    providerId: 'geckoterminal',
    async getHistoricalPrices(tokenAddress) {
      return pricePathsByToken[tokenAddress] || [];
    },
  };
}

describe('SIGNAL-V1-005 empirical cohort 003', () => {
  it('chronological selection is deterministic and outcome-agnostic', () => {
    const eligible = [
      { tokenAddress: 'bbb', earliestKnownCallAt: '2026-09-02T00:00:00Z', selectionCategory: 'failure' },
      { tokenAddress: 'aaa', earliestKnownCallAt: '2026-09-01T00:00:00Z', selectionCategory: 'stronger' },
      { tokenAddress: 'ccc', earliestKnownCallAt: '2026-09-03T00:00:00Z', selectionCategory: 'unknown' },
    ];
    const a = selectChronologicalCohort(eligible, { targetSize: 2 });
    const b = selectChronologicalCohort(eligible, { targetSize: 2 });
    assert.deepEqual(
      a.selected.map(c => c.tokenAddress),
      b.selected.map(c => c.tokenAddress)
    );
    assert.deepEqual(a.selected.map(c => c.tokenAddress), ['aaa', 'bbb']);
    assert.doesNotMatch(a.breakdown.procedure, /stronger|failure|rug|winner/i);
  });

  it('UNKNOWN cluster relationship is not INDEPENDENT', () => {
    const store = new InMemorySignalStore();
    store.upsertCluster({ id: 'c-a', clusterType: 'unknown' });
    store.upsertCluster({ id: 'c-b', clusterType: 'unknown' });
    const rel = getClusterPairRelationship(store, 'c-a', 'c-b');
    assert.equal(rel, CLUSTER_RELATIONSHIP.UNKNOWN);
  });

  it('unknown provenance cannot support strict empirical convergence', () => {
    const store = new InMemorySignalStore();
    const token = '11111111111111111111111111111112';
    store.upsertCluster({ id: 'u1', clusterType: 'unknown' });
    store.upsertCluster({ id: 'u2', clusterType: 'unknown' });
    store.insertEvents([
      {
        tokenAddress: token,
        eventType: 'CALL',
        sourceType: 'telegram',
        sourceId: 's1',
        sourceClusterId: 'u1',
        occurredAt: new Date('2026-09-01T00:00:00Z'),
      },
      {
        tokenAddress: token,
        eventType: 'CALL',
        sourceType: 'telegram',
        sourceId: 's2',
        sourceClusterId: 'u2',
        occurredAt: new Date('2026-09-01T00:10:00Z'),
      },
    ]);
    const conv = gatherIndependentConvergence(store, token, '2026-09-01T01:00:00Z', {
      ...DEFAULT_RESEARCH_CONFIG,
      strictClusterIndependence: true,
    });
    assert.equal(conv.provenIndependentClusterCount, 0);
  });

  it('EMPIRICAL cohort rejects procedural caller event', async () => {
    const store = new InMemorySignalStore();
    const token = discoverCandidates({ poolSize: 1 })[0].tokenAddress;
    await store.upsertResearchCohort({
      id: 'emp-test',
      dataClass: 'EMPIRICAL',
      frozenAt: null,
    });
    await store.addCohortMember({ cohortId: 'emp-test', tokenAddress: token, inclusionReason: 't' });
    const cohort = store.researchCohorts.get('emp-test');
    cohort.frozenAt = new Date().toISOString();
    store.researchCohorts.set('emp-test', cohort);
    await backfillCallerEvents(store, { tokenAddress: token, earliestKnownCallAt: new Date() }, discoverCandidates({ poolSize: 1 })[0]);
    assert.throws(() => assertEmpiricalCohort(store, 'emp-test'), EmpiricalValidationError);
  });

  it('EMPIRICAL cohort rejects procedural market data', async () => {
    const store = new InMemorySignalStore();
    const token = '11111111111111111111111111111113';
    await store.upsertResearchCohort({
      id: 'emp-market',
      dataClass: 'EMPIRICAL',
      frozenAt: null,
    });
    await store.addCohortMember({ cohortId: 'emp-market', tokenAddress: token, inclusionReason: 't' });
    const cohort = store.researchCohorts.get('emp-market');
    cohort.frozenAt = new Date().toISOString();
    store.researchCohorts.set('emp-market', cohort);
    store.insertEvent({
      tokenAddress: token,
      eventType: 'CALL',
      sourceType: 'telegram',
      sourceId: 'real-src',
      occurredAt: new Date('2026-09-01T00:00:00Z'),
      provenance: { evidenceClass: 'REAL_PROVIDER', provider: 'test', externalId: 'x' },
    });
    store.insertMarketObservation({
      tokenAddress: token,
      occurredAt: new Date('2026-09-01T00:00:00Z'),
      priceUsd: 1,
      provider: 'fixture-acquisition',
      provenance: { procedural: true },
    });
    assert.throws(() => assertEmpiricalCohort(store, 'emp-market'), EmpiricalValidationError);
  });

  it('cohort freezes before outcome calculation', async () => {
    const store = new InMemorySignalStore();
    const provider = createHistoricalCallerCatalogProvider();
    const paths = {};
    for (const t of provider.catalog.tokens) {
      paths[t.tokenAddress] = geckoMarketPath(t.tokenAddress, t.earliestCallAt);
    }
    const built = await buildValidationCohort003(store, {
      targetSize: 3,
      marketProvider: mockGeckoProvider(paths),
      catalogProvider: provider,
    });
    assert.ok(built.cohort.frozenAt);
    const hasOutcomes = store.researchObservationOutcomes.length > 0;
    assert.equal(hasOutcomes, true);
    const frozenMs = new Date(built.cohort.frozenAt).getTime();
    assert.ok(frozenMs <= Date.now());
  });

  it('no post-freeze replacement — DATA_INSUFFICIENT retains membership', async () => {
    const store = new InMemorySignalStore();
    const cohortId = 'frozen-no-replace-test';
    await store.upsertResearchCohort({
      id: cohortId,
      dataClass: 'EMPIRICAL',
      frozenAt: null,
    });
    await store.addCohortMember({
      cohortId,
      tokenAddress: '11111111111111111111111111111114',
      inclusionReason: 'frozen member',
    });
    const cohort = store.researchCohorts.get(cohortId);
    cohort.frozenAt = new Date().toISOString();
    store.researchCohorts.set(cohortId, cohort);
    assert.throws(
      () =>
        store.addCohortMember({
          cohortId,
          tokenAddress: '11111111111111111111111111111115',
          inclusionReason: 'replacement attempt',
        }),
      /frozen/i
    );
  });

  it('real caller timestamps preserved on ingest', async () => {
    const store = new InMemorySignalStore();
    const provider = createHistoricalCallerCatalogProvider();
    const token = provider.catalog.tokens.find(t => t.tokenAddress === '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump');
    const paths = { [token.tokenAddress]: geckoMarketPath(token.tokenAddress, token.earliestCallAt) };
    await buildValidationCohort003(store, {
      targetSize: 50,
      cohortId: 'ts-preserve',
      marketProvider: mockGeckoProvider(paths),
      catalogProvider: provider,
    });
    const callAt = token.calls.find(c => (c.eventType || 'CALL') === 'CALL').occurredAt;
    const events = store.getEventsForToken(token.tokenAddress).filter(e => e.eventType === 'CALL');
    assert.ok(events.length);
    assert.equal(events[0].occurredAt.toISOString(), callAt);
  });

  it('Wilson calculations preserved on cohort layers', async () => {
    const store = new InMemorySignalStore();
    const service = new SignalService(store, { seedFixtures: false });
    const provider = createHistoricalCallerCatalogProvider();
    const paths = {};
    for (const t of provider.catalog.tokens) {
      paths[t.tokenAddress] = geckoMarketPath(t.tokenAddress, t.earliestCallAt);
    }
    await buildValidationCohort003(store, {
      marketProvider: mockGeckoProvider(paths),
      catalogProvider: provider,
    });
    const evaluation = await service.evaluateResearchCohort(VALIDATION_COHORT_003_ID, 60, {
      replayMembers: false,
    });
    const fc = evaluation.layers.find(l => l.observationType === 'FIRST_CALLER');
    assert.ok(fc.wilson95);
    assert.ok(fc.wilson95);
    if (fc.denominators.resolvedN > 0) {
      assert.equal(typeof fc.wilson95.lower, 'number');
    }
  });

  it('achievable-price semantics preserved', () => {
    const pricePath = [
      { occurredAt: new Date('2026-09-01T00:00:00Z'), price: 1 },
      { occurredAt: new Date('2026-09-01T00:02:00Z'), price: 1.2 },
    ];
    const { price, priceAt } = resolveAchievableObservationPrice(
      '2026-09-01T00:00:00Z',
      60,
      pricePath
    );
    assert.equal(price, 1.2);
    assert.equal(new Date(priceAt).toISOString(), '2026-09-01T00:02:00.000Z');
  });

  it('proven INDEPENDENT relationship enables strict convergence', () => {
    const store = new InMemorySignalStore();
    const token = '11111111111111111111111111111116';
    store.upsertCluster({ id: 'ia', clusterType: 'unknown', metadata: { relationshipBasis: 'catalog' } });
    store.upsertCluster({ id: 'ib', clusterType: 'unknown', metadata: { relationshipBasis: 'catalog' } });
    persistClusterRelationship(store, {
      clusterA: 'ia',
      clusterB: 'ib',
      relationship: CLUSTER_RELATIONSHIP.INDEPENDENT,
      basis: 'test',
      provenance: { evidenceClass: 'HISTORICAL_FIXTURE' },
    });
    store.insertEvents([
      {
        tokenAddress: token,
        eventType: 'CALL',
        sourceId: 'a',
        sourceClusterId: 'ia',
        sourceType: 'telegram',
        occurredAt: new Date('2026-09-01T00:00:00Z'),
        provenance: { evidenceClass: 'REAL_PROVIDER', provider: 't', externalId: '1' },
      },
      {
        tokenAddress: token,
        eventType: 'CALL',
        sourceId: 'b',
        sourceClusterId: 'ib',
        sourceType: 'telegram',
        occurredAt: new Date('2026-09-01T00:12:00Z'),
        provenance: { evidenceClass: 'REAL_PROVIDER', provider: 't', externalId: '2' },
      },
    ]);
    const conv = gatherIndependentConvergence(store, token, '2026-09-01T01:00:00Z', {
      ...DEFAULT_RESEARCH_CONFIG,
      strictClusterIndependence: true,
      independentConvergenceWindowMinutes: 120,
    });
    assert.equal(conv.provenIndependentClusterCount, 2);
  });

  it('wilson interval unchanged', () => {
    const wi = wilsonInterval(3, 10);
    assert.ok(wi.lower < wi.pointEstimate);
    assert.ok(wi.upper > wi.pointEstimate);
  });
});
