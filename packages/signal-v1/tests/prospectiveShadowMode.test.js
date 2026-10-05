'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { ShadowModeService } = require('../prospective/ShadowModeService');
const { createProceduralTestCollector } = require('../collectors/proceduralTestCollector');
const { FixtureMarketDataProvider } = require('../providers/FixtureMarketDataProvider');
const { knowledgeAt } = require('../prospective/knowledgeClock');
const { PROSPECTIVE_COHORT_001_ID, CLUSTER_RELATIONSHIP, DATA_CLASS } = require('../prospective/constants');
const { createShadowModeServiceFromStore } = require('../prospective/shadowScheduler');

const TOKEN_A = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
const TOKEN_B = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';
const TOKEN_C = '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump';

function pricePathPass(baseMs) {
  const t0 = baseMs;
  return [
    { occurredAt: new Date(t0), price: 0.001 },
    { occurredAt: new Date(t0 + 20_000), price: 0.0011 },
    { occurredAt: new Date(t0 + 70_000), price: 0.0025 },
    { occurredAt: new Date(t0 + 25 * 60 * 60 * 1000), price: 0.003 },
  ];
}

async function setupSources(shadow, pairs) {
  for (const p of pairs) {
    await shadow.upsertSourceRegistryEntry(p);
  }
}

describe('SIGNAL-V1-006 prospective shadow mode', () => {
  /** @type {InMemorySignalStore} */
  let store;
  /** @type {ShadowModeService} */
  let shadow;
  let nowMs;

  beforeEach(async () => {
    store = new InMemorySignalStore();
    nowMs = Date.now();
    const marketProvider = new FixtureMarketDataProvider({
      pricePaths: {
        [TOKEN_A]: pricePathPass(nowMs),
        [TOKEN_B]: pricePathPass(nowMs),
        [TOKEN_C]: pricePathPass(nowMs),
      },
    });
    shadow = new ShadowModeService(store, {
      marketProvider,
      collectors: [],
      now: () => new Date(nowMs),
    });
    await shadow.ensureProspectiveCohortStarted({ market: 'fixture' });
    await setupSources(shadow, [
      {
        sourceId: 'src-a',
        displayName: 'Caller A',
        platform: 'telegram',
        collectorId: 'test',
        sourceRole: 'CALLER',
        clusterId: 'cluster-a',
        clusterRelationshipStatus: CLUSTER_RELATIONSHIP.INDEPENDENT,
      },
      {
        sourceId: 'src-b',
        displayName: 'Caller B',
        platform: 'telegram',
        collectorId: 'test',
        sourceRole: 'CALLER',
        clusterId: 'cluster-b',
        clusterRelationshipStatus: CLUSTER_RELATIONSHIP.INDEPENDENT,
      },
      {
        sourceId: 'src-unknown',
        displayName: 'Caller U',
        platform: 'telegram',
        collectorId: 'test',
        sourceRole: 'CALLER',
        clusterId: 'cluster-u',
        clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
      },
      {
        sourceId: 'src-corr',
        displayName: 'Forwarder',
        platform: 'telegram',
        collectorId: 'test',
        sourceRole: 'CALLER',
        clusterId: 'cluster-corr',
        clusterRelationshipStatus: CLUSTER_RELATIONSHIP.INDEPENDENT,
      },
    ]);
    await store.upsertCluster({ id: 'cluster-a', name: 'A', clusterType: 'unknown', confidence: 1 });
    await store.upsertCluster({ id: 'cluster-b', name: 'B', clusterType: 'unknown', confidence: 1 });
    await store.addClusterMember('src-a', 'cluster-a');
    await store.addClusterMember('src-b', 'cluster-b');
  });

  it('rejects procedural events from prospective empirical cohort membership', async () => {
    const procedural = createProceduralTestCollector({
      observations: [
        {
          sourceId: 'src-a',
          externalMessageId: 'proc-1',
          messageTimestamp: new Date(nowMs).toISOString(),
          tokenCa: TOKEN_A,
        },
      ],
    });
    shadow.collectors = [procedural];
    await shadow.pollCollectorsOnce();
    const members = store.getCohortMembers(PROSPECTIVE_COHORT_001_ID);
    assert.equal(members.length, 0);
  });

  it('rejects pre-start events from prospective cohort', async () => {
    const cohort = store.researchCohorts.get(PROSPECTIVE_COHORT_001_ID);
    cohort.metadata.startedAt = new Date(nowMs + 60_000).toISOString();
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'pre-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    assert.equal(store.getCohortMembers(PROSPECTIVE_COHORT_001_ID).length, 0);
  });

  it('enforces knowledgeAt = max(occurredAt, ingestedAt)', () => {
    const occurred = new Date('2026-10-01T14:00:00Z');
    const ingested = new Date('2026-10-01T14:04:00Z');
    assert.equal(knowledgeAt(occurred, ingested).toISOString(), ingested.toISOString());
  });

  it('does not retroactively trigger FIRST_CALLER before ingestion knowledge time', async () => {
    const occurred = new Date(nowMs + 10_000);
    nowMs += 240_000;
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'delay-knowledge-1',
        messageTimestamp: occurred.toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const obs = store.researchObservations.find(o => o.observationType === 'FIRST_CALLER');
    assert.ok(obs);
    assert.ok(obs.occurredAt.getTime() >= nowMs - 5000);
    assert.notEqual(obs.occurredAt.toISOString(), occurred.toISOString());
  });

  it('strict independent convergence requires PROVEN independent clusters', async () => {
    const t = new Date(nowMs).toISOString();
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'conv-a1',
        messageTimestamp: t,
        tokenCa: TOKEN_B,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-b',
        externalMessageId: 'conv-b1',
        messageTimestamp: t,
        tokenCa: TOKEN_B,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const conv = store.researchObservations.find(o => o.observationType === 'INDEPENDENT_CONVERGENCE');
    assert.ok(conv);
    assert.equal(conv.metadata.strictProvenIndependent, true);
  });

  it('UNKNOWN cluster relationship does not qualify for strict convergence', async () => {
    const t = new Date(nowMs).toISOString();
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'unk-a',
        messageTimestamp: t,
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-unknown',
        externalMessageId: 'unk-u',
        messageTimestamp: t,
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const conv = store.researchObservations.find(o => o.observationType === 'INDEPENDENT_CONVERGENCE');
    assert.equal(conv, undefined);
  });

  it('forwarded/correlated source does not qualify as independent convergence', async () => {
    const t = new Date(nowMs).toISOString();
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'fwd-a',
        messageTimestamp: t,
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-corr',
        externalMessageId: 'fwd-c',
        messageTimestamp: t,
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
        forwarding: { sourceId: 'src-a', clusterId: 'cluster-a' },
      },
      { collectorId: 'operator-json-feed' }
    );
    const conv = store.researchObservations.find(o => o.observationType === 'INDEPENDENT_CONVERGENCE');
    assert.equal(conv, undefined);
  });

  it('duplicate polling is idempotent for CALL events', async () => {
    const raw = {
      sourceId: 'src-a',
      externalMessageId: 'dup-1',
      messageTimestamp: new Date(nowMs).toISOString(),
      tokenCa: TOKEN_A,
      provenance: { dataClass: DATA_CLASS.EMPIRICAL },
    };
    await shadow.ingestRawCallerObservation(raw, { collectorId: 'operator-json-feed' });
    await shadow.ingestRawCallerObservation(raw, { collectorId: 'operator-json-feed' });
    const calls = store.events.filter(e => e.eventType === 'CALL' && e.tokenAddress === TOKEN_A);
    assert.equal(calls.length, 1);
  });

  it('delay captures use first observation at or after target delay', async () => {
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'delay-cap-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_C,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const obs = store.researchObservations.find(
      o => o.observationType === 'FIRST_CALLER' && o.tokenAddress === TOKEN_C
    );
    for (const job of store.prospectiveJobs) {
      job.runAfter = new Date(nowMs - 1000);
    }
    await shadow.runDueJobs();
    const row = store.researchObservationOutcomes.find(o => o.executionDelaySeconds === 60);
    assert.ok(row);
    assert.ok(row.entryPrice != null);
    assert.ok(row.metadata.actualObservationTime);
  });

  it('market failure does not fabricate prices but preserves observation', async () => {
    const failing = new ShadowModeService(store, {
      marketProvider: {
        providerId: 'fail',
        async getTokenSnapshot() {
          throw new Error('provider_down');
        },
      },
      collectors: [],
      now: () => new Date(nowMs),
    });
    await failing.ensureProspectiveCohortStarted();
    await failing.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'mkt-fail-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const obs = store.researchObservations.find(o => o.observationType === 'FIRST_CALLER');
    assert.ok(obs);
    assert.equal(obs.metadata.marketCaptureFailed, true);
    assert.equal(obs.metadata.marketSnapshot, undefined);
  });

  it('restart restores pending outcome jobs and completes 24h evaluation', async () => {
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'outcome-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const jobs = store.loadProspectiveJobs();
    const futureNow = nowMs + 25 * 60 * 60 * 1000;
    const restarted = await createShadowModeServiceFromStore(store, {
      marketProvider: new FixtureMarketDataProvider({
        pricePaths: { [TOKEN_A]: pricePathPass(nowMs) },
      }),
      collectors: [],
      now: () => new Date(futureNow),
    });
    restarted.restoreJobsFromStore(jobs);
    for (const job of store.prospectiveJobs) {
      job.runAfter = new Date(0);
    }
    await restarted.runDueJobs();
    const outcome = store.researchObservationOutcomes.find(o => o.label != null);
    assert.ok(outcome);
    assert.ok(['PASS', 'FAIL', 'UNRESOLVED'].includes(outcome.label));
  });

  it('cohort membership does not depend on outcome success', async () => {
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'member-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const members = store.getCohortMembers(PROSPECTIVE_COHORT_001_ID);
    assert.equal(members.length, 1);
    assert.equal(members[0].inclusionReason, 'FIRST_CALLER');
  });

  it('aggregate primary performance is blinded before unblinding', async () => {
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'blind-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    const evaluation = await shadow.evaluateProspectiveCohort(60, { skipEmpiricalGuard: true });
    assert.equal(evaluation.blinded, true);
    const layer = evaluation.layers.find(l => l.observationType === 'FIRST_CALLER');
    assert.equal(layer.precision, null);
  });

  it('explicit unblinding is required to expose cohort metadata flag', async () => {
    const updated = await shadow.explicitUnblind({
      unblindedBy: 'test@example.com',
      evaluationVersion: 'test-v1',
    });
    assert.ok(updated.metadata.unblindedAt);
    assert.equal(updated.metadata.blinded, false);
  });

  it('empirical contamination blocks evaluation', async () => {
    await shadow.ingestRawCallerObservation(
      {
        sourceId: 'src-a',
        externalMessageId: 'contam-1',
        messageTimestamp: new Date(nowMs).toISOString(),
        tokenCa: TOKEN_A,
        provenance: { dataClass: DATA_CLASS.EMPIRICAL },
      },
      { collectorId: 'operator-json-feed' }
    );
    store.insertEvent({
      tokenAddress: TOKEN_A,
      eventType: 'CALL',
      occurredAt: new Date(nowMs),
      ingestedAt: new Date(nowMs),
      sourceType: 'telegram',
      sourceId: 'src-a',
      provenance: { dataClass: DATA_CLASS.PROCEDURAL },
      payload: { procedural: true },
    });
    await assert.rejects(() => shadow.evaluateProspectiveCohort(60), /EMPIRICAL_CONTAMINATION/);
  });

  it('operator json feed collector reports unavailable without env URL', async () => {
    const { createOperatorJsonFeedCollector } = require('../collectors/operatorJsonFeedCollector');
    const collector = createOperatorJsonFeedCollector({ feedUrl: null });
    const health = await collector.health();
    assert.equal(health.available, false);
  });
});
