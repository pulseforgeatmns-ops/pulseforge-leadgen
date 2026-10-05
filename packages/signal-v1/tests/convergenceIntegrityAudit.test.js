'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { SignalService } = require('../SignalService');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { backfillCallerEvents } = require('../acquisition/backfill/callerBackfill');
const { buildFixturePricePath } = require('../acquisition/backfill/marketBackfill');
const { discoverCandidates } = require('../acquisition/providers/proceduralCandidateProvider');
const {
  runConvergenceIntegrityAudit,
  buildConvergenceAuditRows,
  CALLER_EVENT_PROVENANCE,
} = require('../research/convergenceIntegrityAudit');
const { scanBackfillModulesForLeakage } = require('../research/leakageAudit');
const { wilsonInterval } = require('../research/wilsonInterval');
const { simulateShuffledOutcomeLabels } = require('../research/negativeControls');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const {
  VALIDATION_COHORT_001_ID,
  VALIDATION_COHORT_002_ID,
} = require('../acquisition/candidateTypes');
const { discoverCandidates: discoverHoldout } = require('../acquisition/providers/proceduralCandidateProviderV2');

function serializeEvents(store, tokenAddress) {
  return store
    .getEventsForToken(tokenAddress)
    .filter(e => e.eventType === 'CALL')
    .map(e => ({
      sourceId: e.sourceId,
      clusterId: e.sourceClusterId,
      occurredAt: e.occurredAt.toISOString(),
      payload: e.payload,
      provenance: e.provenance,
    }))
    .sort((a, b) => a.occurredAt.localeCompare(b.occurredAt));
}

describe('SIGNAL-V1-004 convergence integrity audit', () => {
  it('Wilson interval attaches uncertainty to perfect observed precision', () => {
    const wi = wilsonInterval(18, 18);
    assert.equal(wi.n, 18);
    assert.equal(wi.pointEstimate, 1);
    assert.ok(wi.lower < 1);
    assert.ok(wi.upper <= 1);
    assert.ok(wi.upper - wi.lower > 0);
  });

  it('backfill modules do not read selectionCategory or outcome labels', () => {
    const scan = scanBackfillModulesForLeakage();
    assert.equal(scan.clean, true, JSON.stringify(scan.violations));
  });

  it('selection label cannot change caller backfill when acquisitionPayload is fixed', async () => {
    const storeA = new InMemorySignalStore();
    const storeB = new InMemorySignalStore();
    const raw = discoverCandidates({ poolSize: 1 })[0];
    const candidate = {
      tokenAddress: raw.tokenAddress,
      earliestKnownCallAt: raw.earliestKnownCallAt,
    };
    const payload = { ...raw, selectionCategory: 'stronger' };
    const payloadB = { ...raw, selectionCategory: 'failure' };
    await backfillCallerEvents(storeA, candidate, payload);
    await backfillCallerEvents(storeB, candidate, payloadB);
    assert.deepEqual(
      serializeEvents(storeA, raw.tokenAddress),
      serializeEvents(storeB, raw.tokenAddress)
    );
  });

  it('selection label cannot change fixture market path when pattern is fixed', () => {
    const raw = discoverCandidates({ poolSize: 1 })[0];
    const candidate = { tokenAddress: raw.tokenAddress };
    const anchor = '2026-08-01T12:00:00.000Z';
    const a = buildFixturePricePath(candidate, { ...raw, selectionCategory: 'stronger' }, anchor);
    const b = buildFixturePricePath(candidate, { ...raw, selectionCategory: 'failure' }, anchor);
    assert.deepEqual(a, b);
  });

  it('holdout catalog excludes validation 001 token addresses', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 4, targetSize: 8, freeze: true });
    const built = await service.buildValidationCohort002({ perCategory: 4, targetSize: 8, freeze: true });
    const members001 = new Set(
      store.getCohortMembers(VALIDATION_COHORT_001_ID).map(m => m.tokenAddress)
    );
    for (const m of store.getCohortMembers(VALIDATION_COHORT_002_ID)) {
      assert.ok(!members001.has(m.tokenAddress));
    }
    assert.ok(built.excludedTokens.length >= 3);
  });

  it('holdout provider pool does not overlap cohort 001 procedural addresses', () => {
    const v1 = discoverCandidates({ poolSize: 60 }).map(c => c.tokenAddress);
    const v2 = discoverHoldout({ poolSize: 80 }).map(c => c.tokenAddress);
    const overlap = v1.filter(a => v2.includes(a));
    assert.deepEqual(overlap, []);
  });

  it('cohort 001 audit table includes non-converging tokens', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 4, targetSize: 8, freeze: true });
    const rows = buildConvergenceAuditRows(store, VALIDATION_COHORT_001_ID);
    assert.equal(rows.length, 8);
    assert.ok(rows.some(r => !r.independentConvergenceTriggered));
    assert.ok(rows.some(r => r.independentConvergenceTriggered));
  });

  it('shuffled outcomes destroy perfect convergence association under fixed seed', () => {
    const converged = Array.from({ length: 18 }, () => ({ label: 'PASS' }));
    const pool = [...Array(18).fill('PASS'), ...Array(20).fill('FAIL')];
    const sim = simulateShuffledOutcomeLabels(converged, pool, 'test-seed');
    assert.equal(sim.observed.precision, 1);
    assert.equal(sim.summary.destroysPerfectRelationship, true);
  });

  it('convergence definition remains frozen at >=2 clusters / 30 minutes', () => {
    assert.equal(DEFAULT_RESEARCH_CONFIG.minIndependentClusters, 2);
    assert.equal(DEFAULT_RESEARCH_CONFIG.independentConvergenceWindowMinutes, 30);
  });

  it('full audit run classifies cohort 001 INVALID when procedural coupling documented', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 4, targetSize: 8, freeze: true });
    await service.buildValidationCohort002({ perCategory: 4, targetSize: 8, freeze: true });
    const audit = runConvergenceIntegrityAudit(store);
    assert.equal(audit.auditResult.classification, 'INVALID');
    assert.ok(audit.cohort001.layerMetrics.INDEPENDENT_CONVERGENCE.wilson95.lower < 1);
    const proc = audit.cohort001.secondCallerProvenanceCounts[CALLER_EVENT_PROVENANCE.PROCEDURAL_GENERATED];
    assert.ok(proc >= 0);
  });

  it('frozen convergence rule export documents procedural second-caller coupling in v1 catalog source', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '../acquisition/providers/proceduralCandidateProvider.js'),
      'utf8'
    );
    assert.match(src, /selectionCategory === 'stronger'/);
  });
});
