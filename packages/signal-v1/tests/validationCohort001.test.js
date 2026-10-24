'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { SignalService } = require('../SignalService');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { selectBalancedCohort } = require('../acquisition/deterministicSelection');
const { dedupeCandidates, evaluateCandidateEligibility } = require('../acquisition/eligibility');
const { discoverCandidates } = require('../acquisition/providers/proceduralCandidateProvider');
const { VALIDATION_COHORT_001_ID } = require('../acquisition/candidateTypes');
const { buildFeatureSnapshot } = require('../features/featureEngine');
const { getSourceQualityAsOf } = require('../research/sourceQuality');
const { evaluateExecutionDelaySensitivity } = require('../research/cohortEvaluation');

describe('SIGNAL-V1-003 Phase B validation cohort 001', () => {
  it('deterministic candidate selection is stable', () => {
    const eligible = discoverCandidates({ poolSize: 12 }).map((raw, idx) => ({
      id: String(idx),
      tokenAddress: raw.tokenAddress,
      earliestKnownCallAt: raw.earliestKnownCallAt,
      selectionCategory: raw.selectionCategory,
    }));
    const a = selectBalancedCohort(eligible, { perCategory: 3, targetSize: 6 });
    const b = selectBalancedCohort(eligible, { perCategory: 3, targetSize: 6 });
    assert.deepEqual(
      a.selected.map(c => c.tokenAddress),
      b.selected.map(c => c.tokenAddress)
    );
  });

  it('rejects duplicate candidate token/event pairs', () => {
    const raw = discoverCandidates({ poolSize: 1 })[0];
    const { unique, rejectedDuplicates } = dedupeCandidates([raw, raw]);
    assert.equal(unique.length, 1);
    assert.equal(rejectedDuplicates.length, 1);
    assert.equal(rejectedDuplicates[0].exclusionReason, 'duplicate_token_event');
  });

  it('preserves provenance on persisted candidates', async () => {
    const store = new InMemorySignalStore();
    const service = new SignalService(store, { seedFixtures: false });
    const built = await service.buildValidationCohort001({
      perCategory: 4,
      targetSize: 8,
      freeze: true,
    });
    const row = [...store.researchCandidates.values()].find(c =>
      ['SELECTED', 'REPLAYED', 'BACKFILLED'].includes(c.status)
    );
    assert.ok(row?.discoveredFrom);
    assert.ok(row?.provenance && typeof row.provenance === 'object');
    const cohort = store.researchCohorts.get(VALIDATION_COHORT_001_ID);
    assert.ok(cohort?.frozenAt);
  });

  it('frozen cohort membership cannot mutate', async () => {
    const store = new InMemorySignalStore();
    await store.upsertResearchCohort({
      id: 'frozen-test',
      name: 'frozen',
      definitionVersion: 'signal-research-v1',
      frozenAt: new Date().toISOString(),
    });
    assert.throws(
      () =>
        store.addCohortMember({
          cohortId: 'frozen-test',
          tokenAddress: '11111111111111111111111111111111',
          inclusionReason: 'x',
        }),
      /frozen/i
    );
  });

  it('outcome selection category does not enter feature snapshots', async () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 2, targetSize: 4, freeze: true });
    const member = store.getCohortMembers(VALIDATION_COHORT_001_ID)[0];
    const snap = await buildFeatureSnapshot(store, member.tokenAddress, new Date());
    const blob = JSON.stringify(snap.features);
    assert.doesNotMatch(blob, /selectionCategory/);
    assert.doesNotMatch(blob, /"failure"/);
  });

  it('unknown cluster is not treated as independent', async () => {
    const store = new InMemorySignalStore();
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 3, targetSize: 6, freeze: true });
    const hallowObs = store.researchObservations.filter(
      o => o.tokenAddress === '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump'
    );
    if (hallowObs.length) {
      const independent = hallowObs.find(o => o.observationType === 'INDEPENDENT_CONVERGENCE');
      assert.equal(independent, undefined);
    }
  });

  it('future source performance cannot leak backward', () => {
    const store = new InMemorySignalStore();
    seedFrontRunnersFixtures(store);
    const early = getSourceQualityAsOf(store, 'src-independent-alpha', '2026-08-01T00:00:00Z');
    assert.equal(early.quality, 'unavailable');
  });

  it('denominator calculations separate cohort vs resolved N', async () => {
    const store = new InMemorySignalStore();
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 4, targetSize: 8, freeze: true });
    const evaluation = await service.evaluateResearchCohort(VALIDATION_COHORT_001_ID, 60, {
      replayMembers: false,
    });
    const firstCaller = evaluation.layers.find(l => l.observationType === 'FIRST_CALLER');
    assert.ok(firstCaller.denominators.cohortN >= firstCaller.denominators.observationTriggeredN);
    assert.ok(firstCaller.denominators.resolvedN <= firstCaller.denominators.marketEvaluableN);
    if (firstCaller.denominators.resolvedN > 0) {
      assert.equal(
        firstCaller.precision,
        firstCaller.PASS / firstCaller.denominators.resolvedN
      );
    }
  });

  it('evaluates all execution delay buckets', async () => {
    const store = new InMemorySignalStore();
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 2, targetSize: 4, freeze: true });
    const sensitivity = evaluateExecutionDelaySensitivity(store, VALIDATION_COHORT_001_ID);
    assert.deepEqual(
      sensitivity.map(s => s.executionDelaySeconds),
      [15, 30, 60, 180, 300]
    );
  });

  it('replay persists and reloads deterministically from store cache', async () => {
    const store = new InMemorySignalStore();
    const service = new SignalService(store, { seedFixtures: false });
    await service.buildValidationCohort001({ perCategory: 2, targetSize: 4, freeze: true });
    const a = await service.exportResearchCohortEvaluation(VALIDATION_COHORT_001_ID, {
      replayMembers: false,
    });
    const b = await service.exportResearchCohortEvaluation(VALIDATION_COHORT_001_ID, {
      replayMembers: false,
    });
    assert.deepEqual(a.evaluation.layers, b.evaluation.layers);
  });

  it('ineligible candidates record exclusion reason', () => {
    const bad = discoverCandidates({ poolSize: 1 })[0];
    const eligibility = evaluateCandidateEligibility({ ...bad, tokenAddress: 'not-a-valid-address!!!' });
    assert.equal(eligibility.eligible, false);
    assert.match(eligibility.exclusionReason, /unverifiable_ca/);
  });
});
