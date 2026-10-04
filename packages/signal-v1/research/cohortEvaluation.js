'use strict';

const { RESEARCH_OBSERVATION_TYPES } = require('../types');
const { median } = require('./sourcePerformanceEngine');
const { gatherIndependentConvergence } = require('./observationTriggers');

const LAYER_LABELS = Object.freeze({
  FIRST_CALLER: 'First caller',
  INDEPENDENT_CONVERGENCE: 'Independent convergence',
  QUALITY_CONVERGENCE: 'Quality convergence',
  WALLET_CONFIRMATION: '+ wallet confirmation',
  STRUCTURE_GATE: '+ structure gate',
  AMPLIFIER_ARRIVAL: 'Amplifier arrival',
  SIGNAL_ENTRY: 'Signal ENTRY',
});

/**
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {string} cohortId
 * @param {number} executionDelaySeconds
 */
function evaluateCohortLayers(store, cohortId, executionDelaySeconds = 60) {
  const members = store.getCohortMembers(cohortId);
  const rows = [];

  for (const type of RESEARCH_OBSERVATION_TYPES) {
    const stats = aggregateLayer(store, members, type, executionDelaySeconds);
    rows.push({
      observationLayer: LAYER_LABELS[type] || type,
      observationType: type,
      ...stats,
    });
  }

  return {
    cohortId,
    executionDelaySeconds,
    layers: rows,
    convergenceVelocity: evaluateConvergenceVelocityBuckets(store, members, executionDelaySeconds),
    independentClusterAnalysis: evaluateIndependentClusterBuckets(store, members, executionDelaySeconds),
  };
}

function aggregateLayer(store, members, observationType, delaySeconds) {
  const outcomes = [];
  const availabilityCounts = { AVAILABLE: 0, PARTIAL: 0, UNAVAILABLE: 0, INSUFFICIENT_MARKET_DATA: 0 };

  for (const member of members) {
    const obs = store.researchObservations.find(
      o => o.tokenAddress === member.tokenAddress && o.observationType === observationType
    );
    if (!obs) continue;

    const outcome = store.researchObservationOutcomes.find(
      o => o.observationId === obs.id && o.executionDelaySeconds === delaySeconds
    );
    if (!outcome) continue;

    availabilityCounts[outcome.dataAvailability] =
      (availabilityCounts[outcome.dataAvailability] || 0) + 1;
    if (outcome.label) outcomes.push(outcome);
  }

  const n = outcomes.length;
  const pass = outcomes.filter(o => o.label === 'PASS').length;
  const fail = outcomes.filter(o => o.label === 'FAIL').length;
  const unresolved = outcomes.filter(o => o.label === 'UNRESOLVED').length;
  const resolved = pass + fail;
  const precision = resolved ? pass / resolved : null;
  const falsePositiveRate = resolved ? fail / resolved : null;

  return {
    N: n,
    PASS: pass,
    FAIL: fail,
    UNRESOLVED: unresolved,
    precision,
    falsePositiveRate,
    medianMfe: median(outcomes.map(o => o.mfe)),
    medianMae: median(outcomes.map(o => o.mae)),
    medianTimeTo2x: median(outcomes.map(o => o.timeTo2xSeconds)),
    medianTimeToMinus30: median(outcomes.map(o => o.timeToMinus30Seconds)),
    medianReturn15m: median(outcomes.map(o => o.return15m)),
    medianReturn1h: median(outcomes.map(o => o.return1h)),
    medianReturn6h: median(outcomes.map(o => o.return6h)),
    medianReturn24h: median(outcomes.map(o => o.return24h)),
    dataAvailability: availabilityCounts,
  };
}

function evaluateConvergenceVelocityBuckets(store, members, delaySeconds) {
  const buckets = [
    { label: '<= 5m', maxMinutes: 5 },
    { label: '<= 15m', maxMinutes: 15 },
    { label: '<= 30m', maxMinutes: 30 },
    { label: '<= 60m', maxMinutes: 60 },
  ];

  return buckets.map(bucket => {
    const outcomes = [];
    for (const member of members) {
      const obs = store.researchObservations.find(
        o =>
          o.tokenAddress === member.tokenAddress &&
          o.observationType === 'INDEPENDENT_CONVERGENCE'
      );
      if (!obs) continue;
      const duration = obs.metadata?.convergenceDurationMinutes;
      if (duration == null || duration > bucket.maxMinutes) continue;
      const outcome = store.researchObservationOutcomes.find(
        o => o.observationId === obs.id && o.executionDelaySeconds === delaySeconds
      );
      if (outcome?.label) outcomes.push(outcome);
    }
    const pass = outcomes.filter(o => o.label === 'PASS').length;
    const fail = outcomes.filter(o => o.label === 'FAIL').length;
    const resolved = pass + fail;
    return {
      bucket: bucket.label,
      N: outcomes.length,
      precision: resolved ? pass / resolved : null,
      medianMfe: median(outcomes.map(o => o.mfe)),
      medianMae: median(outcomes.map(o => o.mae)),
    };
  });
}

function evaluateIndependentClusterBuckets(store, members, delaySeconds) {
  const bucketLabels = ['1 cluster', '2 clusters', '3 clusters', '4+ clusters'];

  return bucketLabels.map((label, idx) => {
    const outcomes = [];
    for (const member of members) {
      const conv = gatherIndependentConvergence(
        store,
        member.tokenAddress,
        store.researchObservations.find(
          o =>
            o.tokenAddress === member.tokenAddress &&
            o.observationType === 'INDEPENDENT_CONVERGENCE'
        )?.occurredAt || new Date()
      );
      const count = conv.independentClusterCount;
      const matches =
        (idx === 0 && count === 1) ||
        (idx === 1 && count === 2) ||
        (idx === 2 && count === 3) ||
        (idx === 3 && count >= 4);
      if (!matches) continue;

      const obs = store.researchObservations.find(
        o =>
          o.tokenAddress === member.tokenAddress &&
          o.observationType === 'INDEPENDENT_CONVERGENCE'
      );
      if (!obs) continue;
      const outcome = store.researchObservationOutcomes.find(
        o => o.observationId === obs.id && o.executionDelaySeconds === delaySeconds
      );
      if (outcome?.label) outcomes.push(outcome);
    }

    const pass = outcomes.filter(o => o.label === 'PASS').length;
    const fail = outcomes.filter(o => o.label === 'FAIL').length;
    const resolved = pass + fail;
    return {
      bucket: label,
      N: outcomes.length,
      precision: resolved ? pass / resolved : null,
      medianMfe: median(outcomes.map(o => o.mfe)),
      medianMae: median(outcomes.map(o => o.mae)),
    };
  });
}

module.exports = {
  evaluateCohortLayers,
  LAYER_LABELS,
};
