'use strict';

const { RESEARCH_OBSERVATION_TYPES } = require('../types');
const { median } = require('./sourcePerformanceEngine');
const { gatherIndependentConvergence } = require('./observationTriggers');
const { wilsonInterval } = require('./wilsonInterval');
const { resolveCohortDataClass, isEmpiricalDataClass } = require('./dataClass');
const { assertEmpiricalCohort } = require('./empiricalGuard');

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
function evaluateCohortLayers(store, cohortId, executionDelaySeconds = 60, options = {}) {
  const cohort =
    store.researchCohorts?.get?.(cohortId) ||
    [...(store.researchCohorts?.values?.() || [])].find(c => c.id === cohortId);
  const dataClass = resolveCohortDataClass(cohortId, cohort);
  let contamination = null;
  if (isEmpiricalDataClass(dataClass) && !options.skipEmpiricalGuard) {
    const gate = assertEmpiricalCohort(store, cohortId, options);
    contamination = gate.contamination;
  }

  const members = store.getCohortMembers(cohortId);
  const rows = [];
  const researchConfig = isEmpiricalDataClass(dataClass)
    ? { strictClusterIndependence: true, ...(options.researchConfig || {}) }
    : options.researchConfig;

  for (const type of RESEARCH_OBSERVATION_TYPES) {
    const stats = aggregateLayer(store, members, type, executionDelaySeconds);
    rows.push({
      observationLayer: LAYER_LABELS[type] || type,
      observationType: type,
      ...stats,
    });
  }

  const evaluation = {
    cohortId,
    cohortN: members.length,
    dataClass,
    executionDelaySeconds,
    layers: rows,
    dataCoverage: summarizeDataCoverage(store, members),
    convergenceVelocity: evaluateConvergenceVelocityBuckets(store, members, executionDelaySeconds),
    independentClusterAnalysis: evaluateIndependentClusterBuckets(store, members, executionDelaySeconds),
    contamination,
    researchConfig,
    lowNSwarnings: rows
      .filter(r => (r.denominators?.resolvedN ?? 0) < 10)
      .map(r => `${r.observationType}: resolved N=${r.denominators?.resolvedN ?? 0}`),
  };

  if (isEmpiricalDataClass(dataClass)) {
    evaluation.executionDelaySensitivity = evaluateExecutionDelaySensitivity(store, cohortId);
    const { buildEmpiricalExtendedReport } = require('./empiricalCohortReport');
    evaluation.empiricalReport = buildEmpiricalExtendedReport(
      store,
      cohortId,
      evaluation,
      executionDelaySeconds
    );
  }

  return evaluation;
}

function summarizeDataCoverage(store, members) {
  let replayComplete = 0;
  let marketAttempted = 0;
  let walletEvidence = 0;
  let structureEvidence = 0;

  for (const member of members) {
    const obsCount = store.researchObservations.filter(
      o => o.tokenAddress === member.tokenAddress
    ).length;
    if (obsCount > 0) replayComplete += 1;

    const market = store.getMarketObservationsForToken
      ? store.getMarketObservationsForToken(member.tokenAddress)
      : store.marketObservations.filter(o => o.tokenAddress === member.tokenAddress);
    const marketList = market && typeof market.then === 'function' ? [] : market;
    if (marketList.length) marketAttempted += 1;

    const walletEvents = store.events.filter(
      e => e.tokenAddress === member.tokenAddress && e.sourceType === 'wallet'
    );
    if (walletEvents.length) walletEvidence += 1;

    const structure = store.events.filter(
      e =>
        e.tokenAddress === member.tokenAddress &&
        (e.eventType === 'HOLDER_SNAPSHOT' || e.eventType === 'LIQUIDITY_CHANGE')
    );
    if (structure.length) structureEvidence += 1;
  }

  return {
    cohortN: members.length,
    replayWithObservationsN: replayComplete,
    marketHistoryAttemptedN: marketAttempted,
    walletEvidenceN: walletEvidence,
    structureEvidenceN: structureEvidence,
  };
}

function evaluateExecutionDelaySensitivity(store, cohortId) {
  const delays = [15, 30, 60, 180, 300];
  const members = store.getCohortMembers(cohortId);
  return delays.map(delaySeconds => ({
    executionDelaySeconds: delaySeconds,
    layers: RESEARCH_OBSERVATION_TYPES.map(type => ({
      observationType: type,
      ...aggregateLayer(store, members, type, delaySeconds),
    })),
  }));
}

function aggregateLayer(store, members, observationType, delaySeconds) {
  const labeledOutcomes = [];
  const availabilityCounts = { AVAILABLE: 0, PARTIAL: 0, UNAVAILABLE: 0, INSUFFICIENT_MARKET_DATA: 0 };
  let observationTriggeredN = 0;
  let marketEvaluableN = 0;
  let dataUnavailableN = 0;

  for (const member of members) {
    const obs = store.researchObservations.find(
      o => o.tokenAddress === member.tokenAddress && o.observationType === observationType
    );
    if (!obs) continue;
    observationTriggeredN += 1;

    const outcome = store.researchObservationOutcomes.find(
      o => o.observationId === obs.id && o.executionDelaySeconds === delaySeconds
    );
    if (!outcome) continue;

    availabilityCounts[outcome.dataAvailability] =
      (availabilityCounts[outcome.dataAvailability] || 0) + 1;

    if (
      outcome.dataAvailability === 'UNAVAILABLE' ||
      outcome.dataAvailability === 'INSUFFICIENT_MARKET_DATA'
    ) {
      dataUnavailableN += 1;
      continue;
    }

    marketEvaluableN += 1;
    if (outcome.label) labeledOutcomes.push(outcome);
  }

  const pass = labeledOutcomes.filter(o => o.label === 'PASS').length;
  const fail = labeledOutcomes.filter(o => o.label === 'FAIL').length;
  const unresolved = labeledOutcomes.filter(o => o.label === 'UNRESOLVED').length;
  const resolvedN = pass + fail;
  const precision = resolvedN ? pass / resolvedN : null;
  const falsePositiveRate = resolvedN ? fail / resolvedN : null;

  const wi = wilsonInterval(pass, resolvedN);

  return {
    N: labeledOutcomes.length,
    PASS: pass,
    FAIL: fail,
    UNRESOLVED: unresolved,
    precision,
    wilson95: wi,
    falsePositiveRate,
    medianMfe: median(labeledOutcomes.map(o => o.mfe)),
    medianMae: median(labeledOutcomes.map(o => o.mae)),
    medianTimeTo2x: median(labeledOutcomes.map(o => o.timeTo2xSeconds)),
    medianTimeToMinus30: median(labeledOutcomes.map(o => o.timeToMinus30Seconds)),
    medianReturn15m: median(labeledOutcomes.map(o => o.return15m)),
    medianReturn1h: median(labeledOutcomes.map(o => o.return1h)),
    medianReturn6h: median(labeledOutcomes.map(o => o.return6h)),
    medianReturn24h: median(labeledOutcomes.map(o => o.return24h)),
    dataAvailability: availabilityCounts,
    dataUnavailableN,
    denominators: {
      cohortN: members.length,
      observationTriggeredN,
      marketEvaluableN,
      resolvedN,
      labeledN: labeledOutcomes.length,
    },
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
  evaluateExecutionDelaySensitivity,
  summarizeDataCoverage,
  LAYER_LABELS,
};
