'use strict';

const { PROSPECTIVE_TARGET_SAMPLE_SIZE } = require('./constants');

function isCohortBlinded(cohort) {
  if (!cohort) return true;
  if (cohort.metadata?.unblindedAt || cohort.unblindedAt) return false;
  return cohort.metadata?.blinded !== false;
}

function countEvaluableFirstCallerMembers(store, cohortId) {
  const members = store.getCohortMembers(cohortId);
  let evaluable = 0;
  for (const member of members) {
    if (member.inclusionReason !== 'FIRST_CALLER') continue;
    const obs = store.researchObservations.find(
      o =>
        o.tokenAddress === member.tokenAddress &&
        o.observationType === 'FIRST_CALLER' &&
        o.definitionVersion?.includes('prospective')
    );
    if (!obs) continue;
    const outcomes = store.researchObservationOutcomes.filter(o => o.observationId === obs.id);
    const hasTerminal =
      outcomes.some(o => o.label === 'PASS' || o.label === 'FAIL') ||
      outcomes.some(o => o.dataAvailability === 'INSUFFICIENT_MARKET_DATA');
    if (hasTerminal) evaluable += 1;
  }
  return evaluable;
}

function cohortProgress(store, cohort) {
  const members = store.getCohortMembers(cohort.id);
  const firstCallerMembers = members.filter(m => m.inclusionReason === 'FIRST_CALLER');
  const evaluable = countEvaluableFirstCallerMembers(store, cohort.id);
  return {
    targetSampleSize: cohort.metadata?.targetSampleSize || PROSPECTIVE_TARGET_SAMPLE_SIZE,
    evaluableFirstCallerCount: evaluable,
    memberCount: members.length,
    firstCallerMemberCount: firstCallerMembers.length,
    blinded: isCohortBlinded(cohort),
  };
}

function redactEvaluationIfBlinded(evaluation, cohort) {
  if (!isCohortBlinded(cohort)) return evaluation;
  // Use an allowlist: PASS/FAIL counts, returns, Wilson intervals and nested
  // sensitivity/empirical reports all disclose performance even if precision is null.
  return {
    cohortId: evaluation.cohortId,
    cohortN: evaluation.cohortN,
    dataClass: evaluation.dataClass,
    executionDelaySeconds: evaluation.executionDelaySeconds,
    blinded: true,
    primaryHypothesisPrecision: null,
    layers: (evaluation.layers || []).map(layer => ({
      observationType: layer.observationType,
      observationLayer: layer.observationLayer,
      N: layer.N,
      precision: null,
      falsePositiveRate: null,
      blinded: true,
    })),
  };
}

module.exports = {
  isCohortBlinded,
  cohortProgress,
  redactEvaluationIfBlinded,
  countEvaluableFirstCallerMembers,
};
