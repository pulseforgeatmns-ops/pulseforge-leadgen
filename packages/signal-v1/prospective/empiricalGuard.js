'use strict';

const { DATA_CLASS } = require('./constants');

/**
 * Blocks primary evaluation when empirical cohort contains procedural/synthetic evidence.
 *
 * @param {object} store
 * @param {string} cohortId
 */
function assertEmpiricalCohort(store, cohortId) {
  const cohort = store.researchCohorts?.get?.(cohortId);
  if (!cohort) {
    throw new Error(`Cohort not found: ${cohortId}`);
  }
  const dataClass = cohort.metadata?.dataClass || cohort.dataClass;
  if (dataClass !== DATA_CLASS.EMPIRICAL) {
    throw new Error(`Cohort ${cohortId} is not EMPIRICAL (dataClass=${dataClass || 'missing'})`);
  }

  const contamination = {
    proceduralCallerEvidence: 0,
    proceduralMarketEvidence: 0,
    unverifiedSyntheticFeatures: 0,
    details: [],
  };

  const members = store.getCohortMembers(cohortId);
  for (const member of members) {
    const token = member.tokenAddress;
    const events = store.getEventsForToken(token);
    for (const event of events) {
      if (event.provenance?.dataClass === DATA_CLASS.PROCEDURAL || event.payload?.procedural === true) {
        contamination.proceduralCallerEvidence += 1;
        contamination.details.push({ kind: 'procedural_caller', eventId: event.id, token });
      }
      if (event.provenance?.collectorId === 'procedural-test') {
        contamination.proceduralCallerEvidence += 1;
        contamination.details.push({ kind: 'procedural_collector', eventId: event.id, token });
      }
    }
    const market = store.getMarketObservationsForToken
      ? store.getMarketObservationsForToken(token)
      : (store.marketObservations || []).filter(o => o.tokenAddress === token);
    const marketList = market && typeof market.then === 'function' ? [] : market;
    for (const obs of marketList) {
      if (obs.provenance?.dataClass === DATA_CLASS.PROCEDURAL || obs.provenance?.synthetic === true) {
        contamination.proceduralMarketEvidence += 1;
        contamination.details.push({ kind: 'procedural_market', observationId: obs.id, token });
      }
    }
  }

  const rawEvidence = store.rawCallerEvidence || [];
  for (const row of rawEvidence) {
    if (row.provenance?.dataClass === DATA_CLASS.PROCEDURAL || row.collectorId === 'procedural-test') {
      contamination.proceduralCallerEvidence += 1;
      contamination.details.push({ kind: 'procedural_raw', evidenceId: row.id });
    }
  }

  const total =
    contamination.proceduralCallerEvidence +
    contamination.proceduralMarketEvidence +
    contamination.unverifiedSyntheticFeatures;

  if (total > 0) {
    const err = new Error(`EMPIRICAL_CONTAMINATION: cohort ${cohortId} has ${total} contaminated rows`);
    err.code = 'EMPIRICAL_CONTAMINATION';
    err.contamination = contamination;
    throw err;
  }

  return { ok: true, cohortId, contamination };
}

module.exports = {
  assertEmpiricalCohort,
};
