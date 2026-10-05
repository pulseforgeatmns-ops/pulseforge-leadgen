'use strict';

const { callStore } = require('../storage/storeUtils');
const { RESEARCH_DEFINITION_VERSION } = require('../types');

/**
 * Load persisted research observations into store.researchObservations for cohort evaluation.
 *
 * @param {object} store
 * @param {string} cohortId
 */
async function hydrateCohortResearchCache(store, cohortId) {
  if (!store.getCohortMembers) return;
  const members = await callStore(store, 'getCohortMembers', cohortId);
  store.researchObservations = store.researchObservations || [];
  store.researchObservationOutcomes = store.researchObservationOutcomes || [];

  if (store.researchObservations.length > 0 && members.every(m =>
    store.researchObservations.some(o => o.tokenAddress === m.tokenAddress)
  )) {
    return;
  }

  store.researchObservations = [];
  store.researchObservationOutcomes = [];

  for (const member of members) {
    const observations = await callStore(
      store,
      'getResearchObservations',
      member.tokenAddress,
      RESEARCH_DEFINITION_VERSION
    );
    for (const obs of observations) {
      store.researchObservations.push(obs);
      const outcomes = await callStore(store, 'getResearchObservationOutcomes', obs.id);
      for (const outcome of outcomes) {
        store.researchObservationOutcomes.push(outcome);
      }
    }
  }
}

module.exports = {
  hydrateCohortResearchCache,
};
