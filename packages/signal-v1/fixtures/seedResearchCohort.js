'use strict';

const { RESEARCH_DEFINITION_VERSION } = require('../types');
const { RESEARCH_CASES } = require('./frontRunnersCases');

const PILOT_COHORT_ID = 'cohort-signal-v1-pilot';

/**
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 */
function seedResearchCohort(store) {
  store.upsertResearchCohort({
    id: PILOT_COHORT_ID,
    name: 'Signal V1 pilot research cohort (phase A)',
    definitionVersion: RESEARCH_DEFINITION_VERSION,
    metadata: {
      targetSize: 40,
      phase: 'A',
      note: 'Initial proof tokens — expand with provenance before cohort milestone',
    },
  });

  const pilotSlugs = ['DOOM', 'DUPLICATE', 'HALLOW_INU'];
  for (const slug of pilotSlugs) {
    const c = RESEARCH_CASES.find(x => x.slug === slug);
    if (!c?.tokenAddress) continue;
    store.addCohortMember({
      cohortId: PILOT_COHORT_ID,
      tokenAddress: c.tokenAddress,
      inclusionReason: `Phase A proof replay (${slug})`,
      provenance: {
        slug,
        category: slug === 'DUPLICATE' ? 'mixed_fixture' : 'historical_case',
        selectionMethod: 'spec_003_initial_proof',
        frontRunnersOnly: false,
      },
    });
  }

  return PILOT_COHORT_ID;
}

module.exports = {
  seedResearchCohort,
  PILOT_COHORT_ID,
};
