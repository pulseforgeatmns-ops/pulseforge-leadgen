'use strict';

const {
  PROSPECTIVE_COHORT_001_ID,
  PROSPECTIVE_TARGET_SAMPLE_SIZE,
  PROSPECTIVE_RESEARCH_DEFINITION_VERSION,
  PROSPECTIVE_FEATURE_VERSION,
  DATA_CLASS,
  COHORT_MODE,
} = require('./constants');
const { FEATURE_VERSION, RESEARCH_DEFINITION_VERSION } = require('../types');

function buildProspectiveCohortRecord({ startedAt, providerVersions = {} }) {
  const now = startedAt || new Date();
  return {
    id: PROSPECTIVE_COHORT_001_ID,
    name: 'Signal V1 Prospective Empirical Cohort 001',
    definitionVersion: PROSPECTIVE_RESEARCH_DEFINITION_VERSION,
    createdAt: now,
    metadata: {
      dataClass: DATA_CLASS.EMPIRICAL,
      mode: COHORT_MODE.PROSPECTIVE,
      startedAt: new Date(now).toISOString(),
      frozenDefinitionsAt: new Date(now).toISOString(),
      targetSampleSize: PROSPECTIVE_TARGET_SAMPLE_SIZE,
      researchDefinitionVersion: RESEARCH_DEFINITION_VERSION,
      featureVersion: FEATURE_VERSION,
      prospectiveFeatureVersion: PROSPECTIVE_FEATURE_VERSION,
      providerVersions,
      blinded: true,
      primaryHypothesis: 'INDEPENDENT_CONVERGENCE',
    },
  };
}

function eventEligibleForProspectiveCohort(event, cohortStartedAt) {
  const startedMs = new Date(cohortStartedAt).getTime();
  if (event.occurredAt.getTime() < startedMs) return false;
  if (event.provenance?.dataClass === DATA_CLASS.PROCEDURAL) return false;
  if (event.payload?.procedural === true) return false;
  return true;
}

function unblindCohort(cohort, { unblindedBy, evaluationVersion }) {
  return {
    ...cohort,
    metadata: {
      ...cohort.metadata,
      unblindedAt: new Date().toISOString(),
      unblindedBy,
      evaluationVersion,
      blinded: false,
    },
  };
}

module.exports = {
  buildProspectiveCohortRecord,
  eventEligibleForProspectiveCohort,
  unblindCohort,
  PROSPECTIVE_COHORT_001_ID,
};
