'use strict';

/** @typedef {'SYNTHETIC' | 'MIXED' | 'EMPIRICAL'} ResearchDataClass */

const RESEARCH_DATA_CLASS = Object.freeze({
  SYNTHETIC: 'SYNTHETIC',
  MIXED: 'MIXED',
  EMPIRICAL: 'EMPIRICAL',
});

const COHORT_DATA_CLASS_BY_ID = Object.freeze({
  'cohort-signal-v1-validation-001': RESEARCH_DATA_CLASS.SYNTHETIC,
  'cohort-signal-v1-validation-002': RESEARCH_DATA_CLASS.SYNTHETIC,
  'cohort-signal-v1-validation-003': RESEARCH_DATA_CLASS.EMPIRICAL,
});

/**
 * @param {string} cohortId
 * @param {object} [cohortRow]
 * @returns {ResearchDataClass}
 */
function resolveCohortDataClass(cohortId, cohortRow) {
  if (cohortRow?.dataClass) return cohortRow.dataClass;
  if (cohortRow?.metadata?.dataClass) return cohortRow.metadata.dataClass;
  return COHORT_DATA_CLASS_BY_ID[cohortId] || RESEARCH_DATA_CLASS.MIXED;
}

function isEmpiricalDataClass(dataClass) {
  return dataClass === RESEARCH_DATA_CLASS.EMPIRICAL;
}

module.exports = {
  RESEARCH_DATA_CLASS,
  COHORT_DATA_CLASS_BY_ID,
  resolveCohortDataClass,
  isEmpiricalDataClass,
};
