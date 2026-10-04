'use strict';

/** @typedef {(
 *   | 'DISCOVERED'
 *   | 'ELIGIBLE'
 *   | 'INELIGIBLE'
 *   | 'SELECTED'
 *   | 'BACKFILLED'
 *   | 'REPLAYED'
 *   | 'DATA_INSUFFICIENT'
 * )} ResearchCandidateStatus */

/** @typedef {'stronger' | 'failure' | 'mixed_fixture' | 'unknown'} SelectionCategory */

const RESEARCH_CANDIDATE_STATUSES = Object.freeze([
  'DISCOVERED',
  'ELIGIBLE',
  'INELIGIBLE',
  'SELECTED',
  'BACKFILLED',
  'REPLAYED',
  'DATA_INSUFFICIENT',
]);

const VALIDATION_COHORT_001_ID = 'cohort-signal-v1-validation-001';
const VALIDATION_COHORT_001_SELECTION_VERSION = 'validation-001-selection-v1';
const VALIDATION_COHORT_001_TARGET_SIZE = 40;
const VALIDATION_COHORT_001_PER_CATEGORY = 20;

module.exports = {
  RESEARCH_CANDIDATE_STATUSES,
  VALIDATION_COHORT_001_ID,
  VALIDATION_COHORT_001_SELECTION_VERSION,
  VALIDATION_COHORT_001_TARGET_SIZE,
  VALIDATION_COHORT_001_PER_CATEGORY,
};
