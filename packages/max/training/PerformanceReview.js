'use strict';

/**
 * SPEC-102F — performance review dimensions.
 * Ask "How did Max manage Scout?" not "Is Scout done?"
 */

const REVIEW_DIMENSIONS = Object.freeze([
  { id: 'delegation', label: 'Delegation' },
  { id: 'retrieval', label: 'Retrieval' },
  { id: 'judgment', label: 'Judgment' },
  { id: 'evidence', label: 'Evidence' },
  { id: 'communication', label: 'Communication' },
  { id: 'uncertainty', label: 'Uncertainty' },
  { id: 'operator_trust', label: 'Operator trust' },
  { id: 'reflection', label: 'Reflection' },
]);

const REAL_WORK_PRIORITY = Object.freeze([
  { id: 'anchor_cleaning', label: 'Anchor Cleaning', clientId: 10 },
  { id: 'pilot_clients', label: 'Pilot clients (Aji)' },
  { id: 'production_clients', label: 'Production clients' },
  { id: 'synthetic_scenarios', label: 'Synthetic scenarios' },
]);

function listReviewDimensions() {
  return REVIEW_DIMENSIONS.map(d => ({ ...d }));
}

function listRealWorkPriority() {
  return REAL_WORK_PRIORITY.map(p => ({ ...p }));
}

module.exports = {
  REVIEW_DIMENSIONS,
  REAL_WORK_PRIORITY,
  listReviewDimensions,
  listRealWorkPriority,
};
