'use strict';

const {
  CONFIDENCE_THRESHOLD,
  INSPECTION_THRESHOLD,
  classifyDecisionMismatch,
} = require('./mismatchClassifier');

function buildRoutingWarning(row, classify = classifyDecisionMismatch) {
  const warning = classify(row);
  if (!warning) return null;
  return {
    event: 'DECISION_SHADOW_WARNING',
    spec: 'SPEC-JEV-003',
    schema_version: 1,
    mode: 'shadow_warning',
    action: 'review_current_route_without_changing_routing',
    ...warning,
  };
}

/** @deprecated use classifyDecisionMismatch */
function likelyMissionInspection(row) {
  return classifyDecisionMismatch(row) != null;
}

module.exports = {
  THRESHOLD: INSPECTION_THRESHOLD,
  CONFIDENCE_THRESHOLD,
  INSPECTION_THRESHOLD,
  likelyMissionInspection,
  buildRoutingWarning,
  classifyDecisionMismatch,
};
