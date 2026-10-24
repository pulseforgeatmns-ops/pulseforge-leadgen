'use strict';

/**
 * Canonical meaningful-learning predicates for LEARN → IMPROVE eligibility.
 * OutcomeLearning (acquisition_mission_outcome_learnings) and segment learning
 * (acquisition_mission_learning) remain separate stores; both may satisfy IMPROVE
 * when mission-bound and substantively meaningful.
 */

const { STAGES, asText } = require('./types');
const { ACCURACY_LABELS, LEARNING_OBJECT_KINDS } = require('./OutcomeLearning');

const VALID_OUTCOME_LEARNING_KINDS = new Set(Object.values(LEARNING_OBJECT_KINDS));

function evaluationsById(store, missionId) {
  const map = new Map();
  for (const row of store.listEvaluations(missionId)) {
    if (row && row.id) map.set(row.id, row);
  }
  return map;
}

function hasSubstantiveOutcomeContent(row = {}) {
  const statement = asText(row.statement);
  const lesson = asText(row.lesson || (row.payload && row.payload.lesson));
  const hasStrengthDelta = row.previousStrength != null && row.nextStrength != null;
  return Boolean(statement || lesson || hasStrengthDelta);
}

/**
 * Mission-bound OutcomeLearning counts when it represents actual learning.
 * autoApplied === false remains valid and does not block eligibility.
 */
function isMeaningfulOutcomeLearningRow(row = {}, evaluationsMap = new Map()) {
  if (!row) return false;
  if (!asText(row.missionId)) return false;

  const kind = asText(row.kind).toLowerCase();
  if (!kind || !VALID_OUTCOME_LEARNING_KINDS.has(kind)) return false;
  if (!hasSubstantiveOutcomeContent(row)) return false;

  const evaluationId = asText(row.evaluationId);
  if (evaluationId) {
    const evaluation = evaluationsMap.get(evaluationId);
    if (evaluation && evaluation.accuracy === ACCURACY_LABELS.INCONCLUSIVE) {
      return false;
    }
  }

  return true;
}

/**
 * Mission-bound segment learning counts when it carries operational signal.
 */
function isMeaningfulSegmentLearningRow(row = {}, missionId) {
  if (!row) return false;
  if (asText(row.missionId) !== asText(missionId)) return false;

  const segment = asText(row.segment);
  if (!segment) return false;

  const statement = asText(row.statement);
  const sends = Number(row.sends || 0);
  const replies = Number(row.replies || 0);
  if (!statement && sends === 0 && replies === 0) return false;

  return true;
}

function missionOutcomeLearnings(store, mission) {
  return store.listOutcomeLearnings(mission.tenantId, mission.id);
}

function missionSegmentLearnings(store, mission) {
  return store.listLearning(mission.tenantId)
    .filter((row) => asText(row.missionId) === asText(mission.id));
}

function hasMeaningfulLearning(store, mission) {
  if (!store || !mission) return false;

  const evalMap = evaluationsById(store, mission.id);
  const outcomeRows = missionOutcomeLearnings(store, mission);
  if (outcomeRows.some((row) => isMeaningfulOutcomeLearningRow(row, evalMap))) {
    return true;
  }

  const segmentRows = missionSegmentLearnings(store, mission);
  return segmentRows.some((row) => isMeaningfulSegmentLearningRow(row, mission.id));
}

function learningEligibilityFromStore(store, mission) {
  const hasMeaningful = hasMeaningfulLearning(store, mission);
  return {
    hasMeaningfulLearning: hasMeaningful,
    hasLearning: hasMeaningful,
    awaitingMeaningfulLearning: mission.stage === STAGES.LEARN && !hasMeaningful,
    improveEligible: hasMeaningful,
  };
}

module.exports = {
  VALID_OUTCOME_LEARNING_KINDS,
  isMeaningfulOutcomeLearningRow,
  isMeaningfulSegmentLearningRow,
  hasMeaningfulLearning,
  learningEligibilityFromStore,
  missionOutcomeLearnings,
  missionSegmentLearnings,
};
