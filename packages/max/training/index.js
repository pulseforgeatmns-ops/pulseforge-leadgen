'use strict';

const { STAGES, STAGE_ORDER, STAGE_SYMBOLS, isValidStage, canGraduate, stageRank } = require('./CompetencyLifecycle');
const {
  CATEGORIES,
  COMPETENCIES,
  listCompetencies,
  getCompetency,
  listByCategory,
  listByStage,
} = require('./CompetencyRegistry');
const { REQUIRED_FIELDS, validateExercise, validateCompetency } = require('./TrainingExercise');
const {
  REVIEW_DIMENSIONS,
  REAL_WORK_PRIORITY,
  listReviewDimensions,
  listRealWorkPriority,
} = require('./PerformanceReview');
const {
  RECORD_PATH,
  loadRecordOverrides,
  buildTrainingRecord,
  formatTrainingRecordText,
  formatCompetencySummary,
} = require('./TrainingRecord');
const {
  REPO_ROOT,
  resolveTestPath,
  testPathExists,
  buildRegressionSuite,
  assertRegressionSuite,
} = require('./RegressionSuite');

module.exports = {
  STAGES,
  STAGE_ORDER,
  STAGE_SYMBOLS,
  isValidStage,
  canGraduate,
  stageRank,
  CATEGORIES,
  COMPETENCIES,
  listCompetencies,
  getCompetency,
  listByCategory,
  listByStage,
  REQUIRED_FIELDS,
  validateExercise,
  validateCompetency,
  REVIEW_DIMENSIONS,
  REAL_WORK_PRIORITY,
  listReviewDimensions,
  listRealWorkPriority,
  RECORD_PATH,
  loadRecordOverrides,
  buildTrainingRecord,
  formatTrainingRecordText,
  formatCompetencySummary,
  REPO_ROOT,
  resolveTestPath,
  testPathExists,
  buildRegressionSuite,
  assertRegressionSuite,
};
