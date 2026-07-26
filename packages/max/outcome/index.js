'use strict';

const {
  LIFECYCLE,
  OUTCOME_RESULTS,
  LIFECYCLE_TRANSITIONS,
  CONFIDENCE_BANDS,
  STRATEGY_IDS,
  buildRecommendationOutcome,
  canTransitionLifecycle,
  bandForConfidence,
  isTerminalOutcome,
} = require('./OutcomeTypes');
const { OutcomeStore } = require('./OutcomeStore');
const { LifecycleTracker } = require('./LifecycleTracker');
const { buildCalibrationReport } = require('./CalibrationReport');
const { buildStrategyPerformance } = require('./StrategyPerformance');
const { detectDrift } = require('./DriftDetector');
const { buildReviewDashboard } = require('./ReviewDashboard');
const {
  OutcomeEngine,
  createOutcomeEngine,
  collectRecommendationCards,
} = require('./OutcomeEngine');

module.exports = {
  LIFECYCLE,
  OUTCOME_RESULTS,
  LIFECYCLE_TRANSITIONS,
  CONFIDENCE_BANDS,
  STRATEGY_IDS,
  buildRecommendationOutcome,
  canTransitionLifecycle,
  bandForConfidence,
  isTerminalOutcome,
  OutcomeStore,
  LifecycleTracker,
  buildCalibrationReport,
  buildStrategyPerformance,
  detectDrift,
  buildReviewDashboard,
  OutcomeEngine,
  createOutcomeEngine,
  collectRecommendationCards,
};
