'use strict';

/**
 * @pulseforge/learning — Learning & Belief Evolution Engine (SPEC-021)
 *
 * Evidence earns trust. Outcomes calibrate trust.
 *
 * Consumes Claims · Evidence · Outcomes.
 * Produces calibration updates · historical accuracy · confidence adjustments.
 *
 * No machine learning. No neural networks. No black-box optimization.
 * Never mutates history, replay, or runtime confidence.
 */

const {
  LEARNING_RULES,
  OUTCOME_VERDICTS,
  DEFAULT_BLEND_WEIGHT,
} = require('./types');
const {
  LearningEngine,
  createLearningEngine,
} = require('./LearningEngine');
const {
  BeliefTracker,
  createBeliefTracker,
} = require('./BeliefTracker');
const {
  CalibrationEngine,
  createCalibrationEngine,
  blend,
  formatPct,
} = require('./CalibrationEngine');
const {
  OutcomeEvaluator,
  createOutcomeEvaluator,
  resolveVerdict,
  creditFor,
  normalizeConfidence,
} = require('./OutcomeEvaluator');
const {
  LearningSession,
  createLearningSession,
  pairClaimsAndOutcomes,
} = require('./LearningSession');

module.exports = {
  LearningEngine,
  createLearningEngine,
  BeliefTracker,
  createBeliefTracker,
  CalibrationEngine,
  createCalibrationEngine,
  blend,
  formatPct,
  OutcomeEvaluator,
  createOutcomeEvaluator,
  resolveVerdict,
  creditFor,
  normalizeConfidence,
  LearningSession,
  createLearningSession,
  pairClaimsAndOutcomes,
  LEARNING_RULES,
  OUTCOME_VERDICTS,
  DEFAULT_BLEND_WEIGHT,
};
