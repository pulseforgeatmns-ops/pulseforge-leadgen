'use strict';

/**
 * Learning & Belief Evolution types (SPEC-021).
 *
 * Evidence earns trust. Outcomes calibrate trust.
 * No ML / neural nets / black-box optimization.
 */

/** @typedef {'correct'|'incorrect'|'partially_correct'|'unresolved'} OutcomeVerdict */

/**
 * @typedef {object} ClaimRef
 * @property {string} id
 * @property {string} [claimType]
 * @property {string} [statement]
 * @property {string} [subjectId]
 * @property {string} [subject]
 * @property {number} [confidence]
 * @property {string} [strategyPack]
 * @property {string} [assertedAt]
 * @property {unknown[]} [supportingEvidence]
 * @property {unknown[]} [observations]
 */

/**
 * @typedef {object} OutcomeRef
 * @property {string} id
 * @property {string} [claimId]
 * @property {string} [claimType]
 * @property {string} [subjectId]
 * @property {string} [subject]
 * @property {string} [outcomeType]
 * @property {string} [statement]
 * @property {string} [observedAt]
 * @property {OutcomeVerdict|string} [verdict]
 * @property {boolean} [correct]
 * @property {number} [partialScore] - 0–1 when partially_correct
 * @property {string} [strategyPack]
 * @property {Record<string, unknown>} [metadata]
 */

/**
 * @typedef {object} EvaluationRecord
 * @property {string} id
 * @property {string} claimId
 * @property {string|null} claimType
 * @property {string|null} subjectId
 * @property {string|null} strategyPack
 * @property {number|null} confidenceBefore
 * @property {OutcomeVerdict} verdict
 * @property {number} credit - 1 correct, 0 incorrect, (0,1) partial, null unresolved → 0 credit tracked separately
 * @property {string|null} outcomeId
 * @property {string|null} observedAt
 * @property {unknown[]} observationsConsidered
 * @property {string} explanation
 */

/**
 * @typedef {object} BeliefStats
 * @property {string} claimId
 * @property {string|null} claimType
 * @property {string|null} label
 * @property {number} occurrences
 * @property {number} correct
 * @property {number} incorrect
 * @property {number} partiallyCorrect
 * @property {number} unresolved
 * @property {number|null} accuracy - among resolved (correct+incorrect+partial credit)
 * @property {number|null} precision
 * @property {number|null} recall
 * @property {number|null} historicalCalibration
 */

/**
 * @typedef {object} CalibrationResult
 * @property {string} claimId
 * @property {string|null} claimType
 * @property {number|null} confidence
 * @property {number|null} historicalCalibration
 * @property {number|null} adjustedConfidence
 * @property {number} blendWeight
 * @property {BeliefStats} stats
 * @property {object} explanation
 * @property {boolean} mutatesHistory
 * @property {boolean} mutatesReplay
 * @property {boolean} mutatesRuntime
 */

/**
 * @typedef {object} AccuracyReport
 * @property {string} scope
 * @property {string|null} scopeId
 * @property {number} occurrences
 * @property {number} correct
 * @property {number} incorrect
 * @property {number} partiallyCorrect
 * @property {number} unresolved
 * @property {number|null} accuracy
 * @property {number|null} precision
 * @property {number|null} recall
 * @property {number|null} historicalCalibration
 * @property {BeliefStats[]} claims
 * @property {object} explanation
 */

const LEARNING_RULES = Object.freeze({
  EVIDENCE_EARNS_TRUST: 'evidence_earns_trust',
  OUTCOMES_CALIBRATE_TRUST: 'outcomes_calibrate_trust',
  UPDATE_ONLY_AFTER_REALITY: 'confidence_updates_only_after_reality_known',
  NO_HISTORY_MUTATION: 'learning_never_mutates_history',
  NO_REPLAY_MUTATION: 'learning_never_mutates_replay',
  NO_RUNTIME_MUTATION: 'learning_never_mutates_runtime',
  NO_ML: 'learning_uses_no_machine_learning',
  EXPLAINABLE: 'every_calibration_is_explainable',
});

const OUTCOME_VERDICTS = Object.freeze({
  CORRECT: 'correct',
  INCORRECT: 'incorrect',
  PARTIALLY_CORRECT: 'partially_correct',
  UNRESOLVED: 'unresolved',
});

/** Default blend: equal weight between runtime confidence and historical calibration. */
const DEFAULT_BLEND_WEIGHT = 0.5;

module.exports = {
  LEARNING_RULES,
  OUTCOME_VERDICTS,
  DEFAULT_BLEND_WEIGHT,
};
