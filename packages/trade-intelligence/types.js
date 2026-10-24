'use strict';

/**
 * Trade Intelligence Engine types — SPEC-046.
 *
 * Evidence first. Never optimize for a single strategy.
 * Recommendations must be explainable. Everything reproducible through Replay.
 */

const INTELLIGENCE_RULES = Object.freeze({
  EVIDENCE_FIRST: 'evidence_first',
  NO_SINGLE_STRATEGY_OPTIMIZATION: 'never_optimize_for_single_strategy',
  EXPLAINABLE_RECOMMENDATIONS: 'recommendations_must_be_explainable',
  REPLAY_REPRODUCIBLE: 'everything_reproducible_through_replay',
  DERIVED_NOT_EDITED: 'intelligence_derived_from_observations_never_manually_edited',
  FINDINGS_IMMUTABLE: 'findings_are_immutable_and_replayable',
  NO_EXECUTION: 'does_not_execute_trades',
});

const RUNTIME_VERSION = 'trade-intelligence@1.0.0';

const FINDING_TYPES = Object.freeze({
  PATTERN: 'pattern',
  CALIBRATION: 'calibration',
  HYPOTHESIS: 'hypothesis',
  RECOMMENDATION: 'recommendation',
  REVIEW: 'review',
  SIMILARITY: 'similarity',
  TIME_WINDOW: 'time_window',
});

const REVIEW_PERIODS = Object.freeze({
  TODAY: 'Today',
  YESTERDAY: 'Yesterday',
  LAST_WEEK: 'LastWeek',
  THIS_WEEK: 'ThisWeek',
});

const CONFIDENCE_BANDS = Object.freeze([1, 2, 3, 4, 5]);

const MIN_PATTERN_SAMPLE = 3;
const MIN_RECOMMENDATION_SAMPLE = 5;

const RECOMMENDATION_CONFIDENCE = Object.freeze({
  HIGH: 'High',
  MEDIUM: 'Medium',
  LOW: 'Low',
});

module.exports = {
  INTELLIGENCE_RULES,
  RUNTIME_VERSION,
  FINDING_TYPES,
  REVIEW_PERIODS,
  CONFIDENCE_BANDS,
  MIN_PATTERN_SAMPLE,
  MIN_RECOMMENDATION_SAMPLE,
  RECOMMENDATION_CONFIDENCE,
};
