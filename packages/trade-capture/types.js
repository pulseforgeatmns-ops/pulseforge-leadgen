'use strict';

/**
 * Trade Capture Engine types — SPEC-044.
 *
 * Screenshot first. Ask almost nothing. Everything else is inferred.
 * Never interrupt trading. Missing data can be filled later.
 */

const CAPTURE_RULES = Object.freeze({
  SCREENSHOT_FIRST: 'screenshot_first',
  ASK_ALMOST_NOTHING: 'ask_almost_nothing',
  INFER_THE_REST: 'everything_else_is_inferred',
  NEVER_INTERRUPT: 'never_interrupt_trading',
  FILL_LATER: 'missing_data_can_be_filled_later',
  IMAGES_ARE_EVIDENCE: 'images_are_first_class_evidence',
  EXTRACTION_NON_BLOCKING: 'ocr_and_extraction_never_block_capture',
  IMMUTABLE_SCREENSHOTS: 'screenshots_are_immutable_observations',
});

/** Operator outcomes — no typing. */
const OUTCOMES = Object.freeze({
  WIN: 'Win',
  LOSS: 'Loss',
});

/** Trade direction — no typing. */
const DIRECTIONS = Object.freeze({
  LONG: 'Long',
  SHORT: 'Short',
});

/** Hypothesis chips — no typing. */
const HYPOTHESES = Object.freeze({
  VELOCITY: 'Velocity',
  BREAKOUT: 'Breakout',
  PULLBACK: 'Pullback',
  MEAN_REVERSION: 'Mean Reversion',
  OTHER: 'Other',
});

/** Confidence scale 1–5. */
const CONFIDENCE_LEVELS = Object.freeze([1, 2, 3, 4, 5]);

/** Immutable chart screenshot observation type (SPEC-017 market vocabulary). */
const CHART_SNAPSHOT = 'chart_snapshot';

/** Session lifecycle. */
const SESSION_STATUS = Object.freeze({
  AWAITING_ANSWERS: 'awaiting_answers',
  CAPTURED: 'captured',
  PROCESSING: 'processing',
  COMPLETE: 'complete',
});

/** Background job kinds. */
const EXTRACTION_JOBS = Object.freeze({
  OCR: 'ocr',
  INDICATOR: 'indicator',
  PRICE: 'price',
  METADATA: 'metadata',
  PATTERN: 'pattern',
});

/** Built-in extractor ids (pluggable). */
const EXTRACTOR_IDS = Object.freeze({
  OCR: 'ocr',
  CHART: 'chart',
  PATTERN: 'pattern',
  INDICATOR: 'indicator',
  COMPUTER_VISION: 'computer_vision',
});

const OPERATOR_QUESTIONS = Object.freeze([
  Object.freeze({
    id: 'result',
    label: 'Outcome',
    options: Object.freeze([OUTCOMES.WIN, OUTCOMES.LOSS]),
  }),
  Object.freeze({
    id: 'direction',
    label: 'Direction',
    options: Object.freeze([DIRECTIONS.LONG, DIRECTIONS.SHORT]),
  }),
  Object.freeze({
    id: 'hypothesis',
    label: 'Hypothesis',
    options: Object.freeze(Object.values(HYPOTHESES)),
  }),
  Object.freeze({
    id: 'confidence',
    label: 'Confidence',
    options: CONFIDENCE_LEVELS,
  }),
]);

module.exports = {
  CAPTURE_RULES,
  OUTCOMES,
  DIRECTIONS,
  HYPOTHESES,
  CONFIDENCE_LEVELS,
  CHART_SNAPSHOT,
  SESSION_STATUS,
  EXTRACTION_JOBS,
  EXTRACTOR_IDS,
  OPERATOR_QUESTIONS,
};
