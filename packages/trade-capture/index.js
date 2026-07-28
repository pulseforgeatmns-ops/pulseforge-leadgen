'use strict';

/**
 * @pulseforge/trade-capture — Trade Capture Engine (SPEC-044)
 *
 * Screenshot first. Ask almost nothing. Everything else is inferred.
 * Never interrupt trading. Images are first-class Evidence.
 *
 * Draft label was SPEC-021; that number is Learning & Belief Evolution —
 * this package ships as SPEC-044.
 *
 * @example
 *   const { createCaptureEngine } = require('@pulseforge/trade-capture');
 *   const engine = createCaptureEngine({ runExtractorsSync: true });
 *   const session = engine.pasteScreenshot(png);
 *   engine.answer(session.id, {
 *     result: 'Win',
 *     direction: 'Long',
 *     hypothesis: 'Velocity',
 *     confidence: 4,
 *   });
 *   const captured = engine.save(session.id);
 */

const {
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
} = require('./types');
const {
  CaptureEngine,
  createCaptureEngine,
} = require('./CaptureEngine');
const {
  CaptureSession,
  createCaptureSession,
} = require('./CaptureSession');
const {
  ScreenshotProcessor,
  createScreenshotProcessor,
  hashImageBytes,
} = require('./ScreenshotProcessor');
const {
  TradeBuilder,
  createTradeBuilder,
  normalizeAnswers,
  confidenceToScore,
} = require('./TradeBuilder');
const {
  ObservationExtractor,
  createObservationExtractor,
  emptyExtraction,
  defaultExtractors,
} = require('./ObservationExtractor');

module.exports = {
  CaptureEngine,
  createCaptureEngine,
  CaptureSession,
  createCaptureSession,
  ScreenshotProcessor,
  createScreenshotProcessor,
  hashImageBytes,
  TradeBuilder,
  createTradeBuilder,
  normalizeAnswers,
  confidenceToScore,
  ObservationExtractor,
  createObservationExtractor,
  emptyExtraction,
  defaultExtractors,
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
