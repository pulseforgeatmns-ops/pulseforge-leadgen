'use strict';

/**
 * @pulseforge/trade-intelligence — Trade Intelligence Engine (SPEC-046)
 *
 * Converts captured trades into actionable, evidence-backed intelligence.
 * Does not execute trades.
 *
 * Draft label was SPEC-045; that number is Command Deck UX Polish —
 * this package ships as SPEC-046.
 *
 * @example
 *   const { createTradeIntelligenceEngine } = require('@pulseforge/trade-intelligence');
 *   const intel = createTradeIntelligenceEngine({ captureEngine });
 *   intel.analyze();
 *   const daily = intel.generateDailyReview();
 *   const recs = intel.getRecommendations();
 */

const {
  INTELLIGENCE_RULES,
  RUNTIME_VERSION,
  FINDING_TYPES,
  REVIEW_PERIODS,
  CONFIDENCE_BANDS,
  MIN_PATTERN_SAMPLE,
  MIN_RECOMMENDATION_SAMPLE,
  RECOMMENDATION_CONFIDENCE,
} = require('./types');
const {
  TradeIntelligenceEngine,
  createTradeIntelligenceEngine,
  similarityScore,
} = require('./TradeIntelligenceEngine');
const {
  TradeAnalyzer,
  createTradeAnalyzer,
  isWin,
  isLoss,
  dayKey,
  groupBy,
} = require('./TradeAnalyzer');
const {
  PatternDiscovery,
  createPatternDiscovery,
  evidenceRef,
} = require('./PatternDiscovery');
const {
  CalibrationAnalyzer,
  createCalibrationAnalyzer,
} = require('./CalibrationAnalyzer');
const {
  ReviewGenerator,
  createReviewGenerator,
} = require('./ReviewGenerator');
const {
  RecommendationEngine,
  createRecommendationEngine,
} = require('./RecommendationEngine');

module.exports = {
  TradeIntelligenceEngine,
  createTradeIntelligenceEngine,
  similarityScore,
  TradeAnalyzer,
  createTradeAnalyzer,
  isWin,
  isLoss,
  dayKey,
  groupBy,
  PatternDiscovery,
  createPatternDiscovery,
  evidenceRef,
  CalibrationAnalyzer,
  createCalibrationAnalyzer,
  ReviewGenerator,
  createReviewGenerator,
  RecommendationEngine,
  createRecommendationEngine,
  INTELLIGENCE_RULES,
  RUNTIME_VERSION,
  FINDING_TYPES,
  REVIEW_PERIODS,
  CONFIDENCE_BANDS,
  MIN_PATTERN_SAMPLE,
  MIN_RECOMMENDATION_SAMPLE,
  RECOMMENDATION_CONFIDENCE,
};
