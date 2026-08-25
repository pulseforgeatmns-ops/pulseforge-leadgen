'use strict';

/**
 * SPEC-162 — Business Heuristics Engine public exports.
 * ADR-082 — Business judgment through reusable heuristics.
 */

const {
  HEURISTIC_CATEGORIES,
  OUTCOME_KINDS,
  buildBusinessHeuristic,
  buildActivatedHeuristic,
  buildHeuristicContradiction,
} = require('./types');

const { INITIAL_HEURISTICS, cloneHeuristicLibrary, getHeuristicById } = require('./HeuristicLibrary');

const {
  activateHeuristics,
  explainJudgment,
  buildRecommendationFromHeuristics,
  buildBusinessJudgmentReport,
  learnFromOutcome,
  matchHeuristicsForContext,
  detectHeuristicContradictions,
  computeOverallJudgment,
  scoreHeuristicActivation,
  collectEntityContext,
  collectMarketContext,
} = require('./BusinessHeuristicsEngine');

module.exports = {
  HEURISTIC_CATEGORIES,
  OUTCOME_KINDS,
  buildBusinessHeuristic,
  buildActivatedHeuristic,
  buildHeuristicContradiction,
  INITIAL_HEURISTICS,
  cloneHeuristicLibrary,
  getHeuristicById,
  activateHeuristics,
  explainJudgment,
  buildRecommendationFromHeuristics,
  buildBusinessJudgmentReport,
  learnFromOutcome,
  matchHeuristicsForContext,
  detectHeuristicContradictions,
  computeOverallJudgment,
  scoreHeuristicActivation,
  collectEntityContext,
  collectMarketContext,
};
