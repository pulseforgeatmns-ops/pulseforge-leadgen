'use strict';

const { ReasoningEngine, createReasoningEngine } = require('./ReasoningEngine');
const {
  STRATEGY_IDS,
  DEFAULT_STRATEGY_WEIGHTS,
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  PERFORMANCE_TARGET_MS,
  deepFreeze,
  clamp,
  round,
  recommendationId,
  evidenceRef,
} = require('./ReasoningTypes');

module.exports = {
  ReasoningEngine,
  createReasoningEngine,
  STRATEGY_IDS,
  DEFAULT_STRATEGY_WEIGHTS,
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  PERFORMANCE_TARGET_MS,
  deepFreeze,
  clamp,
  round,
  recommendationId,
  evidenceRef,
  ConceptGraph: require('./ConceptGraph'),
};
