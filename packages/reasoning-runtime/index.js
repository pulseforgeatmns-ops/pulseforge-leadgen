'use strict';

const {
  ReasoningRuntime,
  createReasoningRuntime,
  DEFAULT_PERFORMANCE_TARGET_MS,
} = require('./ReasoningRuntime');
const {
  assertStrategyPack,
  REQUIRED_METHODS,
} = require('./interfaces/StrategyPack');
const { assertContextProvider } = require('./interfaces/ContextProvider');
const {
  assertRecommendationProvider,
} = require('./interfaces/RecommendationProvider');
const {
  CRMStrategyPack,
  createCRMStrategyPack,
} = require('./packs/CRMStrategyPack');
const {
  CRMContextProvider,
  createCRMContextProvider,
} = require('./providers/CRMContextProvider');
const {
  NextBestActionProvider,
  createNextBestActionProvider,
} = require('./providers/NextBestActionProvider');

module.exports = {
  ReasoningRuntime,
  createReasoningRuntime,
  DEFAULT_PERFORMANCE_TARGET_MS,
  assertStrategyPack,
  REQUIRED_METHODS,
  assertContextProvider,
  assertRecommendationProvider,
  CRMStrategyPack,
  createCRMStrategyPack,
  CRMContextProvider,
  createCRMContextProvider,
  NextBestActionProvider,
  createNextBestActionProvider,
};
