'use strict';

const {
  estimateCandidateUniverse,
  reviseCandidateUniverseEstimate,
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
  estimateUniverseFromPlan,
  INDUSTRY_DENSITY_RATIOS,
} = require('./CandidateUniverseEstimate');

module.exports = {
  estimateCandidateUniverse,
  reviseCandidateUniverseEstimate,
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
  estimateUniverseFromPlan,
  INDUSTRY_DENSITY_RATIOS,
};
