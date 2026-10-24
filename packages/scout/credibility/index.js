'use strict';

/**
 * SPEC-144 — Scout Intelligence Credibility Framework public exports.
 */

const weights = require('./EvidenceWeights');
const freshness = require('./EvidenceFreshness');
const contradictions = require('./ContradictionAnalysis');
const framework = require('./CredibilityFramework');

module.exports = {
  ...weights,
  ...freshness,
  ...contradictions,
  ...framework,
};
