'use strict';

/**
 * SPEC-177 — Coverage module exports.
 */

module.exports = {
  ...require('./EvidenceRequirements'),
  ...require('./EvidenceProviderAssignment'),
  ...require('./ProviderEvidenceContract'),
  ...require('./ProviderExecution'),
  ...require('./HypothesisInvestigationPlanner'),
  ...require('./HypothesisDrivenDiscoveryEngine'),
  executeHypothesisDrivenCoverage: require('./HypothesisDrivenDiscovery').executeHypothesisDrivenCoverage,
};
