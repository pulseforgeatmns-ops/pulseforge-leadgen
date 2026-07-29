'use strict';

/**
 * @pulseforge/capabilities — Capability Framework (SPEC-023 / ADR-011)
 *
 * Capabilities are the stable API of Pulseforge. Agents are implementation details.
 */

const {
  CAPABILITY_CATEGORIES,
  CAPABILITY_RESULT_STATUS,
  CAPABILITY_EXECUTION_MODES,
  PROGRESS_KINDS,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  buildCapabilityContext,
} = require('./types');
const {
  CapabilityRegistry,
  createCapabilityRegistry,
  assertCapability,
  formatMissingCapabilityError,
  normalizeArtifactKey,
  DEFAULT_ACQUISITION_COST,
} = require('./CapabilityRegistry');
const {
  CapabilityRunner,
  createCapabilityRunner,
} = require('./CapabilityRunner');
const {
  resolveCapabilityExecutionMode,
  normalizeDiagnoseCanRun,
  buildPreconditionBlockedResult,
} = require('./executionMode');
const {
  createProspectDiscoveryStub,
  createCompanyEnrichmentStub,
  createKnowledgeUpdateStub,
  createOpportunityRankingStub,
  createCampaignBuilderStub,
  createProposalGeneratorCapability,
  createMailPackageGeneratorCapability,
  createCampaignReviewCapability,
  createDirectMailExecutionCapability,
  createOutcomeIntelligenceCapability,
  createOperatorInboxCapability,
  createDiscoveryDiagnosticsCapability,
  registerBuiltinCapabilities,
  createBuiltinRegistry,
} = require('./builtins/stubs');
const {
  CAPABILITY_ARTIFACT_CONTRACTS,
  CAPABILITY_MISSION_ALIASES,
  DEFAULT_CAPABILITY_VERSION,
  withArtifactContracts,
} = require('./artifactContracts');

module.exports = {
  CAPABILITY_CATEGORIES,
  CAPABILITY_RESULT_STATUS,
  CAPABILITY_EXECUTION_MODES,
  PROGRESS_KINDS,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  buildCapabilityContext,
  resolveCapabilityExecutionMode,
  normalizeDiagnoseCanRun,
  buildPreconditionBlockedResult,
  CapabilityRegistry,
  createCapabilityRegistry,
  assertCapability,
  formatMissingCapabilityError,
  normalizeArtifactKey,
  DEFAULT_ACQUISITION_COST,
  CapabilityRunner,
  createCapabilityRunner,
  createProspectDiscoveryStub,
  createCompanyEnrichmentStub,
  createKnowledgeUpdateStub,
  createOpportunityRankingStub,
  createCampaignBuilderStub,
  createProposalGeneratorCapability,
  createMailPackageGeneratorCapability,
  createCampaignReviewCapability,
  createDirectMailExecutionCapability,
  createOutcomeIntelligenceCapability,
  createOperatorInboxCapability,
  createDiscoveryDiagnosticsCapability,
  registerBuiltinCapabilities,
  createBuiltinRegistry,
  CAPABILITY_ARTIFACT_CONTRACTS,
  CAPABILITY_MISSION_ALIASES,
  DEFAULT_CAPABILITY_VERSION,
  withArtifactContracts,
  discovery: require('./discovery'),
  ranking: require('./ranking'),
  proposal: require('./proposal'),
  playbook: require('./playbook'),
  signals: require('./signals'),
  businessIntelligence: require('./businessIntelligence'),
  salesIntelligence: require('./salesIntelligence'),
  mail: require('./mail'),
  campaignReview: require('./campaignReview'),
  directMailExecution: require('./directMailExecution'),
  outcomeIntelligence: require('./outcomeIntelligence'),
  operatorInbox: require('./operatorInbox'),
};

// Re-export production capabilities at top level for convenience
module.exports.createProspectDiscoveryCapability =
  require('./discovery').createProspectDiscoveryCapability;
module.exports.createOpportunityRankingCapability =
  require('./ranking').createOpportunityRankingCapability;
module.exports.createBusinessIntelligenceCapability =
  require('./businessIntelligence').createBusinessIntelligenceCapability;
module.exports.createSalesIntelligenceCapability =
  require('./salesIntelligence').createSalesIntelligenceCapability;
module.exports.createProposalGeneratorCapability =
  require('./proposal').createProposalGeneratorCapability;
module.exports.createMailPackageGeneratorCapability =
  require('./mail').createMailPackageGeneratorCapability;
module.exports.createCampaignReviewCapability =
  require('./campaignReview').createCampaignReviewCapability;
module.exports.createDirectMailExecutionCapability =
  require('./directMailExecution').createDirectMailExecutionCapability;
module.exports.createOutcomeIntelligenceCapability =
  require('./outcomeIntelligence').createOutcomeIntelligenceCapability;
module.exports.createOperatorInboxCapability =
  require('./operatorInbox').createOperatorInboxCapability;
module.exports.buildBusinessSignalsForProspect =
  require('./signals').buildBusinessSignalsForProspect;
module.exports.buildBusinessSignalsStage =
  require('./signals').buildBusinessSignalsStage;
module.exports.resolveActiveSignals = require('./signals').resolveActiveSignals;
