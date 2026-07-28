'use strict';

/**
 * @pulseforge/capabilities — Capability Framework (SPEC-023 / ADR-011)
 *
 * Capabilities are the stable API of Pulseforge. Agents are implementation details.
 */

const {
  CAPABILITY_CATEGORIES,
  CAPABILITY_RESULT_STATUS,
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
} = require('./CapabilityRegistry');
const {
  CapabilityRunner,
  createCapabilityRunner,
} = require('./CapabilityRunner');
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
  registerBuiltinCapabilities,
  createBuiltinRegistry,
} = require('./builtins/stubs');
const {
  CAPABILITY_ARTIFACT_CONTRACTS,
  withArtifactContracts,
} = require('./artifactContracts');

module.exports = {
  CAPABILITY_CATEGORIES,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  buildCapabilityContext,
  CapabilityRegistry,
  createCapabilityRegistry,
  assertCapability,
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
  registerBuiltinCapabilities,
  createBuiltinRegistry,
  CAPABILITY_ARTIFACT_CONTRACTS,
  withArtifactContracts,
  discovery: require('./discovery'),
  ranking: require('./ranking'),
  proposal: require('./proposal'),
  playbook: require('./playbook'),
  signals: require('./signals'),
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
