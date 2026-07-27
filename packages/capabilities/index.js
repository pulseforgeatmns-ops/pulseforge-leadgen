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
  registerBuiltinCapabilities,
  createBuiltinRegistry,
} = require('./builtins/stubs');

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
  registerBuiltinCapabilities,
  createBuiltinRegistry,
  discovery: require('./discovery'),
  ranking: require('./ranking'),
  proposal: require('./proposal'),
  playbook: require('./playbook'),
};

// Re-export production capabilities at top level for convenience
module.exports.createProspectDiscoveryCapability =
  require('./discovery').createProspectDiscoveryCapability;
module.exports.createOpportunityRankingCapability =
  require('./ranking').createOpportunityRankingCapability;
module.exports.createProposalGeneratorCapability =
  require('./proposal').createProposalGeneratorCapability;
