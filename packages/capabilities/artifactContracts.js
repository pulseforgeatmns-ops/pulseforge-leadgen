'use strict';

/**
 * Capability artifact contracts (SPEC-051 / ADR-035 + SPEC-054 / ADR-038).
 * Mirrors Stage Library consumes/produces — capabilities declare what they
 * require and produce so the Artifact Resolver can treat them as acquisition
 * strategies rather than fixed sequence steps.
 * SPEC-054: also attaches version, enabled, and missionAliases for planner
 * discovery via the Capability Registry.
 */

const { BUILTIN_IDS } = require('./types');

/** @type {Record<string, { requires: string[], produces: string[] }>} */
const CAPABILITY_ARTIFACT_CONTRACTS = Object.freeze({
  [BUILTIN_IDS.PROSPECT_DISCOVERY]: Object.freeze({
    requires: ['discovery_profile'],
    produces: ['prospect_list'],
  }),
  [BUILTIN_IDS.COMPANY_ENRICHMENT]: Object.freeze({
    requires: ['prospect_list'],
    produces: ['enriched_list', 'company_intelligence'],
  }),
  [BUILTIN_IDS.KNOWLEDGE_UPDATE]: Object.freeze({
    requires: [],
    produces: ['knowledge'],
  }),
  [BUILTIN_IDS.OPPORTUNITY_RANKING]: Object.freeze({
    requires: ['prospect_list', 'company_intelligence'],
    produces: ['ranked_prospects'],
  }),
  [BUILTIN_IDS.BUSINESS_INTELLIGENCE]: Object.freeze({
    requires: ['ranked_prospects'],
    produces: ['business_intelligence_profile'],
  }),
  [BUILTIN_IDS.SALES_INTELLIGENCE]: Object.freeze({
    requires: ['ranked_prospects', 'business_intelligence_profile'],
    produces: ['sales_intelligence_profile'],
  }),
  [BUILTIN_IDS.CAMPAIGN_BUILDER]: Object.freeze({
    requires: ['ranked_prospects', 'sales_intelligence_profile'],
    produces: ['campaign'],
  }),
  [BUILTIN_IDS.MAIL_PACKAGE_GENERATOR]: Object.freeze({
    requires: ['campaign', 'sales_intelligence_profile'],
    produces: ['mail_packages'],
  }),
  [BUILTIN_IDS.CAMPAIGN_REVIEW]: Object.freeze({
    requires: ['campaign', 'mail_packages'],
    produces: ['approved_campaign', 'review_decision'],
  }),
  [BUILTIN_IDS.DIRECT_MAIL_EXECUTION]: Object.freeze({
    requires: ['approved_campaign', 'mail_packages'],
    produces: ['execution_package', 'delivery_results'],
  }),
  [BUILTIN_IDS.OUTCOME_INTELLIGENCE]: Object.freeze({
    requires: ['execution_record'],
    produces: ['outcome_summary'],
  }),
  [BUILTIN_IDS.OPERATOR_INBOX]: Object.freeze({
    requires: [],
    produces: ['inbox_items'],
  }),
  [BUILTIN_IDS.PROPOSAL_GENERATOR]: Object.freeze({
    requires: [],
    produces: ['proposal'],
  }),
  [BUILTIN_IDS.DISCOVERY_DIAGNOSTICS]: Object.freeze({
    requires: [],
    produces: [
      'DiscoveryExecution',
      'DiscoveryTrace',
      'DiscoveryDiagnostics',
      'ProviderSelection',
      'CandidateCounts',
      'VerificationResults',
      'Exceptions',
      'CapabilityExecution',
      'CapabilityFailure',
      'MissionDiagnostics',
    ],
  }),
});

/**
 * Operator / mission-text aliases for registry resolution (SPEC-054).
 * @type {Record<string, string[]>}
 */
const CAPABILITY_MISSION_ALIASES = Object.freeze({
  [BUILTIN_IDS.PROSPECT_DISCOVERY]: Object.freeze([
    'Prospect Discovery',
    'Discover Prospects',
    'Find Prospects',
  ]),
  [BUILTIN_IDS.COMPANY_ENRICHMENT]: Object.freeze([
    'Company Enrichment',
    'Enrich Companies',
  ]),
  [BUILTIN_IDS.KNOWLEDGE_UPDATE]: Object.freeze([
    'Knowledge Update',
    'Update Knowledge',
  ]),
  [BUILTIN_IDS.OPPORTUNITY_RANKING]: Object.freeze([
    'Opportunity Ranking',
    'Rank Opportunities',
  ]),
  [BUILTIN_IDS.BUSINESS_INTELLIGENCE]: Object.freeze([
    'Business Intelligence',
    'Build Business Intelligence',
    'Analyze Business',
    'Company Intelligence',
    'Analyze Company',
  ]),
  [BUILTIN_IDS.SALES_INTELLIGENCE]: Object.freeze([
    'Sales Intelligence',
    'Build Sales Intelligence',
  ]),
  [BUILTIN_IDS.CAMPAIGN_BUILDER]: Object.freeze([
    'Campaign Builder',
    'Build Campaign',
    'Create Campaign',
    'Campaign Execution',
    'Execute Campaign',
  ]),
  [BUILTIN_IDS.MAIL_PACKAGE_GENERATOR]: Object.freeze([
    'Mail Package Generator',
    'Mail Package',
    'Mail Packages',
    'Generate Mail Package',
    'Generate Mail Packages',
    'Direct Mail Campaign',
  ]),
  [BUILTIN_IDS.CAMPAIGN_REVIEW]: Object.freeze([
    'Campaign Review',
    'Review Campaign',
  ]),
  [BUILTIN_IDS.DIRECT_MAIL_EXECUTION]: Object.freeze([
    'Direct Mail Execution',
    'Execute Direct Mail',
  ]),
  [BUILTIN_IDS.OUTCOME_INTELLIGENCE]: Object.freeze([
    'Outcome Intelligence',
  ]),
  [BUILTIN_IDS.OPERATOR_INBOX]: Object.freeze(['Operator Inbox']),
  [BUILTIN_IDS.PROPOSAL_GENERATOR]: Object.freeze([
    'Proposal Generator',
    'Generate Proposal',
  ]),
  [BUILTIN_IDS.DISCOVERY_DIAGNOSTICS]: Object.freeze([
    'Discovery Diagnostics',
    'Diagnose Discovery',
    'Discovery Diagnostic',
  ]),
});

/** Default planner contract version when capability omits `version`. */
const DEFAULT_CAPABILITY_VERSION = 1;

/**
 * Attach requires/produces + SPEC-054 planner fields onto a capability descriptor.
 * @param {object} capability
 * @returns {object}
 */
function withArtifactContracts(capability) {
  if (!capability || !capability.id) return capability;
  const contract = CAPABILITY_ARTIFACT_CONTRACTS[capability.id];
  const aliases = CAPABILITY_MISSION_ALIASES[capability.id] || [];
  const requires =
    Array.isArray(capability.requires) && capability.requires.length
      ? capability.requires
      : contract
        ? [...contract.requires]
        : [];
  const produces =
    Array.isArray(capability.produces) && capability.produces.length
      ? capability.produces
      : contract
        ? [...contract.produces]
        : [];
  const missionAliases =
    Array.isArray(capability.missionAliases) && capability.missionAliases.length
      ? capability.missionAliases.map(String)
      : [...aliases];
  const version =
    capability.version != null
      ? Number(capability.version) || DEFAULT_CAPABILITY_VERSION
      : DEFAULT_CAPABILITY_VERSION;
  const enabled = capability.enabled == null ? true : Boolean(capability.enabled);

  return {
    ...capability,
    requires,
    produces,
    // SPEC-054 / ADR-038 — planner-facing contract
    consumes: Array.isArray(capability.consumes)
      ? capability.consumes
      : [...requires],
    version,
    enabled,
    missionAliases,
  };
}

module.exports = {
  CAPABILITY_ARTIFACT_CONTRACTS,
  CAPABILITY_MISSION_ALIASES,
  DEFAULT_CAPABILITY_VERSION,
  withArtifactContracts,
};
