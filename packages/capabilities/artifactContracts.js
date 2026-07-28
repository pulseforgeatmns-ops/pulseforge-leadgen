'use strict';

/**
 * Capability artifact contracts (SPEC-051 / ADR-035).
 * Mirrors Stage Library consumes/produces — capabilities declare what they
 * require and produce so the Artifact Resolver can treat them as acquisition
 * strategies rather than fixed sequence steps.
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
  [BUILTIN_IDS.SALES_INTELLIGENCE]: Object.freeze({
    requires: ['ranked_prospects', 'company_intelligence'],
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
});

/**
 * Attach requires/produces onto a capability descriptor.
 * @param {object} capability
 * @returns {object}
 */
function withArtifactContracts(capability) {
  if (!capability || !capability.id) return capability;
  const contract = CAPABILITY_ARTIFACT_CONTRACTS[capability.id];
  if (!contract) {
    return {
      ...capability,
      requires: Array.isArray(capability.requires) ? capability.requires : [],
      produces: Array.isArray(capability.produces) ? capability.produces : [],
    };
  }
  return {
    ...capability,
    requires:
      Array.isArray(capability.requires) && capability.requires.length
        ? capability.requires
        : [...contract.requires],
    produces:
      Array.isArray(capability.produces) && capability.produces.length
        ? capability.produces
        : [...contract.produces],
  };
}

module.exports = {
  CAPABILITY_ARTIFACT_CONTRACTS,
  withArtifactContracts,
};
