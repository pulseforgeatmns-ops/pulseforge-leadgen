'use strict';

/**
 * SPEC-141 Stage 2 — Evidence Planning.
 * Decide what evidence must exist before recommendations are trustworthy.
 * Nothing is searched yet.
 */

const { EVIDENCE_REQUIREMENTS } = require('./types');
const { EVIDENCE_CAPABILITIES } = require('./ProviderCapabilityRegistry');

const REQUIREMENT_TO_CAPABILITY = Object.freeze({
  [EVIDENCE_REQUIREMENTS.CANDIDATE_UNIVERSE]: EVIDENCE_CAPABILITIES.BUSINESSES,
  [EVIDENCE_REQUIREMENTS.DECISION_MAKERS]: EVIDENCE_CAPABILITIES.PEOPLE,
  [EVIDENCE_REQUIREMENTS.PROPERTY_COUNT]: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  [EVIDENCE_REQUIREMENTS.CONTACT_PATH]: EVIDENCE_CAPABILITIES.EMAILS,
  [EVIDENCE_REQUIREMENTS.BUYING_SIGNALS]: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  [EVIDENCE_REQUIREMENTS.BUSINESS_MATURITY]: EVIDENCE_CAPABILITIES.WEBSITE,
  [EVIDENCE_REQUIREMENTS.EXISTING_VENDORS]: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  [EVIDENCE_REQUIREMENTS.CLEANING_RESPONSIBILITY]: EVIDENCE_CAPABILITIES.WEBSITE,
  [EVIDENCE_REQUIREMENTS.GEOGRAPHIC_COVERAGE]: EVIDENCE_CAPABILITIES.BUSINESSES,
});

const DEFAULT_REQUIRED = Object.freeze([
  EVIDENCE_REQUIREMENTS.CANDIDATE_UNIVERSE,
  EVIDENCE_REQUIREMENTS.DECISION_MAKERS,
  EVIDENCE_REQUIREMENTS.CONTACT_PATH,
  EVIDENCE_REQUIREMENTS.BUYING_SIGNALS,
  EVIDENCE_REQUIREMENTS.GEOGRAPHIC_COVERAGE,
]);

const SEGMENT_EXTRA_REQUIREMENTS = Object.freeze({
  property_management: [
    EVIDENCE_REQUIREMENTS.PROPERTY_COUNT,
    EVIDENCE_REQUIREMENTS.CLEANING_RESPONSIBILITY,
    EVIDENCE_REQUIREMENTS.EXISTING_VENDORS,
  ],
  law_firm: [EVIDENCE_REQUIREMENTS.BUSINESS_MATURITY],
  short_term_rental: [
    EVIDENCE_REQUIREMENTS.PROPERTY_COUNT,
    EVIDENCE_REQUIREMENTS.CLEANING_RESPONSIBILITY,
  ],
  hospitality: [EVIDENCE_REQUIREMENTS.PROPERTY_COUNT],
});

/**
 * Build evidence plan from market definition.
 * @param {object} marketDefinition
 * @param {object} [opts]
 * @returns {object}
 */
function buildEvidencePlan(marketDefinition, opts = {}) {
  const required = new Set(DEFAULT_REQUIRED);
  const segments = marketDefinition.segments || [];

  for (const segment of segments) {
    const key = String(segment).toLowerCase().replace(/\s+/g, '_');
    for (const extra of SEGMENT_EXTRA_REQUIREMENTS[key] || []) {
      required.add(extra);
    }
  }

  if (opts.additionalRequirements) {
    for (const req of opts.additionalRequirements) required.add(req);
  }

  const requiredList = [...required];
  const capabilities = requiredList.map(
    (req) => REQUIREMENT_TO_CAPABILITY[req] || req
  );

  return {
    required: requiredList,
    capabilities: [...new Set(capabilities)],
    requirementCapabilities: requiredList.map((req) => ({
      requirement: req,
      capability: REQUIREMENT_TO_CAPABILITY[req] || req,
    })),
    satisfied: [],
    missing: requiredList.slice(),
    blocked: false,
    rationale:
      'Evidence plan derived from market segment and mission goal. No search executed yet.',
  };
}

module.exports = {
  DEFAULT_REQUIRED,
  SEGMENT_EXTRA_REQUIREMENTS,
  REQUIREMENT_TO_CAPABILITY,
  buildEvidencePlan,
};
