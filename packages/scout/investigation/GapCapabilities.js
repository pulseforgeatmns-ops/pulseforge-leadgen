'use strict';

/**
 * Shared gap → capability mappings for investigation planning (SPEC-142/145).
 */

const { EVIDENCE_CAPABILITIES } = require('../intelligence/ProviderCapabilityRegistry');

const GAP_TO_CAPABILITY = Object.freeze({
  decision_maker: EVIDENCE_CAPABILITIES.PEOPLE,
  portfolio_size: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  cleaning_responsibility: EVIDENCE_CAPABILITIES.WEBSITE,
  contact_path: EVIDENCE_CAPABILITIES.EMAILS,
  buying_signals: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  business_fit: EVIDENCE_CAPABILITIES.WEBSITE,
  geographic_fit: EVIDENCE_CAPABILITIES.BUSINESSES,
  vendor_relationship: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  company_size: EVIDENCE_CAPABILITIES.GROWTH,
  ownership: EVIDENCE_CAPABILITIES.OWNERSHIP,
  current_vendor: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  revenue_estimate: EVIDENCE_CAPABILITIES.GROWTH,
  contract_timing: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  expansion_plans: EVIDENCE_CAPABILITIES.GROWTH,
  office_hours: EVIDENCE_CAPABILITIES.HOURS,
});

const EVIDENCE_TYPE_TO_CAPABILITY = Object.freeze({
  website: EVIDENCE_CAPABILITIES.WEBSITE,
  linkedin: EVIDENCE_CAPABILITIES.PEOPLE,
  contacts: EVIDENCE_CAPABILITIES.CONTACTS,
  people: EVIDENCE_CAPABILITIES.PEOPLE,
  emails: EVIDENCE_CAPABILITIES.EMAILS,
  phone: EVIDENCE_CAPABILITIES.PHONE,
  reviews: EVIDENCE_CAPABILITIES.REVIEWS,
  news: EVIDENCE_CAPABILITIES.NEWS,
  hiring_activity: EVIDENCE_CAPABILITIES.HIRING,
  property_portfolio: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  county_records: EVIDENCE_CAPABILITIES.COUNTY_RECORDS,
  vendor_references: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
});

function resolveCapability(gapOrEvidenceType) {
  const key = String(gapOrEvidenceType || '').toLowerCase();
  if (GAP_TO_CAPABILITY[key]) return GAP_TO_CAPABILITY[key];
  if (EVIDENCE_TYPE_TO_CAPABILITY[key]) return EVIDENCE_TYPE_TO_CAPABILITY[key];
  return key;
}

module.exports = {
  GAP_TO_CAPABILITY,
  EVIDENCE_TYPE_TO_CAPABILITY,
  resolveCapability,
};
