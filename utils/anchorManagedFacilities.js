'use strict';

const crypto = require('crypto');

const MANAGED_FACILITIES_COHORT = 'managed_facilities';
const PORTAL_ARM = 'portal_led';
const CONTROL_ARM = 'service_led';
const HIGH_PRIORITY_THRESHOLD = 7;

const VERTICAL_SCORES = Object.freeze({
  property_manager: 2,
  property_management: 2,
  commercial_office: 2,
  dealership: 2,
  auto_dealership: 2,
  school: 2,
  private_school: 2,
  campus: 2,
  college: 2,
  university: 2,
  manufacturer: 2,
  manufacturing: 2,
  warehouse: 2,
  distribution: 2,
});

function text(value) {
  return value == null ? '' : String(value).trim();
}

function boolSignal(record, ...keys) {
  return keys.some((key) => record?.[key] === true || record?.[key] === 'true');
}

function normalizeVertical(value) {
  return text(value).toLowerCase().replace(/[\s-]+/g, '_');
}

function managedFacilitiesScore(record = {}, company = {}) {
  const combined = { ...company, ...record };
  const vertical = normalizeVertical(
    combined.vertical || combined.industry || combined.segment || combined.business_type
  );
  let score = VERTICAL_SCORES[vertical] || 0;

  if (boolSignal(combined, 'multi_location', 'multiLocation', 'multi_site', 'multiSite')
      || Number(combined.location_count || combined.locationCount || 0) > 1) score += 3;

  if (boolSignal(combined, 'large_facility', 'largeFacility', 'meaningful_square_footage', 'facility_complexity')
      || Number(combined.square_footage || combined.squareFootage || 0) >= 10000) score += 3;

  const title = text(combined.contact_title || combined.title || combined.job_title).toLowerCase();
  if (/facilit|operations|property manager|business manager|administrator|general manager|fixed operations/.test(title)) {
    score += 3;
  }

  if (boolSignal(combined, 'recurring_janitorial_need', 'recurringCleaningNeed', 'recurring_cleaning_need')) score += 2;
  if (boolSignal(combined, 'vendor_management_complexity', 'vendorManagementComplexity')) score += 2;
  if (boolSignal(combined, 'growth_signal', 'renovation_signal', 'new_location_signal')) score += 1;

  if (boolSignal(combined, 'tiny_facility', 'tinyFacility', 'simple_facility', 'simpleFacility')) score -= 3;
  if (boolSignal(combined, 'price_only_microbusiness', 'priceSensitiveMicrobusiness')) score -= 3;

  return score;
}

function isManagedFacilitiesCandidate(record = {}, company = {}, threshold = HIGH_PRIORITY_THRESHOLD) {
  return managedFacilitiesScore(record, company) >= Number(threshold || HIGH_PRIORITY_THRESHOLD);
}

function experimentArm(identity = '') {
  const digest = crypto.createHash('sha256').update(String(identity || '')).digest();
  return digest[0] % 2 === 0 ? CONTROL_ARM : PORTAL_ARM;
}

function appendPortalDifferentiator(body = '', cta = '') {
  const paragraph = 'We also give clients a simple portal to see service history, upcoming visits, reports, photos, and flag issues without chasing anyone down.';
  const normalized = text(body);
  if (!normalized || normalized.includes(paragraph)) return body;

  const close = text(cta);
  if (close && normalized.includes(close)) {
    return normalized.replace(close, `${paragraph}\n\n${close}`);
  }
  return `${normalized}\n\n${paragraph}`;
}

function managedFacilitiesExperiment({ prospect = {}, company = {}, candidateId = null } = {}) {
  const score = managedFacilitiesScore(prospect, company);
  if (score < HIGH_PRIORITY_THRESHOLD) {
    return { cohort: null, score, arm: null };
  }
  const identity = candidateId
    || prospect.id
    || prospect.prospect_id
    || prospect.company_id
    || company.id
    || company.domain
    || company.name
    || prospect.company_name
    || '';
  return {
    cohort: MANAGED_FACILITIES_COHORT,
    score,
    arm: experimentArm(identity),
  };
}

module.exports = {
  MANAGED_FACILITIES_COHORT,
  PORTAL_ARM,
  CONTROL_ARM,
  HIGH_PRIORITY_THRESHOLD,
  managedFacilitiesScore,
  isManagedFacilitiesCandidate,
  experimentArm,
  appendPortalDifferentiator,
  managedFacilitiesExperiment,
};
