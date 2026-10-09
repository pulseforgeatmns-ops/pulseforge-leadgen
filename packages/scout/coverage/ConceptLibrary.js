'use strict';

/**
 * SPEC-153 — Discovery concept expansion.
 * Mission target terminology expands into searchable concept variants
 * (e.g. "short-term rental operators" → STR, Vacation Rental, Airbnb Host, …).
 */

const { asText } = require('../../max/scoutAcquisition/Types');
const { founderLedSmallBusinessSearchTerms } = require('../../../utils/canonicalBusinessTaxonomy');

const SEGMENT_CONCEPTS = Object.freeze({
  small_business_owners: Object.freeze(founderLedSmallBusinessSearchTerms()),
  small_business_owner: Object.freeze(founderLedSmallBusinessSearchTerms()),
  founder_led_smb: Object.freeze(founderLedSmallBusinessSearchTerms()),
  founder_led_small_business: Object.freeze(founderLedSmallBusinessSearchTerms()),
  short_term_rental: Object.freeze([
    'STR',
    'Vacation Rental',
    'Airbnb Host',
    'Vacation Property Manager',
    'Property Manager',
    'Hospitality Operator',
  ]),
  str: Object.freeze([
    'STR',
    'Vacation Rental',
    'Airbnb Host',
    'Vacation Property Manager',
    'Property Manager',
    'Hospitality Operator',
  ]),
  property_management: Object.freeze([
    'Property Management',
    'Property Manager',
    'Commercial Property Manager',
    'Residential Property Manager',
  ]),
  law_firm: Object.freeze(['Law Firm', 'Attorney', 'Legal Office', 'Law Practice']),
  accounting: Object.freeze(['Accounting Firm', 'CPA', 'Certified Public Accountant', 'Bookkeeping']),
  commercial_cleaning: Object.freeze([
    'Commercial Cleaning',
    'Janitorial Services',
    'Office Cleaning',
    'Facility Cleaning',
  ]),
  cleaning: Object.freeze([
    'Commercial Cleaning',
    'Janitorial Services',
    'Office Cleaning',
  ]),
  restaurant: Object.freeze(['Restaurant', 'Food Service', 'Catering']),
  restaurant_foh: Object.freeze(['Restaurant', 'Cafe', 'Bistro', 'Bar and Grill']),
  salon: Object.freeze(['Salon', 'Hair Salon', 'Beauty Salon', 'Spa']),
  fitness: Object.freeze(['Gym', 'Fitness Center', 'Personal Training']),
  landscaping: Object.freeze(['Landscaping', 'Lawn Care', 'Grounds Maintenance']),
  home_renovation: Object.freeze(['Home Renovation', 'General Contractor', 'Remodeling']),
  home_services: Object.freeze(['Home Services', 'Handyman', 'Home Repair']),
  med_spa: Object.freeze(['Med Spa', 'Medical Spa', 'Aesthetic Clinic']),
  auto: Object.freeze(['Auto Repair', 'Auto Service', 'Automotive']),
});

const SEGMENT_CONCEPT_ROTATIONS = Object.freeze({
  property_manager: Object.freeze([
    Object.freeze(['Property Management', 'Property Manager']),
    Object.freeze(['HOA Management', 'Condominium Management', 'Apartment Management']),
    Object.freeze(['Residential Property Management', 'Commercial Property Management', 'Rental Management']),
    Object.freeze(['Association Management', 'Multifamily Management', 'Leasing Office']),
  ]),
  property_management: Object.freeze([
    Object.freeze(['Property Management', 'Property Manager']),
    Object.freeze(['HOA Management', 'Condominium Management', 'Apartment Management']),
    Object.freeze(['Residential Property Management', 'Commercial Property Management', 'Rental Management']),
    Object.freeze(['Association Management', 'Multifamily Management', 'Leasing Office']),
  ]),
  str_manager: Object.freeze([
    Object.freeze(['Vacation Rental Management', 'Airbnb Property Management']),
    Object.freeze(['Short Term Rental Manager', 'Vacation Property Manager']),
    Object.freeze(['Airbnb Co-host', 'Vacation Home Management']),
    Object.freeze(['Guest Stay Management', 'Hospitality Property Manager']),
  ]),
  realtor: Object.freeze([
    Object.freeze(['Real Estate Agency', 'Realtor Office']),
    Object.freeze(['Real Estate Brokerage', 'Commercial Real Estate Office']),
    Object.freeze(['Real Estate Broker', 'Residential Brokerage']),
    Object.freeze(['Property Sales Office', 'Local Realty Group']),
  ]),
  commercial_office: Object.freeze([
    Object.freeze(['Commercial Office', 'Office Park', 'Business Center']),
    Object.freeze(['Coworking Space', 'Executive Office Suites']),
    Object.freeze(['Professional Office Building', 'Managed Office Space']),
    Object.freeze(['Corporate Office', 'Business Campus']),
  ]),
  restaurant_foh: Object.freeze([
    Object.freeze(['Restaurant', 'Cafe', 'Bistro']),
    Object.freeze(['Bar and Grill', 'Tavern', 'Gastropub']),
    Object.freeze(['Breakfast Restaurant', 'Family Restaurant', 'Diner']),
    Object.freeze(['Fine Dining Restaurant', 'Independent Restaurant', 'Eatery']),
  ]),
});

function normalizeSegmentKey(value) {
  return asText(value).toLowerCase().replace(/[\s-]+/g, '_');
}

function conceptsFromText(text) {
  if (text == null || text === '') return [];
  const hay = String(text).toLowerCase();
  if (/short.term.rental|\bstr\b|vacation rental|airbnb|vrbo|hospitality operator/.test(hay)) {
    return SEGMENT_CONCEPTS.short_term_rental.slice();
  }
  if (/property manag/.test(hay)) return SEGMENT_CONCEPTS.property_management.slice();
  if (/law firm|attorney|legal office/.test(hay)) return SEGMENT_CONCEPTS.law_firm.slice();
  if (/accounting|cpa\b|bookkeeping/.test(hay)) return SEGMENT_CONCEPTS.accounting.slice();
  if (/commercial cleaning|janitorial|office cleaning/.test(hay)) {
    return SEGMENT_CONCEPTS.commercial_cleaning.slice();
  }
  return [];
}

/**
 * Expand a search definition into executable discovery concepts.
 * When a semantic market definition is provided, terminology drives expansion.
 * @param {object} searchDefinition
 * @param {object} [marketDefinition]
 * @returns {string[]}
 */
function expandConcepts(searchDefinition = {}, marketDefinition = null) {
  if (marketDefinition && Array.isArray(marketDefinition.terminology) && marketDefinition.terminology.length) {
    const fromSemantic = new Set();
    for (const term of marketDefinition.terminology) {
      const text = asText(term);
      if (text) fromSemantic.add(text);
    }
    for (const ct of marketDefinition.customerTypes || []) {
      const text = asText(ct);
      if (text) fromSemantic.add(text);
    }
    if (fromSemantic.size) return [...fromSemantic];
  }

  const concepts = new Set();
  const segments = Array.isArray(searchDefinition.segments) ? searchDefinition.segments : [];
  const businessNeed = normalizeSegmentKey(searchDefinition.businessNeed || '');
  const generation = Math.max(0, Number(searchDefinition.discoveryGeneration || 0));

  for (const segment of segments) {
    const key = normalizeSegmentKey(segment);
    const rotations = SEGMENT_CONCEPT_ROTATIONS[key];
    if (rotations && rotations.length) {
      for (const concept of rotations[generation % rotations.length]) concepts.add(concept);
      continue;
    }
    const mapped = SEGMENT_CONCEPTS[key];
    if (mapped) {
      for (const concept of mapped) concepts.add(concept);
    } else {
      concepts.add(asText(segment).replace(/_/g, ' '));
    }
  }

  if (!segments.length && businessNeed && SEGMENT_CONCEPTS[businessNeed]) {
    for (const concept of SEGMENT_CONCEPTS[businessNeed]) concepts.add(concept);
  }

  const population = asText(searchDefinition.populationStatement);
  for (const concept of conceptsFromText(population)) concepts.add(concept);

  const operatorDirection = asText(searchDefinition.operatorDirection);
  for (const concept of conceptsFromText(operatorDirection)) concepts.add(concept);

  if (!concepts.size) {
    if (segments.length) {
      for (const segment of segments) concepts.add(asText(segment).replace(/_/g, ' '));
    } else if (searchDefinition.businessNeed) {
      concepts.add(asText(searchDefinition.businessNeed).replace(/_/g, ' '));
    } else {
      concepts.add('commercial');
    }
  }

  return [...concepts];
}

module.exports = {
  SEGMENT_CONCEPTS,
  SEGMENT_CONCEPT_ROTATIONS,
  expandConcepts,
  conceptsFromText,
  normalizeSegmentKey,
};
