'use strict';

/**
 * SPEC-153 — Discovery concept expansion.
 * Mission target terminology expands into searchable concept variants
 * (e.g. "short-term rental operators" → STR, Vacation Rental, Airbnb Host, …).
 */

const { asText } = require('../../max/scoutAcquisition/Types');

const SEGMENT_CONCEPTS = Object.freeze({
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
  salon: Object.freeze(['Salon', 'Hair Salon', 'Beauty Salon', 'Spa']),
  fitness: Object.freeze(['Gym', 'Fitness Center', 'Personal Training']),
  landscaping: Object.freeze(['Landscaping', 'Lawn Care', 'Grounds Maintenance']),
  home_renovation: Object.freeze(['Home Renovation', 'General Contractor', 'Remodeling']),
  home_services: Object.freeze(['Home Services', 'Handyman', 'Home Repair']),
  med_spa: Object.freeze(['Med Spa', 'Medical Spa', 'Aesthetic Clinic']),
  auto: Object.freeze(['Auto Repair', 'Auto Service', 'Automotive']),
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
 * @param {object} searchDefinition
 * @returns {string[]}
 */
function expandConcepts(searchDefinition = {}) {
  const concepts = new Set();
  const segments = Array.isArray(searchDefinition.segments) ? searchDefinition.segments : [];
  const businessNeed = normalizeSegmentKey(searchDefinition.businessNeed || '');

  for (const segment of segments) {
    const key = normalizeSegmentKey(segment);
    const mapped = SEGMENT_CONCEPTS[key];
    if (mapped) {
      for (const concept of mapped) concepts.add(concept);
    } else {
      concepts.add(asText(segment).replace(/_/g, ' '));
    }
  }

  if (businessNeed && SEGMENT_CONCEPTS[businessNeed]) {
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
  expandConcepts,
  conceptsFromText,
  normalizeSegmentKey,
};
