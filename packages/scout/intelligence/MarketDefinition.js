'use strict';

/**
 * SPEC-158 — Market Definition & Hypothesis Engine (Scout brain).
 * Converts operator intent into a structured semantic market model before investigation.
 *
 * Invariant: every investigation begins from a Market Definition, not operator wording.
 */

const { asText } = require('../../max/scoutAcquisition/Types');
const { normalizeSegmentKey } = require('../coverage/ConceptLibrary');
const {
  inferTargetSegmentFromObjective,
  segmentToSearchKey,
} = require('../../acquisition-mission/MissionNaming');

/** ADR-093 / SPEC-178 — resolution sources in precedence order. */
const SEGMENT_RESOLUTION_SOURCES = Object.freeze([
  'operator_objective',
  'mission_objective',
  'evidence',
  'mission_constraints',
  'defaults',
]);

/**
 * @typedef {object} MarketDefinition
 * @property {string} market
 * @property {string|null} geography
 * @property {string[]} customerTypes
 * @property {string[]} decisionMakers
 * @property {string[]} businessModels
 * @property {string[]} terminology
 * @property {string[]} adjacentMarkets
 * @property {string[]} exclusions
 * @property {string[]} buyingSignals
 * @property {string[]} expectedEvidence
 * @property {string|null} [operatorSegment]
 * @property {string[]} [segments]
 * @property {object|null} [revisionHistory]
 */

const MARKET_SEMANTIC_MODELS = Object.freeze({
  short_term_rental: Object.freeze({
    market: 'Short-Term Rental Operations',
    customerTypes: Object.freeze([
      'Airbnb Hosts',
      'Vacation Rental Managers',
      'Property Managers',
      'Boutique Hospitality',
      'Executive Housing',
    ]),
    decisionMakers: Object.freeze([
      'Owner',
      'Operations Manager',
      'Property Manager',
      'Hospitality Director',
    ]),
    businessModels: Object.freeze(['Self-managed', 'Agency-managed', 'Management Company']),
    terminology: Object.freeze([
      'Airbnb',
      'Vacation Rental',
      'STR',
      'Holiday Rental',
      'Executive Stay',
      'Guest Accommodation',
      'Vacation Property Manager',
      'Hospitality Operator',
    ]),
    adjacentMarkets: Object.freeze([
      'Property Management',
      'Corporate Housing',
      'Real Estate Investors',
    ]),
    exclusions: Object.freeze(['Hotels', 'Long-term apartments', 'Residential homeowners']),
    buyingSignals: Object.freeze([
      'Growing inventory',
      'Hiring cleaners',
      'Recent reviews',
      'New listings',
      'Expansion',
    ]),
    expectedEvidence: Object.freeze([
      'website',
      'listing_count',
      'property_portfolio',
      'reviews',
      'hiring_activity',
    ]),
  }),
  property_management: Object.freeze({
    market: 'Property Management Operations',
    customerTypes: Object.freeze([
      'Residential Property Managers',
      'Commercial Property Managers',
      'Multi-family Operators',
    ]),
    decisionMakers: Object.freeze(['Owner', 'Property Manager', 'Operations Director']),
    businessModels: Object.freeze(['Independent PM', 'Regional PM Company', 'National PM Brand']),
    terminology: Object.freeze([
      'Property Management',
      'Property Manager',
      'Commercial Property Manager',
      'Residential Property Manager',
    ]),
    adjacentMarkets: Object.freeze(['Short-Term Rental Operations', 'Real Estate Investors']),
    exclusions: Object.freeze(['Individual landlords', 'HOA-only managers']),
    buyingSignals: Object.freeze(['Portfolio growth', 'New acquisitions', 'Hiring maintenance staff']),
    expectedEvidence: Object.freeze(['website', 'property_portfolio', 'reviews', 'contacts']),
  }),
  law_firm: Object.freeze({
    market: 'Legal Practice Operations',
    customerTypes: Object.freeze(['Law Firms', 'Legal Practices', 'Attorney Offices']),
    decisionMakers: Object.freeze(['Managing Partner', 'Office Manager', 'Firm Administrator']),
    businessModels: Object.freeze(['Solo practice', 'Small firm', 'Mid-size firm']),
    terminology: Object.freeze(['Law Firm', 'Attorney', 'Legal Office', 'Law Practice']),
    adjacentMarkets: Object.freeze(['Accounting Firms', 'Professional Services']),
    exclusions: Object.freeze(['Multi-tenant towers', 'National law firms']),
    buyingSignals: Object.freeze(['Office expansion', 'New partner', 'Hiring admin staff']),
    expectedEvidence: Object.freeze(['website', 'address', 'company_size', 'contacts']),
  }),
  accounting: Object.freeze({
    market: 'Accounting Practice Operations',
    customerTypes: Object.freeze(['CPA Firms', 'Accounting Practices', 'Bookkeeping Services']),
    decisionMakers: Object.freeze(['Managing Partner', 'Office Manager', 'Firm Owner']),
    businessModels: Object.freeze(['Solo CPA', 'Small practice', 'Regional firm']),
    terminology: Object.freeze(['Accounting Firm', 'CPA', 'Certified Public Accountant', 'Bookkeeping']),
    adjacentMarkets: Object.freeze(['Law Firms', 'Professional Services']),
    exclusions: Object.freeze(['National accounting chains', 'Virtual-only firms']),
    buyingSignals: Object.freeze(['Office move', 'Staff hiring', 'Client growth']),
    expectedEvidence: Object.freeze(['website', 'address', 'company_size', 'contacts']),
  }),
  commercial_cleaning: Object.freeze({
    market: 'Commercial Facility Operations',
    customerTypes: Object.freeze(['Office Operators', 'Facility Managers', 'Building Owners']),
    decisionMakers: Object.freeze(['Facility Manager', 'Office Manager', 'Owner']),
    businessModels: Object.freeze(['Single-tenant office', 'Managed building', 'Owner-occupied']),
    terminology: Object.freeze(['Commercial Cleaning', 'Janitorial Services', 'Office Cleaning', 'Facility Cleaning']),
    adjacentMarkets: Object.freeze(['Property Management', 'Facility Management']),
    exclusions: Object.freeze(['Residential cleaning only', 'Industrial manufacturing']),
    buyingSignals: Object.freeze(['New lease', 'Renovation', 'Staff complaints about cleanliness']),
    expectedEvidence: Object.freeze(['website', 'address', 'company_size', 'contacts']),
  }),
});

const DEFAULT_SEMANTIC_MODEL = Object.freeze({
  market: 'Target Market Operations',
  customerTypes: Object.freeze([]),
  decisionMakers: Object.freeze(['Owner', 'Manager']),
  businessModels: Object.freeze([]),
  terminology: Object.freeze([]),
  adjacentMarkets: Object.freeze([]),
  exclusions: Object.freeze([]),
  buyingSignals: Object.freeze(['Growth', 'Hiring', 'Expansion']),
  expectedEvidence: Object.freeze(['website', 'contacts', 'business_fit']),
});

function inferSegmentKeyFromText(text) {
  const hay = asText(text);
  if (!hay) return null;
  const fromLabel = inferTargetSegmentFromObjective(hay);
  if (fromLabel) return segmentToSearchKey(fromLabel);
  if (/short.term.rental|\bstr\b|vacation rental|airbnb|vrbo|holiday rental/.test(hay.toLowerCase())) {
    return 'short_term_rental';
  }
  if (/property manag/.test(hay.toLowerCase())) return 'property_management';
  if (/law firm|attorney|legal office/.test(hay.toLowerCase())) return 'law_firm';
  if (/accounting|cpa\b|bookkeeping/.test(hay.toLowerCase())) return 'accounting';
  if (/commercial cleaning|janitorial|office cleaning/.test(hay.toLowerCase())) return 'commercial_cleaning';
  return null;
}

function normalizeKnownSegmentKey(value) {
  const key = normalizeSegmentKey(value);
  if (key && MARKET_SEMANTIC_MODELS[key]) return key;
  const inferred = inferSegmentKeyFromText(value);
  if (inferred && MARKET_SEMANTIC_MODELS[inferred]) return inferred;
  return key || null;
}

function missionPlanFromInput(mission = {}) {
  return (
    mission.structuredMission ||
    (mission.plan && mission.plan.missionPlan) ||
    mission.missionPlan ||
    {}
  );
}

function constraintVertical(mission = {}) {
  const constraints = mission.constraints;
  if (!constraints || Array.isArray(constraints)) return null;
  return asText(constraints.vertical) || asText(constraints.industry) || null;
}

/**
 * ADR-093 — Canonical segment resolution.
 * Operator objective is authoritative; mission constraints refine but never redefine.
 *
 * Resolution order:
 *   1. Operator objective
 *   2. Mission objective (locked plan segment)
 *   3. Evidence (dominant terminology / evidence-driven segment)
 *   4. Mission constraints (vertical/industry — refinement only when higher layers silent)
 *   5. Defaults (supplied segments, delegation hints)
 *
 * @param {object} input
 * @returns {{ segmentKey: string, source: string }}
 */
function resolveCanonicalSegmentKey(input = {}) {
  const mission = input.mission || {};
  const plan = missionPlanFromInput(mission);
  const market = plan.market || {};

  const operatorTexts = [
    input.operatorObjective,
    mission.objectiveText,
    mission.objective,
    input.operatorDirection,
    mission.title,
  ].filter(Boolean);

  for (const text of operatorTexts) {
    const key = inferSegmentKeyFromText(text);
    if (key && MARKET_SEMANTIC_MODELS[key]) {
      return { segmentKey: key, source: 'operator_objective' };
    }
  }

  const missionSegment = asText(input.missionSegment) || asText(market.segment);
  if (missionSegment) {
    const key = normalizeKnownSegmentKey(missionSegment);
    if (key && MARKET_SEMANTIC_MODELS[key]) {
      return { segmentKey: key, source: 'mission_objective' };
    }
  }

  const evidenceTexts = [input.dominantTerminology, input.evidenceSegment].filter(Boolean);
  for (const text of evidenceTexts) {
    const key = inferSegmentKeyFromText(text) || normalizeKnownSegmentKey(text);
    if (key && MARKET_SEMANTIC_MODELS[key]) {
      return { segmentKey: key, source: 'evidence' };
    }
  }

  const vertical = asText(input.constraintVertical) || constraintVertical(mission);
  if (vertical) {
    const key = normalizeKnownSegmentKey(vertical);
    if (key && MARKET_SEMANTIC_MODELS[key]) {
      return { segmentKey: key, source: 'mission_constraints' };
    }
  }

  const supplied = Array.isArray(input.segments) ? input.segments : [];
  for (const segment of supplied) {
    const key = normalizeKnownSegmentKey(segment);
    if (key && MARKET_SEMANTIC_MODELS[key]) {
      return { segmentKey: key, source: 'defaults' };
    }
  }

  const fallback = supplied.length ? normalizeSegmentKey(supplied[0]) : '';
  return { segmentKey: fallback || 'general', source: 'defaults' };
}

/**
 * @deprecated Prefer resolveCanonicalSegmentKey — kept for callers that pass segments + mission only.
 */
function resolveSegmentKey(segments = [], mission = {}) {
  return resolveCanonicalSegmentKey({ mission, segments }).segmentKey;
}

function cloneList(value) {
  return Array.isArray(value) ? value.slice() : [];
}

/**
 * Build the canonical semantic Market Definition from mission context.
 * @param {object} input
 * @returns {MarketDefinition}
 */
function buildSemanticMarketDefinition(input = {}) {
  const mission = input.mission || {};
  const canonical =
    input.segmentKey && MARKET_SEMANTIC_MODELS[input.segmentKey]
      ? { segmentKey: input.segmentKey, source: input.segmentSource || 'mission_objective' }
      : resolveCanonicalSegmentKey({
          mission,
          segments: input.segments,
          operatorObjective: input.operatorObjective,
          operatorDirection: input.operatorDirection,
          missionSegment: input.missionSegment,
          dominantTerminology: input.dominantTerminology,
          evidenceSegment: input.evidenceSegment,
          constraintVertical: input.constraintVertical,
        });
  const segmentKey = canonical.segmentKey;
  const segments = Array.isArray(input.segments)
    ? input.segments.filter((s) => s != null && s !== '')
    : [segmentKey];
  const template = MARKET_SEMANTIC_MODELS[segmentKey] || DEFAULT_SEMANTIC_MODEL;

  const operatorSegment =
    asText(input.operatorSegment) ||
    asText(segments[0] && String(segments[0]).replace(/_/g, ' ')) ||
    null;

  const geography =
    asText(input.geography) ||
    asText(mission.constraints && mission.constraints.locationHint) ||
    null;

  const terminology = cloneList(template.terminology);
  if (operatorSegment && !terminology.some((t) => t.toLowerCase() === operatorSegment.toLowerCase())) {
    terminology.unshift(operatorSegment);
  }

  const exclusions = cloneList(template.exclusions);
  const searchExclusions = input.searchDefinition && input.searchDefinition.exclusions;
  if (Array.isArray(searchExclusions)) {
    for (const item of searchExclusions) {
      const text = asText(item);
      if (text && !exclusions.includes(text)) exclusions.push(text);
    }
  }

  return {
    market: template.market,
    geography,
    customerTypes: cloneList(template.customerTypes),
    decisionMakers: cloneList(template.decisionMakers),
    businessModels: cloneList(template.businessModels),
    terminology,
    adjacentMarkets: cloneList(template.adjacentMarkets),
    exclusions,
    buyingSignals: cloneList(template.buyingSignals),
    expectedEvidence: cloneList(template.expectedEvidence),
    operatorSegment,
    segments: [segmentKey],
    segmentKey,
    segmentResolutionSource: canonical.source,
    source: 'semantic_model',
    revisionHistory: [],
  };
}

/**
 * Revise market definition when evidence contradicts original terminology.
 * @param {MarketDefinition} marketDefinition
 * @param {object} context
 * @returns {MarketDefinition}
 */
function reviseMarketDefinition(marketDefinition = {}, context = {}) {
  const revised = {
    ...marketDefinition,
    customerTypes: cloneList(marketDefinition.customerTypes),
    decisionMakers: cloneList(marketDefinition.decisionMakers),
    businessModels: cloneList(marketDefinition.businessModels),
    terminology: cloneList(marketDefinition.terminology),
    adjacentMarkets: cloneList(marketDefinition.adjacentMarkets),
    exclusions: cloneList(marketDefinition.exclusions),
    buyingSignals: cloneList(marketDefinition.buyingSignals),
    expectedEvidence: cloneList(marketDefinition.expectedEvidence),
    segments: cloneList(marketDefinition.segments),
    revisionHistory: cloneList(marketDefinition.revisionHistory),
  };

  const dominantTerminology = asText(context.dominantTerminology);
  const addedTerms = Array.isArray(context.addedTerminology) ? context.addedTerminology : [];
  const removedTerms = Array.isArray(context.removedTerminology) ? context.removedTerminology : [];
  const reason = asText(context.reason) || 'Evidence contradicted original terminology';

  if (dominantTerminology && !revised.terminology.includes(dominantTerminology)) {
    revised.terminology.unshift(dominantTerminology);
  }
  for (const term of addedTerms) {
    const text = asText(term);
    if (text && !revised.terminology.includes(text)) revised.terminology.push(text);
  }
  revised.terminology = revised.terminology.filter((term) => !removedTerms.includes(term));

  if (context.promoteToCustomerType) {
    const ct = asText(context.promoteToCustomerType);
    if (ct && !revised.customerTypes.includes(ct)) revised.customerTypes.unshift(ct);
  }

  revised.revisionHistory.push({
    at: new Date().toISOString(),
    reason,
    dominantTerminology: dominantTerminology || null,
    addedTerminology: addedTerms,
    removedTerminology: removedTerms,
  });
  revised.revised = true;
  revised.source = 'evidence_revision';

  return revised;
}

/**
 * Extract searchable concepts from a semantic market definition.
 * @param {MarketDefinition} marketDefinition
 * @returns {string[]}
 */
function conceptsFromMarketDefinition(marketDefinition = {}) {
  const terms = new Set();
  for (const term of marketDefinition.terminology || []) {
    const text = asText(term);
    if (text) terms.add(text);
  }
  for (const ct of marketDefinition.customerTypes || []) {
    const text = asText(ct);
    if (text) terms.add(text);
  }
  if (!terms.size && marketDefinition.operatorSegment) {
    terms.add(marketDefinition.operatorSegment);
  }
  return [...terms];
}

module.exports = {
  MARKET_SEMANTIC_MODELS,
  SEGMENT_RESOLUTION_SOURCES,
  buildSemanticMarketDefinition,
  reviseMarketDefinition,
  conceptsFromMarketDefinition,
  inferSegmentKeyFromText,
  resolveCanonicalSegmentKey,
  resolveSegmentKey,
};
