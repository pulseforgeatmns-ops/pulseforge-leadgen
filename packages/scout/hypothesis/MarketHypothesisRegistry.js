'use strict';

/**
 * ADR-092 — Market Hypothesis Registry.
 *
 * Vertical keys (e.g. property_manager) are market hypotheses — not search terms.
 * Each hypothesis expands into multiple search strategies across independent sources.
 */

const { asText } = require('../../max/scoutAcquisition/Types');

const SEARCH_SOURCES = Object.freeze({
  GOOGLE_PLACES: 'google_places',
  PUBLIC_BUSINESS_DATA: 'public_business_data',
  BUSINESS_REGISTRY: 'business_registry',
  LINKEDIN: 'linkedin',
  FACEBOOK: 'facebook',
  BROKERAGE_SITES: 'brokerage_sites',
  LOCAL_CHAMBER: 'local_chamber',
  INDUSTRY_ASSOCIATION: 'industry_association',
});

/**
 * @typedef {object} SearchStrategy
 * @property {string} source
 * @property {number} priority
 * @property {string[]} queryTemplates
 * @property {string} [rationale]
 */

/**
 * @typedef {object} MarketHypothesis
 * @property {string} id
 * @property {string} statement
 * @property {string} buyerRole
 * @property {string} [segmentKey]
 * @property {SearchStrategy[]} searchStrategies
 */

const MARKET_HYPOTHESES = Object.freeze({
  cleaning_company_overflow: Object.freeze({
    id: 'cleaning_company_overflow',
    statement: 'Commercial cleaning companies with overflow work are likely referral partners.',
    buyerRole: 'referral_partner',
    segmentKey: 'commercial_cleaning',
    searchStrategies: Object.freeze([
      Object.freeze({
        source: SEARCH_SOURCES.GOOGLE_PLACES,
        priority: 1,
        queryTemplates: Object.freeze([
          'commercial cleaning company {city} {state}',
          'janitorial service {city} {state}',
          'office cleaning company {city} {state}',
        ]),
        rationale: 'Local operators list on Google Business Profile.',
      }),
      Object.freeze({
        source: SEARCH_SOURCES.PUBLIC_BUSINESS_DATA,
        priority: 2,
        queryTemplates: Object.freeze([
          'commercial janitorial {city} {state}',
          'office cleaning contractor {city} {state}',
        ]),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.LOCAL_CHAMBER,
        priority: 3,
        queryTemplates: Object.freeze(['cleaning services member {city} {state}']),
      }),
    ]),
  }),
  str_manager: Object.freeze({
    id: 'str_manager',
    statement: 'Short-term rental managers are likely buyers for recurring turnover cleaning.',
    buyerRole: 'buyer',
    segmentKey: 'short_term_rental',
    searchStrategies: Object.freeze([
      Object.freeze({
        source: SEARCH_SOURCES.GOOGLE_PLACES,
        priority: 1,
        queryTemplates: Object.freeze([
          'short term rental management {city} {state}',
          'Airbnb property management {city} {state}',
          'vacation rental management {city} {state}',
        ]),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.FACEBOOK,
        priority: 2,
        queryTemplates: Object.freeze(['Airbnb host group {city} {state}']),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.LINKEDIN,
        priority: 3,
        queryTemplates: Object.freeze(['vacation rental manager {city} {state}']),
      }),
    ]),
  }),
  property_manager: Object.freeze({
    id: 'property_manager',
    statement: 'Property managers are likely buyers.',
    buyerRole: 'buyer',
    segmentKey: 'property_management',
    searchStrategies: Object.freeze([
      Object.freeze({
        source: SEARCH_SOURCES.GOOGLE_PLACES,
        priority: 1,
        queryTemplates: Object.freeze([
          'property management company {city} {state}',
          'commercial property management {city} {state}',
        ]),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.BUSINESS_REGISTRY,
        priority: 2,
        queryTemplates: Object.freeze(['property management registered {city} {state}']),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.INDUSTRY_ASSOCIATION,
        priority: 3,
        queryTemplates: Object.freeze(['property management association member {state}']),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.LINKEDIN,
        priority: 4,
        queryTemplates: Object.freeze(['property manager {city} {state}']),
      }),
    ]),
  }),
  realtor: Object.freeze({
    id: 'realtor',
    statement: 'Real estate agencies are likely referral partners for move-in/move-out cleaning.',
    buyerRole: 'referral_partner',
    segmentKey: 'real_estate',
    searchStrategies: Object.freeze([
      Object.freeze({
        source: SEARCH_SOURCES.GOOGLE_PLACES,
        priority: 1,
        queryTemplates: Object.freeze([
          'real estate agency {city} {state}',
          'realtor office {city} {state}',
          'real estate broker {city} {state}',
        ]),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.BROKERAGE_SITES,
        priority: 2,
        queryTemplates: Object.freeze(['real estate office {city} {state}']),
      }),
    ]),
  }),
  restoration_remodeling_partner: Object.freeze({
    id: 'restoration_remodeling_partner',
    statement: 'Restoration and remodeling contractors are likely referral partners.',
    buyerRole: 'referral_partner',
    segmentKey: 'home_renovation',
    searchStrategies: Object.freeze([
      Object.freeze({
        source: SEARCH_SOURCES.GOOGLE_PLACES,
        priority: 1,
        queryTemplates: Object.freeze([
          'water damage restoration {city} {state}',
          'restoration company {city} {state}',
          'remodeling contractor {city} {state}',
        ]),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.PUBLIC_BUSINESS_DATA,
        priority: 2,
        queryTemplates: Object.freeze(['general contractor restoration {city} {state}']),
      }),
    ]),
  }),
  commercial_office: Object.freeze({
    id: 'commercial_office',
    statement: 'Commercial office operators are likely direct buyers.',
    buyerRole: 'buyer',
    segmentKey: 'commercial_office',
    searchStrategies: Object.freeze([
      Object.freeze({
        source: SEARCH_SOURCES.GOOGLE_PLACES,
        priority: 1,
        queryTemplates: Object.freeze([
          'commercial office {city} {state}',
          'office park {city} {state}',
          'business center {city} {state}',
        ]),
      }),
      Object.freeze({
        source: SEARCH_SOURCES.BUSINESS_REGISTRY,
        priority: 2,
        queryTemplates: Object.freeze(['office building management {city} {state}']),
      }),
    ]),
  }),
});

function normalizeHypothesisKey(value) {
  return asText(value).toLowerCase().replace(/[\s-]+/g, '_');
}

/**
 * Resolve a market hypothesis from a vertical key or operator segment label.
 * @param {string} verticalKey
 * @returns {MarketHypothesis|null}
 */
function resolveMarketHypothesis(verticalKey) {
  const key = normalizeHypothesisKey(verticalKey);
  return MARKET_HYPOTHESES[key] || null;
}

function renderQueryTemplate(template, geo = {}) {
  const city = asText(geo.city);
  const state = asText(geo.state);
  return template
    .replace(/\{city\}/g, city)
    .replace(/\{state\}/g, state)
    .trim();
}

/**
 * Expand a market hypothesis into concrete search workloads.
 * @param {MarketHypothesis} hypothesis
 * @param {object} geo
 * @param {object} [opts]
 * @param {string|string[]} [opts.sources] — filter to specific sources
 * @returns {object[]}
 */
function expandSearchStrategies(hypothesis, geo = {}, opts = {}) {
  if (!hypothesis) return [];
  const sourceFilter = opts.sources
    ? new Set(Array.isArray(opts.sources) ? opts.sources : [opts.sources])
    : null;

  const workloads = [];
  const strategies = [...(hypothesis.searchStrategies || [])].sort(
    (a, b) => (a.priority || 99) - (b.priority || 99)
  );

  for (const strategy of strategies) {
    if (sourceFilter && !sourceFilter.has(strategy.source)) continue;
    for (const template of strategy.queryTemplates || []) {
      workloads.push({
        hypothesisId: hypothesis.id,
        statement: hypothesis.statement,
        buyerRole: hypothesis.buyerRole,
        segmentKey: hypothesis.segmentKey || null,
        source: strategy.source,
        priority: strategy.priority || 99,
        query: renderQueryTemplate(template, geo),
        rationale: strategy.rationale || null,
      });
    }
  }
  return workloads;
}

/**
 * Return executable Google Places / SerpAPI query strings for a vertical.
 * Used by leadgen.js until full HypothesisDrivenDiscovery owns CLI sourcing.
 * @param {string} verticalKey
 * @param {object} geo
 * @returns {string[]}
 */
function expandPlacesQueriesForVertical(verticalKey, geo = {}) {
  const hypothesis = resolveMarketHypothesis(verticalKey);
  if (!hypothesis) return [];
  const workloads = expandSearchStrategies(hypothesis, geo, {
    sources: [SEARCH_SOURCES.GOOGLE_PLACES, SEARCH_SOURCES.PUBLIC_BUSINESS_DATA],
  });
  const seen = new Set();
  const queries = [];
  for (const row of workloads) {
    if (!row.query || seen.has(row.query)) continue;
    seen.add(row.query);
    queries.push(row.query);
  }
  return queries;
}

/**
 * List registered market hypothesis ids.
 * @returns {string[]}
 */
function listMarketHypotheses() {
  return Object.keys(MARKET_HYPOTHESES);
}

module.exports = {
  SEARCH_SOURCES,
  MARKET_HYPOTHESES,
  resolveMarketHypothesis,
  expandSearchStrategies,
  expandPlacesQueriesForVertical,
  listMarketHypotheses,
  renderQueryTemplate,
  normalizeHypothesisKey,
};
