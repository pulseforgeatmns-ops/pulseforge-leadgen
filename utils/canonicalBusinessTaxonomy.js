'use strict';

/**
 * Shared producer/consumer taxonomy for founder-led small-business discovery.
 * Scout query generation and replenishment admission must use these same keys.
 */

const { normalizeVertical } = require('./normalize');

// Explicit service trades are separate from the founder-led SMB audience. Adding
// an enrichable trade must not silently expand an existing tenant's mission.
const SERVICE_TRADE_TAXONOMY = Object.freeze({
  plumbing: Object.freeze({
    customerType: 'Plumbing contractors',
    aliases: Object.freeze(['plumbing', 'plumber', 'plumbers', 'plumbing contractor', 'plumbing company', 'plumbing service', 'plumbing services']),
    searchTerms: Object.freeze(['Plumbing Contractor', 'Plumbing Company']),
    evidence: Object.freeze([/\bplumb(?:ing|er|ers)\b/i]),
  }),
  heating: Object.freeze({
    customerType: 'Heating contractors',
    aliases: Object.freeze(['heating', 'heating contractor', 'heating company', 'heating service', 'heating services', 'furnace repair', 'boiler service']),
    searchTerms: Object.freeze(['Heating Contractor', 'Heating Company']),
    evidence: Object.freeze([/\bheating\b/i, /\bfurnace repair\b/i, /\bboiler service\b/i]),
  }),
  hvac: Object.freeze({
    customerType: 'HVAC contractors',
    aliases: Object.freeze(['hvac', 'hvac contractor', 'hvac company', 'hvac service', 'hvac services', 'air conditioning', 'air conditioning contractor', 'air conditioning company', 'heating and cooling', 'heating & cooling', 'heating and cooling company', 'heating & cooling company', 'heating ventilation and air conditioning']),
    searchTerms: Object.freeze(['HVAC Contractor', 'Heating and Cooling Company', 'Air Conditioning Contractor']),
    evidence: Object.freeze([/\bhvac\b/i, /\bheating (?:and|&) cooling\b/i, /\bair conditioning\b/i]),
  }),
});
const SERVICE_TRADE_VERTICALS = Object.freeze(Object.keys(SERVICE_TRADE_TAXONOMY));
const SERVICE_TRADE_ALIASES = Object.freeze(Object.fromEntries(
  Object.entries(SERVICE_TRADE_TAXONOMY).flatMap(([vertical, entry]) =>
    entry.aliases.map(alias => [normalizeVertical(alias), vertical]))
));

// Exact aliases, then explicit compounds. Unknown members reject the whole
// expression; generic contractors/home services and query slugs are not aliases.
function serviceTradeVerticals(value) {
  const key = normalizeVertical(value);
  if (SERVICE_TRADE_ALIASES[key]) return [SERVICE_TRADE_ALIASES[key]];
  const parts = String(value || '').replace(/_/g, ' ').split(/\s*(?:[,/&+]|\band\b)\s*/i);
  if (parts.length < 2) return [];
  const mapped = parts.map(part => SERVICE_TRADE_ALIASES[normalizeVertical(part)]);
  return mapped.every(Boolean) ? [...new Set(mapped)] : [];
}

function serviceTradeMissionVerticals(segments) {
  if (!Array.isArray(segments) || !segments.length) return [];
  const mapped = segments.map(serviceTradeVerticals);
  return mapped.every(values => values.length) ? [...new Set(mapped.flat())] : [];
}

function serviceTradeSearchTerms(segments) {
  return [...new Set(serviceTradeMissionVerticals(segments)
    .flatMap(vertical => SERVICE_TRADE_TAXONOMY[vertical].searchTerms))];
}

const FOUNDER_LED_SMALL_BUSINESS_TAXONOMY = Object.freeze({
  cleaning: Object.freeze({
    customerType: 'Owner-operated cleaning companies',
    searchTerms: Object.freeze(['Cleaning Company', 'Commercial Cleaning', 'Janitorial Service']),
  }),
  landscaping: Object.freeze({
    customerType: 'Owner-operated landscaping companies',
    searchTerms: Object.freeze(['Landscaping Company', 'Lawn Care Service']),
  }),
  painting: Object.freeze({
    customerType: 'Owner-operated painting companies',
    searchTerms: Object.freeze(['Painting Contractor', 'House Painting Company']),
  }),
  hvac: Object.freeze({
    customerType: 'Owner-operated HVAC companies',
    searchTerms: Object.freeze(['HVAC Contractor', 'Heating and Cooling Company']),
  }),
  electrician: Object.freeze({
    customerType: 'Owner-operated electrical contractors',
    searchTerms: Object.freeze(['Electrical Contractor', 'Electrician Company']),
  }),
  home_services: Object.freeze({
    customerType: 'Owner-operated home-service companies',
    searchTerms: Object.freeze(['Handyman Service', 'Home Repair Company']),
  }),
  restaurant: Object.freeze({
    customerType: 'Independent restaurants',
    searchTerms: Object.freeze(['Independent Restaurant', 'Family Owned Restaurant']),
  }),
  salon: Object.freeze({
    customerType: 'Independent salons and spas',
    searchTerms: Object.freeze(['Independent Hair Salon', 'Independent Beauty Salon']),
  }),
  fitness: Object.freeze({
    customerType: 'Independent gyms and fitness studios',
    searchTerms: Object.freeze(['Independent Gym', 'Fitness Studio']),
  }),
  auto: Object.freeze({
    customerType: 'Independent automotive repair shops',
    searchTerms: Object.freeze(['Independent Auto Repair Shop', 'Automotive Service Shop']),
  }),
});

const FOUNDER_LED_SMALL_BUSINESS_VERTICALS = Object.freeze(
  Object.keys(FOUNDER_LED_SMALL_BUSINESS_TAXONOMY)
);

const SMALL_BUSINESS_OWNER_SEGMENT_KEYS = Object.freeze([
  'small_business_owner',
  'small_business_owners',
  'founder_led_smb',
  'founder_led_small_business',
]);

function isSmallBusinessOwnerSegment(value) {
  const key = String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_');
  return SMALL_BUSINESS_OWNER_SEGMENT_KEYS.includes(key);
}

function founderLedSmallBusinessSearchTerms() {
  return [...new Set(Object.values(FOUNDER_LED_SMALL_BUSINESS_TAXONOMY)
    .flatMap(entry => entry.searchTerms))];
}

function founderLedSmallBusinessCustomerTypes() {
  return Object.values(FOUNDER_LED_SMALL_BUSINESS_TAXONOMY)
    .map(entry => entry.customerType);
}

module.exports = {
  SERVICE_TRADE_TAXONOMY,
  SERVICE_TRADE_VERTICALS,
  serviceTradeVerticals,
  serviceTradeMissionVerticals,
  serviceTradeSearchTerms,
  FOUNDER_LED_SMALL_BUSINESS_TAXONOMY,
  FOUNDER_LED_SMALL_BUSINESS_VERTICALS,
  SMALL_BUSINESS_OWNER_SEGMENT_KEYS,
  isSmallBusinessOwnerSegment,
  founderLedSmallBusinessSearchTerms,
  founderLedSmallBusinessCustomerTypes,
};
