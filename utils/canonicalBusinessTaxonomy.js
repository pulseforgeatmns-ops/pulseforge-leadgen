'use strict';

/**
 * Shared producer/consumer taxonomy for founder-led small-business discovery.
 * Scout query generation and replenishment admission must use these same keys.
 */

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
  FOUNDER_LED_SMALL_BUSINESS_TAXONOMY,
  FOUNDER_LED_SMALL_BUSINESS_VERTICALS,
  SMALL_BUSINESS_OWNER_SEGMENT_KEYS,
  isSmallBusinessOwnerSegment,
  founderLedSmallBusinessSearchTerms,
  founderLedSmallBusinessCustomerTypes,
};
