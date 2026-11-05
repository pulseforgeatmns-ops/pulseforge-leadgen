'use strict';

// SPEC-SCOUT-P1-001 Rev. 2 (#1048). Tenant 10 only.
// Registry of IMMUTABLE allocation versions. A page's source-query tier is
// always computed with the version whose activation window contains the page,
// never with today's mapping. Any change to shares, window, scope, concepts,
// the category map, the category classifier or the query builder that alters
// a version's universe must ship as a NEW version; universeSha256 pins it and
// test/scoutFacilityTierPlanner.test.js fails if a version's universe drifts.
const V1 = Object.freeze({
  version: 'anchor-p1-001-v1',
  tenantId: '10',
  shares: Object.freeze({ 1: 0.50, 2: 0.35, 3: 0.15 }),
  // Every tier with an eligible query is selected at least once per window.
  window: 10,
  // Pinned to the approved facility-buyer mission (activateAnchorFacilityBuyerMission).
  scope: Object.freeze({
    eligibleSubsegments: Object.freeze(['managed_facilities', 'restaurant_foh']),
    cities: Object.freeze(['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack']),
    state: 'NH',
  }),
  generations: 4, // Concept rotations repeat every four search generations.
  tierByCategory: Object.freeze({
    early_learning: 1,
    school: 1,
    campus: 1,
    property_manager: 1,
    manufacturer: 2,
    warehouse: 2,
    dealership: 2,
    medical: 2,
    // Office parks, multi-tenant and professional office buildings. A generic
    // single-practice concept would be Tier 3; none is configured here.
    commercial_office: 2,
    restaurant: 3,
  }),
  // Added to the approved managed-facilities concepts for this tenant only.
  additionalConcepts: Object.freeze({
    managed_facilities: Object.freeze([
      'daycare',
      'preschool',
      'early learning center',
      'medical office',
      'dental office',
      'multi-tenant office building',
    ]),
  }),
  universeSha256: '31a0d717316a5e0e9ce839220ebaeb63f6788a03171b50d54e02fed2393ee225',
});

const ANCHOR_FACILITY_TIER_VERSIONS = Object.freeze({ [V1.version]: V1 });
// The version this code plans with. Older versions remain for attribution.
const ANCHOR_FACILITY_TIERS = V1;

module.exports = { ANCHOR_FACILITY_TIERS, ANCHOR_FACILITY_TIER_VERSIONS };
