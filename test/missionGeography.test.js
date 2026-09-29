'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  isLocationInMissionGeography,
  extractCityFromAddress,
  isProspectServiceAreaConfirmed,
} = require('../utils/missionGeography');
const { evaluateBasicFit, buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');

const GREATER_MANCHESTER_CITIES = [
  'Manchester',
  'Bedford',
  'Hooksett',
  'Auburn',
  'Goffstown',
  'Londonderry',
];

const MISSION_CONTEXT = {
  missionSegment: 'short_term_rental',
  missionCities: GREATER_MANCHESTER_CITIES.map(city => city.toLowerCase()),
  region: 'Greater Manchester',
};

test('canonical geography accepts approved mission cities', () => {
  for (const city of GREATER_MANCHESTER_CITIES) {
    assert.equal(
      isLocationInMissionGeography({
        location: `${city}, NH`,
        allowedCities: GREATER_MANCHESTER_CITIES,
      }),
      true,
      `${city} should be in scope`
    );
  }
});

test('canonical geography rejects out-of-area cities', () => {
  for (const city of ['Nashua', 'Merrimack', 'Derry', 'Henniker', 'Wolfeboro', 'North Conway', 'Boston']) {
    assert.equal(
      isLocationInMissionGeography({
        location: `${city}, NH`,
        allowedCities: GREATER_MANCHESTER_CITIES,
      }),
      false,
      `${city} should be rejected`
    );
  }
  assert.equal(
    isLocationInMissionGeography({
      location: 'Portland, ME',
      allowedCities: GREATER_MANCHESTER_CITIES,
    }),
    false
  );
});

test('extractCityFromAddress handles street-level NH addresses', () => {
  assert.equal(
    extractCityFromAddress('166 State Rte 101, Bedford, NH 03110'),
    'bedford'
  );
  assert.equal(
    extractCityFromAddress('46 Benton Rd, Hooksett, NH 03106'),
    'hooksett'
  );
});

test('regression live contradictions are in scope for replenishment admission', () => {
  const adventure = evaluateReplenishmentAdmission({
    name: 'Adventure Awaits Real Estate',
    description: 'Property management and short term rental services',
    location: '166 State Rte 101, Bedford, NH 03110',
    website: 'https://adventureawaits.example',
    domain: 'adventureawaits.example',
  }, MISSION_CONTEXT);
  assert.notEqual(adventure.reason, 'outside_geography');
  assert.equal(adventure.admitted, true);

  const northcity = evaluateReplenishmentAdmission({
    name: 'Northcity Property Management',
    description: 'Residential property management services',
    location: '46 Benton Rd, Hooksett, NH 03106',
    website: 'https://northcity.example',
    domain: 'northcity.example',
  }, MISSION_CONTEXT);
  assert.notEqual(northcity.reason, 'outside_geography');
  assert.equal(northcity.admitted, true);
});

test('regression out-of-area operators stay outside_geography', () => {
  const henniker = evaluateReplenishmentAdmission({
    name: 'Cozy Vacation Rental LLC',
    description: 'Vacation rental management',
    location: 'Henniker, NH',
    website: 'https://cozy.example',
    domain: 'cozy.example',
  }, MISSION_CONTEXT);
  assert.equal(henniker.admitted, false);
  assert.equal(henniker.reason, 'outside_geography');

  const wolfeboro = evaluateReplenishmentAdmission({
    name: 'Surefire Property Management',
    description: 'Property management services',
    location: 'Wolfeboro, NH',
    website: 'https://surefire.example',
    domain: 'surefire.example',
  }, MISSION_CONTEXT);
  assert.equal(wolfeboro.admitted, false);
  assert.equal(wolfeboro.reason, 'outside_geography');
});

test('Scout basic fit uses mission city list for Greater Manchester', () => {
  const definition = buildAcquisitionSearchDefinition({
    authorizedTenantId: '10',
    targetContext: {
      geography: 'Greater Manchester',
      segments: ['property_management'],
      businessType: 'commercial_cleaning',
    },
    businessContext: {
      serviceGeography: 'Greater Manchester',
      commercialCapability: 'commercial_cleaning',
    },
  });
  assert.ok(definition.geography.cities.includes('Bedford'));

  const bedfordFit = evaluateBasicFit({
    name: 'Adventure Awaits Real Estate',
    industry: 'property_management',
    location: '166 State Rte 101, Bedford, NH 03110',
    snippet: 'operating 12 managed properties',
  }, definition);
  assert.notEqual(bedfordFit.reasonCode, 'outside_geography');
  assert.equal(bedfordFit.basicFit, true);

  const hennikerFit = evaluateBasicFit({
    name: 'Cozy Vacation Rental LLC',
    industry: 'property_management',
    location: 'Henniker, NH',
    snippet: 'vacation rental management',
  }, definition);
  assert.equal(hennikerFit.basicFit, false);
  assert.equal(hennikerFit.reasonCode, 'outside_geography');
});

test('Max inventory accepts canonical service_area_match strings in mission cities', () => {
  const scope = {
    segment: 'short_term_rental',
    cities: GREATER_MANCHESTER_CITIES.map(city => city.toLowerCase()),
    region: 'Greater Manchester',
  };
  assert.equal(
    isProspectServiceAreaConfirmed({
      service_area_match: 'Bedford',
      company_location: '166 State Rte 101, Bedford, NH 03110',
      vertical: 'str_manager',
    }, scope),
    true
  );
  assert.equal(
    isProspectServiceAreaConfirmed({
      service_area_match: 'Henniker',
      company_location: 'Henniker, NH',
      vertical: 'str_manager',
    }, scope),
    false
  );
});
