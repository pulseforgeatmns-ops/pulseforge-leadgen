const assert = require('node:assert/strict');
const test = require('node:test');
const { computeBaseScore } = require('../utils/icpScoring');
const {
  normalizeVertical,
  resolveVerticalTier,
  autonomousTargetVerticals,
} = require('../utils/verticalTiers');
const { _test: scoutTest } = require('../leadgen');

const client = {
  city: 'Providence',
  state: 'RI',
  service_area: ['Providence', 'Cranston'],
  target_verticals: [
    { vertical: 'commercial_electrical', tier: 'A', autonomous_sourcing: true, seed_terms: ['commercial electrical contractor {city}'] },
    { vertical: 'b2b_accounting', tier: 'B', autonomous_sourcing: false, seed_terms: [] },
  ],
  vertical_tiers: {
    commercial_electrical: 'A',
    b2b_accounting: 'B',
    restaurant: 'C',
    marketing_agency: 'W',
  },
};

test('vertical values normalize before tier lookup and never default to A', () => {
  assert.equal(normalizeVertical(' Commercial Electrical '), 'commercial_electrical');
  assert.equal(resolveVerticalTier(' Commercial Electrical ', client).tier, 'A');
  const unknown = resolveVerticalTier('Mystery Vertical', client);
  assert.equal(unknown.tier, 'unknown');
  assert.equal(unknown.score_ceiling, 60);
  assert.equal(unknown.autonomous_sourcing, false);
});

test('only configured Tier A entries source autonomously', () => {
  assert.deepEqual(autonomousTargetVerticals(client), ['commercial_electrical']);
});

test('Places B2B classifier rejects residential matches and buckets ambiguity', () => {
  assert.equal(scoutTest.classifyPlacesB2B({
    company: 'Acme Commercial Electrical', url: 'acmeelectrical.example', place_types: ['electrician'],
  }).classification, 'b2b');
  assert.equal(scoutTest.classifyPlacesB2B({
    company: 'Smith Residential Electric', url: 'smith.example', place_types: ['electrician'],
  }).classification, 'b2c');
  assert.equal(scoutTest.classifyPlacesB2B({
    company: 'Smith Electric', url: 'smith.example', place_types: ['electrician'],
  }).classification, 'ambiguous');
});

test('dynamic scorer uses the client market, structured client fit, and tier clamps', () => {
  const tierA = computeBaseScore({
    client_id: 1,
    vertical: 'commercial_electrical',
    company_name: 'Acme Electrical LLC',
    service_area_match: 'Providence RI',
    email: 'owner@acme.example',
    phone: '4015550100',
    has_website: true,
    company_size: 'team of 10',
  }, client);
  assert.equal(tierA.components.vertical, 25);
  assert.equal(tierA.components.location, 20);
  assert.equal(tierA.components.client_fit, 8);

  const tierC = computeBaseScore({
    client_id: 1,
    vertical: 'restaurant',
    service_area_match: 'Providence RI',
    email: 'owner@restaurant.example',
    phone: '4015550101',
    has_website: true,
    company_size: 'team',
  }, client);
  assert.equal(tierC.components.vertical, 0);
  assert.equal(tierC.total, 30);
  assert.equal(tierC.score_ceiling, 30);

  const wrongMarket = computeBaseScore({
    client_id: 1,
    vertical: 'commercial_electrical',
    service_area_match: 'Manchester NH',
  }, client);
  assert.equal(wrongMarket.components.location, 0);
});
