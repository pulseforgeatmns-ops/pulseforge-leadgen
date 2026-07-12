const assert = require('node:assert/strict');
const test = require('node:test');
const { buildPairs, cityRanking } = require('../scripts/marketBakeoff');

const client = {
  target_verticals: [
    ...['commercial_electrical', 'janitorial', 'property_management', 'staffing_recruiting', 'msp_it_services']
      .map(vertical => ({ vertical, tier: 'A', autonomous_sourcing: true, seed_terms: [`${vertical} {city} {state}`] })),
  ],
};

test('market bake-off is capped to the requested five city-vertical pairs per city', () => {
  const pairs = buildPairs(client);
  assert.equal(pairs.length, 15);
  assert.ok(pairs.every(pair => pair.query.includes(pair.city) && pair.query.includes(pair.state)));
});

test('market ranking uses fresh yield per search', () => {
  const ranking = cityRanking([
    { city: 'Boston', state: 'MA', searches: 5, fresh: 4 },
    { city: 'Hartford', state: 'CT', searches: 5, fresh: 8 },
    { city: 'Providence', state: 'RI', searches: 5, fresh: 3 },
  ]);
  assert.equal(ranking[0].city, 'Hartford, CT');
});
