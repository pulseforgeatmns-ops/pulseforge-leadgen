const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const pool = require('../db');
const {
  matchServiceAreaFromLocation,
  matchServiceAreaLocality,
  parsePlacesAddressComponents,
} = require('../utils/serviceArea');
const { promotionServiceAreaMatch, promoteRecord } = require('../scripts/promoteUnenriched');

const clientConfig = {
  id: 1,
  state: 'RI',
  service_area: ['Providence', 'Cranston', 'Warwick', 'Pawtucket', 'East Providence', 'Boston'],
  target_verticals: [
    { vertical: 'property_management', tier: 'A', autonomous_sourcing: true, seed_terms: ['property management {city}'] },
    { vertical: 'b2b_accounting', tier: 'B', autonomous_sourcing: false, seed_terms: [] },
  ],
};

test('Places address components preserve provenance and Boston matches the configured city', () => {
  const parsed = parsePlacesAddressComponents([
    { long_name: 'Boston', short_name: 'Boston', types: ['locality', 'political'] },
    { long_name: 'Massachusetts', short_name: 'MA', types: ['administrative_area_level_1', 'political'] },
    { long_name: '02110', short_name: '02110', types: ['postal_code'] },
  ]);

  assert.deepEqual(parsed, {
    locality: 'Boston',
    administrativeAreaLevel1: 'Massachusetts',
    postalCode: '02110',
  });
  assert.equal(matchServiceAreaLocality(parsed.locality.toUpperCase(), clientConfig.service_area), 'Boston');
  assert.equal(matchServiceAreaLocality('Morgantown', clientConfig.service_area), null);
  assert.equal(matchServiceAreaFromLocation('East Providence, Rhode Island', clientConfig.service_area), 'East Providence');
});

test('Scout skips and audits a Places locality outside the configured service area', async () => {
  const originalQuery = pool.query;
  const skipWrites = [];
  const companyWrites = [];
  try {
    pool.query = async (sql, params = []) => {
      if (/SELECT \* FROM clients WHERE id/.test(sql)) return { rows: [clientConfig] };
      if (/SELECT p\.id[\s\S]+FROM prospects p/.test(sql)) return { rows: [] };
      if (/SELECT id[\s\S]+FROM companies/.test(sql)) return { rows: [] };
      if (/INSERT INTO companies/.test(sql)) {
        companyWrites.push({ sql, params });
        return { rows: [{ id: 'company-1' }] };
      }
      if (/INSERT INTO scout_skip_log/.test(sql)) {
        skipWrites.push({ sql, params });
        return { rows: [] };
      }
      return { rows: [] };
    };

    delete require.cache[require.resolve('../leadgen')];
    const { configureScoringContext, _test } = require('../leadgen');
    await configureScoringContext({ client_id: 1, vertical: 'property_management', location: 'Boston MA' });
    assert(_test.getPlannedLocations(1, 'Providence RI', 'property_management').includes('Boston MA'));
    assert.deepEqual(_test.getPlannedLocations(1, 'Providence RI', 'b2b_accounting'), []);
    const result = await _test.saveToDatabase([{
      company: 'Morgantown Property Management',
      url: 'morgantown-property.example',
      address: '123 High St, Morgantown, WV 26505',
      places_locality: 'Morgantown',
      places_administrative_area_level_1: 'West Virginia',
      places_postal_code: '26505',
      source: ['google_places'],
      score: 80,
    }], { runId: 'service-area-test' });

    assert.equal(result.saved, 0);
    assert.equal(result.skipped_breakdown.out_of_area, 1);
    assert.equal(companyWrites.length, 1);
    assert.equal(companyWrites[0].params[8], 'Morgantown');
    assert.equal(companyWrites[0].params[9], 'West Virginia');
    assert.equal(companyWrites[0].params[10], '26505');
    assert.equal(skipWrites.length, 1);
    const detail = JSON.parse(skipWrites[0].params[8]);
    assert.equal(detail.rejected_locality, 'Morgantown');
    assert.equal(detail.rejected_state, 'West Virginia');
  } finally {
    pool.query = originalQuery;
    delete require.cache[require.resolve('../leadgen')];
  }
});

test('every production prospect insert explicitly writes service_area_match', () => {
  const files = [
    'leadgen.js',
    'dbClient.js',
    'sketchAgent.js',
    'scripts/promoteUnenriched.js',
    'scripts/importAnchorCallList.js',
  ];

  for (const file of files) {
    const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    const inserts = [...source.matchAll(/INSERT\s+INTO\s+prospects\s*\(([\s\S]*?)\)\s*VALUES/gi)];
    assert(inserts.length > 0, `${file} should contain a prospect insert`);
    for (const insert of inserts) {
      assert.match(insert[1], /\bservice_area_match\b/i, `${file} omitted service_area_match`);
    }
  }
});

test('promoteUnenriched rejects an out-of-area record before enrichment', async () => {
  let enriched = false;
  const accepted = await promoteRecord({
    id: 'unenriched-1',
    client_id: 1,
    company: 'Morgantown Property Management',
    domain: 'morgantown-property.example',
    location: 'Morgantown WV',
  }, {
    db: { query: async () => { throw new Error('out-of-area record reached persistence'); } },
    enrich: async () => { enriched = true; throw new Error('out-of-area record reached enrichment'); },
    loadClientConfig: async () => clientConfig,
  });

  assert.equal(accepted, false);
  assert.equal(enriched, false);
  assert.equal(promotionServiceAreaMatch({ location: 'boston, ma' }, clientConfig), 'Boston');
  assert.equal(matchServiceAreaFromLocation('Morgantown WV', clientConfig.service_area), null);
});
