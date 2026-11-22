'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const discovery = require('../services/leviScoutBorrowerDiscovery');
const { evaluateProspect } = require('../services/leviScoutProductionContract');

const nycRow = {
  permit_si_no: 'NYC-100', owner_s_first_name: 'Ada', owner_s_last_name: 'Borrower',
  owner_s_business_name: 'Ada Holdings LLC', filing_date: '2026-11-02T00:00:00.000',
  borough: 'MANHATTAN', permit_type: 'NB', owner_s_house__: '10', owner_s_house_street_name: 'WALL STREET',
  job_description: 'Interior renovation of a 3-family',
};
const chicagoRow = {
  permit_: 'CHI-200', contact_1_name: 'Ben Operator', issue_date: '2026-11-03T00:00:00.000',
  street_number: '100', street_direction: 'N', street_name: 'MICHIGAN', suffix: 'AVE',
  permit_type: 'PERMIT', work_description: 'Rehab of a two-flat',
};

function fetchFor(rowsByHost) {
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(String(url));
    if (discovery.isPlacesUrl(url)) throw new Error('places_forbidden');
    const host = /cityofnewyork/.test(url) ? 'nyc' : /cityofchicago/.test(url) ? 'chicago' : 'other';
    return { ok: true, json: async () => rowsByHost[host] || [] };
  };
  fetchImpl.calls = calls;
  return fetchImpl;
}

test('maps NYC and Chicago open-data permits as property-record signals in the two cities', () => {
  const nyc = discovery.mapNyc(nycRow);
  const chicago = discovery.mapChicago(chicagoRow);
  assert.equal(nyc.name, 'Ada Borrower');
  assert.equal(nyc.acquisition_metadata.city, 'New York');
  assert.equal(nyc.acquisition_metadata.sourceType, 'property_record_signal');
  assert.equal(nyc.email, null);
  assert.match(nyc.notes, /Sources: https:\/\/data\.cityofnewyork\.us/);
  assert.equal(chicago.name, 'Ben Operator');
  assert.equal(chicago.acquisition_metadata.city, 'Chicago');
  assert.equal(chicago.service_area_match, 'Chicago, IL');
  assert.deepEqual(discovery.CITIES, ['New York', 'Chicago']);
});

test('discover caps candidates, splits cities, and never calls Places or paid providers', async () => {
  const fetchImpl = fetchFor({ nyc: [nycRow, { ...nycRow, permit_si_no: 'NYC-101' }], chicago: [chicagoRow] });
  const rows = await discovery.discover({ maxCandidates: 2, maxProviderRequests: 0, fetch: fetchImpl });
  assert.equal(rows.length, 2);
  assert.deepEqual(discovery.citiesOf(rows).sort(), ['Chicago', 'New York']);
  assert.ok(fetchImpl.calls.every(url => /cityofnewyork|cityofchicago/.test(url)));
  assert.ok(fetchImpl.calls.every(url => !discovery.isPlacesUrl(url)));
  assert.ok(fetchImpl.calls.every(url => !/serpapi|prospeo|hunter|googleapis/.test(url)));
});

test('maxProviderRequests above zero is rejected so paid providers stay off', async () => {
  await assert.rejects(discovery.discover({ maxCandidates: 2, maxProviderRequests: 1, fetch: fetchFor({}) }),
    { code: 'levi_research_paid_provider_approval_required' });
});

test('tenant 10 cannot use this discover adapter', async () => {
  await assert.rejects(discovery.discover({ tenantId: 10, maxCandidates: 1, fetch: fetchFor({}) }),
    { code: 'levi_research_tenant_invalid' });
});

test('a permit row never qualifies even with an approved financing range', () => {
  const row = discovery.mapNyc(nycRow);
  const criteria = { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 };
  const result = evaluateProspect(row, { criteria });
  assert.equal(result.qualified, false);
  assert.ok(result.blockers.includes('borrower_intent_unverified'));
  assert.ok(result.blockers.includes('contact_path_missing'));
});

test('nameless permits are dropped rather than invented', () => {
  assert.equal(discovery.mapNyc({ permit_si_no: 'X' }), null);
  assert.equal(discovery.mapChicago({ permit_: 'Y' }), null);
});

test('failed open-data reads yield zero rather than substituting another source', async () => {
  const fetchImpl = async (url) => {
    if (discovery.isPlacesUrl(url)) throw new Error('places');
    return { ok: false, json: async () => { throw new Error('no'); } };
  };
  const rows = await discovery.discover({ maxCandidates: 5, fetch: fetchImpl });
  assert.deepEqual(rows, []);
});
