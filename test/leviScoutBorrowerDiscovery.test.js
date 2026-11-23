'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const discovery = require('../services/leviScoutBorrowerDiscovery');
const { evaluateProspect } = require('../services/leviScoutProductionContract');

const NOW = new Date('2026-11-22T15:00:00Z');

const nycRow = {
  job_filing_number: 'B00-TEST-1',
  job_type: 'Alteration',
  first_permit_date: '2026-11-18T00:00:00.000',
  owner_s_business_name: 'Ada Holdings LLC',
  owner_type: 'Corporation',
  house_no: '10',
  street_name: 'WALL STREET',
  borough: 'Manhattan',
  job_description: 'Interior renovation of dwelling units on floors 2-4',
  sidewalk_shed_work_type_: 'NO',
};
const nycShedOnly = {
  ...nycRow,
  job_filing_number: 'B00-SHED',
  owner_s_business_name: 'Shed Holdings LLC',
  job_description: 'HEAVY DUTY SIDEWALK SHED DURING NB CONST. SIDEWALK SHED SHALL COMPLY WITH CH #33.',
  sidewalk_shed_work_type_: 'YES',
};
const chicagoRow = {
  permit_: '100519123',
  permit_type: 'PERMIT - RENOVATION/ALTERATION',
  issue_date: '2026-11-18T00:00:00.000',
  contact_1_type: 'OWNER',
  contact_1_name: 'Ben Operator LLC',
  contact_1_city: 'SKOKIE',
  street_number: '100',
  street_direction: 'N',
  street_name: 'MICHIGAN',
  suffix: 'AVE',
  work_description: 'Interior renovation of a two-flat dwelling',
};

function fetchFor(rowsByHost) {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), headers: options.headers || {} });
    if (discovery.isPlacesUrl(url)) throw new Error('places_forbidden');
    const host = /cityofnewyork/.test(url) ? 'nyc' : /cityofchicago/.test(url) ? 'chicago' : 'other';
    return { ok: true, json: async () => rowsByHost[host] || [] };
  };
  fetchImpl.calls = calls;
  return fetchImpl;
}

test('maps NYC DOB NOW and Chicago renovation permits as property-record signals', () => {
  const nyc = discovery.mapNyc(nycRow, NOW);
  const chicago = discovery.mapChicago(chicagoRow, NOW);
  assert.equal(nyc.name, 'Ada Holdings LLC');
  assert.equal(nyc.email, null);
  assert.equal(nyc.phone, null);
  assert.equal(nyc.acquisition_metadata.sourceType, 'property_record_signal');
  assert.equal(nyc.acquisition_metadata.evidenceDate, '2026-11-18');
  assert.match(nyc.notes, /The operator owns the property at 10 WALL STREET, Manhattan, New York, NY\. Renovation is underway/);
  assert.doesNotMatch(nyc.notes, /needs \$/);
  assert.match(nyc.notes, /w9ak-ipjd/);
  assert.equal(chicago.name, 'Ben Operator LLC');
  assert.equal(chicago.service_area_match, '100 N MICHIGAN AVE, Chicago, IL');
  assert.doesNotMatch(chicago.notes, /SKOKIE/);
  assert.match(chicago.notes, /The operator owns the property at 100 N MICHIGAN AVE, Chicago, IL\. Renovation is underway/);
  assert.deepEqual(discovery.CITIES, ['New York', 'Chicago']);
  assert.equal(discovery.NYC_URL, 'https://data.cityofnewyork.us/resource/w9ak-ipjd.json');
});

test('drops sidewalk-shed-only, tuckpointing-only, individuals and future evidence dates', () => {
  assert.equal(discovery.mapNyc(nycShedOnly, NOW), null);
  assert.equal(discovery.mapNyc({ ...nycRow, job_description: 'Tuckpointing of the front facade only' }, NOW), null);
  assert.equal(discovery.mapNyc({ ...nycRow, owner_s_business_name: 'Ada Borrower' }, NOW), null);
  assert.equal(discovery.mapNyc({ ...nycRow, first_permit_date: '2026-11-23T00:00:00.000' }, NOW), null);
  assert.equal(discovery.mapChicago({ ...chicagoRow, contact_1_name: 'JAMES TASSEV', contact_1_type: 'OWNER' }, NOW), null);
  assert.equal(discovery.mapChicago({ ...chicagoRow, permit_type: 'PERMIT - SIGNS' }, NOW), null);
});

test('discover interleaves cities, caps at 10, sends User-Agent, and never calls Places', async () => {
  const fetchImpl = fetchFor({
    nyc: [nycRow, { ...nycRow, job_filing_number: 'B00-2', owner_s_business_name: 'Second Realty LLC' }],
    chicago: [chicagoRow],
  });
  const rows = await discovery.discover({ maxCandidates: 10, maxProviderRequests: 0, fetch: fetchImpl, now: NOW });
  assert.equal(rows.length, 3);
  assert.deepEqual(discovery.citiesOf(rows).sort(), ['Chicago', 'New York']);
  assert.equal(rows[0].acquisition_metadata.city, 'New York');
  assert.equal(rows[1].acquisition_metadata.city, 'Chicago');
  assert.ok(fetchImpl.calls.every(call => /w9ak-ipjd|ydr8-5enu/.test(call.url)));
  assert.ok(fetchImpl.calls.every(call => !/ipu4-2q9a/.test(call.url)));
  assert.ok(fetchImpl.calls.every(call => !discovery.isPlacesUrl(call.url)));
  assert.ok(fetchImpl.calls.every(call => call.headers['User-Agent'] === discovery.USER_AGENT));
  assert.ok(fetchImpl.calls.some(call => /first_permit_date/.test(decodeURIComponent(call.url))));
});

test('maxProviderRequests above zero is rejected so paid providers stay off', async () => {
  await assert.rejects(discovery.discover({ maxCandidates: 2, maxProviderRequests: 1, fetch: fetchFor({}), now: NOW }),
    { code: 'levi_research_paid_provider_approval_required' });
});

test('tenant 10 cannot use this discover adapter', async () => {
  await assert.rejects(discovery.discover({ tenantId: 10, maxCandidates: 1, fetch: fetchFor({}), now: NOW }),
    { code: 'levi_research_tenant_invalid' });
});

test('a permit row never qualifies even with an approved financing range', () => {
  const row = discovery.mapNyc(nycRow, NOW);
  const criteria = { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 };
  const result = evaluateProspect(row, { criteria });
  assert.equal(result.qualified, false);
  assert.ok(result.blockers.includes('borrower_intent_unverified'));
  assert.ok(result.blockers.includes('contact_path_missing'));
});

test('failed open-data reads yield zero rather than substituting another source', async () => {
  const fetchImpl = async (url) => {
    if (discovery.isPlacesUrl(url)) throw new Error('places');
    return { ok: false, json: async () => { throw new Error('no'); } };
  };
  const rows = await discovery.discover({ maxCandidates: 5, fetch: fetchImpl, now: NOW });
  assert.deepEqual(rows, []);
});
