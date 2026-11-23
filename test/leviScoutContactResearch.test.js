'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const research = require('../services/leviScoutContactResearch');

function fetchFor(rowsById) {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), headers: options.headers || {} });
    if (research.isPaidEnrichmentUrl(url)) throw new Error('paid_forbidden');
    const id = (/resource\/([a-z0-9]{4}-[a-z0-9]{4})/.exec(url) || [])[1];
    return { ok: true, json: async () => rowsById[id] || [] };
  };
  fetchImpl.calls = calls;
  return fetchImpl;
}

test('exact Chicago license and owner filings become provenance-backed contacts', async () => {
  const fetchImpl = fetchFor({
    'ezma-pppn': [{
      doing_business_as_name: 'Ben Operator LLC',
      owner_first_name: 'Ben',
      owner_last_name: 'Operator',
      owner_title: 'MANAGING MEMBER',
    }],
    'r5kz-chrr': [{
      doing_business_as_name: 'Ben Operator LLC',
      legal_name: 'Ben Operator LLC',
      address: '100 N MICHIGAN AVE',
      city: 'CHICAGO',
      state: 'IL',
      zip_code: '60601',
      license_id: '2222',
      license_status: 'AAI',
      phone: '3125550199',
    }],
  });
  const result = await research.researchContact({
    entityName: 'Ben Operator LLC',
    city: 'Chicago',
    permalink: 'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=100519123',
    observedAt: '2026-11-18',
    fetch: fetchImpl,
  });
  assert.equal(result.fabricated, false);
  assert.equal(result.paidProvidersUsed, false);
  assert.equal(result.verifiable, true);
  assert.equal(result.operatingBusiness.value, 'Ben Operator LLC');
  assert.match(result.decisionMakerRole.value, /MANAGING MEMBER/);
  assert.equal(result.phone.value, '(312) 555-0199');
  assert.match(result.address.value, /100 N MICHIGAN AVE/);
  assert.equal(result.phone.source, research.CHICAGO_LICENSES_URL);
  assert.ok(fetchImpl.calls.every(call => /ezma-pppn|r5kz-chrr/.test(call.url)));
  assert.ok(fetchImpl.calls.every(call => !research.isPaidEnrichmentUrl(call.url)));
});

test('no match leaves names, emails and phones empty instead of inventing them', async () => {
  const result = await research.researchContact({
    entityName: 'Unknown Holdings LLC',
    city: 'Chicago',
    sourceRow: { contact_1_type: 'OWNER', contact_1_name: 'Unknown Holdings LLC', contact_1_city: 'SKOKIE' },
    observedAt: '2026-11-18',
    fetch: fetchFor({ 'ezma-pppn': [], 'r5kz-chrr': [] }),
  });
  assert.equal(result.verifiable, false);
  assert.equal(result.phone, null);
  assert.equal(result.email, null);
  assert.equal(result.address, null);
  assert.equal(result.fabricated, false);
  assert.ok(result.limitations.some(line => /No verifiable public business/i.test(line)));
});

test('near-name license rows are not treated as the same business', async () => {
  const result = await research.researchContact({
    entityName: 'Ben Operator LLC',
    city: 'Chicago',
    observedAt: '2026-11-18',
    fetch: fetchFor({
      'ezma-pppn': [{ doing_business_as_name: 'Ben Operator Holdings LLC', owner_title: 'OWNER' }],
      'r5kz-chrr': [{ doing_business_as_name: 'Ben Operator Inc', address: '1 E WACKER' }],
    }),
  });
  assert.equal(result.verifiable, false);
  assert.equal(result.decisionMakerRole, null);
  assert.equal(result.address, null);
});

test('paid enrichment URLs are rejected and tenant 10 cannot research contacts', async () => {
  assert.equal(research.isPaidEnrichmentUrl('https://hunter.io/v2/email-finder'), true);
  assert.equal(research.isPaidEnrichmentUrl('https://api.prospeo.io/enrich'), true);
  assert.equal(research.isPaidEnrichmentUrl('https://places.googleapis.com/v1/places:searchText'), true);
  assert.equal(research.isPaidEnrichmentUrl('https://data.cityofchicago.org/resource/ezma-pppn.json'), false);
  await assert.rejects(research.researchContact({
    tenantId: 10,
    entityName: 'Ada Holdings LLC',
    fetch: fetchFor({}),
  }), { code: 'levi_research_tenant_invalid' });
});

test('NYC public owner phone is copied with source and never used to invent an email', async () => {
  const result = await research.researchContact({
    entityName: 'Ada Holdings LLC',
    city: 'New York',
    sourceRow: { owner_phone: '2125550100', owner_type: 'Corporation' },
    permalink: 'https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-TEST-1',
    observedAt: '2026-11-18',
    fetch: fetchFor({}),
  });
  assert.equal(result.verifiable, true);
  assert.equal(result.phone.value, '(212) 555-0100');
  assert.equal(result.email, null);
  assert.match(result.phone.evidence, /NYC DOB NOW/);
});
