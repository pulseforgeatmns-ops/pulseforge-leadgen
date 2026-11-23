'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const research = require('../services/leviScoutContactResearch');
const { enrichCandidate } = require('../services/leviScoutEnrichment');

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
  assert.equal(result.lookup.completeness, 'authoritative');
  assert.equal(result.lookup.authoritativeZeroMatch, true);
  assert.ok(result.limitations.some(line => /No Chicago Business Owners or Business License row matched/i.test(line)));
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

test('TimeoutError code 23 does not abort enrichment or invent a contact', async () => {
  const fetchImpl = async () => {
    throw Object.assign(new Error('The operation was aborted due to timeout'), {
      name: 'TimeoutError',
      code: 23,
    });
  };
  const result = await research.researchContact({
    entityName: 'Ben Operator LLC',
    city: 'Chicago',
    observedAt: '2026-11-18',
    fetch: fetchImpl,
    wait: async () => {},
  });
  assert.equal(result.fabricated, false);
  assert.equal(result.verifiable, false);
  assert.equal(result.phone, null);
  assert.equal(result.email, null);
  assert.equal(result.lookup.timeouts >= 1, true);
  assert.equal(result.lookup.completeness, 'incomplete');
  assert.equal(result.lookup.authoritativeZeroMatch, false);
  assert.ok(result.limitations.some(line => /not an authoritative zero-match/i.test(line)));
  assert.ok(!result.limitations.some(line => /No Chicago Business Owners or Business License row matched/i.test(line)));
});

test('Chicago contact lookup never issues a fifth HTTP request including retries', async () => {
  let calls = 0;
  const fetchImpl = async () => {
    calls += 1;
    throw Object.assign(new Error('The operation was aborted due to timeout'), {
      name: 'TimeoutError',
      code: 23,
    });
  };
  await research.researchContact({
    entityName: 'Ben Operator LLC',
    city: 'Chicago',
    observedAt: '2026-11-18',
    fetch: fetchImpl,
    wait: async () => {},
  });
  assert.equal(calls, 4);
});

test('Chicago lookups run from a trusted source URL when city was not persisted', async () => {
  const fetchImpl = fetchFor({
    'r5kz-chrr': [{
      doing_business_as_name: 'Ben Operator LLC',
      legal_name: 'Ben Operator LLC',
      address: '100 N MICHIGAN AVE',
      city: 'CHICAGO',
      state: 'IL',
      zip_code: '60601',
      license_id: '2222',
      phone: '3125550199',
    }],
  });
  const result = await research.researchContact({
    entityName: 'Ben Operator LLC',
    city: null,
    permalink: 'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=100519123',
    observedAt: '2026-11-18',
    fetch: fetchImpl,
  });
  assert.equal(result.lookup.city, 'Chicago');
  assert.equal(result.lookup.citySource, 'source_url_host');
  assert.equal(result.verifiable, true);
  assert.equal(result.phone.value, '(312) 555-0199');
  assert.ok(fetchImpl.calls.length <= 4);
});

test('owner timeout still keeps an exact license match instead of fabricating the missing dataset', async () => {
  const fetchImpl = async (url) => {
    if (/ezma-pppn/.test(String(url))) {
      throw Object.assign(new Error('The operation was aborted due to timeout'), {
        name: 'TimeoutError',
        code: 23,
      });
    }
    return {
      ok: true,
      json: async () => [{
        doing_business_as_name: 'Ben Operator LLC',
        legal_name: 'Ben Operator LLC',
        address: '100 N MICHIGAN AVE',
        city: 'CHICAGO',
        state: 'IL',
        zip_code: '60601',
        license_id: '2222',
        phone: '3125550199',
      }],
    };
  };
  const result = await research.researchContact({
    entityName: 'Ben Operator LLC',
    city: 'Chicago',
    observedAt: '2026-11-18',
    fetch: fetchImpl,
    wait: async () => {},
  });
  assert.equal(result.fabricated, false);
  assert.equal(result.lookup.ownersExactMatch, false);
  assert.equal(result.lookup.licensesExactMatch, true);
  assert.equal(result.lookup.partial, true);
  assert.equal(result.lookup.completeness, 'incomplete');
  assert.equal(result.lookup.authoritativeZeroMatch, false);
  assert.equal(result.phone.value, '(312) 555-0199');
  assert.equal(result.decisionMakerRole, null);
});

test('enrichCandidate backfills city from a trusted Chicago URL before contact research', async () => {
  const fetchImpl = fetchFor({
    'r5kz-chrr': [{
      doing_business_as_name: 'Ben Operator LLC',
      legal_name: 'Ben Operator LLC',
      address: '100 N MICHIGAN AVE',
      city: 'CHICAGO',
      state: 'IL',
      zip_code: '60601',
      license_id: '2222',
      phone: '3125550199',
    }],
  });
  const enriched = await enrichCandidate({
    candidate: {
      name: 'Ben Operator LLC',
      company: 'Ben Operator LLC',
      acquisition_metadata: {
        sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1'],
        sourceType: 'property_record_signal',
      },
    },
    fetch: fetchImpl,
  });
  assert.equal(enriched.acquisition_metadata.city, 'Chicago');
  assert.equal(enriched.acquisition_metadata.cityInference.source, 'source_url_host');
  assert.equal(enriched.acquisition_metadata.contactResearch.verifiable, true);
  assert.equal(enriched.acquisition_metadata.contactResearch.fabricated, false);
});

test('coverage measurement reports exact-match rates without inventing contacts', async () => {
  const coverage = await research.measureChicagoExactMatchCoverage({
    names: ['Ben Operator LLC', 'Unknown Holdings LLC'],
    fetch: fetchFor({
      'r5kz-chrr': [{
        doing_business_as_name: 'Ben Operator LLC',
        legal_name: 'Ben Operator LLC',
        address: '1 E WACKER',
      }],
    }),
  });
  assert.equal(coverage.queried, 2);
  assert.equal(coverage.licensesExactMatches, 1);
  assert.equal(coverage.eitherExactMatch, 1);
  assert.equal(coverage.authoritativeZeroMatches, 1);
  assert.equal(coverage.incompleteLookups, 0);
  assert.equal(coverage.fabricatedContacts, 0);
  assert.equal(coverage.licensesMatchRate, 0.5);
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
