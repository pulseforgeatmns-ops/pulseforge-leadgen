const assert = require('node:assert/strict');
const axios = require('axios');
const {
  buildLinkedInQueries,
  canonicalizeLinkedInUrl,
  parseEmployeeCountSignal,
  parseLinkedInResult,
  sourceLinkedInProspects,
} = require('../utils/linkedinSerpSource');

async function run() {
  assert.equal(
    canonicalizeLinkedInUrl('https://www.linkedin.com/in/Jane-Doe-123/?trk=public_profile'),
    'https://www.linkedin.com/in/jane-doe-123'
  );
  assert.equal(canonicalizeLinkedInUrl('https://www.linkedin.com/company/acme'), null);

  const parsed = parseLinkedInResult({
    title: 'Jane Doe - Founder at Acme Services - Boston, Massachusetts | LinkedIn',
    link: 'https://linkedin.com/in/jane-doe-123?trk=google',
    snippet: '500+ connections · Acme Services has 10-50 employees',
    position: 2,
  }, { query: 'test query', page: 1, timestamp: '2026-06-30T12:00:00.000Z' });
  assert.equal(parsed.parsed, true);
  assert.equal(parsed.record.first_name, 'Jane');
  assert.equal(parsed.record.last_name, 'Doe');
  assert.equal(parsed.record.job_title, 'Founder');
  assert.equal(parsed.record.company, 'Acme Services');
  assert.equal(parsed.record.linkedin_location, 'Boston, Massachusetts');
  assert.equal(parsed.record.employee_count_estimate, '10-50 employees');
  assert.equal(parsed.record.linkedin_source_query.result_rank, 2);

  const ofCompany = parseLinkedInResult({
    title: 'Jonathan Cohen - CEO of Boston Property Management LLC | LinkedIn',
    link: 'https://linkedin.com/in/jonathan-cohen',
    snippet: 'Boston, Massachusetts, United States · 500+ connections',
  });
  assert.equal(ofCompany.record.job_title.toLowerCase(), 'ceo');
  assert.equal(ofCompany.record.company, 'Boston Property Management LLC');
  assert.equal(ofCompany.record.linkedin_location, 'Boston, Massachusetts, United States');

  const snippetRole = parseLinkedInResult({
    title: 'Travis Snell - Concord Property Management, Inc. | LinkedIn',
    link: 'https://linkedin.com/in/travis-snell',
    snippet: 'President of Concord Property Management, Inc. · Concord, Massachusetts, United States',
  });
  assert.equal(snippetRole.record.job_title.toLowerCase(), 'president');
  assert.equal(snippetRole.record.company, 'Concord Property Management, Inc.');
  assert.equal(snippetRole.record.linkedin_location, 'Concord, Massachusetts, United States');
  assert.notEqual(snippetRole.record.linkedin_location, 'President of Concord Property Management, Inc.');

  const noisyLocation = parseLinkedInResult({
    title: 'Test Person - Test Property Management, Inc. | LinkedIn',
    link: 'https://linkedin.com/in/test-person',
    snippet: 'President of Test Property Management, Inc. · Experienced real estate broker. Licenses & Certifications. Massachusetts Licensed Real Estate Broker.',
  });
  assert.equal(noisyLocation.record.linkedin_location, null);

  assert.equal(parseEmployeeCountSignal('500+ connections · 5K followers'), null);
  assert.equal(parseEmployeeCountSignal('Founder with 12,000 followers'), null);
  const socialCountsOnly = parseLinkedInResult({
    title: 'Alex Owner - Founder at Example Company | LinkedIn',
    link: 'https://linkedin.com/in/alex-owner',
    snippet: '500+ connections · 5K followers',
  });
  assert.equal(socialCountsOnly.record.employee_count_estimate, null);
  assert.equal(parseEmployeeCountSignal('We are a team of 35 serving New England'), 'team of 35');
  assert.equal(parseEmployeeCountSignal('Company size: 50+ employees'), '50+ employees');

  const queries = buildLinkedInQueries({
    vertical: 'property_management',
    geo: 'Massachusetts',
    titleFilter: 'owner|founder|ceo|president',
    titleExclude: 'consultant|agency',
  });
  assert(queries.length >= 5);
  assert(queries.every(query => query.includes('site:linkedin.com/in/')));
  assert(queries.every(query => query.includes('"Massachusetts"')));
  assert(queries.every(query => query.includes('-"consultant"')));

  let calls = 0;
  const capped = await sourceLinkedInProspects({
    vertical: 'property_management',
    geo: 'Massachusetts',
    maxRequests: 2,
    pageDepth: 5,
    search: async ({ page }) => {
      calls++;
      return {
        organicResults: [{
          title: `Person ${page} - Owner at Company ${page} | LinkedIn`,
          link: `https://www.linkedin.com/in/person-${page}`,
          snippet: 'Massachusetts, United States',
          position: page + 1,
        }],
        requestCount: 1,
        durationMs: 4,
        error: null,
      };
    },
  });
  assert.equal(calls, 2);
  assert.equal(capped.requestCount, 2);
  assert.equal(capped.queries.length, 2);

  // Default Scout wrapper regression: raw SerpAPI results still become the
  // same business-domain shape and social URLs are attached, not emitted.
  const originalGet = axios.get;
  axios.get = async () => ({ data: { organic_results: [
    { title: 'Acme LLC | Home', link: 'https://acme.example/about', snippet: 'Acme services' },
    { title: 'Acme LLC', link: 'https://facebook.com/acmellc', snippet: '' },
    { title: 'Directory', link: 'https://yelp.com/biz/acme', snippet: '' },
  ] } });
  process.env.SERPAPI_KEY = process.env.SERPAPI_KEY || 'test-key';
  delete require.cache[require.resolve('../leadgen')];
  const { _test } = require('../leadgen');
  const defaultResults = await _test.searchGoogle('acme', 10);
  axios.get = originalGet;
  assert.deepEqual(defaultResults, [{
    company: 'Acme LLC',
    url: 'acme.example',
    snippet: 'Acme services',
    source: ['google'],
    facebook_url: 'https://facebook.com/acmellc',
  }]);

  const pool = require('../db');
  const originalQuery = pool.query;
  const record = {
    first_name: 'Jane', last_name: 'Doe', name: 'Jane Doe', company: null,
    job_title: 'Owner', linkedin_url: 'https://www.linkedin.com/in/jane-doe',
    linkedin_headline: 'Jane Doe - Owner', linkedin_location: 'Boston, Massachusetts',
    employee_count_estimate: null, linkedin_source_query: { query: 'test' },
  };
  let queryCalls = 0;
  pool.query = async sql => {
    queryCalls++;
    if (/FROM prospects p/.test(sql)) return { rows: [{
      id: 'existing-id', first_name: 'Jane', last_name: 'Doe', job_title: null,
      linkedin_url: record.linkedin_url, linkedin_headline: null,
      linkedin_location: null, employee_count_estimate: null,
    }] };
    throw new Error('dry-run attempted a write');
  };
  const dryRunOutcome = await _test.saveLinkedInProspect(record, { dryRun: true });
  assert.equal(dryRunOutcome.action, 'enriched_existing');
  assert.equal(queryCalls, 1);

  let prospectSelects = 0;
  let prospectInsert = null;
  pool.query = async (sql, params) => {
    if (/FROM prospects p/.test(sql)) {
      prospectSelects++;
      return { rows: [] };
    }
    if (/INSERT INTO prospects/.test(sql)) {
      prospectInsert = { sql, params };
      return { rows: [{ id: 'new-id' }] };
    }
    throw new Error(`unexpected persistence query: ${sql}`);
  };
  const writeOutcome = await _test.saveLinkedInProspect(record, { dryRun: false });
  assert.equal(writeOutcome.action, 'written');
  assert.equal(writeOutcome.prospectId, 'new-id');
  assert.equal(prospectSelects, 1);
  assert.match(prospectInsert.sql, /client_id, service_area_match, discovery_method/);
  assert.equal(prospectInsert.params[11], null);
  pool.query = originalQuery;

  console.log('LinkedIn Scout source tests passed');
}

run().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
