'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  scorePlaceIdentity,
  normalizeWebsite,
  isCanonicalWebsite,
  resolveUpstreamWebsite,
  discoverMissionBoundWebsite,
  buildDiscoveryPlan,
  applyDiscoveryPlan,
  discoveryProvenance,
  nameTokens,
} = require('../packages/max/workspace/MissionBoundWebsiteDiscovery');
const { parseArgs, summarizePlans } = require('../scripts/discoverAnchorMissionBoundWebsites');

const SCRIPT = path.join(__dirname, '..', 'scripts', 'discoverAnchorMissionBoundWebsites.js');
const scriptSource = fs.readFileSync(SCRIPT, 'utf8');

const PLACE_BLUE = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';
const PLACE_WRONG = 'ChIJwrong-company-place-id01';
const MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const CLIENT_ID = 10;

function createMockDb(seed = {}) {
  const companies = new Map((seed.companies || []).map((row) => [String(row.id), { ...row }]));
  const prospects = new Map((seed.prospects || []).map((row) => [String(row.id), { ...row }]));

  return {
    companies,
    prospects,
    query: async (sql, params = []) => {
      const text = String(sql);
      if (/UPDATE companies/.test(text)) {
        const company = companies.get(String(params[3]));
        if (!company) return { rows: [] };
        Object.assign(company, {
          domain: company.domain || params[0],
          website: company.website || params[1],
          enrichment_provenance: {
            ...(company.enrichment_provenance || {}),
            ...(JSON.parse(params[2])),
          },
        });
        return { rows: [company] };
      }
      if (/UPDATE prospects/.test(text)) {
        const prospect = prospects.get(String(params[3]));
        if (!prospect) return { rows: [] };
        Object.assign(prospect, {
          website_url: prospect.website_url || params[0],
          has_website: prospect.has_website || params[1],
          enrichment_provenance: {
            ...(prospect.enrichment_provenance || {}),
            ...(JSON.parse(params[2])),
          },
        });
        return { rows: [prospect] };
      }
      return { rows: [] };
    },
  };
}

describe('MissionBoundWebsiteDiscovery', () => {
  it('1. Place-ID-backed business website resolves correctly', async () => {
    const candidate = {
      candidateId: PLACE_BLUE,
      placeId: PLACE_BLUE,
      company: 'Blue Door Living Property Management',
      location: 'Manchester NH',
    };
    const resolution = await discoverMissionBoundWebsite(candidate, {
      apiKey: 'test-key',
      fetchImpl: async (url) => {
        assert.match(String(url), new RegExp(`place_id=${PLACE_BLUE}`));
        return {
          ok: true,
          status: 200,
          json: async () => ({
            status: 'OK',
            result: {
              place_id: PLACE_BLUE,
              name: 'Blue Door Living Property Management',
              formatted_address: '123 Main St, Manchester, NH 03101',
              website: 'https://www.bluedoorliving.com/',
            },
          }),
        };
      },
    });
    assert.equal(resolution.action, 'resolve');
    assert.equal(resolution.domain, 'bluedoorliving.com');
    assert.equal(resolution.source, 'google_places.place_details');
    assert.ok(scorePlaceIdentity(candidate, {
      place_id: PLACE_BLUE,
      name: 'Blue Door Living Property Management',
    }).confidence >= 4);
  });

  it('2. resolved website normalizes to domain', () => {
    const normalized = normalizeWebsite('https://www.millcitypm.com/about-us');
    assert.equal(normalized.domain, 'millcitypm.com');
    assert.equal(normalized.website, 'https://www.millcitypm.com/about-us');
  });

  it('3. similarly named wrong company is rejected', async () => {
    const candidate = {
      candidateId: PLACE_WRONG,
      placeId: PLACE_WRONG,
      company: 'Blue Door Living Property Management',
      location: 'Manchester NH',
    };
    const resolution = await discoverMissionBoundWebsite(candidate, {
      apiKey: 'test-key',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        json: async () => ({
          status: 'OK',
          result: {
            place_id: PLACE_WRONG,
            name: 'Blue Door Realty Group',
            formatted_address: '999 Other Ave, Boston, MA',
            website: 'https://www.bluedoorrealty.com/',
          },
        }),
      }),
    });
    assert.equal(resolution.action, 'manual_review');
    assert.equal(resolution.source, 'manual_review');
    assert.deepEqual(resolution.matchBasis, ['name_mismatch']);
  });

  it('4. directory/listing URL is not treated as canonical website', () => {
    assert.equal(isCanonicalWebsite('https://www.yelp.com/biz/example'), false);
    assert.equal(isCanonicalWebsite('https://www.facebook.com/example'), false);
    assert.equal(normalizeWebsite('https://www.yelp.com/biz/example'), null);
  });

  it('5. no confident match → manual_review', async () => {
    const candidate = {
      candidateId: 'ChIJlot202-example-place-id01',
      placeId: 'ChIJlot202-example-place-id01',
      company: 'Lot 202 - Property Management Company',
      location: 'Manchester NH',
    };
    const resolution = await discoverMissionBoundWebsite(candidate, {
      apiKey: 'test-key',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        json: async () => ({ status: 'OK', result: { place_id: candidate.placeId, name: candidate.company } }),
      }),
    });
    assert.equal(resolution.action, 'manual_review');
    assert.equal(resolution.reason, 'no_confident_website_match');
  });

  it('6. no guessed domains from company name', () => {
    const tokens = nameTokens('Blue Door Living Property Management');
    assert.ok(tokens.includes('blue'));
    assert.ok(tokens.includes('door'));
    assert.ok(tokens.includes('living'));
    const upstream = resolveUpstreamWebsite({
      company: 'Lot 202 - Property Management Company',
      domain: null,
      website: null,
    });
    assert.equal(upstream, null);
  });

  it('7. existing stronger CRM website is not overwritten', async () => {
    const candidate = {
      candidateId: PLACE_BLUE,
      placeId: PLACE_BLUE,
      company: 'Blue Door Living Property Management',
    };
    const crmRow = {
      company_id: 'co-1',
      prospect_id: 'pr-1',
      domain: 'existing-strong.com',
      website: 'https://existing-strong.com/',
      website_url: 'https://existing-strong.com/',
      google_place_id: PLACE_BLUE,
    };
    const resolution = await discoverMissionBoundWebsite(candidate, {
      crmRow,
      apiKey: 'test-key',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        json: async () => ({
          status: 'OK',
          result: {
            place_id: PLACE_BLUE,
            name: candidate.company,
            website: 'https://www.bluedoorliving.com/',
          },
        }),
      }),
    });
    assert.equal(resolution.action, 'skip_existing_website');
    const plan = buildDiscoveryPlan(candidate, crmRow, resolution, MISSION_ID);
    assert.equal(plan.action, 'skip_existing_website');
    assert.equal(plan.persisted, false);
  });

  it('8. provenance persisted on apply', async () => {
    const db = createMockDb({
      companies: [{ id: 'co-1', client_id: CLIENT_ID, domain: null, website: null }],
      prospects: [{ id: 'pr-1', client_id: CLIENT_ID, website_url: null, has_website: false }],
    });
    const candidate = {
      candidateId: PLACE_BLUE,
      placeId: PLACE_BLUE,
      company: 'Blue Door Living Property Management',
    };
    const resolution = {
      action: 'resolve',
      website: 'https://www.bluedoorliving.com/',
      domain: 'bluedoorliving.com',
      source: 'google_places.place_details',
      resolver: 'google_places',
      confidence: 7,
      matchBasis: ['place_id', 'company_name'],
      evidence: { placeId: PLACE_BLUE },
    };
    const plan = buildDiscoveryPlan(candidate, {
      company_id: 'co-1',
      prospect_id: 'pr-1',
      domain: null,
      website: null,
      website_url: null,
    }, resolution, MISSION_ID);
    assert.equal(plan.action, 'persist');
    const applied = await applyDiscoveryPlan(db, plan, false, CLIENT_ID);
    assert.equal(applied.persisted, true);
    const provenance = db.companies.get('co-1').enrichment_provenance;
    assert.equal(provenance.mission_bound_website_discovery.mission_id, MISSION_ID);
    assert.equal(provenance.mission_bound_website_discovery.domain, 'bluedoorliving.com');
    assert.equal(provenance.mission_bound_website_discovery.place_id, PLACE_BLUE);
    assert.ok(Array.isArray(provenance.mission_bound_website_discovery.match_basis));
  });

  it('9. tenant isolation via client_id in apply queries', async () => {
    const db = createMockDb({
      companies: [{ id: 'co-1', client_id: CLIENT_ID }],
      prospects: [{ id: 'pr-1', client_id: CLIENT_ID }],
    });
    const queries = [];
    const wrapped = {
      query: async (sql, params) => {
        queries.push({ sql, params });
        return db.query(sql, params);
      },
    };
    const plan = buildDiscoveryPlan(
      { candidateId: PLACE_BLUE, placeId: PLACE_BLUE, company: 'Blue Door Living Property Management' },
      { company_id: 'co-1', prospect_id: 'pr-1' },
      {
        action: 'resolve',
        website: 'https://www.bluedoorliving.com/',
        domain: 'bluedoorliving.com',
        source: 'google_places.place_details',
        confidence: 7,
        matchBasis: ['place_id'],
      },
      MISSION_ID
    );
    await applyDiscoveryPlan(wrapped, plan, false, CLIENT_ID);
    assert.equal(queries.length, 2);
    assert.equal(queries[0].params[4], CLIENT_ID);
    assert.equal(queries[1].params[4], CLIENT_ID);
  });

  it('10. discovery script has no outbound side effects', () => {
    assert.doesNotMatch(scriptSource, /api\.brevo\.com\/v3\/smtp/);
    assert.doesNotMatch(scriptSource, /sendEmail\s*\(/);
    assert.doesNotMatch(scriptSource, /routeExecutionRequest/);
    assert.doesNotMatch(scriptSource, /EXECUTE_OUTBOUND/);
    assert.doesNotMatch(scriptSource, /UPDATE\s+clients/i);
    assert.doesNotMatch(scriptSource, /enrichProspectRow/);
    assert.doesNotMatch(scriptSource, /runEnrichmentChain/);
    assert.match(scriptSource, /No mail side effects|No mail/i);
  });
});

describe('discoverAnchorMissionBoundWebsites script', () => {
  it('defaults to dry-run and accepts --confirm-production', () => {
    assert.equal(parseArgs([]).dryRun, true);
    assert.equal(parseArgs(['--confirm-production']).dryRun, false);
  });

  it('summarizePlans counts resolved and manual review rows', () => {
    const summary = summarizePlans([
      { currentDomain: null, currentWebsite: null, action: 'persist', resolvedDomain: 'a.com', persisted: true },
      { currentDomain: null, currentWebsite: null, action: 'manual_review', resolvedDomain: null },
      { currentDomain: 'x.com', currentWebsite: 'https://x.com', action: 'skip_existing_website' },
    ]);
    assert.equal(summary.candidateCount, 3);
    assert.equal(summary.missingWebsiteCount, 2);
    assert.equal(summary.persistedWebsiteCount, 1);
    assert.equal(summary.manualReviewCount, 1);
  });

  it('discoveryProvenance includes resolver and evidence refs', () => {
    const provenance = discoveryProvenance(
      { candidateId: PLACE_BLUE, placeId: PLACE_BLUE, company: 'Blue Door Living Property Management' },
      MISSION_ID,
      {
        source: 'google_places.place_details',
        resolver: 'google_places',
        domain: 'bluedoorliving.com',
        website: 'https://www.bluedoorliving.com/',
        confidence: 7,
        matchBasis: ['place_id'],
        evidence: { placeId: PLACE_BLUE },
      }
    );
    assert.equal(provenance.mission_bound_website_discovery.resolver, 'google_places');
    assert.equal(provenance.mission_bound_website_discovery.evidence.placeId, PLACE_BLUE);
  });
});
