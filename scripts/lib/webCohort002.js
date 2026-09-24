'use strict';

/**
 * AUDIT-WEB-001 — WEB-COHORT-002 live read-only validation.
 * Scout discovery (leadgen Places/Serp path) + live deterministic audit.
 * NO outreach. NO fixtures.
 */

const {
  configureScoringContext,
  getSearchQueriesForTarget,
  searchGooglePlaces,
  normalizeDomain,
} = require('../../leadgen');
const { getClientConfig } = require('../../utils/clientContext');
const { findMaynardWebClient, ensureMaynardWebMission } = require('../../utils/maynardWebTenant');
const { assessWebsiteOpportunity } = require('../../packages/capabilities/websiteOpportunityIntelligence');
const { assessDiscoveredBusiness } = require('../../services/webDesignScout');
const { insertWebsiteOpportunityEvent } = require('../../services/websiteOpportunityPersistence');
const { emitWebEvent, WEB_EVENT_TYPES } = require('../../packages/capabilities/websiteOpportunityIntelligence/observability');
const {
  formatCohortRow,
  attachMaxPriority,
  countDistribution,
  identifyFalsePositives,
  buildTopFiveDetail,
  compareCohorts,
} = require('./webCohortShared');

const COHORT_TAG = 'WEB-COHORT-002';
const COHORT_SIZE = 25;

/** Geographic/vertical rotation — tests Scout surfacing, not cherry-picked bad sites. */
const DISCOVERY_ROTATION = Object.freeze([
  { location: 'Austin TX', vertical: 'legal', industry: 'law firm' },
  { location: 'Denver CO', vertical: 'accounting', industry: 'accounting firm' },
  { location: 'Nashville TN', vertical: 'dental', industry: 'dental practice' },
  { location: 'Charlotte NC', vertical: 'hvac', industry: 'HVAC company' },
  { location: 'Phoenix AZ', vertical: 'home_services', industry: 'home services company' },
  { location: 'Columbus OH', vertical: 'roofing', industry: 'roofing contractor' },
  { location: 'Portland OR', vertical: 'landscaping', industry: 'landscaping company' },
  { location: 'Raleigh NC', vertical: 'legal', industry: 'law firm' },
  { location: 'Tampa FL', vertical: 'fitness', industry: 'fitness studio' },
  { location: 'Salt Lake City UT', vertical: 'plumbing', industry: 'plumbing company' },
  { location: 'Milwaukee WI', vertical: 'restaurant', industry: 'restaurant' },
  { location: 'Indianapolis IN', vertical: 'salon', industry: 'hair salon' },
  { location: 'Boise ID', vertical: 'electrical', industry: 'electrician' },
  { location: 'Richmond VA', vertical: 'home_renovation', industry: 'home renovation contractor' },
  { location: 'Madison WI', vertical: 'med_spa', industry: 'med spa' },
]);

const SERP_EXCLUDE =
  '-indeed -ziprecruiter -thumbtack -glassdoor -yelp -yellowpages -mapquest -bbb -patch -avvo';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function assertDiscoveryEnv() {
  const hasPlaces = Boolean(process.env.GOOGLE_PLACES_KEY);
  const hasSerp = Boolean(process.env.SERPAPI_KEY);
  if (!hasPlaces && !hasSerp) {
    throw Object.assign(
      new Error('Scout discovery unavailable: set GOOGLE_PLACES_KEY and/or SERPAPI_KEY for live cohort discovery'),
      { code: 'scout_discovery_unavailable' }
    );
  }
  return { hasPlaces, hasSerp };
}

async function discoverCandidatesViaScout(clientId, { targetSize = COHORT_SIZE } = {}) {
  assertDiscoveryEnv();
  const { _test: { searchGoogle } } = require('../../leadgen');
  const candidates = [];
  const seenDomains = new Set();
  const discoveryLog = [];

  for (const pass of DISCOVERY_ROTATION) {
    if (candidates.length >= targetSize) break;

    await configureScoringContext({
      client_id: clientId,
      industry: pass.industry,
      location: pass.location,
      vertical: pass.vertical,
    });

    const queries = getSearchQueriesForTarget();
    for (const query of queries) {
      if (candidates.length >= targetSize) break;

      const batch = [];

      if (process.env.SERPAPI_KEY) {
        const googleQuery = `"${query}" ${SERP_EXCLUDE}`;
        const serpLeads = await searchGoogle(googleQuery, 8);
        for (const lead of serpLeads) {
          batch.push({
            lead,
            discovery: {
              scout_path: 'leadgen.searchGoogle',
              source: ['serpapi'],
              search_query: query,
              vertical: pass.vertical,
              industry: pass.industry,
              location: pass.location,
            },
          });
        }
      }

      if (process.env.GOOGLE_PLACES_KEY) {
        const placesLeads = await searchGooglePlaces(query, '', 8);
        for (const lead of placesLeads) {
          batch.push({
            lead,
            discovery: {
              scout_path: 'leadgen.searchGooglePlaces',
              source: ['google_places'],
              search_query: query,
              vertical: pass.vertical,
              industry: pass.industry,
              location: pass.location,
            },
          });
        }
      }

      for (const { lead, discovery } of batch) {
        const domain = normalizeDomain(lead.url);
        if (!domain || seenDomains.has(domain)) continue;
        seenDomains.add(domain);

        candidates.push({
          company: lead.company,
          domain,
          url: lead.url,
          industry: pass.industry,
          vertical: pass.vertical,
          location: lead.address || pass.location,
          phone: lead.phone || null,
          email: lead.email || null,
          contact: lead.contact || null,
          google_rating: lead.google_rating ?? null,
          google_review_count: lead.google_review_count ?? null,
          discovery: {
            ...discovery,
            cohort_tag: COHORT_TAG,
            discovered_at: new Date().toISOString(),
            acceptance_order: candidates.length + 1,
          },
        });

        discoveryLog.push({
          business: lead.company,
          domain,
          ...discovery,
        });

        if (candidates.length >= targetSize) break;
      }

      await sleep(400);
    }
  }

  return { candidates, discoveryLog };
}

async function runLiveAssessment(candidate, ctx, deps = {}) {
  const input = {
    ...candidate,
    client_id: ctx.clientId,
    mission_id: ctx.missionId,
    cohort_tag: COHORT_TAG,
    skipPuppeteer: deps.skipPuppeteer === true ? true : false,
    skipPageSpeed: false,
  };

  if (deps.persist && deps.pool) {
    const out = await assessDiscoveredBusiness(input, {
      pool: deps.pool,
      skipPuppeteer: input.skipPuppeteer,
    });
    return out.assessment;
  }

  return assessWebsiteOpportunity(input, {
    pool: deps.pool || null,
    skipPuppeteer: input.skipPuppeteer,
    onEvent: async (event) => {
      if (deps.pool) await insertWebsiteOpportunityEvent(deps.pool, event);
    },
  });
}

async function runWebCohort002(pool, options = {}) {
  const targetSize = options.targetSize || COHORT_SIZE;
  const persist = Boolean(options.persist && pool);
  const skipPuppeteer = options.skipPuppeteer === true;

  let client;
  let missionId = null;
  if (pool) {
    const ensured = await ensureMaynardWebMission(pool);
    client = ensured.client;
    missionId = ensured.missionId;
    const clientConfig = await getClientConfig(client.id);
    if (clientConfig?.scoring_profile !== 'web_design') {
      throw Object.assign(new Error(`Expected scoring_profile=web_design, got ${clientConfig?.scoring_profile}`), {
        code: 'wrong_tenant_profile',
      });
    }
  } else {
    client = { id: null, slug: 'maynard-web', scoring_profile: 'web_design' };
  }

  const env = assertDiscoveryEnv();
  const { candidates, discoveryLog } = await discoverCandidatesViaScout(client.id, { targetSize });

  if (candidates.length < targetSize) {
    return {
      cohort_tag: COHORT_TAG,
      client_id: client.id,
      mission_id: missionId,
      live_mode: true,
      fixture_mode: false,
      discovery_env: env,
      incomplete: true,
      discovered_count: candidates.length,
      target_size: targetSize,
      discovery_log: discoveryLog,
      error: `Only discovered ${candidates.length}/${targetSize} candidates via Scout`,
      rows: [],
    };
  }

  const results = [];
  for (const candidate of candidates) {
    const assessment = await runLiveAssessment(candidate, {
      clientId: client.id,
      missionId,
    }, { pool, persist, skipPuppeteer });
    results.push({ candidate, assessment });
    await sleep(1500);
  }

  let rows = results.map(({ candidate, assessment }) =>
    formatCohortRow(candidate, assessment, candidate.discovery)
  );
  rows = attachMaxPriority(rows);
  const distribution = countDistribution(rows);
  const falsePositiveReview = identifyFalsePositives(rows);
  const topFiveDetail = buildTopFiveDetail(rows);

  let fixtureReport = null;
  try {
    fixtureReport = require('../../artifacts/spec-web-001/cohort-report.json');
  } catch {
    fixtureReport = null;
  }
  const cohortComparison = compareCohorts(fixtureReport, { rows, distribution });

  if (pool) {
    await emitWebEvent(WEB_EVENT_TYPES.COHORT_COMPLETED, {
      tenant_id: client.id,
      mission_id: missionId,
      payload: {
        cohort_tag: COHORT_TAG,
        size: rows.length,
        distribution,
        live_mode: true,
        fixture_mode: false,
      },
    }, { pool });
  }

  return {
    cohort_tag: COHORT_TAG,
    client_id: client.id,
    mission_id: missionId,
    live_mode: true,
    fixture_mode: false,
    discovery_env: env,
    discovery_log: discoveryLog,
    rows,
    distribution,
    top_five: topFiveDetail,
    false_positive_review: falsePositiveReview,
    cohort_comparison: cohortComparison,
    stats: {
      total: rows.length,
      no_outreach: true,
      puppeteer_enabled: !skipPuppeteer,
      pagespeed_configured: Boolean(process.env.GOOGLE_API_KEY || process.env.GOOGLE_PLACES_KEY),
    },
  };
}

module.exports = {
  COHORT_TAG,
  COHORT_SIZE,
  DISCOVERY_ROTATION,
  discoverCandidatesViaScout,
  runWebCohort002,
};
