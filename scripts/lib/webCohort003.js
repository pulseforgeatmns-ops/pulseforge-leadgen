'use strict';

/**
 * SPEC-WEB-001A — WEB-COHORT-003 live read-only revalidation.
 * Stratified Scout discovery + admission filters + live deterministic audit.
 * NO outreach. NO fixtures.
 */

const {
  configureScoringContext,
  getSearchQueriesForTarget,
  searchGooglePlaces,
} = require('../../leadgen');
const { getClientConfig } = require('../../utils/clientContext');
const { findMaynardWebClient, ensureMaynardWebMission } = require('../../utils/maynardWebTenant');
const { assessWebsiteOpportunity } = require('../../packages/capabilities/websiteOpportunityIntelligence');
const { assessDiscoveredBusiness } = require('../../services/webDesignScout');
const { insertWebsiteOpportunityEvent } = require('../../services/websiteOpportunityPersistence');
const { emitWebEvent, WEB_EVENT_TYPES } = require('../../packages/capabilities/websiteOpportunityIntelligence/observability');
const {
  evaluateCohortAdmission,
  assembleStratifiedCohort,
  normalizeCompanyKey,
} = require('../../packages/capabilities/websiteOpportunityIntelligence/discoveryAdmission');
const { partitionEvidence } = require('../../packages/capabilities/websiteOpportunityIntelligence/evidence');
const {
  formatCohortRow,
  attachMaxPriority,
  countDistribution,
  identifyFalsePositives,
  buildTopFiveDetail,
  compareCohorts,
} = require('./webCohortShared');

const COHORT_TAG = 'WEB-COHORT-003';
const COHORT_SIZE = 25;
const CANDIDATES_PER_STRATUM = 12;

const DISCOVERY_STRATA = Object.freeze([
  { key: 'austin_legal', location: 'Austin TX', vertical: 'legal', industry: 'law firm' },
  { key: 'denver_accounting', location: 'Denver CO', vertical: 'accounting', industry: 'accounting firm' },
  { key: 'nashville_dental', location: 'Nashville TN', vertical: 'dental', industry: 'dental practice' },
  { key: 'charlotte_hvac', location: 'Charlotte NC', vertical: 'hvac', industry: 'HVAC company' },
  { key: 'phoenix_home_services', location: 'Phoenix AZ', vertical: 'home_services', industry: 'home services company' },
  { key: 'columbus_roofing', location: 'Columbus OH', vertical: 'roofing', industry: 'roofing contractor' },
  { key: 'portland_landscaping', location: 'Portland OR', vertical: 'landscaping', industry: 'landscaping company' },
  { key: 'raleigh_legal', location: 'Raleigh NC', vertical: 'legal', industry: 'law firm' },
  { key: 'tampa_fitness', location: 'Tampa FL', vertical: 'fitness', industry: 'fitness studio' },
  { key: 'salt_lake_plumbing', location: 'Salt Lake City UT', vertical: 'plumbing', industry: 'plumbing company' },
  { key: 'milwaukee_restaurant', location: 'Milwaukee WI', vertical: 'restaurant', industry: 'restaurant' },
  { key: 'indianapolis_salon', location: 'Indianapolis IN', vertical: 'salon', industry: 'hair salon' },
  { key: 'boise_electrical', location: 'Boise ID', vertical: 'electrical', industry: 'electrician' },
  { key: 'richmond_renovation', location: 'Richmond VA', vertical: 'home_renovation', industry: 'home renovation contractor' },
  { key: 'madison_med_spa', location: 'Madison WI', vertical: 'med_spa', industry: 'med spa' },
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

async function discoverStratumCandidates(stratum, clientId) {
  const { _test: { searchGoogle } } = require('../../leadgen');
  const batch = [];

  await configureScoringContext({
    client_id: clientId,
    industry: stratum.industry,
    location: stratum.location,
    vertical: stratum.vertical,
  });

  const queries = getSearchQueriesForTarget();
  for (const query of queries) {
    if (batch.length >= CANDIDATES_PER_STRATUM) break;

    if (process.env.SERPAPI_KEY) {
      const googleQuery = `"${query}" ${SERP_EXCLUDE}`;
      const serpLeads = await searchGoogle(googleQuery, 8);
      for (const lead of serpLeads) {
        batch.push({
          ...lead,
          vertical: stratum.vertical,
          industry: stratum.industry,
          location: lead.address || stratum.location,
          discovery: {
            scout_path: 'leadgen.searchGoogle',
            source: ['serpapi'],
            search_query: query,
            vertical: stratum.vertical,
            industry: stratum.industry,
            location: stratum.location,
            stratum: stratum.key,
          },
        });
      }
    }

    if (process.env.GOOGLE_PLACES_KEY) {
      const placesLeads = await searchGooglePlaces(query, '', 8);
      for (const lead of placesLeads) {
        batch.push({
          ...lead,
          vertical: stratum.vertical,
          industry: stratum.industry,
          location: lead.address || stratum.location,
          discovery: {
            scout_path: 'leadgen.searchGooglePlaces',
            source: ['google_places'],
            search_query: query,
            vertical: stratum.vertical,
            industry: stratum.industry,
            location: stratum.location,
            stratum: stratum.key,
          },
        });
      }
    }

    await sleep(350);
  }

  return batch.slice(0, CANDIDATES_PER_STRATUM);
}

async function discoverStratifiedCandidates(clientId, { targetSize = COHORT_SIZE } = {}) {
  assertDiscoveryEnv();
  const stratumPools = [];

  for (const stratum of DISCOVERY_STRATA) {
    const candidates = await discoverStratumCandidates(stratum, clientId);
    stratumPools.push({
      key: stratum.key,
      vertical: stratum.vertical,
      location: stratum.location,
      candidates,
    });
  }

  const assembled = assembleStratifiedCohort(stratumPools, {
    targetSize,
    minStrata: 3,
  });

  return {
    candidates: assembled.admitted.map((c) => ({
      ...c,
      discovery: {
        ...(c.discovery || {}),
        cohort_tag: COHORT_TAG,
        discovered_at: new Date().toISOString(),
      },
    })),
    rejected: assembled.rejected,
    composition: assembled.composition,
    vertical_distribution: assembled.vertical_distribution,
    market_distribution: assembled.market_distribution,
    stratification_met: assembled.stratification_met,
  };
}

async function runLiveAssessment(candidate, ctx, deps = {}) {
  const input = {
    ...candidate,
    client_id: ctx.clientId,
    mission_id: ctx.missionId,
    cohort_tag: COHORT_TAG,
    skipPuppeteer: deps.skipPuppeteer === true,
    skipPageSpeed: false,
    buying_signal_research: 'not_researched',
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

function aggregatePsiTelemetry(results) {
  const totals = { psi_attempted: 0, psi_success: 0, psi_failed: 0, psi_unknown: 0 };
  for (const { assessment } of results) {
    const t = assessment?.technical_evidence?.psi_telemetry
      || assessment?.psi_telemetry
      || {};
    totals.psi_attempted += t.psi_attempted || 0;
    totals.psi_success += t.psi_success || 0;
    totals.psi_failed += t.psi_failed || 0;
    totals.psi_unknown += t.psi_unknown || 0;
  }
  return totals;
}

function diagnosisDistribution(rows) {
  const out = {};
  for (const row of rows) {
    const dc = row.raw_payload?.commercial_diagnosis?.diagnosis_class || 'unknown';
    out[dc] = (out[dc] || 0) + 1;
  }
  return out;
}

function evidenceCompleteness(rows) {
  let measured = 0;
  let observed = 0;
  let inferred = 0;
  let unknown = 0;
  for (const row of rows) {
    const parts = partitionEvidence(
      row.raw_payload?.assessment?.verified_findings
      || row.raw_payload?.evidence_refs
      || []
    );
    measured += parts.measured.length;
    observed += parts.observed.length;
    inferred += parts.inferred.length;
    unknown += parts.unknown.length;
  }
  const n = rows.length || 1;
  return {
    avg_measured: Math.round((measured / n) * 10) / 10,
    avg_observed: Math.round((observed / n) * 10) / 10,
    avg_inferred: Math.round((inferred / n) * 10) / 10,
    avg_unknown: Math.round((unknown / n) * 10) / 10,
  };
}

function maxPriorityDistribution(rows) {
  const scores = rows.map((r) => r.max_priority).filter((v) => v != null);
  if (!scores.length) return { min: null, max: null, unique: 0 };
  return {
    min: Math.min(...scores),
    max: Math.max(...scores),
    unique: new Set(scores.map((s) => Math.round(s * 100) / 100)).size,
  };
}

async function runWebCohort003(pool, options = {}) {
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
  const discovery = await discoverStratifiedCandidates(client.id, { targetSize });

  if (discovery.candidates.length < targetSize) {
    return {
      cohort_tag: COHORT_TAG,
      client_id: client.id,
      mission_id: missionId,
      live_mode: true,
      fixture_mode: false,
      discovery_env: env,
      incomplete: true,
      discovered_count: discovery.candidates.length,
      target_size: targetSize,
      rejected_candidates: discovery.rejected,
      cohort_composition: discovery.composition,
      error: `Only admitted ${discovery.candidates.length}/${targetSize} candidates via stratified Scout discovery`,
      rows: [],
    };
  }

  const results = [];
  for (const candidate of discovery.candidates) {
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

  const psiTelemetry = aggregatePsiTelemetry(results);

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
        psi_telemetry: psiTelemetry,
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
    rejected_candidates: discovery.rejected,
    cohort_composition: discovery.composition,
    vertical_distribution: discovery.vertical_distribution,
    market_distribution: discovery.market_distribution,
    stratification_met: discovery.stratification_met,
    rows,
    distribution,
    diagnosis_distribution: diagnosisDistribution(rows),
    evidence_completeness: evidenceCompleteness(rows),
    psi_telemetry: psiTelemetry,
    max_priority_distribution: maxPriorityDistribution(rows),
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
  DISCOVERY_STRATA,
  evaluateCohortAdmission,
  discoverStratifiedCandidates,
  runWebCohort003,
};
