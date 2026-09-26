'use strict';

/**
 * Live workload diagnostic for Scout Places discovery coverage.
 * Mirrors PlacesProvider.search() exactly (DiscoveryAdapters path) and reports
 * text-search vs details vs website-filter breakdown per workload.
 *
 * Guardrails: no CRM writes, no full API key logging.
 */

const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
const {
  buildDiscoveryPlan,
} = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const {
  fingerprintApiKey,
  railwayRuntimeIdentity,
} = require('./scoutPlacesDiagnostic');
const {
  SCOUT_PLACES_TEXTSEARCH_URL,
  SCOUT_PLACES_DETAILS_URL,
  SCOUT_PLACES_DETAILS_FIELDS,
} = require('./scoutPublicSourcing');

const STR_GREATER_MANCHESTER_DEFINITION = Object.freeze({
  tenantId: '1',
  targetContext: {
    geography: 'Greater Manchester',
    segments: ['short_term_rental'],
    businessType: 'short_term_rental',
    missionBound: true,
  },
  businessContext: {
    serviceGeography: 'Greater Manchester',
    commercialCapability: 'commercial_cleaning',
  },
});

async function readJsonSafe(res) {
  const text = await res.text();
  if (!text) return { data: null, parseError: null };
  try {
    return { data: JSON.parse(text), parseError: null };
  } catch (err) {
    return {
      data: null,
      parseError: err && err.message ? String(err.message) : 'json_parse_failed',
    };
  }
}

/**
 * Probe one query using the same construction as PlacesProvider.search().
 * @param {object} opts
 * @returns {Promise<object>}
 */
async function probePlacesProviderQuery(opts = {}) {
  const apiKey = String(opts.apiKey || process.env.GOOGLE_PLACES_KEY || '');
  const fetchImpl =
    opts.fetchImpl ||
    (typeof fetch === 'function' ? fetch.bind(globalThis) : null);
  const industry = String(opts.industry || '').trim();
  const location = String(opts.location || '').trim();
  const limit = Math.min(Number(opts.limit) || 20, 20);
  const queryText = `${industry} ${location}`.trim();

  const base = {
    queryText,
    industry,
    location,
    httpStatus: null,
    googleStatus: null,
    googleErrorMessage: null,
    textSearchResultCount: 0,
    detailsFetchedCount: 0,
    withWebsiteCount: 0,
    droppedNoWebsiteCount: 0,
    sampleTextSearchNames: [],
    error: null,
  };

  if (!apiKey || !fetchImpl) {
    return {
      ...base,
      error: !apiKey ? 'GOOGLE_PLACES_KEY_missing' : 'fetch_unavailable',
    };
  }
  if (!queryText) {
    return { ...base, error: 'empty_query' };
  }

  const url = new URL(SCOUT_PLACES_TEXTSEARCH_URL);
  url.searchParams.set('query', queryText);
  url.searchParams.set('key', apiKey);

  try {
    const res = await fetchImpl(url.toString());
    base.httpStatus = res.status;
    const { data, parseError } = await readJsonSafe(res);
    if (parseError) {
      return { ...base, error: 'google_response_not_json', googleErrorMessage: parseError };
    }
    base.googleStatus = data?.status || null;
    base.googleErrorMessage = data?.error_message || null;

    if (data?.status !== 'OK' && data?.status !== 'ZERO_RESULTS') {
      return {
        ...base,
        error: `google_places_status_${data?.status || 'unknown'}`,
      };
    }

    const results = (data?.results || []).slice(0, limit);
    base.textSearchResultCount = results.length;
    base.sampleTextSearchNames = results
      .slice(0, 3)
      .map((hit) => hit?.name)
      .filter(Boolean);

    for (const hit of results) {
      const details = await fetchPlaceDetails(hit.place_id, apiKey, fetchImpl);
      base.detailsFetchedCount += 1;
      const website = details?.website || null;
      if (!website) {
        base.droppedNoWebsiteCount += 1;
        continue;
      }
      base.withWebsiteCount += 1;
    }

    return base;
  } catch (err) {
    return {
      ...base,
      error: 'google_places_fetch_failed',
      googleErrorMessage: err?.message || String(err),
    };
  }
}

async function fetchPlaceDetails(placeId, apiKey, fetchImpl) {
  if (!placeId) return null;
  const url = new URL(SCOUT_PLACES_DETAILS_URL);
  url.searchParams.set('place_id', placeId);
  url.searchParams.set('fields', SCOUT_PLACES_DETAILS_FIELDS);
  url.searchParams.set('key', apiKey);
  const res = await fetchImpl(url.toString());
  if (!res.ok) return null;
  const { data } = await readJsonSafe(res);
  return data?.result || null;
}

function buildStrGreaterManchesterPlan() {
  const searchDefinition = buildAcquisitionSearchDefinition(STR_GREATER_MANCHESTER_DEFINITION);
  const adapter = createPlacesDiscoveryAdapter();
  const plan = buildDiscoveryPlan(searchDefinition, {
    adapters: [adapter],
    enabledSources: ['public_business_data'],
  });
  return { searchDefinition, plan, adapterAvailable: adapter.available() };
}

/**
 * Run all workloads for the canonical Greater Manchester STR mission.
 * @param {object} [opts]
 * @returns {Promise<object>}
 */
async function diagnoseStrGreaterManchesterPlacesWorkload(opts = {}) {
  const apiKey = opts.apiKey != null ? String(opts.apiKey) : String(process.env.GOOGLE_PLACES_KEY || '');
  const fetchImpl =
    opts.fetchImpl ||
    (typeof fetch === 'function' ? fetch.bind(globalThis) : null);
  const { searchDefinition, plan, adapterAvailable } = buildStrGreaterManchesterPlan();
  const railway = railwayRuntimeIdentity();
  const workloads = (plan.workloads || []).filter((w) => w.source === 'public_business_data');

  const report = {
    ok: false,
    diagnostic: 'scout_places_str_greater_manchester_workload',
    mission: 'Greater Manchester STR (short_term_rental)',
    adapterAvailable,
    keyPresent: Boolean(apiKey && apiKey.trim()),
    keyFingerprint: fingerprintApiKey(apiKey),
    railway,
    planTotals: plan.totals,
    workloadsPlanned: workloads.length,
    workloads: [],
    summary: {
      executed: 0,
      apiErrors: 0,
      zeroTextSearch: 0,
      textSearchHits: 0,
      droppedByWebsiteFilter: 0,
      finalCandidates: 0,
    },
    conclusion: null,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    placeholdersCreated: false,
    fullKeyLogged: false,
    diagnosedAt: new Date().toISOString(),
  };

  if (!report.keyPresent) {
    report.error = 'GOOGLE_PLACES_KEY_missing';
    report.conclusion =
      'Cannot confirm live Places execution — GOOGLE_PLACES_KEY is not present in this runtime.';
    return report;
  }
  if (!fetchImpl) {
    report.error = 'fetch_unavailable';
    report.conclusion = 'Cannot confirm live Places execution — fetch unavailable.';
    return report;
  }

  for (const workload of workloads) {
    const probe = await probePlacesProviderQuery({
      apiKey,
      fetchImpl,
      industry: String(workload.concept).replace(/_/g, ' '),
      location: workload.city,
    });

    const row = {
      id: workload.id,
      city: workload.city,
      concept: workload.concept,
      queryText: probe.queryText,
      httpStatus: probe.httpStatus,
      googleStatus: probe.googleStatus,
      googleErrorMessage: probe.googleErrorMessage,
      textSearchResultCount: probe.textSearchResultCount,
      detailsFetchedCount: probe.detailsFetchedCount,
      withWebsiteCount: probe.withWebsiteCount,
      droppedNoWebsiteCount: probe.droppedNoWebsiteCount,
      sampleTextSearchNames: probe.sampleTextSearchNames,
      error: probe.error,
      status: probe.error ? 'failed' : 'executed',
    };
    report.workloads.push(row);

    if (row.status === 'executed') {
      report.summary.executed += 1;
      if (row.googleStatus === 'ZERO_RESULTS' || row.textSearchResultCount === 0) {
        report.summary.zeroTextSearch += 1;
      } else {
        report.summary.textSearchHits += 1;
      }
      report.summary.droppedByWebsiteFilter += row.droppedNoWebsiteCount;
      report.summary.finalCandidates += row.withWebsiteCount;
    } else {
      report.summary.apiErrors += 1;
    }
  }

  report.ok = report.summary.apiErrors === 0;

  if (report.summary.apiErrors > 0) {
    report.conclusion =
      'Places queries did not all complete successfully — zero candidates may be caused by API/query failures, not an empty market.';
  } else if (report.summary.textSearchHits > 0 && report.summary.finalCandidates === 0) {
    report.conclusion =
      'Google Places returned businesses for at least one workload, but all were dropped by the website-required filter in PlacesProvider — not a true empty market.';
  } else if (report.summary.zeroTextSearch === workloads.length) {
    report.conclusion =
      'All workloads executed successfully with ZERO_RESULTS (or empty results) from Google Text Search — consistent with a genuinely empty market for these query strings.';
  } else if (report.summary.finalCandidates > 0) {
    report.conclusion =
      'Google Places returned website-bearing businesses for at least one workload.';
  } else {
    report.conclusion =
      'Mixed zero-result workloads executed without API errors; review per-workload breakdown.';
  }

  return report;
}

module.exports = {
  STR_GREATER_MANCHESTER_DEFINITION,
  buildStrGreaterManchesterPlan,
  probePlacesProviderQuery,
  diagnoseStrGreaterManchesterPlacesWorkload,
};
