'use strict';

/**
 * Google Places discovery provider (SPEC-024).
 * SPEC-181 — evidence-native execution: query generation lives here, not in Scout.
 * Capability-layer adapter — no Scout/agent module imports.
 */

const {
  resolveMarketHypothesisBySegmentKey,
  expandSearchStrategies,
  SEARCH_SOURCES,
} = require('../../../scout/hypothesis/MarketHypothesisRegistry');
const { INVESTIGATIVE_EVIDENCE } = require('../../../scout/coverage/EvidenceRequirements');

function isValidEvidenceRequest(value) {
  return (
    value &&
    typeof value === 'object' &&
    typeof value.segment === 'string' &&
    typeof value.evidenceType === 'string' &&
    value.geography &&
    Array.isArray(value.geography.cities)
  );
}

const DEFAULT_LIMIT = 20;
const MAX_RETRIES = 2;
const RETRYABLE_GOOGLE_STATUS = new Set(['OVER_QUERY_LIMIT', 'UNKNOWN_ERROR']);
const SUCCESS_GOOGLE_STATUS = new Set(['OK', 'ZERO_RESULTS']);

function createProviderExecution(partial = {}) {
  return {
    providerId: 'google_places',
    executed: partial.executed === true,
    abortReason: partial.abortReason || null,
    queries: Array.isArray(partial.queries) ? partial.queries : [],
    totals: partial.totals || {
      queries: 0,
      results: 0,
      retries: 0,
      latencyMs: 0,
    },
    errors: Array.isArray(partial.errors) ? partial.errors : [],
    quota: partial.quota || null,
  };
}

/**
 * @param {object} [deps]
 * @param {string} [deps.apiKey]
 * @param {typeof fetch} [deps.fetchImpl]
 */
function createPlacesProvider(deps = {}) {
  const apiKey = deps.apiKey || process.env.GOOGLE_PLACES_KEY || '';
  const fetchImpl =
    deps.fetchImpl ||
    (typeof fetch === 'function'
      ? fetch.bind(globalThis)
      : null);

  const provider = {
    id: 'google_places',
    lastExecution: null,
    available() {
      return !!(apiKey && fetchImpl);
    },

    /**
     * SPEC-181 — collect evidence from a structured request.
     * Provider owns query generation, pagination, retries, and localization.
     *
     * @param {object} evidenceRequest
     * @param {string} evidenceRequest.segment
     * @param {string} evidenceRequest.evidenceType
     * @param {object} evidenceRequest.geography
     * @param {string[]} evidenceRequest.geography.cities
     * @param {string|null} [evidenceRequest.geography.state]
     * @returns {Promise<object[]>}
     */
    async collectEvidence(evidenceRequest) {
      if (!this.available()) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'provider_unavailable',
          errors: [{ code: 'provider_unavailable', message: 'Google Places provider unavailable.' }],
        });
        return [];
      }
      if (!isValidEvidenceRequest(evidenceRequest)) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'invalid_evidence_request',
          errors: [{ code: 'invalid_evidence_request', message: 'Evidence request missing segment, evidenceType, or geography.cities.' }],
        });
        return [];
      }

      const { segment, evidenceType, geography = {} } = evidenceRequest;
      const cities = geography.cities || [];
      const state = geography.state || null;
      if (!cities.length) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'empty_geography',
          errors: [{ code: 'empty_geography', message: 'Evidence request geography.cities is empty — Places was not called.' }],
        });
        return [];
      }

      const queries = buildQueriesForEvidence({ segment, evidenceType, cities, state });
      const requireWebsite = evidenceType !== INVESTIGATIVE_EVIDENCE.IDENTITY;
      const limit = DEFAULT_LIMIT;
      const out = [];
      const seenPlaceIds = new Set();
      const queryRecords = [];
      const errors = [];
      let totalRetries = 0;
      let totalLatency = 0;
      let quota = null;

      for (const query of queries) {
        const searchResult = await searchWithRetry(
          { industry: query, location: '', limit },
          apiKey,
          fetchImpl,
          { requireWebsite, seenPlaceIds, out }
        );
        queryRecords.push(...(searchResult.attempts || []));
        totalRetries += searchResult.retries || 0;
        totalLatency += searchResult.latencyMs || 0;
        if (searchResult.quota) quota = searchResult.quota;
        if (searchResult.error) errors.push(searchResult.error);
      }

      this.lastExecution = createProviderExecution({
        executed: queryRecords.some(
          (row) => row.httpStatus != null || row.googleStatus != null || row.googleError
        ),
        abortReason: queryRecords.length ? null : 'no_queries_generated',
        queries: queryRecords,
        totals: {
          queries: queryRecords.length,
          results: queryRecords.reduce((sum, row) => sum + (row.resultCount || 0), 0),
          retries: totalRetries,
          latencyMs: totalLatency,
        },
        errors,
        quota,
      });

      return out;
    },

    /**
     * Legacy keyword search — retained for cron/backward compatibility until Phase 3 migration.
     * @param {object} query
     * @param {string} query.industry
     * @param {string} query.location
     * @param {number} [query.limit]
     * @returns {Promise<object[]>}
     */
    async search(query) {
      if (!this.available()) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'provider_unavailable',
          errors: [{ code: 'provider_unavailable', message: 'Google Places provider unavailable.' }],
        });
        return [];
      }
      const limit = Math.min(Number(query.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);
      const q = `${query.industry || ''} ${query.location || ''}`.trim();
      if (!q) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'empty_query',
          errors: [{ code: 'empty_query', message: 'Legacy Places search had no industry/location.' }],
        });
        return [];
      }

      const out = [];
      const seenPlaceIds = new Set();
      const searchResult = await searchWithRetry(
        { industry: q, location: '', limit },
        apiKey,
        fetchImpl,
        { requireWebsite: true, seenPlaceIds, out }
      );
      this.lastExecution = createProviderExecution({
        executed: true,
        queries: searchResult.attempts || [],
        totals: {
          queries: (searchResult.attempts || []).length,
          results: (searchResult.attempts || []).reduce((sum, row) => sum + (row.resultCount || 0), 0),
          retries: searchResult.retries || 0,
          latencyMs: searchResult.latencyMs || 0,
        },
        errors: searchResult.error ? [searchResult.error] : [],
        quota: searchResult.quota || null,
      });
      return out;
    },
  };
  return provider;
}

/**
 * Derive Places query strings from segment + evidence type.
 * Query templates live in the market hypothesis registry — not in Scout execution.
 */
function buildQueriesForEvidence({ segment, evidenceType, cities, state }) {
  const hypothesis = resolveMarketHypothesisBySegmentKey(segment);
  const geoRows = cities.map((city) => ({ city, state: state || '' }));

  if (hypothesis) {
    const sourceFilter =
      evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY
        ? [SEARCH_SOURCES.GOOGLE_PLACES, SEARCH_SOURCES.PUBLIC_BUSINESS_DATA]
        : [SEARCH_SOURCES.GOOGLE_PLACES];

    const seen = new Set();
    const queries = [];
    for (const geo of geoRows) {
      const workloads = expandSearchStrategies(hypothesis, geo, { sources: sourceFilter });
      for (const row of workloads) {
        if (!row.query || seen.has(row.query)) continue;
        seen.add(row.query);
        queries.push(row.query);
      }
    }
    if (queries.length) return queries;
  }

  // Fallback when no registry hypothesis — provider still owns query shape.
  const segmentLabel = String(segment || 'business').replace(/_/g, ' ');
  return geoRows.map((geo) => {
    const loc = geo.state ? `${geo.city} ${geo.state}` : geo.city;
    return `${segmentLabel} ${loc}`.trim();
  });
}

async function searchWithRetry(querySpec, apiKey, fetchImpl, opts = {}) {
  const { requireWebsite = true, seenPlaceIds, out } = opts;
  const attempts = [];
  let retries = 0;
  let latencyMs = 0;
  let quota = null;
  let error = null;
  let nextPageToken = null;
  let attempt = 0;

  while (attempt <= MAX_RETRIES) {
    attempt += 1;
    const page = await fetchPlacesPage(querySpec, apiKey, fetchImpl, nextPageToken);
    attempts.push(page.attempt);
    latencyMs += page.attempt.latencyMs || 0;
    if (page.attempt.quota) quota = page.attempt.quota;

    if (page.failed) {
      error = {
        code: page.attempt.googleStatus
          ? `google_places_status_${page.attempt.googleStatus}`
          : 'google_places_fetch_failed',
        message: page.attempt.googleError || page.attempt.googleStatus || 'Places Text Search failed',
        query: page.attempt.query,
        httpStatus: page.attempt.httpStatus,
        googleStatus: page.attempt.googleStatus,
      };
      const retryable = RETRYABLE_GOOGLE_STATUS.has(page.attempt.googleStatus);
      if (retryable && attempt <= MAX_RETRIES) {
        retries += 1;
        continue;
      }
      break;
    }

    for (const hit of page.results || []) {
      if (seenPlaceIds.has(hit.place_id)) continue;
      const details = await fetchPlaceDetails(hit.place_id, apiKey, fetchImpl);
      const website = details?.website || null;
      if (requireWebsite && !website) continue;

      seenPlaceIds.add(hit.place_id);
      out.push({
        companyName: details?.name || hit.name || 'Unknown',
        website: website ? normalizeDomain(website) : null,
        phone: details?.formatted_phone_number || null,
        address: details?.formatted_address || hit.formatted_address || '',
        placeId: details?.place_id || hit.place_id,
        placeTypes: details?.types || hit.types || [],
        googleRating: details?.rating ?? hit.rating ?? null,
        source: 'google_places',
        industry: querySpec.industry || null,
        snippet: '',
      });
    }

    nextPageToken = page.nextPageToken || null;
    if (!nextPageToken) break;
  }

  return { attempts, retries, latencyMs, quota, error };
}

async function fetchPlacesPage(querySpec, apiKey, fetchImpl, pageToken = null) {
  const q = `${querySpec.industry || ''} ${querySpec.location || ''}`.trim();
  const started = Date.now();
  if (!q) {
    return {
      results: [],
      nextPageToken: null,
      failed: true,
      attempt: {
        query: '',
        httpStatus: null,
        googleStatus: null,
        googleError: 'empty_query',
        resultCount: 0,
        latencyMs: 0,
        retries: 0,
        quota: null,
      },
    };
  }

  const url = new URL('https://maps.googleapis.com/maps/api/place/textsearch/json');
  url.searchParams.set('query', q);
  url.searchParams.set('key', apiKey);
  if (pageToken) url.searchParams.set('pagetoken', pageToken);

  try {
    const res = await fetchImpl(url.toString());
    const latencyMs = Date.now() - started;
    const httpStatus = res.status;
    if (!res.ok) {
      return {
        results: [],
        nextPageToken: null,
        failed: true,
        attempt: {
          query: q,
          httpStatus,
          googleStatus: null,
          googleError: `http_${httpStatus}`,
          resultCount: 0,
          latencyMs,
          retries: 0,
          quota: null,
        },
      };
    }

    const data = await res.json();
    const googleStatus = data.status || null;
    const googleError = data.error_message || null;
    const ok = SUCCESS_GOOGLE_STATUS.has(googleStatus);
    const limit = Math.min(Number(querySpec.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);
    const results = ok ? (data.results || []).slice(0, limit) : [];
    return {
      results,
      nextPageToken: ok ? data.next_page_token || null : null,
      failed: !ok,
      attempt: {
        query: q,
        httpStatus,
        googleStatus,
        googleError,
        resultCount: Array.isArray(data.results) ? data.results.length : 0,
        latencyMs,
        retries: 0,
        quota: googleStatus === 'OVER_QUERY_LIMIT' ? { status: googleStatus } : null,
      },
    };
  } catch (err) {
    return {
      results: [],
      nextPageToken: null,
      failed: true,
      attempt: {
        query: q,
        httpStatus: null,
        googleStatus: null,
        googleError: err.message || String(err),
        resultCount: 0,
        latencyMs: Date.now() - started,
        retries: 0,
        quota: null,
      },
    };
  }
}

async function fetchPlaceDetails(placeId, apiKey, fetchImpl) {
  if (!placeId) return null;
  const url = new URL(
    'https://maps.googleapis.com/maps/api/place/details/json'
  );
  url.searchParams.set('place_id', placeId);
  url.searchParams.set(
    'fields',
    'name,formatted_address,formatted_phone_number,website,place_id,types,rating,address_component,business_status'
  );
  url.searchParams.set('key', apiKey);
  const res = await fetchImpl(url.toString());
  if (!res.ok) return null;
  const data = await res.json();
  return data.result || null;
}

function normalizeDomain(website) {
  try {
    const u = new URL(
      String(website).startsWith('http') ? website : `https://${website}`
    );
    return u.hostname.replace(/^www\./, '');
  } catch {
    return String(website)
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split('/')[0];
  }
}

module.exports = {
  createPlacesProvider,
  normalizeDomain,
  buildQueriesForEvidence,
  createProviderExecution,
};
