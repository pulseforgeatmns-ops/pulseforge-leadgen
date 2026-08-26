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

  return {
    id: 'google_places',
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
      if (!this.available() || !isValidEvidenceRequest(evidenceRequest)) return [];

      const { segment, evidenceType, geography = {} } = evidenceRequest;
      const cities = geography.cities || [];
      const state = geography.state || null;
      if (!cities.length) return [];

      const queries = buildQueriesForEvidence({ segment, evidenceType, cities, state });
      const requireWebsite = evidenceType !== INVESTIGATIVE_EVIDENCE.IDENTITY;
      const limit = DEFAULT_LIMIT;
      const out = [];
      const seenPlaceIds = new Set();

      for (const query of queries) {
        const hits = await searchWithRetry(
          { industry: query, location: '', limit },
          apiKey,
          fetchImpl,
          { requireWebsite, seenPlaceIds, out }
        );
        void hits;
      }

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
      if (!this.available()) return [];
      const limit = Math.min(Number(query.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);
      const q = `${query.industry || ''} ${query.location || ''}`.trim();
      if (!q) return [];

      const out = [];
      const seenPlaceIds = new Set();
      await searchWithRetry(
        { industry: q, location: '', limit },
        apiKey,
        fetchImpl,
        { requireWebsite: true, seenPlaceIds, out }
      );
      return out;
    },
  };
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
  let attempt = 0;
  let nextPageToken = null;

  while (attempt <= MAX_RETRIES) {
    attempt += 1;
    try {
      const page = await fetchPlacesPage(querySpec, apiKey, fetchImpl, nextPageToken);
      if (!page) break;

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
    } catch {
      if (attempt > MAX_RETRIES) break;
    }
  }

  return out;
}

async function fetchPlacesPage(querySpec, apiKey, fetchImpl, pageToken = null) {
  const q = `${querySpec.industry || ''} ${querySpec.location || ''}`.trim();
  if (!q) return null;

  const url = new URL('https://maps.googleapis.com/maps/api/place/textsearch/json');
  url.searchParams.set('query', q);
  url.searchParams.set('key', apiKey);
  if (pageToken) url.searchParams.set('pagetoken', pageToken);

  const res = await fetchImpl(url.toString());
  if (!res.ok) return null;
  const data = await res.json();
  if (data.status !== 'OK' && data.status !== 'ZERO_RESULTS') {
    return null;
  }

  const limit = Math.min(Number(querySpec.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);
  return {
    results: (data.results || []).slice(0, limit),
    nextPageToken: data.next_page_token || null,
  };
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
};
