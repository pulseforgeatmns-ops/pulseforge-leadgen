'use strict';

/**
 * Traced Google Places / Geocoding HTTP client (AUDIT-063).
 * All production Places traffic should route through these helpers.
 */

const axios = require('axios');
const {
  PLACES_ENDPOINTS,
  recordPlacesRequest,
} = require('./placesCostAttribution');

async function readFetchJson(res) {
  if (res && typeof res.json === 'function') return res.json();
  if (res && typeof res.text === 'function') {
    const text = await res.text();
    return text ? JSON.parse(text) : null;
  }
  return null;
}

const LEGACY_TEXT_SEARCH = 'https://maps.googleapis.com/maps/api/place/textsearch/json';
const LEGACY_PLACE_DETAILS = 'https://maps.googleapis.com/maps/api/place/details/json';
const LEGACY_NEARBY_SEARCH = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json';
const LEGACY_AUTOCOMPLETE = 'https://maps.googleapis.com/maps/api/place/autocomplete/json';
const LEGACY_FIND_PLACE = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json';
const GEOCODE_URL = 'https://maps.googleapis.com/maps/api/geocode/json';
const V1_SEARCH_TEXT = 'https://places.googleapis.com/v1/places:searchText';
const V1_PLACE_DETAILS = 'https://places.googleapis.com/v1/places';

async function tracedRequest(recordBase, requestFn, deps = {}) {
  const started = Date.now();
  let httpStatus = null;
  let googleStatus = null;

  try {
    const result = await requestFn();
    httpStatus = result.httpStatus ?? null;
    googleStatus = result.googleStatus ?? null;
    return result;
  } catch (err) {
    httpStatus = err.response?.status ?? null;
    googleStatus = err.response?.data?.status || err.response?.data?.error?.status || null;
    throw err;
  } finally {
    const latencyMs = Date.now() - started;
    try {
      await recordPlacesRequest(
        {
          ...recordBase,
          httpStatus,
          googleStatus,
          latencyMs,
        },
        deps
      );
    } catch (err) {
      console.warn('[placesApi] attribution record failed:', err.message);
    }
  }
}

/**
 * Legacy Places Text Search (maps/api/place/textsearch/json).
 */
async function legacyTextSearch(params = {}, deps = {}) {
  const { query, apiKey, pageToken = null, fetchImpl, timeout = 30000 } = params;
  const useFetch = typeof fetchImpl === 'function';

  return tracedRequest(
    {
      endpoint: PLACES_ENDPOINTS.TEXT_SEARCH,
      ...params.record,
    },
    async () => {
      if (useFetch) {
        const url = new URL(LEGACY_TEXT_SEARCH);
        url.searchParams.set('query', String(query || ''));
        url.searchParams.set('key', apiKey);
        if (pageToken) url.searchParams.set('pagetoken', pageToken);
        const res = await fetchImpl(url.toString());
        const data = await readFetchJson(res);
        return {
          httpStatus: res.status,
          googleStatus: data?.status || null,
          data,
          ok: res.ok,
        };
      }

      const axiosParams = { query, key: apiKey };
      if (pageToken) axiosParams.pagetoken = pageToken;
      const res = await axios.get(LEGACY_TEXT_SEARCH, {
        params: axiosParams,
        timeout,
      });
      return {
        httpStatus: res.status,
        googleStatus: res.data?.status || null,
        data: res.data,
        ok: res.status >= 200 && res.status < 300,
      };
    },
    deps
  );
}

/**
 * Legacy Places Details (maps/api/place/details/json).
 */
async function legacyPlaceDetails(params = {}, deps = {}) {
  const {
    placeId,
    fields,
    apiKey,
    fetchImpl,
    timeout = 30000,
  } = params;
  const useFetch = typeof fetchImpl === 'function';

  return tracedRequest(
    {
      endpoint: PLACES_ENDPOINTS.PLACE_DETAILS,
      ...params.record,
    },
    async () => {
      if (useFetch) {
        const url = new URL(LEGACY_PLACE_DETAILS);
        url.searchParams.set('place_id', String(placeId || ''));
        url.searchParams.set('fields', fields);
        url.searchParams.set('key', apiKey);
        const res = await fetchImpl(url.toString());
        const data = await readFetchJson(res);
        return {
          httpStatus: res.status,
          googleStatus: data?.status || null,
          data,
          ok: res.ok,
        };
      }

      const res = await axios.get(LEGACY_PLACE_DETAILS, {
        params: { place_id: placeId, fields, key: apiKey },
        timeout,
      });
      return {
        httpStatus: res.status,
        googleStatus: res.data?.status || null,
        data: res.data,
        ok: res.status >= 200 && res.status < 300,
      };
    },
    deps
  );
}

/**
 * Places API (New) — searchText.
 */
async function v1SearchText(params = {}, deps = {}) {
  const { body, fieldMask, apiKey, timeout = 30000 } = params;

  return tracedRequest(
    {
      endpoint: PLACES_ENDPOINTS.SEARCH_TEXT_V1,
      ...params.record,
    },
    async () => {
      const res = await axios.post(V1_SEARCH_TEXT, body, {
        headers: {
          'Content-Type': 'application/json',
          'X-Goog-Api-Key': apiKey,
          'X-Goog-FieldMask': fieldMask,
        },
        timeout,
        validateStatus: () => true,
      });
      return {
        httpStatus: res.status,
        googleStatus: res.data?.error?.status || (res.status < 300 ? 'OK' : null),
        data: res.data,
        ok: res.status >= 200 && res.status < 300,
      };
    },
    deps
  );
}

/**
 * Places API (New) — place details GET /v1/places/{id}.
 */
async function v1PlaceDetails(params = {}, deps = {}) {
  const { placeId, fieldMask, apiKey, timeout = 30000 } = params;

  return tracedRequest(
    {
      endpoint: PLACES_ENDPOINTS.PLACE_DETAILS_V1,
      ...params.record,
    },
    async () => {
      const res = await axios.get(`${V1_PLACE_DETAILS}/${encodeURIComponent(placeId)}`, {
        headers: {
          'Content-Type': 'application/json',
          'X-Goog-Api-Key': apiKey,
          'X-Goog-FieldMask': fieldMask,
        },
        timeout,
        validateStatus: () => true,
      });
      return {
        httpStatus: res.status,
        googleStatus: res.data?.error?.status || (res.status < 300 ? 'OK' : null),
        data: res.data,
        ok: res.status >= 200 && res.status < 300,
      };
    },
    deps
  );
}

/**
 * Geocoding API (same key; tracked for completeness when GOOGLE_PLACES_KEY is reused).
 */
async function geocodeAddress(params = {}, deps = {}) {
  const { address, apiKey, timeout = 8000 } = params;

  return tracedRequest(
    {
      endpoint: PLACES_ENDPOINTS.GEOCODE,
      ...params.record,
    },
    async () => {
      const res = await axios.get(GEOCODE_URL, {
        params: { address, key: apiKey },
        timeout,
        validateStatus: () => true,
      });
      return {
        httpStatus: res.status,
        googleStatus: res.data?.status || null,
        data: res.data,
        ok: res.status >= 200 && res.status < 300,
      };
    },
    deps
  );
}

module.exports = {
  LEGACY_TEXT_SEARCH,
  LEGACY_PLACE_DETAILS,
  LEGACY_NEARBY_SEARCH,
  LEGACY_AUTOCOMPLETE,
  LEGACY_FIND_PLACE,
  GEOCODE_URL,
  V1_SEARCH_TEXT,
  V1_PLACE_DETAILS,
  legacyTextSearch,
  legacyPlaceDetails,
  v1SearchText,
  v1PlaceDetails,
  geocodeAddress,
};
