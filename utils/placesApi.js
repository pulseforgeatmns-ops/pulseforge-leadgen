'use strict';

/**
 * Traced Google Places / Geocoding HTTP client (AUDIT-063).
 */

const axios = require('axios');
const {
  PLACES_ENDPOINTS,
  recordPlacesRequest,
  normalizePlacesQuery,
  countFromResult,
} = require('./placesCostAttribution');
const {
  buildCacheKey,
  lookup,
  storeResult,
  DEFAULT_STRATEGY,
} = require('./placesQueryCache');

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

function buildQueryRecord(endpoint, params = {}) {
  const originalQuery =
    params.query ||
    params.address ||
    (params.body && params.body.textQuery) ||
    null;
  const normalizedQuery = originalQuery ? normalizePlacesQuery(originalQuery) : null;
  return {
    endpoint,
    originalQuery,
    normalizedQuery,
    ...params.record,
  };
}

function mergeYield(recordBase, result, params = {}) {
  const manual = params.yield || {};
  const data = result?.data;
  const businessesReturned =
    manual.businessesReturned ??
    manual.businesses_returned ??
    countFromResult(recordBase.endpoint, data);
  return {
    businessesReturned,
    businessesAccepted: manual.businessesAccepted ?? manual.businesses_accepted ?? null,
    candidatesCreated: manual.candidatesCreated ?? manual.candidates_created ?? null,
    qualifiedCandidates: manual.qualifiedCandidates ?? manual.qualified_candidates ?? null,
  };
}

async function recordCall(recordBase, result, params, deps, started, cacheMeta = {}) {
  const latencyMs = Date.now() - started;
  try {
    await recordPlacesRequest(
      {
        ...recordBase,
        ...cacheMeta,
        ...mergeYield(recordBase, result, params),
        httpStatus: result?.httpStatus ?? null,
        googleStatus: result?.googleStatus ?? null,
        latencyMs,
      },
      deps
    );
  } catch (err) {
    console.warn('[placesApi] attribution record failed:', err.message);
  }
}

async function executeLegacyTextSearch(params = {}) {
  const { query, apiKey, pageToken = null, fetchImpl, timeout = 30000 } = params;
  const useFetch = typeof fetchImpl === 'function';

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
  const res = await axios.get(LEGACY_TEXT_SEARCH, { params: axiosParams, timeout });
  return {
    httpStatus: res.status,
    googleStatus: res.data?.status || null,
    data: res.data,
    ok: res.status >= 200 && res.status < 300,
  };
}

async function executeLegacyPlaceDetails(params = {}) {
  const { placeId, fields, apiKey, fetchImpl, timeout = 30000 } = params;
  const useFetch = typeof fetchImpl === 'function';

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
}

async function legacyTextSearch(params = {}, deps = {}) {
  const recordBase = buildQueryRecord(PLACES_ENDPOINTS.TEXT_SEARCH, params);
  const useCache = params.useCache !== false;
  const cacheKey = buildCacheKey({
    endpoint: PLACES_ENDPOINTS.TEXT_SEARCH,
    normalizedQuery: recordBase.normalizedQuery,
  });
  const started = Date.now();

  if (useCache && recordBase.normalizedQuery) {
    const cached = lookup(cacheKey);
    if (cached.hit && cached.payload) {
      await recordCall(
        recordBase,
        cached.payload,
        params,
        deps,
        started,
        {
          cacheHit: true,
          cacheMiss: false,
          cacheAge: cached.age,
          cacheKey: cached.key,
          cacheStrategy: cached.strategy,
        }
      );
      return cached.payload;
    }
  }

  let result;
  try {
    result = await executeLegacyTextSearch(params);
  } catch (err) {
    await recordCall(
      recordBase,
      {
        httpStatus: err.response?.status ?? null,
        googleStatus: err.response?.data?.status || null,
        data: err.response?.data || null,
        ok: false,
      },
      params,
      deps,
      started,
      {
        cacheHit: false,
        cacheMiss: Boolean(useCache && recordBase.normalizedQuery),
        cacheKey: useCache ? cacheKey : null,
        cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
      }
    );
    throw err;
  }

  if (useCache && recordBase.normalizedQuery && result.ok) {
    storeResult(cacheKey, result, PLACES_ENDPOINTS.TEXT_SEARCH);
  }

  if (params.deferRecord) {
    return {
      ...result,
      async commitRecord(yieldMetrics = {}) {
        params.yield = { ...(params.yield || {}), ...yieldMetrics };
        await recordCall(recordBase, result, params, deps, started, {
          cacheHit: false,
          cacheMiss: Boolean(useCache && recordBase.normalizedQuery),
          cacheKey: useCache ? cacheKey : null,
          cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
        });
      },
    };
  }

  await recordCall(recordBase, result, params, deps, started, {
    cacheHit: false,
    cacheMiss: Boolean(useCache && recordBase.normalizedQuery),
    cacheKey: useCache ? cacheKey : null,
    cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
  });

  return result;
}

async function legacyPlaceDetails(params = {}, deps = {}) {
  const recordBase = buildQueryRecord(PLACES_ENDPOINTS.PLACE_DETAILS, {
    ...params,
    query: params.placeId,
  });
  const useCache = params.useCache !== false;
  const cacheKey = buildCacheKey({
    endpoint: PLACES_ENDPOINTS.PLACE_DETAILS,
    placeId: params.placeId,
    fieldMask: params.fields,
  });
  const started = Date.now();

  if (useCache && params.placeId) {
    const cached = lookup(cacheKey);
    if (cached.hit && cached.payload) {
      await recordCall(
        recordBase,
        cached.payload,
        params,
        deps,
        started,
        {
          cacheHit: true,
          cacheMiss: false,
          cacheAge: cached.age,
          cacheKey: cached.key,
          cacheStrategy: cached.strategy,
        }
      );
      return cached.payload;
    }
  }

  let result;
  try {
    result = await executeLegacyPlaceDetails(params);
  } catch (err) {
    await recordCall(
      recordBase,
      {
        httpStatus: err.response?.status ?? null,
        googleStatus: err.response?.data?.status || null,
        data: err.response?.data || null,
        ok: false,
      },
      params,
      deps,
      started,
      {
        cacheHit: false,
        cacheMiss: Boolean(useCache && params.placeId),
        cacheKey: useCache ? cacheKey : null,
        cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
      }
    );
    throw err;
  }

  if (useCache && params.placeId && result.ok) {
    storeResult(cacheKey, result, PLACES_ENDPOINTS.PLACE_DETAILS);
  }

  if (params.deferRecord) {
    return {
      ...result,
      async commitRecord(yieldMetrics = {}) {
        params.yield = { ...(params.yield || {}), ...yieldMetrics };
        await recordCall(recordBase, result, params, deps, started, {
          cacheHit: false,
          cacheMiss: Boolean(useCache && params.placeId),
          cacheKey: useCache ? cacheKey : null,
          cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
        });
      },
    };
  }

  await recordCall(recordBase, result, params, deps, started, {
    cacheHit: false,
    cacheMiss: Boolean(useCache && params.placeId),
    cacheKey: useCache ? cacheKey : null,
    cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
  });

  return result;
}

async function v1SearchText(params = {}, deps = {}) {
  const recordBase = buildQueryRecord(PLACES_ENDPOINTS.SEARCH_TEXT_V1, params);
  const started = Date.now();
  let result;

  try {
    const res = await axios.post(V1_SEARCH_TEXT, params.body, {
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': params.apiKey,
        'X-Goog-FieldMask': params.fieldMask,
      },
      timeout: params.timeout || 30000,
      validateStatus: () => true,
    });
    result = {
      httpStatus: res.status,
      googleStatus: res.data?.error?.status || (res.status < 300 ? 'OK' : null),
      data: res.data,
      ok: res.status >= 200 && res.status < 300,
    };
  } catch (err) {
    result = {
      httpStatus: err.response?.status ?? null,
      googleStatus: err.response?.data?.error?.status || null,
      data: err.response?.data || null,
      ok: false,
    };
    await recordCall(recordBase, result, params, deps, started, {
      cacheHit: false,
      cacheMiss: true,
      cacheStrategy: DEFAULT_STRATEGY,
    });
    throw err;
  }

  await recordCall(recordBase, result, params, deps, started, {
    cacheHit: false,
    cacheMiss: true,
    cacheStrategy: DEFAULT_STRATEGY,
  });
  return result;
}

async function v1PlaceDetails(params = {}, deps = {}) {
  const recordBase = buildQueryRecord(PLACES_ENDPOINTS.PLACE_DETAILS_V1, {
    ...params,
    query: params.placeId,
  });
  const started = Date.now();
  let result;

  try {
    const res = await axios.get(`${V1_PLACE_DETAILS}/${encodeURIComponent(params.placeId)}`, {
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': params.apiKey,
        'X-Goog-FieldMask': params.fieldMask,
      },
      timeout: params.timeout || 30000,
      validateStatus: () => true,
    });
    result = {
      httpStatus: res.status,
      googleStatus: res.data?.error?.status || (res.status < 300 ? 'OK' : null),
      data: res.data,
      ok: res.status >= 200 && res.status < 300,
    };
  } catch (err) {
    result = {
      httpStatus: err.response?.status ?? null,
      googleStatus: err.response?.data?.error?.status || null,
      data: err.response?.data || null,
      ok: false,
    };
    await recordCall(recordBase, result, params, deps, started, {
      cacheHit: false,
      cacheMiss: true,
      cacheStrategy: DEFAULT_STRATEGY,
    });
    throw err;
  }

  await recordCall(recordBase, result, params, deps, started, {
    cacheHit: false,
    cacheMiss: true,
    cacheStrategy: DEFAULT_STRATEGY,
  });
  return result;
}

async function geocodeAddress(params = {}, deps = {}) {
  const recordBase = buildQueryRecord(PLACES_ENDPOINTS.GEOCODE, params);
  const started = Date.now();
  let result;

  try {
    const res = await axios.get(GEOCODE_URL, {
      params: { address: params.address, key: params.apiKey },
      timeout: params.timeout || 8000,
      validateStatus: () => true,
    });
    result = {
      httpStatus: res.status,
      googleStatus: res.data?.status || null,
      data: res.data,
      ok: res.status >= 200 && res.status < 300,
    };
  } catch (err) {
    result = {
      httpStatus: err.response?.status ?? null,
      googleStatus: err.response?.data?.status || null,
      data: err.response?.data || null,
      ok: false,
    };
    await recordCall(recordBase, result, params, deps, started, {});
    throw err;
  }

  await recordCall(recordBase, result, params, deps, started, {});
  return result;
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
