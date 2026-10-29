'use strict';

/**
 * Traced Google Places / Geocoding HTTP client (AUDIT-063).
 */

const axios = require('axios');
const { hasResearchProviderScope, assertResearchProviderAllowed, rethrowResearchProviderControl } = require('./anchorResearchProviderPause');
const { reserveApiCall, settleApiCall } = require('./apiSpendControl');
const { PLACES_UPPER_BOUND } = require('./apiPricing');
const {
  PLACES_ENDPOINTS,
  getPlacesContext,
  buildAttributionRow,
  recordPlacesRequest,
  normalizePlacesQuery,
  countFromResult,
} = require('./placesCostAttribution');
const {
  buildCacheKey,
  shareInFlight,
  lookup,
  storeResult,
  holdFailure,
  DEFAULT_STRATEGY,
  legacyApplicationFailure,
} = require('./placesQueryCache');

async function guardedPlacesCall(recordBase, deps, execute) {
  const row = buildAttributionRow(recordBase, getPlacesContext());
  // SPEC-SCOUT-P0-003: inside a claimed Tenant 10 research-only cycle each
  // possible HTTP attempt first takes a durable Tenant 10 debit, and the paused
  // Places provider opens only inside that debit. Outside one this is null.
  const meter = require('../services/scoutResearchOnlyGrant').googleMeter();
  if (!meter) return guardedAttempt(row, deps, execute, null);
  const debit = await meter.reserve({ endpoint: row.endpoint, tenantId: row.tenantId });
  return debit.run(() => guardedAttempt(row, deps, execute, debit));
}

async function guardedAttempt(row, deps, execute, debit) {
  const research = Boolean(debit) || hasResearchProviderScope() || row.endpoint !== PLACES_ENDPOINTS.GEOCODE || ['Leadgen', 'Discovery', 'Diagnostic'].includes(row.feature);
  const checkResearch = () => {
    if (!research) return; // Field-route geocoding is outside this research pause.
    assertResearchProviderAllowed('google_places');
    if (row.tenantId != null) assertResearchProviderAllowed('google_places', row.tenantId);
  };
  try { checkResearch(); } catch (err) { await debit?.settle('error'); throw err; }
  const reserve = deps.reserveApiCall || reserveApiCall;
  const settle = deps.settleApiCall || settleApiCall;
  const estimate = PLACES_UPPER_BOUND[row.endpoint] ?? null;
  let id;
  try { id = await reserve({ provider: 'google_places', agentName: row.caller,
    tenantId: row.tenantId, missionId: row.missionId, operation: row.endpoint,
    estimatedUsd: estimate, pricingBasis: 'places_gross_upper_field_tier_2026_10_14' }, { db: deps.db });
  } catch (err) {
    await debit?.settle('error');
    rethrowResearchProviderControl(err); err.apiUsageNoNetwork = true; throw err; }
  if (debit) {
    try { await debit.link(id); } catch (err) {
      await settle(id, { status: 'error' }, { db: deps.db }).catch(() => {});
      await debit.settle('error');
      throw err;
    }
  }
  let result;
  try { checkResearch(); result = await execute(); }
  catch (err) {
    await debit?.settle('uncertain', err.response?.status);
    rethrowResearchProviderControl(err);
    await settle(id, { status: 'uncertain', httpStatus: err.response?.status }, { db: deps.db })
      .catch(() => console.warn('[api-usage] Places attempt remains reserved.'));
    throw err;
  }
  const status = result.httpStatus ?? result.status;
  await debit?.settle(status >= 200 && status < 300 ? 'ok' : 'error', status);
  await settle(id, { status: status >= 200 && status < 300 ? 'ok' : 'error',
    estimatedUsd: estimate, httpStatus: status }, { db: deps.db })
    .catch(() => console.warn('[api-usage] Places result remains reserved.'));
  return result;
}

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

function failureResult(failure) {
  return {
    httpStatus: failure.httpStatus,
    googleStatus: failure.googleStatus,
    data: { status: failure.googleStatus },
    ok: false,
    applicationFailure: true,
  };
}

function cachedLegacyResult(cacheKey, cached) {
  const failure = cached.failure || legacyApplicationFailure(cached.payload);
  if (!failure) return cached.payload;
  if (!cached.failure) holdFailure(cacheKey, failure, cached.endpoint, { preserveAge: true });
  return failureResult(failure);
}

function failedYieldParams(params) {
  return { ...params, yield: { businessesReturned: 0, businessesAccepted: 0,
    candidatesCreated: 0, qualifiedCandidates: 0 } };
}

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
    rethrowResearchProviderControl(err);
    if (deps.strictAttribution === true) throw Object.assign(Error('places_attribution_unconfirmed'), { code:'places_attribution_unconfirmed' });
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
    const res = await fetchImpl(url.toString(), { signal: AbortSignal.timeout(timeout) });
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
    const res = await fetchImpl(url.toString(), { signal: AbortSignal.timeout(timeout) });
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
    query: params.query,
    pageToken: params.pageToken,
    apiKey: params.apiKey,
  });
  const started = Date.now();

  if (useCache && recordBase.normalizedQuery) {
    const cached = lookup(cacheKey);
    if (cached.hit && (cached.payload || cached.failure)) {
      const result = cachedLegacyResult(cacheKey, cached);
      await recordCall(
        recordBase,
        result,
        result.ok ? params : failedYieldParams(params),
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
      return { ...result, cacheHit: true, cacheStrategy: cached.strategy,
        cacheAgeMs: cached.age, cacheExpiresInMs: cached.expiresInMs };
    }
  }

  let result;
  let reused = false;
  try {
    const execute = () => guardedPlacesCall(recordBase, deps, () => executeLegacyTextSearch(params));
    const pending = useCache && recordBase.normalizedQuery
      ? shareInFlight(`${cacheKey}:timeout:${params.timeout ?? 30000}`, execute)
      : { promise: execute(), reused: false };
    reused = pending.reused;
    result = await pending.promise;
  } catch (err) {
    rethrowResearchProviderControl(err);
    if (err.apiUsageNoNetwork) throw err;
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
        cacheHit: reused,
        cacheMiss: !reused && Boolean(useCache && recordBase.normalizedQuery),
        cacheKey: useCache ? cacheKey : null,
        cacheStrategy: reused ? 'memory_inflight' : useCache ? DEFAULT_STRATEGY : null,
      }
    );
    throw err;
  }

  const failure = legacyApplicationFailure(result);
  if (reused) {
    const sharedResult = failure ? failureResult(failure) : result;
    await recordCall(recordBase, sharedResult, failure ? failedYieldParams(params) : params, deps, started, {
      cacheHit: true, cacheMiss: false, cacheAge: 0, cacheKey, cacheStrategy: 'memory_inflight',
    });
    return { ...sharedResult, cacheHit: true, cacheStrategy: 'memory_inflight' };
  }
  if (useCache && recordBase.normalizedQuery && result.ok) {
    if (failure) holdFailure(cacheKey, failure, PLACES_ENDPOINTS.TEXT_SEARCH);
    else storeResult(cacheKey, result, PLACES_ENDPOINTS.TEXT_SEARCH);
  }
  if (failure) {
    const uncachedResponse = !useCache ? result.data : null;
    result = failureResult(failure);
    // A failed provider page may exit before its deferred yield commit. Record
    // it now and return no commit callback, so the failure is counted once.
    await recordCall(recordBase, result, failedYieldParams(params), deps, started, {
      cacheHit: false,
      cacheMiss: Boolean(useCache && recordBase.normalizedQuery),
      cacheKey: useCache ? cacheKey : null,
      cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
    });
    // Explicit uncached diagnostics already inspect the original provider
    // response. Preserve that return contract, never its cache/telemetry data.
    return { ...result, ...(!useCache ? { data: uncachedResponse } : {}),
      cacheHit: false, cacheStrategy: useCache ? DEFAULT_STRATEGY : null };
  }

  if (params.deferRecord) {
    return {
      ...result,
      cacheHit: false, cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
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

  return { ...result, cacheHit: false, cacheStrategy: useCache ? DEFAULT_STRATEGY : null };
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
    apiKey: params.apiKey,
  });
  const started = Date.now();

  if (useCache && params.placeId) {
    const cached = lookup(cacheKey);
    if (cached.hit && (cached.payload || cached.failure)) {
      const result = cachedLegacyResult(cacheKey, cached);
      await recordCall(
        recordBase,
        result,
        result.ok ? params : failedYieldParams(params),
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
      return { ...result, cacheHit: true, cacheStrategy: cached.strategy,
        cacheAgeMs: cached.age, cacheExpiresInMs: cached.expiresInMs };
    }
  }

  let result;
  let reused = false;
  try {
    const execute = () => guardedPlacesCall(recordBase, deps, () => executeLegacyPlaceDetails(params));
    const pending = useCache && params.placeId
      ? shareInFlight(`${cacheKey}:timeout:${params.timeout ?? 30000}`, execute)
      : { promise: execute(), reused: false };
    reused = pending.reused;
    result = await pending.promise;
  } catch (err) {
    rethrowResearchProviderControl(err);
    if (err.apiUsageNoNetwork) throw err;
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
        cacheHit: reused,
        cacheMiss: !reused && Boolean(useCache && params.placeId),
        cacheKey: useCache ? cacheKey : null,
        cacheStrategy: reused ? 'memory_inflight' : useCache ? DEFAULT_STRATEGY : null,
      }
    );
    throw err;
  }

  const failure = legacyApplicationFailure(result);
  if (reused) {
    const sharedResult = failure ? failureResult(failure) : result;
    await recordCall(recordBase, sharedResult, failure ? failedYieldParams(params) : params, deps, started, {
      cacheHit: true, cacheMiss: false, cacheAge: 0, cacheKey, cacheStrategy: 'memory_inflight',
    });
    return sharedResult;
  }
  if (useCache && params.placeId && result.ok) {
    if (failure) holdFailure(cacheKey, failure, PLACES_ENDPOINTS.PLACE_DETAILS);
    else storeResult(cacheKey, result, PLACES_ENDPOINTS.PLACE_DETAILS);
  }
  if (failure) {
    const uncachedResponse = !useCache ? result.data : null;
    result = failureResult(failure);
    await recordCall(recordBase, result, failedYieldParams(params), deps, started, {
      cacheHit: false,
      cacheMiss: Boolean(useCache && params.placeId),
      cacheKey: useCache ? cacheKey : null,
      cacheStrategy: useCache ? DEFAULT_STRATEGY : null,
    });
    return !useCache ? { ...result, data: uncachedResponse } : result;
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
    const res = await guardedPlacesCall(recordBase, deps, () => axios.post(V1_SEARCH_TEXT, params.body, {
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': params.apiKey,
        'X-Goog-FieldMask': params.fieldMask,
      },
      timeout: params.timeout || 30000,
      validateStatus: () => true,
    }));
    result = {
      httpStatus: res.status,
      googleStatus: res.data?.error?.status || (res.status < 300 ? 'OK' : null),
      data: res.data,
      ok: res.status >= 200 && res.status < 300,
    };
  } catch (err) {
    rethrowResearchProviderControl(err);
    if (err.apiUsageNoNetwork) throw err;
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
    const res = await guardedPlacesCall(recordBase, deps, () => axios.get(`${V1_PLACE_DETAILS}/${encodeURIComponent(params.placeId)}`, {
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': params.apiKey,
        'X-Goog-FieldMask': params.fieldMask,
      },
      timeout: params.timeout || 30000,
      validateStatus: () => true,
    }));
    result = {
      httpStatus: res.status,
      googleStatus: res.data?.error?.status || (res.status < 300 ? 'OK' : null),
      data: res.data,
      ok: res.status >= 200 && res.status < 300,
    };
  } catch (err) {
    rethrowResearchProviderControl(err);
    if (err.apiUsageNoNetwork) throw err;
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
    const res = await guardedPlacesCall(recordBase, deps, () => axios.get(GEOCODE_URL, {
      params: { address: params.address, key: params.apiKey },
      timeout: params.timeout || 8000,
      validateStatus: () => true,
    }));
    result = {
      httpStatus: res.status,
      googleStatus: res.data?.status || null,
      data: res.data,
      ok: res.status >= 200 && res.status < 300,
    };
  } catch (err) {
    rethrowResearchProviderControl(err);
    if (err.apiUsageNoNetwork) throw err;
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

