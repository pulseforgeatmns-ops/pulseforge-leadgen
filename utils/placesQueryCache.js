'use strict';

/**
 * In-process Places query cache (AUDIT-063 completeness).
 * Reduces duplicate Google spend; observability fields flow to places_api_requests.
 */

const { normalizePlacesQuery } = require('./placesCostAttribution');
const { createHash } = require('node:crypto');

const DEFAULT_STRATEGY = 'memory_ttl';
const DEFAULT_TTL_MS = Number(process.env.PLACES_CACHE_TTL_MS) || 24 * 60 * 60 * 1000;
// Failures suppress duplicate work briefly, but must not hide provider recovery
// for the full success TTL. This does not schedule requests or bypass budgets.
const DEFAULT_FAILURE_TTL_MS = 15 * 60 * 1000;

const LEGACY_SUCCESS_STATUSES = new Set(['OK', 'ZERO_RESULTS']);
const LEGACY_FAILURE_STATUSES = new Set([
  'REQUEST_DENIED', 'OVER_QUERY_LIMIT', 'OVER_DAILY_LIMIT', 'INVALID_REQUEST',
  'UNKNOWN_ERROR', 'NOT_FOUND', 'PERMISSION_DENIED', 'RESOURCE_EXHAUSTED', 'UNAVAILABLE',
]);

function legacyApplicationFailure(result) {
  if (!result?.ok || LEGACY_SUCCESS_STATUSES.has(result.googleStatus)) return null;
  return {
    httpStatus: Number.isInteger(result.httpStatus) ? result.httpStatus : null,
    googleStatus: LEGACY_FAILURE_STATUSES.has(result.googleStatus)
      ? result.googleStatus : 'UNKNOWN_STATUS',
  };
}


/** @type {Map<string, { storedAt: number, payload: object, endpoint: string }>} */
const store = new Map();
const inFlight = new Map();

function buildCacheKey({ endpoint, normalizedQuery, query = null, placeId, fieldMask = null, pageToken = null, apiKey = null }) {
  // Credentials and page tokens are never retained in cache keys or telemetry.
  const account = apiKey ? `:account:${createHash('sha256').update(String(apiKey)).digest('hex')}` : '';
  if (placeId) {
    const fields = fieldMask ? String(fieldMask).split(',').map(v => v.trim()).sort().join(',') : 'default';
    return `details:${String(placeId)}:${fields}${account}`;
  }
  // Reporting normalization removes state names; request identity must preserve geography.
  const q = query == null ? normalizePlacesQuery(normalizedQuery || '')
    : String(query).toLowerCase().replace(/\s+/g, ' ').trim();
  const page = pageToken ? `:page:${createHash('sha256').update(String(pageToken)).digest('hex')}` : '';
  return `search:${endpoint || 'text_search'}:${q}${page}${account}`;
}

function shareInFlight(key, execute) {
  if (inFlight.has(key)) return { promise: inFlight.get(key), reused: true };
  const promise = Promise.resolve().then(execute);
  inFlight.set(key, promise);
  const release = () => { if (inFlight.get(key) === promise) inFlight.delete(key); };
  // Both handlers resolve: cleanup must not create an unhandled rejected promise.
  promise.then(release, release);
  return { promise, reused: false };
}

function lookup(key, ttlMs = DEFAULT_TTL_MS) {
  let entry = store.get(key);
  if (!entry) {
    return {
      hit: false,
      miss: true,
      key,
      age: null,
      strategy: DEFAULT_STRATEGY,
      payload: null,
    };
  }
  // Older caches may contain HTTP-200 application errors as success payloads.
  // Quarantine before TTL evaluation, retaining the original observation age.
  const failure = entry.failure || legacyApplicationFailure(entry.payload);
  if (failure && !entry.failure) {
    holdFailure(key, failure, entry.endpoint, { preserveAge: true });
    entry = store.get(key);
  }
  const effectiveTtlMs = failure ? Math.min(ttlMs, DEFAULT_FAILURE_TTL_MS) : ttlMs;
  const age = Date.now() - entry.storedAt;
  if (age > effectiveTtlMs) {
    store.delete(key);
    return {
      hit: false,
      miss: true,
      key,
      age,
      strategy: DEFAULT_STRATEGY,
      payload: null,
      expired: true,
    };
  }
  return {
    hit: true,
    miss: false,
    key,
    age,
    strategy: DEFAULT_STRATEGY,
    payload: entry.payload,
    failure: entry.failure || null,
    endpoint: entry.endpoint,
    expiresInMs: Math.max(0, effectiveTtlMs - age),
  };
}

function storeResult(key, payload, endpoint) {
  store.set(key, {
    storedAt: Date.now(),
    payload,
    endpoint: endpoint || 'unknown',
  });
}

// Failure holds use a bounded negative TTL and never contain business evidence.
// Hits and old-entry quarantine preserve age; only a new provider response starts
// another hold. Expiry permits the next ordinary call, not an automatic retry.
function holdFailure(key, failure, endpoint, { preserveAge = false } = {}) {
  const existing = store.get(key);
  store.set(key, {
    storedAt: preserveAge && existing ? existing.storedAt : Date.now(),
    failure: { httpStatus: failure.httpStatus, googleStatus: failure.googleStatus },
    endpoint: endpoint || existing?.endpoint || 'unknown',
  });
}

function clearPlacesQueryCache() {
  store.clear();
  inFlight.clear();
}

function cacheStats() {
  return { entries: store.size, strategy: DEFAULT_STRATEGY, ttlMs: DEFAULT_TTL_MS };
}

module.exports = {
  DEFAULT_STRATEGY,
  DEFAULT_TTL_MS,
  DEFAULT_FAILURE_TTL_MS,
  legacyApplicationFailure,
  buildCacheKey,
  shareInFlight,
  lookup,
  storeResult,
  holdFailure,
  clearPlacesQueryCache,
  cacheStats,
};

