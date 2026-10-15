'use strict';

/**
 * In-process Places query cache (AUDIT-063 completeness).
 * Reduces duplicate Google spend; observability fields flow to places_api_requests.
 */

const { normalizePlacesQuery } = require('./placesCostAttribution');
const { createHash } = require('node:crypto');

const DEFAULT_STRATEGY = 'memory_ttl';
const DEFAULT_TTL_MS = Number(process.env.PLACES_CACHE_TTL_MS) || 24 * 60 * 60 * 1000;

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
  const entry = store.get(key);
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
  const age = Date.now() - entry.storedAt;
  if (age > ttlMs) {
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
    endpoint: entry.endpoint,
  };
}

function storeResult(key, payload, endpoint) {
  store.set(key, {
    storedAt: Date.now(),
    payload,
    endpoint: endpoint || 'unknown',
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
  buildCacheKey,
  shareInFlight,
  lookup,
  storeResult,
  clearPlacesQueryCache,
  cacheStats,
};

