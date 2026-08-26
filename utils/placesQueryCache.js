'use strict';

/**
 * In-process Places query cache (AUDIT-063 completeness).
 * Reduces duplicate Google spend; observability fields flow to places_api_requests.
 */

const { normalizePlacesQuery } = require('./placesCostAttribution');

const DEFAULT_STRATEGY = 'memory_ttl';
const DEFAULT_TTL_MS = Number(process.env.PLACES_CACHE_TTL_MS) || 24 * 60 * 60 * 1000;

/** @type {Map<string, { storedAt: number, payload: object, endpoint: string }>} */
const store = new Map();

function buildCacheKey({ endpoint, normalizedQuery, placeId, fieldMask = null }) {
  if (placeId) {
    const fields = fieldMask ? String(fieldMask).split(',').sort().join(',') : 'default';
    return `details:${String(placeId)}:${fields}`;
  }
  const q = normalizePlacesQuery(normalizedQuery || '');
  return `search:${endpoint || 'text_search'}:${q}`;
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
}

function cacheStats() {
  return { entries: store.size, strategy: DEFAULT_STRATEGY, ttlMs: DEFAULT_TTL_MS };
}

module.exports = {
  DEFAULT_STRATEGY,
  DEFAULT_TTL_MS,
  buildCacheKey,
  lookup,
  storeResult,
  clearPlacesQueryCache,
  cacheStats,
};
