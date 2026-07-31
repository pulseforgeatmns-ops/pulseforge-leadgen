'use strict';

/**
 * Acquisition provider contract (SPEC-060).
 *
 * Every provider implements: available(), acquire(), metadata(), health()
 * No provider may publish ProspectLists — only Candidates.
 */

const REQUIRED_METHODS = Object.freeze([
  'available',
  'acquire',
  'metadata',
  'health',
]);

/**
 * @param {object} provider
 * @returns {{ ok: boolean, errors: string[] }}
 */
function assertProviderContract(provider) {
  const errors = [];
  if (!provider || typeof provider !== 'object') {
    return { ok: false, errors: ['Provider must be an object'] };
  }
  for (const method of REQUIRED_METHODS) {
    if (typeof provider[method] !== 'function') {
      errors.push(`Provider missing ${method}()`);
    }
  }
  const meta =
    typeof provider.metadata === 'function' ? provider.metadata() : null;
  if (!meta || typeof meta !== 'object' || !meta.id) {
    errors.push('Provider metadata() must return { id, ... }');
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Wrap a legacy search-only provider (available + search) into the SPEC-060 contract.
 * @param {object} legacy
 * @param {object} [meta]
 */
function adaptSearchProvider(legacy, meta = {}) {
  if (!legacy || typeof legacy.search !== 'function') {
    throw new Error('adaptSearchProvider requires legacy.search()');
  }
  const id = meta.id || legacy.id || 'legacy_search';
  const label = meta.label || meta.name || id;

  return {
    id,
    available() {
      return typeof legacy.available === 'function'
        ? Boolean(legacy.available())
        : true;
    },
    async acquire(request = {}) {
      const query = {
        industry: request.industry || request.query || '',
        location: request.location || request.geography || '',
        limit: request.limit || request.targetCount || 50,
        ...(request.query && typeof request.query === 'object'
          ? request.query
          : {}),
      };
      const hits = await legacy.search(query, request.profile || null);
      return {
        candidates: Array.isArray(hits) ? hits : [],
        evidence: [
          {
            kind: 'provider_search',
            provider: id,
            summary: `Search via ${label}`,
            hitCount: Array.isArray(hits) ? hits.length : 0,
          },
        ],
        warnings: [],
      };
    },
    metadata() {
      return {
        id,
        label,
        category: meta.category || 'discovery',
        acquisitionSource: meta.acquisitionSource || id,
        supports: meta.supports || ['search'],
        publishes: 'candidates',
        ...meta,
      };
    },
    health() {
      const available =
        typeof legacy.available === 'function'
          ? Boolean(legacy.available())
          : true;
      return {
        ok: available,
        status: available ? 'healthy' : 'unavailable',
        provider: id,
        checkedAt: new Date().toISOString(),
        details: meta.healthDetails || {},
      };
    },
  };
}

module.exports = {
  REQUIRED_METHODS,
  assertProviderContract,
  adaptSearchProvider,
};
