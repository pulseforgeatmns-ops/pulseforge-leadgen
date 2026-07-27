'use strict';

/**
 * Google Places discovery provider (SPEC-024).
 * Capability-layer adapter — no Scout/agent module imports.
 */

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
     * @param {object} query
     * @param {string} query.industry
     * @param {string} query.location
     * @param {number} [query.limit]
     * @returns {Promise<object[]>}
     */
    async search(query) {
      if (!this.available()) return [];
      const limit = Math.min(Number(query.limit) || 20, 20);
      const q = `${query.industry || ''} ${query.location || ''}`.trim();
      if (!q) return [];

      const url = new URL(
        'https://maps.googleapis.com/maps/api/place/textsearch/json'
      );
      url.searchParams.set('query', q);
      url.searchParams.set('key', apiKey);

      const res = await fetchImpl(url.toString());
      if (!res.ok) return [];
      const data = await res.json();
      if (data.status !== 'OK' && data.status !== 'ZERO_RESULTS') {
        return [];
      }

      const results = (data.results || []).slice(0, limit);
      const out = [];
      for (const hit of results) {
        const details = await fetchPlaceDetails(hit.place_id, apiKey, fetchImpl);
        const website = details?.website || null;
        if (!website) continue;
        out.push({
          companyName: details?.name || hit.name || 'Unknown',
          website: normalizeDomain(website),
          phone: details?.formatted_phone_number || null,
          address: details?.formatted_address || hit.formatted_address || '',
          placeId: details?.place_id || hit.place_id,
          placeTypes: details?.types || hit.types || [],
          googleRating: details?.rating ?? hit.rating ?? null,
          source: 'google_places',
          industry: query.industry || null,
          snippet: '',
        });
      }
      return out;
    },
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
};
