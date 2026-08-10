'use strict';

/**
 * Scout public-source prospect sourcing (SPEC-077).
 *
 * Reads an approved Scout work request, inspects public sources (Google Places),
 * and returns 15–25 evidenced, review-only candidates.
 *
 * Guardrails:
 * - No CRM writes
 * - No outreach copy
 * - No sends
 * - No account / DNS / GBP / social / tracking changes
 * - No fabricated placeholder companies
 */

const {
  createPlacesProvider,
} = require('../packages/capabilities/discovery/providers/PlacesProvider');

const TARGET_COUNT_MIN = 15;
const TARGET_COUNT_MAX = 25;

/**
 * Scout public sourcing uses the legacy Places Text Search + Details endpoints
 * (maps.googleapis.com) with `?key=` query auth — NOT Places API (New).
 * Keep these constants/builders as the single source of truth for Scout + diagnostics.
 */
const SCOUT_PLACES_ENDPOINT_FAMILY = 'legacy_places_text_search_details';
const SCOUT_PLACES_AUTH_STYLE = 'query_param_key';
const SCOUT_PLACES_TEXTSEARCH_URL =
  'https://maps.googleapis.com/maps/api/place/textsearch/json';
const SCOUT_PLACES_DETAILS_URL =
  'https://maps.googleapis.com/maps/api/place/details/json';
const SCOUT_PLACES_DETAILS_FIELDS =
  'name,formatted_address,formatted_phone_number,website,place_id,types,rating,business_status';

function buildScoutPlacesTextSearchUrl({ query, apiKey }) {
  const url = new URL(SCOUT_PLACES_TEXTSEARCH_URL);
  url.searchParams.set('query', String(query || '').trim());
  url.searchParams.set('key', String(apiKey || ''));
  return url;
}

function buildScoutPlacesDetailsUrl({ placeId, apiKey }) {
  const url = new URL(SCOUT_PLACES_DETAILS_URL);
  url.searchParams.set('place_id', String(placeId || ''));
  url.searchParams.set('fields', SCOUT_PLACES_DETAILS_FIELDS);
  url.searchParams.set('key', String(apiKey || ''));
  return url;
}

/** Host + pathname only — never include query/key. */
function scoutPlacesUrlHostPath(urlLike) {
  try {
    const url =
      urlLike instanceof URL ? urlLike : new URL(String(urlLike || ''));
    return { host: url.host, path: url.pathname };
  } catch {
    return { host: null, path: null };
  }
}

const DEFAULT_CONTACT_ROLE_BY_SEGMENT = Object.freeze({
  property_managers: 'Owner / property manager',
  property_manager: 'Owner / property manager',
  'property managers': 'Owner / property manager',
  law_firm: 'Office manager / managing partner',
  'law firm': 'Office manager / managing partner',
  accounting: 'Office manager / principal',
  cleaning: 'Owner / operations manager',
  restaurant: 'Owner / general manager',
  salon: 'Owner / studio manager',
  fitness: 'Owner / general manager',
  landscaping: 'Owner / operations manager',
  home_services: 'Owner / office manager',
  home_renovation: 'Owner / project manager',
});

/**
 * Whether Scout can run live public-source sourcing in this environment.
 * Available when GOOGLE_PLACES_KEY is set, or a search provider / fn is injected.
 */
function isScoutPublicSourcingAvailable(opts = {}) {
  if (opts.scoutPublicSourcingSupported === false) return false;
  if (opts.scoutPublicSourcingSupported === true) return true;
  if (typeof opts.publicSearchFn === 'function') return true;
  if (opts.searchProvider && typeof opts.searchProvider.search === 'function') {
    if (typeof opts.searchProvider.available === 'function') {
      return Boolean(opts.searchProvider.available());
    }
    return true;
  }
  const places = createPlacesProvider({
    apiKey: opts.apiKey || process.env.GOOGLE_PLACES_KEY || '',
    fetchImpl: opts.fetchImpl,
  });
  return places.available();
}

function normalizeWebsiteUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    const u = new URL(withProto);
    if (!u.hostname) return null;
    return u.toString().replace(/\/$/, '') === `${u.protocol}//${u.host}`
      ? `${u.protocol}//${u.host}/`
      : u.toString();
  } catch {
    return null;
  }
}

function mapsSourceUrl(hit) {
  const placeId = hit.placeId || hit.place_id || null;
  const name = hit.companyName || hit.name || '';
  const address = hit.address || hit.formatted_address || hit.location || '';
  if (placeId) {
    return `https://www.google.com/maps/search/?api=1&query_place_id=${encodeURIComponent(
      placeId
    )}`;
  }
  const q = `${name} ${address}`.trim();
  if (!q) return null;
  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(q)}`;
}

function resolveSourceUrl(hit) {
  const website =
    normalizeWebsiteUrl(hit.website) ||
    normalizeWebsiteUrl(hit.sourceUrl) ||
    normalizeWebsiteUrl(hit.url);
  if (website) return website;
  return mapsSourceUrl(hit);
}

function segmentKey(workRequest) {
  return String(
    (workRequest && (workRequest.targetSegment || workRequest.segment)) || ''
  )
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, ' ');
}

function suggestedContactRole(workRequest, hit) {
  if (hit && (hit.suggestedContactRole || hit.contactRole)) {
    return hit.suggestedContactRole || hit.contactRole;
  }
  const key = segmentKey(workRequest);
  if (DEFAULT_CONTACT_ROLE_BY_SEGMENT[key]) {
    return DEFAULT_CONTACT_ROLE_BY_SEGMENT[key];
  }
  const compact = key.replace(/\s+/g, '_');
  if (DEFAULT_CONTACT_ROLE_BY_SEGMENT[compact]) {
    return DEFAULT_CONTACT_ROLE_BY_SEGMENT[compact];
  }
  return 'Owner / decision-maker';
}

function buildSearchQueries(workRequest) {
  const segment =
    (workRequest && (workRequest.targetSegment || workRequest.segment)) ||
    'local businesses';
  const subtype =
    (workRequest && (workRequest.targetSubtype || workRequest.subtype)) || null;
  const market =
    (workRequest && (workRequest.marketBounds || workRequest.location)) || '';

  const queries = [];
  const primary = [segment, subtype, market].filter(Boolean).join(' ').trim();
  if (primary) queries.push(primary);

  if (subtype && market) {
    const alt = `${subtype} ${market}`.trim();
    if (alt !== primary) queries.push(alt);
  }
  if (segment && market && !subtype) {
    // already primary
  } else if (segment && market) {
    const alt2 = `${segment} ${market}`.trim();
    if (!queries.includes(alt2)) queries.push(alt2);
  }

  // Inclusion criteria can seed additional public-directory queries.
  const inclusions = Array.isArray(workRequest && workRequest.inclusionCriteria)
    ? workRequest.inclusionCriteria
    : [];
  for (const inc of inclusions.slice(0, 3)) {
    const text = String(inc || '').trim();
    if (!text || text.length > 80) continue;
    const q = `${text} ${market}`.trim();
    if (q && !queries.includes(q)) queries.push(q);
  }

  return queries.length ? queries : [`${segment} ${market}`.trim()].filter(Boolean);
}

function exclusionMatched(hit, workRequest) {
  const exclusions = Array.isArray(workRequest && workRequest.exclusionCriteria)
    ? workRequest.exclusionCriteria
    : [];
  if (!exclusions.length) return null;
  const hay = [
    hit.companyName,
    hit.name,
    hit.address,
    hit.location,
    hit.industry,
    hit.snippet,
    ...(hit.placeTypes || []),
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  for (const ex of exclusions) {
    const token = String(ex || '')
      .trim()
      .toLowerCase();
    if (!token || token.length < 3) continue;
    // Prefer distinctive tokens (skip very generic words).
    const words = token
      .split(/[^a-z0-9]+/i)
      .map((w) => w.trim())
      .filter((w) => w.length >= 4);
    for (const w of words) {
      if (
        [
          'local',
          'with',
          'that',
          'from',
          'this',
          'have',
          'national',
          'chains',
        ].includes(w)
      ) {
        // "national" / "chains" are meaningful exclusions — keep those.
        if (w !== 'national' && w !== 'chains') continue;
      }
      if (hay.includes(w)) return ex;
    }
  }
  return null;
}

function confidenceForHit(hit, workRequest, sourceUrl) {
  if (hit.confidence) return String(hit.confidence);
  const hasWebsite = Boolean(
    normalizeWebsiteUrl(hit.website) || normalizeWebsiteUrl(hit.sourceUrl)
  );
  const location = String(hit.location || hit.address || '').toLowerCase();
  const market = String(
    (workRequest && (workRequest.marketBounds || workRequest.location)) || ''
  ).toLowerCase();
  const marketTokens = market
    .split(/[^a-z0-9]+/i)
    .filter((t) => t.length >= 3)
    .slice(0, 4);
  const inMarket =
    !marketTokens.length ||
    marketTokens.some((t) => location.includes(t));
  const segment = segmentKey(workRequest);
  const hay = `${hit.companyName || ''} ${hit.industry || ''} ${(
    hit.placeTypes || []
  ).join(' ')}`.toLowerCase();
  const segmentHit =
    !segment ||
    segment
      .split(/\s+/)
      .filter((t) => t.length >= 4)
      .some((t) => hay.includes(t));

  if (hasWebsite && inMarket && segmentHit && sourceUrl) return 'high';
  if (sourceUrl && inMarket) return 'medium';
  return 'review_required';
}

function fitRationaleForHit(hit, workRequest) {
  if (hit.fitRationale || hit.fitReason || hit.rationale) {
    return hit.fitRationale || hit.fitReason || hit.rationale;
  }
  const segment =
    (workRequest && (workRequest.targetSegment || workRequest.segment)) ||
    'target segment';
  const market =
    (workRequest && (workRequest.marketBounds || workRequest.location)) ||
    'approved market';
  const subtype =
    (workRequest && (workRequest.targetSubtype || workRequest.subtype)) || null;
  const parts = [
    `Public listing matches ${segment}${subtype ? ` / ${subtype}` : ''}`,
    `Location evidence: ${hit.location || hit.address || 'see source'}`,
    `Market bounds: ${market}`,
  ];
  if (hit.googleRating != null) {
    parts.push(`Google rating ${hit.googleRating}`);
  }
  return parts.join(' — ');
}

function risksForHit(hit, workRequest) {
  if (hit.risks || hit.disqualifyRisk || hit.risk || hit.uncertainty) {
    return hit.risks || hit.disqualifyRisk || hit.risk || hit.uncertainty;
  }
  const risks = [];
  const website = normalizeWebsiteUrl(hit.website) || normalizeWebsiteUrl(hit.sourceUrl);
  if (!website) {
    risks.push('No company website on listing — using maps listing as source URL');
  }
  if (!hit.phone) {
    risks.push('Phone not confirmed on public listing');
  }
  const exclusion = exclusionMatched(hit, workRequest);
  if (exclusion) {
    risks.push(`Possible exclusion match: ${exclusion}`);
  }
  const market = String(
    (workRequest && (workRequest.marketBounds || workRequest.location)) || ''
  ).toLowerCase();
  const location = String(hit.location || hit.address || '').toLowerCase();
  const marketTokens = market
    .split(/[^a-z0-9]+/i)
    .filter((t) => t.length >= 4)
    .slice(0, 3);
  if (
    marketTokens.length &&
    !marketTokens.some((t) => location.includes(t))
  ) {
    risks.push('Location may be outside approved market bounds — verify');
  }
  if (!risks.length) {
    risks.push('Public-source only — contact role not verified beyond listing signals');
  }
  return risks.join('; ');
}

/**
 * Map a public-source hit into a Scout candidate row.
 * Returns null when company name or source URL is missing (never fabricate).
 */
function mapPublicHitToScoutCandidate(hit, workRequest, idx = 0) {
  const companyName =
    (hit &&
      (hit.companyName ||
        hit.name ||
        hit.propertyManagerName ||
        hit.company)) ||
    null;
  const sourceUrl = resolveSourceUrl(hit || {});
  if (!companyName || !sourceUrl) return null;

  const website =
    normalizeWebsiteUrl(hit.website) ||
    normalizeWebsiteUrl(hit.sourceUrl) ||
    sourceUrl;

  return {
    id: hit.id || `scout-public-${idx + 1}`,
    companyName: String(companyName).trim(),
    sourceUrl,
    website,
    location: hit.location || hit.address || hit.marketTown || null,
    marketTown: hit.marketTown || hit.location || hit.address || null,
    segment:
      hit.segment ||
      (workRequest && workRequest.targetSegment) ||
      null,
    subtype:
      hit.subtype ||
      hit.segmentSubtype ||
      (workRequest && workRequest.targetSubtype) ||
      null,
    fitRationale: fitRationaleForHit(hit, workRequest),
    risks: risksForHit(hit, workRequest),
    suggestedContactRole: suggestedContactRole(workRequest, hit),
    confidence: confidenceForHit(hit, workRequest, sourceUrl),
    reviewOnly: true,
    placeholder: false,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    publicSource: hit.source || 'google_places',
    placeId: hit.placeId || hit.place_id || null,
  };
}

/**
 * Scout-oriented Places search that preserves full website URLs as source URLs.
 */
function createScoutPlacesSearchProvider(deps = {}) {
  const apiKey = deps.apiKey || process.env.GOOGLE_PLACES_KEY || '';
  const fetchImpl =
    deps.fetchImpl ||
    (typeof fetch === 'function' ? fetch.bind(globalThis) : null);

  return {
    id: 'google_places_scout',
    available() {
      return !!(apiKey && fetchImpl);
    },
    async search(query) {
      if (!this.available()) return [];
      const limit = Math.min(Number(query.limit) || 20, 20);
      const q = String(query.query || query.industry || '').trim();
      if (!q) return [];

      const url = buildScoutPlacesTextSearchUrl({ query: q, apiKey });

      const res = await fetchImpl(url.toString());
      if (!res.ok) {
        throw new Error(`google_places_http_${res.status}`);
      }
      const data = await res.json();
      if (data.status === 'ZERO_RESULTS') return [];
      if (data.status !== 'OK') {
        throw new Error(`google_places_status_${data.status || 'unknown'}`);
      }

      const results = (data.results || []).slice(0, limit);
      const out = [];
      for (const hit of results) {
        const details = await fetchPlaceDetails(hit.place_id, apiKey, fetchImpl);
        const websiteRaw = details?.website || null;
        const website = normalizeWebsiteUrl(websiteRaw);
        out.push({
          companyName: details?.name || hit.name || null,
          website,
          sourceUrl: website || null,
          phone: details?.formatted_phone_number || null,
          address: details?.formatted_address || hit.formatted_address || '',
          location: details?.formatted_address || hit.formatted_address || '',
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
  const url = buildScoutPlacesDetailsUrl({ placeId, apiKey });
  const res = await fetchImpl(url.toString());
  if (!res.ok) return null;
  const data = await res.json();
  return data.result || null;
}

function dedupeCandidates(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = `${String(row.companyName || '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '')}|${String(row.sourceUrl || '')
      .toLowerCase()
      .replace(/\/$/, '')}`;
    if (!row.companyName || !row.sourceUrl) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

/**
 * Execute public-source sourcing for an approved Scout work request.
 * Returns evidenced candidates only — never placeholders, never CRM writes.
 *
 * @param {object} input
 * @param {object} input.workRequest
 * @param {object} [input.handoff]
 * @param {object} [input.opts]
 * @returns {Promise<{ ok: boolean, candidates: object[], warnings: string[], error: string|null, queried: string[], crmWritesMade: boolean, outreachCopyGenerated: boolean, accountChangesMade: boolean }>}
 */
async function sourceScoutCandidatesFromPublicSources(input = {}) {
  const workRequest = input.workRequest || input;
  const opts = input.opts || {};
  const warnings = [];
  const queried = [];

  if (!workRequest || typeof workRequest !== 'object') {
    return {
      ok: false,
      candidates: [],
      warnings,
      error: 'missing_work_request',
      queried,
      crmWritesMade: false,
      outreachCopyGenerated: false,
      accountChangesMade: false,
    };
  }

  const targetMin = Number(workRequest.targetCountMin) || TARGET_COUNT_MIN;
  const targetMax = Number(workRequest.targetCountMax) || TARGET_COUNT_MAX;
  const fetchLimit = Math.min(Math.max(targetMax, targetMin), TARGET_COUNT_MAX);

  let searchFn = null;
  if (typeof opts.publicSearchFn === 'function') {
    searchFn = opts.publicSearchFn;
  } else if (
    opts.searchProvider &&
    typeof opts.searchProvider.search === 'function'
  ) {
    searchFn = (query) => opts.searchProvider.search(query);
  } else {
    const provider = createScoutPlacesSearchProvider(opts);
    if (!provider.available()) {
      return {
        ok: false,
        candidates: [],
        warnings: [
          'Scout public-source sourcing unavailable — set GOOGLE_PLACES_KEY or inject a search provider.',
        ],
        error: 'public_sourcing_unavailable',
        queried,
        crmWritesMade: false,
        outreachCopyGenerated: false,
        accountChangesMade: false,
      };
    }
    searchFn = (query) => provider.search(query);
  }

  const queries = buildSearchQueries(workRequest);
  const rawHits = [];

  try {
    for (const q of queries) {
      queried.push(q);
      const hits = await searchFn({
        query: q,
        industry:
          workRequest.targetSegment || workRequest.segment || q,
        location: workRequest.marketBounds || workRequest.location || '',
        limit: fetchLimit,
        workRequest,
      });
      if (!Array.isArray(hits)) continue;
      for (const hit of hits) {
        rawHits.push(hit);
      }
      if (rawHits.length >= targetMax * 2) break;
    }
  } catch (err) {
    return {
      ok: false,
      candidates: [],
      warnings,
      error: err && err.message ? String(err.message) : 'public_sourcing_failed',
      queried,
      crmWritesMade: false,
      outreachCopyGenerated: false,
      accountChangesMade: false,
    };
  }

  const mapped = [];
  for (let i = 0; i < rawHits.length; i += 1) {
    const candidate = mapPublicHitToScoutCandidate(rawHits[i], workRequest, i);
    if (!candidate) continue;
    // Soft-skip hard exclusion matches when they look like national chains etc.
    const exclusion = exclusionMatched(rawHits[i], workRequest);
    if (
      exclusion &&
      /national|chain|franchise/i.test(String(exclusion)) &&
      /national|chain|franchise|inc\.|llc/i.test(
        `${rawHits[i].companyName || ''} ${(rawHits[i].placeTypes || []).join(' ')}`
      )
    ) {
      warnings.push(
        `Skipped ${candidate.companyName} — exclusion: ${exclusion}`
      );
      continue;
    }
    mapped.push(candidate);
  }

  const candidates = dedupeCandidates(mapped).slice(0, targetMax);

  if (!candidates.length) {
    return {
      ok: false,
      candidates: [],
      warnings: warnings.concat([
        'Public-source search returned no usable candidates with source URLs.',
        'No placeholder rows were generated.',
      ]),
      error: 'no_usable_candidates',
      queried,
      crmWritesMade: false,
      outreachCopyGenerated: false,
      accountChangesMade: false,
    };
  }

  if (candidates.length < targetMin) {
    warnings.push(
      `Returned ${candidates.length} evidenced candidates (target ${targetMin}–${targetMax}). Shortfall preserved for operator review — no placeholders filled.`
    );
  }

  return {
    ok: true,
    candidates,
    warnings,
    error: null,
    queried,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
  };
}

module.exports = {
  TARGET_COUNT_MIN,
  TARGET_COUNT_MAX,
  SCOUT_PLACES_ENDPOINT_FAMILY,
  SCOUT_PLACES_AUTH_STYLE,
  SCOUT_PLACES_TEXTSEARCH_URL,
  SCOUT_PLACES_DETAILS_URL,
  SCOUT_PLACES_DETAILS_FIELDS,
  buildScoutPlacesTextSearchUrl,
  buildScoutPlacesDetailsUrl,
  scoutPlacesUrlHostPath,
  isScoutPublicSourcingAvailable,
  buildSearchQueries,
  mapPublicHitToScoutCandidate,
  createScoutPlacesSearchProvider,
  sourceScoutCandidatesFromPublicSources,
  normalizeWebsiteUrl,
  resolveSourceUrl,
};
