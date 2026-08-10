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
const {
  CANDIDATE_STATUS,
  interpretAnchorMarket,
  buildNhScopedSearchQueries,
  evaluateScoutCandidate,
  selectBatchWithManchesterFill,
  groupCandidatesByStatus,
  formatSuggestedContactRole,
  isGenericCriteriaCopy,
} = require('./scoutQualityGate');

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
  // Bias legacy Text Search toward USA — never UK Greater Manchester.
  url.searchParams.set('region', 'us');
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

function suggestedContactRole(workRequest, hit) {
  // Always label as suggested unless a verified named+title contact is present
  // (formatSuggestedContactRole enforces that rule).
  if (
    hit &&
    hit.suggestedContactRole &&
    /^suggested contact role:/i.test(String(hit.suggestedContactRole))
  ) {
    return hit.suggestedContactRole;
  }
  return formatSuggestedContactRole(workRequest, hit);
}

/**
 * Build NH-scoped Places queries: every query includes town + "NH" or
 * "New Hampshire". Interprets Anchor / Greater Manchester as New Hampshire, USA.
 */
function buildSearchQueries(workRequest, opts = {}) {
  const { queries, market } = buildNhScopedSearchQueries(workRequest, {
    clientId: opts.clientId || (workRequest && workRequest.clientId),
    clientSlug: opts.clientSlug || (workRequest && workRequest.clientSlug),
    scoringProfile:
      opts.scoringProfile || (workRequest && workRequest.scoringProfile),
    forceNewHampshire:
      opts.forceNewHampshire !== false &&
      shouldForceNewHampshire(workRequest, opts),
  });
  // Attach interpreted market for callers/tests (non-enumerable-safe via return only).
  buildSearchQueries.lastMarket = market;
  return queries;
}

function shouldForceNewHampshire(workRequest, opts = {}) {
  if (opts.forceNewHampshire === true) return true;
  if (opts.forceNewHampshire === false) return false;
  const market = String(
    (workRequest && (workRequest.marketBounds || workRequest.location)) || ''
  );
  const segment = String(
    (workRequest && (workRequest.targetSegment || workRequest.segment)) || ''
  );
  // Anchor PM campaigns and any NH / Greater Manchester brief → NH USA.
  if (
    opts.clientId === 10 ||
    opts.clientSlug === 'cleaning-co' ||
    (workRequest && workRequest.clientId === 10)
  ) {
    return true;
  }
  if (/property\s*manager/i.test(segment)) return true;
  if (
    /new hampshire|\bnh\b|bedford|hooksett|londonderry|auburn|goffstown|greater\s+manchester|manchester/i.test(
      market
    )
  ) {
    return true;
  }
  return true; // Scout public sourcing for this product path is NH-first.
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
  const gate = evaluateScoutCandidate(
    { ...hit, sourceUrl: sourceUrl || hit.sourceUrl || hit.website },
    workRequest,
    { sourceUrl }
  );
  return gate.confidence;
}

function fitRationaleForHit(hit, workRequest) {
  if (hit.fitRationale || hit.fitReason || hit.rationale) {
    const provided = hit.fitRationale || hit.fitReason || hit.rationale;
    if (!isGenericCriteriaCopy(provided, workRequest)) {
      return provided;
    }
  }
  const gate = evaluateScoutCandidate(hit, workRequest, {});
  return gate.fitRationale;
}

function risksForHit(hit, workRequest) {
  if (hit.risks || hit.disqualifyRisk || hit.risk || hit.uncertainty) {
    return hit.risks || hit.disqualifyRisk || hit.risk || hit.uncertainty;
  }
  const gate = evaluateScoutCandidate(hit, workRequest, {});
  return gate.risks;
}

/**
 * Map a public-source hit into a Scout candidate row.
 * Returns null when company name or source URL is missing (never fabricate).
 * Applies NH / cleaning / institutional quality gates and status labels.
 */
function mapPublicHitToScoutCandidate(hit, workRequest, idx = 0, opts = {}) {
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

  const enrichedHit = {
    ...hit,
    companyName: String(companyName).trim(),
    sourceUrl,
    website,
    location: hit.location || hit.address || hit.marketTown || null,
  };

  const gate = evaluateScoutCandidate(enrichedHit, workRequest, {
    sourceUrl,
    market: opts.market,
    clientId: opts.clientId || (workRequest && workRequest.clientId),
    forceNewHampshire: shouldForceNewHampshire(workRequest, opts),
  });

  return {
    id: hit.id || `scout-public-${idx + 1}`,
    companyName: String(companyName).trim(),
    sourceUrl,
    website,
    location: enrichedHit.location,
    marketTown: hit.marketTown || geoTownLabel(gate) || enrichedHit.location,
    segment:
      hit.segment ||
      (workRequest && workRequest.targetSegment) ||
      null,
    subtype:
      hit.subtype ||
      hit.segmentSubtype ||
      (workRequest && workRequest.targetSubtype) ||
      null,
    fitRationale: gate.fitRationale,
    risks: gate.risks,
    suggestedContactRole: gate.suggestedContactRole,
    confidence: gate.confidence,
    status: gate.status,
    statusReason: gate.statusReason,
    rejectionReason:
      gate.status === CANDIDATE_STATUS.REJECTED
        ? gate.rejectionReason || gate.statusReason
        : null,
    exclusionRisk: Boolean(gate.exclusionRisk),
    signals: gate.signals || null,
    geo: gate.geo || null,
    reviewOnly: true,
    placeholder: false,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    publicSource: hit.source || 'google_places',
    placeId: hit.placeId || hit.place_id || null,
  };
}

function geoTownLabel(gate) {
  return (gate && gate.geo && gate.geo.town) || null;
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

  const market = interpretAnchorMarket(
    workRequest.marketBounds || workRequest.location || '',
    {
      clientId: opts.clientId || workRequest.clientId,
      clientSlug: opts.clientSlug || workRequest.clientSlug,
      forceNewHampshire: shouldForceNewHampshire(workRequest, opts),
    }
  );
  if (market.interpretedAsNewHampshire) {
    warnings.push(
      `Market interpreted as ${market.marketLabel} (priority towns: ${market.priorityTowns.join(', ')}; Manchester NH is nearby/fill).`
    );
  }

  const queries = buildSearchQueries(workRequest, opts);
  const rawHits = [];

  try {
    for (const q of queries) {
      queried.push(q);
      const hits = await searchFn({
        query: q,
        industry:
          workRequest.targetSegment || workRequest.segment || q,
        location: `${market.priorityTowns[0] || 'Bedford'} NH`,
        limit: fetchLimit,
        workRequest,
        market,
      });
      if (!Array.isArray(hits)) continue;
      for (const hit of hits) {
        rawHits.push(hit);
      }
      if (rawHits.length >= targetMax * 3) break;
    }
  } catch (err) {
    return {
      ok: false,
      candidates: [],
      rejected: [],
      warnings,
      error: err && err.message ? String(err.message) : 'public_sourcing_failed',
      queried,
      market,
      crmWritesMade: false,
      outreachCopyGenerated: false,
      accountChangesMade: false,
    };
  }

  const mapped = [];
  const rejectedEarly = [];
  for (let i = 0; i < rawHits.length; i += 1) {
    const candidate = mapPublicHitToScoutCandidate(rawHits[i], workRequest, i, {
      market,
      ...opts,
    });
    if (!candidate) continue;
    if (candidate.status === CANDIDATE_STATUS.REJECTED) {
      rejectedEarly.push(candidate);
      warnings.push(
        `Rejected ${candidate.companyName} — ${candidate.statusReason}`
      );
      continue;
    }
    // Legacy soft exclusionCriteria still surfaces as risk, but institutional/
    // national hard-rejects are handled by the quality gate above.
    const exclusion = exclusionMatched(rawHits[i], workRequest);
    if (exclusion && !candidate.exclusionRisk) {
      candidate.risks = candidate.risks
        ? `${candidate.risks}; Possible exclusion match: ${exclusion}`
        : `Possible exclusion match: ${exclusion}`;
      if (/national|chain|franchise|institutional/i.test(String(exclusion))) {
        candidate.status = CANDIDATE_STATUS.REVIEW_REQUIRED;
        candidate.statusReason = `Exclusion criteria match: ${exclusion}`;
        candidate.exclusionRisk = true;
        if (candidate.confidence === 'high') {
          candidate.confidence = 'review_required';
        }
      }
    }
    mapped.push(candidate);
  }

  const deduped = dedupeCandidates(mapped);
  const selected = selectBatchWithManchesterFill(
    deduped,
    targetMax,
    targetMin
  );
  const candidates = selected.candidates;
  const rejected = dedupeCandidates(
    rejectedEarly.concat(selected.rejected || [])
  );
  const groups =
    selected.groups || groupCandidatesByStatus(candidates, rejected);

  if (!candidates.length) {
    return {
      ok: false,
      candidates: [],
      rejected,
      groups,
      warnings: warnings.concat([
        'Public-source search returned no usable in-market candidates after quality gates.',
        'No placeholder rows were generated.',
      ]),
      error: 'no_usable_candidates',
      queried,
      market,
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

  const acceptedCount = (groups.accepted || []).length;
  if (acceptedCount === 0) {
    warnings.push(
      'No candidates reached accepted status — batch is review_required only and must not be treated as outreach-ready.'
    );
  }

  return {
    ok: true,
    candidates,
    rejected,
    groups,
    warnings,
    error: null,
    queried,
    market,
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
  shouldForceNewHampshire,
  CANDIDATE_STATUS,
};
