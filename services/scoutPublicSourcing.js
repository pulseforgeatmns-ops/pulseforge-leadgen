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

/** Alternate Scout public-source fallback (SerpAPI / Custom Search) is not wired on this path. */
const SCOUT_PUBLIC_SOURCE_FALLBACK = Object.freeze({
  id: 'serpapi_or_custom_search',
  available: false,
  status: 'unavailable',
  message:
    'SerpAPI / Google Custom Search fallback is not wired for Scout public-source handoff sourcing. Fix Google Places config, then retry the work request.',
});

const PLACES_SETUP_STEPS = Object.freeze([
  'Confirm GOOGLE_PLACES_KEY is set on the Railway service that runs Scout (node server.js).',
  'In Google Cloud Console for that API key’s project, enable Places API (Places API / Places API legacy — Text Search + Place Details).',
  'Confirm billing is enabled on the Google Cloud project (Places requires a billing account).',
  'Relax or fix API key restrictions so Railway server calls are allowed (prefer IP/server restrictions — not HTTP-referrer-only browser keys; allow Places API under API restrictions).',
  'After fixing config, retry the same Scout work request (POST /api/v1/scout/work-requests/:id/execute).',
]);

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
 *
 * Note: a set key only means the wiring check passes — Google may still return
 * REQUEST_DENIED at search time (billing / API enablement / key restrictions).
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

/**
 * Build an operator-facing Google Places setup diagnosis for Scout UI.
 * Does not mutate CRM / outreach / accounts.
 *
 * @param {object} detail
 * @param {string} [detail.status] Google Places status (e.g. REQUEST_DENIED)
 * @param {string} [detail.errorMessage] Google error_message field
 * @param {string} [detail.code] Internal error code
 * @param {boolean} [detail.keyPresent]
 */
function diagnosePlacesConfig(detail = {}) {
  const status = String(detail.status || '')
    .trim()
    .toUpperCase();
  const googleMsg = String(detail.errorMessage || detail.error_message || '')
    .trim();
  const keyPresent =
    detail.keyPresent != null
      ? Boolean(detail.keyPresent)
      : Boolean(String(detail.apiKey || process.env.GOOGLE_PLACES_KEY || '').trim());

  let code = detail.code || null;
  if (!code) {
    if (!keyPresent) code = 'google_places_key_missing';
    else if (status) code = `google_places_status_${status}`;
    else code = 'google_places_config_error';
  }

  const setupNeeded = [...PLACES_SETUP_STEPS];
  const lines = [];

  if (!keyPresent || code === 'google_places_key_missing' || code === 'public_sourcing_unavailable') {
    lines.push(
      'Scout public-source sourcing is blocked: GOOGLE_PLACES_KEY is missing or empty in this environment.'
    );
    lines.push(
      'Set GOOGLE_PLACES_KEY on Railway to a Google Cloud API key with Places API enabled, then retry.'
    );
  } else if (status === 'REQUEST_DENIED' || /REQUEST_DENIED/i.test(code)) {
    lines.push(
      'Scout public-source sourcing failed: Google Places returned REQUEST_DENIED.'
    );
    lines.push(
      'The API key is present, but Google rejected the request — this is an integration/config issue, not a Scout criteria failure.'
    );
  } else if (status === 'OVER_QUERY_LIMIT' || /OVER_QUERY_LIMIT/i.test(code)) {
    lines.push(
      'Scout public-source sourcing failed: Google Places returned OVER_QUERY_LIMIT (quota / billing).'
    );
  } else if (status === 'INVALID_REQUEST' || /INVALID_REQUEST/i.test(code)) {
    lines.push(
      'Scout public-source sourcing failed: Google Places returned INVALID_REQUEST.'
    );
  } else {
    lines.push(
      `Scout public-source sourcing failed: Google Places error (${code}).`
    );
  }

  if (googleMsg) {
    lines.push(`Google message: ${googleMsg}`);
  }

  lines.push('');
  lines.push('Setup needed:');
  for (let i = 0; i < setupNeeded.length; i += 1) {
    lines.push(`${i + 1}. ${setupNeeded[i]}`);
  }
  lines.push('');
  lines.push(`Fallback: ${SCOUT_PUBLIC_SOURCE_FALLBACK.message}`);
  lines.push('Work request remains retryable after Places config is fixed.');

  return {
    code,
    status: status || null,
    googleErrorMessage: googleMsg || null,
    keyPresent,
    retryable: true,
    setupNeeded,
    fallback: { ...SCOUT_PUBLIC_SOURCE_FALLBACK },
    operatorMessage: lines.join('\n'),
  };
}

/**
 * Parse thrown Places errors into a diagnosis payload.
 */
function diagnosePlacesError(err, opts = {}) {
  const message = err && err.message ? String(err.message) : '';
  const statusMatch = message.match(/google_places_status_([A-Z0-9_]+)/i);
  const httpMatch = message.match(/google_places_http_(\d+)/i);
  const googleMsg =
    (err && (err.googleErrorMessage || err.error_message)) ||
    (opts && opts.errorMessage) ||
    '';

  if (/public_sourcing_unavailable|google_places_key_missing/i.test(message)) {
    return diagnosePlacesConfig({
      code: 'google_places_key_missing',
      keyPresent: false,
      errorMessage: googleMsg,
      apiKey: opts.apiKey,
    });
  }

  if (statusMatch) {
    return diagnosePlacesConfig({
      status: statusMatch[1],
      code: `google_places_status_${statusMatch[1].toUpperCase()}`,
      errorMessage: googleMsg,
      keyPresent: true,
      apiKey: opts.apiKey,
    });
  }

  if (httpMatch) {
    return diagnosePlacesConfig({
      code: `google_places_http_${httpMatch[1]}`,
      errorMessage: googleMsg || `HTTP ${httpMatch[1]} from Google Places`,
      keyPresent: true,
      apiKey: opts.apiKey,
    });
  }

  return null;
}

class ScoutPlacesConfigError extends Error {
  constructor(diagnosis) {
    super(diagnosis.code || 'google_places_config_error');
    this.name = 'ScoutPlacesConfigError';
    this.code = diagnosis.code;
    this.diagnosis = diagnosis;
    this.googleErrorMessage = diagnosis.googleErrorMessage;
    this.retryable = true;
  }
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
      if (!this.available()) {
        const diagnosis = diagnosePlacesConfig({
          code: 'google_places_key_missing',
          keyPresent: false,
          apiKey,
        });
        throw new ScoutPlacesConfigError(diagnosis);
      }
      const limit = Math.min(Number(query.limit) || 20, 20);
      const q = String(query.query || query.industry || '').trim();
      if (!q) return [];

      const url = new URL(
        'https://maps.googleapis.com/maps/api/place/textsearch/json'
      );
      url.searchParams.set('query', q);
      url.searchParams.set('key', apiKey);

      const res = await fetchImpl(url.toString());
      if (!res.ok) {
        const diagnosis = diagnosePlacesConfig({
          code: `google_places_http_${res.status}`,
          errorMessage: `HTTP ${res.status} from Google Places Text Search`,
          keyPresent: true,
          apiKey,
        });
        throw new ScoutPlacesConfigError(diagnosis);
      }
      const data = await res.json();
      if (data.status === 'ZERO_RESULTS') return [];
      if (data.status !== 'OK') {
        const diagnosis = diagnosePlacesConfig({
          status: data.status,
          errorMessage: data.error_message || '',
          keyPresent: true,
          apiKey,
        });
        throw new ScoutPlacesConfigError(diagnosis);
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
  const url = new URL(
    'https://maps.googleapis.com/maps/api/place/details/json'
  );
  url.searchParams.set('place_id', placeId);
  url.searchParams.set(
    'fields',
    'name,formatted_address,formatted_phone_number,website,place_id,types,rating,business_status'
  );
  url.searchParams.set('key', apiKey);
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
 * @returns {Promise<{ ok: boolean, candidates: object[], warnings: string[], error: string|null, queried: string[], crmWritesMade: boolean, outreachCopyGenerated: boolean, accountChangesMade: boolean, retryable?: boolean, operatorMessage?: string|null, setupNeeded?: string[], placesDiagnosis?: object|null, fallback?: object|null }>}
 */
async function sourceScoutCandidatesFromPublicSources(input = {}) {
  const workRequest = input.workRequest || input;
  const opts = input.opts || {};
  const warnings = [];
  const queried = [];

  const emptyFailure = (extra = {}) => ({
    ok: false,
    candidates: [],
    warnings: extra.warnings || warnings,
    error: extra.error || null,
    queried,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    retryable: extra.retryable !== false,
    operatorMessage: extra.operatorMessage || null,
    setupNeeded: extra.setupNeeded || null,
    placesDiagnosis: extra.placesDiagnosis || null,
    fallback: extra.fallback || { ...SCOUT_PUBLIC_SOURCE_FALLBACK },
  });

  if (!workRequest || typeof workRequest !== 'object') {
    return emptyFailure({
      error: 'missing_work_request',
      retryable: false,
      fallback: null,
    });
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
      const diagnosis = diagnosePlacesConfig({
        code: 'public_sourcing_unavailable',
        keyPresent: false,
        apiKey: opts.apiKey || process.env.GOOGLE_PLACES_KEY || '',
      });
      return emptyFailure({
        warnings: [diagnosis.operatorMessage],
        error: diagnosis.code,
        retryable: true,
        operatorMessage: diagnosis.operatorMessage,
        setupNeeded: diagnosis.setupNeeded,
        placesDiagnosis: diagnosis,
        fallback: diagnosis.fallback,
      });
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
    const diagnosis =
      (err && err.diagnosis) ||
      diagnosePlacesError(err, {
        apiKey: opts.apiKey || process.env.GOOGLE_PLACES_KEY || '',
        errorMessage: err && err.googleErrorMessage,
      });
    if (diagnosis) {
      return emptyFailure({
        warnings: [diagnosis.operatorMessage],
        error: diagnosis.code,
        retryable: true,
        operatorMessage: diagnosis.operatorMessage,
        setupNeeded: diagnosis.setupNeeded,
        placesDiagnosis: diagnosis,
        fallback: diagnosis.fallback,
      });
    }
    return emptyFailure({
      error: err && err.message ? String(err.message) : 'public_sourcing_failed',
      retryable: true,
      operatorMessage: err && err.message ? String(err.message) : null,
    });
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
    return emptyFailure({
      warnings: warnings.concat([
        'Public-source search returned no usable candidates with source URLs.',
        'No placeholder rows were generated.',
        `Fallback: ${SCOUT_PUBLIC_SOURCE_FALLBACK.message}`,
      ]),
      error: 'no_usable_candidates',
      retryable: true,
      operatorMessage:
        'Public-source search returned no usable candidates with source URLs. No placeholders were generated. Work request remains retryable.',
      fallback: { ...SCOUT_PUBLIC_SOURCE_FALLBACK },
    });
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
    retryable: true,
    operatorMessage: null,
    setupNeeded: null,
    placesDiagnosis: null,
    fallback: null,
  };
}

module.exports = {
  TARGET_COUNT_MIN,
  TARGET_COUNT_MAX,
  SCOUT_PUBLIC_SOURCE_FALLBACK,
  PLACES_SETUP_STEPS,
  isScoutPublicSourcingAvailable,
  diagnosePlacesConfig,
  diagnosePlacesError,
  ScoutPlacesConfigError,
  buildSearchQueries,
  mapPublicHitToScoutCandidate,
  createScoutPlacesSearchProvider,
  sourceScoutCandidatesFromPublicSources,
  normalizeWebsiteUrl,
  resolveSourceUrl,
};
