'use strict';

/**
 * AUDIT-057 — Google Places candidate loss instrumentation.
 *
 * Traces each Places workload through:
 *   Text Search → Detail Fetch → Has Website → Scout acceptance gates
 *
 * Workload grid for client_id=10 (Anchor Cleaning): 6 NH cities × 6 verticals = 36.
 */

const axios = require('axios');
const {
  buildScoutPlacesTextSearchUrl,
  buildScoutPlacesDetailsUrl,
} = require('../../../services/scoutPublicSourcing');
const { parsePlacesAddressComponents } = require('../../../utils/serviceArea');

const PLACES_TEXTSEARCH =
  'https://maps.googleapis.com/maps/api/place/textsearch/json';
const PLACES_DETAILS =
  'https://maps.googleapis.com/maps/api/place/details/json';
const DETAIL_FIELDS =
  'name,formatted_address,address_components,formatted_phone_number,website,place_id,rating,user_ratings_total,types,business_status';

const REJECTION_BUCKETS = Object.freeze({
  MISSING_WEBSITE: 'missing_website',
  MISSING_PHONE: 'missing_phone',
  MISSING_CATEGORY: 'missing_category',
  UNSUPPORTED_TYPE: 'unsupported_type',
  DUPLICATE: 'duplicate',
  DISTANCE: 'distance',
  DETAIL_FETCH_FAILED: 'detail_fetch_failed',
  OTHER: 'other',
});

const CLIENT_10_CITIES = Object.freeze([
  'Manchester',
  'Bedford',
  'Goffstown',
  'Hooksett',
  'Londonderry',
  'Auburn',
]);

const CLIENT_10_VERTICALS = Object.freeze({
  cleaning_company_overflow: 'commercial cleaning company {city} {state}',
  str_manager: 'short term rental management {city} {state}',
  property_manager: 'property management company {city} {state}',
  realtor: 'real estate agency {city} {state}',
  restoration_remodeling_partner: 'water damage restoration {city} {state}',
  commercial_office: 'commercial office {city} {state}',
});

const SERVICE_AREA = Object.freeze([...CLIENT_10_CITIES]);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname.replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    const domain = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return domain || null;
  }
}

function isBlacklistedDomain(domain) {
  const host = String(domain || '').toLowerCase().replace(/^www\./, '');
  if (!host) return true;
  if (host.endsWith('.gov')) return true;
  const blocks = [
    'yelp.com', 'facebook.com', 'linkedin.com', 'instagram.com', 'twitter.com',
    'yellowpages.com', 'bbb.org', 'mapquest.com', 'nextdoor.com', 'thumbtack.com',
    'angi.com', 'porch.com', 'homeguide.com', 'co.uk',
  ];
  return blocks.some(
    (blocked) =>
      host === blocked || host.endsWith(`.${blocked}`) || host.includes(blocked)
  );
}

function matchServiceAreaLocality(locality, serviceAreas = SERVICE_AREA) {
  const normalizedLocality = String(locality || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalizedLocality) return null;
  return (
    serviceAreas.find(
      (area) =>
        String(area || '')
          .trim()
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, ' ')
          .replace(/\s+/g, ' ')
          .trim() === normalizedLocality
    ) || null
  );
}

function classifyPlacesB2B(lead) {
  const B2B_SIGNAL_RE =
    /\b(commercial|industrial|business|facility|facilit(?:y|ies)|corporate|office|property management|hoa|association management|janitorial|staffing|recruiting|freight|logistics|insurance|managed (?:it|service)|msp|fire protection|fire sprinkler|access control|low voltage|mechanical contractor)\b/i;
  const B2C_SIGNAL_RE =
    /\b(residential|homeowner|homeowners|home service|household|house call|house calls|electrical repair|home repair|our homes|your home)\b/i;
  const evidence = [
    lead.company,
    lead.url,
    lead.snippet,
    ...(lead.place_types || []),
  ]
    .filter(Boolean)
    .join(' ');
  const b2b = B2B_SIGNAL_RE.test(evidence);
  const b2c = B2C_SIGNAL_RE.test(evidence);
  if (b2c && !b2b) return 'b2c';
  if (b2b && !b2c) return 'b2b';
  return 'ambiguous';
}

/**
 * Build the 36 Anchor Cleaning (client_id=10) Places workloads.
 * @returns {object[]}
 */
function buildClient10PlacesWorkloads(state = 'NH') {
  const workloads = [];
  for (const city of CLIENT_10_CITIES) {
    for (const [vertical, template] of Object.entries(CLIENT_10_VERTICALS)) {
      const query = template.replace('{city}', city).replace('{state}', state);
      workloads.push({
        id: `${city}|${vertical}|google_places`,
        city: `${city} ${state}`,
        vertical,
        query,
      });
    }
  }
  return workloads;
}

async function fetchTextSearch(query, apiKey, fetchImpl) {
  const url = buildScoutPlacesTextSearchUrl({ query, apiKey });
  const res = await fetchImpl(url.toString());
  const data = await res.json();
  return { httpStatus: res.status, data };
}

async function fetchPlaceDetails(placeId, apiKey, fetchImpl) {
  const url = buildScoutPlacesDetailsUrl({ placeId, apiKey });
  url.searchParams.set(
    'fields',
    'name,formatted_address,address_components,formatted_phone_number,website,place_id,rating,user_ratings_total,types,business_status'
  );
  const res = await fetchImpl(url.toString());
  if (!res.ok) return null;
  const data = await res.json();
  return data.status === 'OK' ? data.result : null;
}

function classifyCandidate(hit, details, seen, opts = {}) {
  const placeId = details?.place_id || hit.place_id || null;
  const website = details?.website || null;
  const domain = website ? normalizeDomain(website) : null;
  const phone = details?.formatted_phone_number || null;
  const placeTypes = details?.types || hit.types || [];
  const address = parsePlacesAddressComponents(details?.address_components || []);
  const company = details?.name || hit.name || 'Unknown';

  const record = {
    placeId,
    company,
    website,
    domain,
    phone,
    placeTypes,
    locality: address.locality,
    address: details?.formatted_address || hit.formatted_address || '',
  };

  if (!details) {
    return {
      ...record,
      accepted: false,
      bucket: REJECTION_BUCKETS.DETAIL_FETCH_FAILED,
      reason: 'detail_fetch_failed',
    };
  }

  if (!website || !domain) {
    return {
      ...record,
      accepted: false,
      bucket: REJECTION_BUCKETS.MISSING_WEBSITE,
      reason: 'missing_website',
    };
  }

  if (!placeTypes.length) {
    return {
      ...record,
      accepted: false,
      bucket: REJECTION_BUCKETS.MISSING_CATEGORY,
      reason: 'missing_category',
    };
  }

  if (isBlacklistedDomain(domain)) {
    return {
      ...record,
      accepted: false,
      bucket: REJECTION_BUCKETS.UNSUPPORTED_TYPE,
      reason: 'unsupported_type:blacklisted_domain',
    };
  }

  const dedupeKey = placeId || domain;
  if (dedupeKey && seen.has(dedupeKey)) {
    return {
      ...record,
      accepted: false,
      bucket: REJECTION_BUCKETS.DUPLICATE,
      reason: 'duplicate',
    };
  }

  if (opts.enforceServiceArea !== false) {
    const inArea =
      matchServiceAreaLocality(address.locality, opts.serviceAreas) ||
      matchServiceAreaLocality(
        String(record.address).split(',')[0],
        opts.serviceAreas
      );
    if (!inArea && address.locality) {
      return {
        ...record,
        accepted: false,
        bucket: REJECTION_BUCKETS.DISTANCE,
        reason: `distance:out_of_service_area (${address.locality})`,
      };
    }
  }

  if (opts.enforceB2B !== false) {
    const b2b = classifyPlacesB2B({
      company,
      url: domain,
      snippet: '',
      place_types: placeTypes,
    });
    if (b2b === 'b2c') {
      return {
        ...record,
        accepted: false,
        bucket: REJECTION_BUCKETS.UNSUPPORTED_TYPE,
        reason: 'unsupported_type:b2c_classification',
      };
    }
  }

  if (dedupeKey) seen.add(dedupeKey);

  const flags = [];
  if (!phone) flags.push('missing_phone');

  return {
    ...record,
    accepted: true,
    bucket: null,
    reason: flags.length ? flags.join(',') : null,
    flags,
  };
}

/**
 * Audit one Places query workload.
 * @param {object} workload
 * @param {object} opts
 * @returns {Promise<object>}
 */
async function auditPlacesWorkload(workload, opts = {}) {
  const apiKey = opts.apiKey || process.env.GOOGLE_PLACES_KEY || '';
  const fetchImpl =
    opts.fetchImpl ||
    (typeof fetch === 'function'
      ? fetch.bind(globalThis)
      : (url) => axios.get(url).then((res) => ({
          ok: res.status >= 200 && res.status < 300,
          status: res.status,
          json: async () => res.data,
        })));
  const limit = Math.min(Number(opts.limit) || 20, 20);
  const detailDelayMs = Number(opts.detailDelayMs) || 200;
  const seen = opts.seen || new Set();

  const row = {
    workload,
    query: workload.query,
    textSearchHits: 0,
    detailFetchSuccess: 0,
    hasWebsite: 0,
    accepted: 0,
    rejected: 0,
    rejectionReasons: {},
    bucketCounts: {},
    googleStatus: null,
    error: null,
    candidates: [],
  };

  if (!apiKey) {
    row.error = 'GOOGLE_PLACES_KEY_missing';
    return row;
  }

  try {
    const { data } = await fetchTextSearch(workload.query, apiKey, fetchImpl);
    row.googleStatus = data?.status || null;
    if (data?.status !== 'OK' && data?.status !== 'ZERO_RESULTS') {
      row.error = `google_places_status_${data?.status || 'unknown'}`;
      return row;
    }

    const hits = (data?.results || []).slice(0, limit);
    row.textSearchHits = hits.length;

    for (const hit of hits) {
      let details = null;
      try {
        details = await fetchPlaceDetails(hit.place_id, apiKey, fetchImpl);
      } catch {
        details = null;
      }

      if (details) row.detailFetchSuccess += 1;
      if (details?.website) row.hasWebsite += 1;

      const verdict = classifyCandidate(hit, details, seen, opts);
      row.candidates.push(verdict);

      if (verdict.accepted) {
        row.accepted += 1;
      } else {
        row.rejected += 1;
        const bucket = verdict.bucket || REJECTION_BUCKETS.OTHER;
        row.bucketCounts[bucket] = (row.bucketCounts[bucket] || 0) + 1;
        const reasonKey = verdict.reason || bucket;
        row.rejectionReasons[reasonKey] =
          (row.rejectionReasons[reasonKey] || 0) + 1;
      }

      if (detailDelayMs > 0) await sleep(detailDelayMs);
    }
  } catch (err) {
    row.error = err?.message || 'audit_failed';
  }

  row.rejectionReasonSummary = summarizeReasons(row.rejectionReasons);
  return row;
}

function summarizeReasons(reasonMap = {}) {
  const parts = Object.entries(reasonMap)
    .sort((a, b) => b[1] - a[1])
    .map(([reason, count]) => `${count} ${reason}`);
  return parts.length ? parts.join('; ') : '';
}

/**
 * Run AUDIT-057 across all workloads.
 * @param {object[]} [workloads]
 * @param {object} [opts]
 * @returns {Promise<object>}
 */
async function runPlacesCandidateLossAudit(workloads = buildClient10PlacesWorkloads(), opts = {}) {
  const seen = new Set();
  const rows = [];
  const totals = {
    workloads: workloads.length,
    textSearchHits: 0,
    detailFetchSuccess: 0,
    hasWebsite: 0,
    accepted: 0,
    rejected: 0,
    buckets: {},
  };

  for (const workload of workloads) {
    const row = await auditPlacesWorkload(workload, { ...opts, seen });
    rows.push(row);
    totals.textSearchHits += row.textSearchHits;
    totals.detailFetchSuccess += row.detailFetchSuccess;
    totals.hasWebsite += row.hasWebsite;
    totals.accepted += row.accepted;
    totals.rejected += row.rejected;
    for (const [bucket, count] of Object.entries(row.bucketCounts || {})) {
      totals.buckets[bucket] = (totals.buckets[bucket] || 0) + count;
    }
  }

  const returned = totals.textSearchHits;
  const dropped = totals.rejected;
  const summary = {
    returned,
    dropped,
    accepted: totals.accepted,
    why: {
      missing_website: totals.buckets[REJECTION_BUCKETS.MISSING_WEBSITE] || 0,
      missing_phone: countMissingPhone(rows),
      missing_category: totals.buckets[REJECTION_BUCKETS.MISSING_CATEGORY] || 0,
      unsupported_type: totals.buckets[REJECTION_BUCKETS.UNSUPPORTED_TYPE] || 0,
      duplicate: totals.buckets[REJECTION_BUCKETS.DUPLICATE] || 0,
      distance: totals.buckets[REJECTION_BUCKETS.DISTANCE] || 0,
      other:
        (totals.buckets[REJECTION_BUCKETS.OTHER] || 0) +
        (totals.buckets[REJECTION_BUCKETS.DETAIL_FETCH_FAILED] || 0),
    },
  };

  return {
    audit: 'AUDIT-057',
    title: 'Google Places Candidate Loss',
    workloads: rows,
    totals,
    summary,
    diagnosedAt: new Date().toISOString(),
  };
}

function countMissingPhone(rows) {
  let count = 0;
  for (const row of rows) {
    for (const candidate of row.candidates || []) {
      if (candidate.accepted && candidate.flags?.includes('missing_phone')) {
        count += 1;
      }
      if (
        !candidate.accepted &&
        candidate.bucket === REJECTION_BUCKETS.MISSING_PHONE
      ) {
        count += 1;
      }
    }
  }
  return count;
}

function formatWorkloadTable(report) {
  const header = [
    'Query',
    'Text Search Hits',
    'Detail Fetch Success',
    'Has Website',
    'Accepted',
    'Rejected',
    'Rejection Reason',
  ];
  const lines = [
    `| ${header.join(' | ')} |`,
    `| ${header.map(() => '---').join(' | ')} |`,
  ];
  for (const row of report.workloads) {
    lines.push(
      `| ${row.query} | ${row.textSearchHits} | ${row.detailFetchSuccess} | ${row.hasWebsite} | ${row.accepted} | ${row.rejected} | ${row.rejectionReasonSummary || row.error || '—'} |`
    );
  }
  return lines.join('\n');
}

function formatSummary(report) {
  const { returned, dropped, accepted, why } = report.summary;
  const lines = [
    '## Funnel Summary',
    '',
    `${returned} businesses returned (text search hits)`,
    '↓',
    `${dropped} businesses dropped`,
    '↓',
    `${accepted} accepted`,
    '',
    '### Why dropped',
    `- Missing website: ${why.missing_website}`,
    `- Missing phone: ${why.missing_phone} (accepted rows flagged only; phone is not a Places-stage hard reject)`,
    `- Missing category: ${why.missing_category}`,
    `- Unsupported type: ${why.unsupported_type}`,
    `- Duplicate: ${why.duplicate}`,
    `- Distance: ${why.distance}`,
    `- Other: ${why.other}`,
    '',
    '### Interpretation',
  ];

  if (returned === 0) {
    lines.push(
      'Scout is blind because Google returned zero text-search hits across all workloads — the candidate universe is empty at the source.'
    );
  } else if (accepted === 0 && why.missing_website > 0) {
    lines.push(
      'Google has businesses, but Scout discards most of them before acceptance — primarily at the **missing website** gate in `searchGooglePlaces`.'
    );
  } else if (accepted > 0 && dropped > accepted) {
    lines.push(
      'Google returns candidates, but Scout throws away a large share — review the rejection buckets above to see whether loss is structural (missing website) or policy-driven (distance, B2C, duplicate).'
    );
  } else {
    lines.push(
      'Google returns candidates and a meaningful share survive Scout gates — blind spots are more likely downstream (enrichment, ICP, setter threshold) than at Places retrieval.'
    );
  }

  return lines.join('\n');
}

function formatAuditReport(report) {
  return [
    `# AUDIT-057 — Google Places Candidate Loss`,
    '',
    `Diagnosed: ${report.diagnosedAt}`,
    `Workloads: ${report.totals.workloads}`,
    '',
    formatWorkloadTable(report),
    '',
    formatSummary(report),
  ].join('\n');
}

module.exports = {
  REJECTION_BUCKETS,
  CLIENT_10_CITIES,
  CLIENT_10_VERTICALS,
  buildClient10PlacesWorkloads,
  auditPlacesWorkload,
  runPlacesCandidateLossAudit,
  classifyCandidate,
  formatAuditReport,
  formatWorkloadTable,
  formatSummary,
};
