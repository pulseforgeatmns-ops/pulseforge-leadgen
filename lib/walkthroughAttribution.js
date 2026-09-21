/**
 * Anchor walkthrough first-party attribution — sanitize, merge, normalize.
 * sourceKind is always FIRST_PARTY_ATTRIBUTION (never PLATFORM_API).
 */

const SOURCE_KIND = 'FIRST_PARTY_ATTRIBUTION';

const ATTRIBUTION_FIELD_KEYS = Object.freeze([
  'campaign_id',
  'ad_group_id',
  'ad_id',
  'ad_account_id',
  'oppref',
  'opref',
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_content',
  'utm_term',
  'landing_page_url',
  'referrer',
]);

const MAX_LENGTHS = Object.freeze({
  campaign_id: 128,
  ad_group_id: 128,
  ad_id: 128,
  ad_account_id: 128,
  oppref: 256,
  opref: 256,
  utm_source: 128,
  utm_medium: 128,
  utm_campaign: 256,
  utm_content: 256,
  utm_term: 256,
  landing_page_url: 2048,
  referrer: 2048,
});

const SEARCH_ENGINE_HOSTS = /(?:^|\.)((?:google|bing|yahoo|duckduckgo)\.[a-z.]+)$/i;

function cleanAttributionString(value, max) {
  if (value == null) return '';
  if (typeof value === 'object') return '';
  const text = String(value).replace(/[\0-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '').trim();
  if (!text) return '';
  return text.slice(0, max);
}

function sanitizeAttributionFields(input) {
  if (input == null || typeof input !== 'object' || Array.isArray(input)) {
    return null;
  }
  const out = {};
  for (const key of ATTRIBUTION_FIELD_KEYS) {
    const cleaned = cleanAttributionString(input[key], MAX_LENGTHS[key]);
    if (cleaned) out[key] = cleaned;
  }
  return Object.keys(out).length ? out : null;
}

function hasPaidAttributionSignals(raw) {
  if (!raw || typeof raw !== 'object') return false;
  return Boolean(
    raw.campaign_id
    || raw.ad_group_id
    || raw.ad_id
    || raw.ad_account_id
    || raw.oppref
    || raw.opref
    || raw.utm_source
    || raw.utm_medium
    || raw.utm_campaign
  );
}

function parseQueryAttribution(search = '') {
  const query = search.startsWith('?') ? search.slice(1) : String(search || '');
  if (!query) return null;
  const params = new URLSearchParams(query);
  const raw = {};
  for (const key of ATTRIBUTION_FIELD_KEYS) {
    if (key === 'landing_page_url' || key === 'referrer') continue;
    if (!params.has(key)) continue;
    raw[key] = params.get(key);
  }
  return sanitizeAttributionFields(raw);
}

function mergeSessionAttribution(existing, incoming) {
  const existingSan = sanitizeAttributionFields(existing);
  const incomingSan = sanitizeAttributionFields(incoming);

  if (!existingSan) {
    return incomingSan;
  }
  if (!incomingSan) {
    return existingSan;
  }

  const existingPaid = hasPaidAttributionSignals(existingSan);
  const incomingPaid = hasPaidAttributionSignals(incomingSan);

  if (existingPaid && !incomingPaid) {
    return existingSan;
  }
  if (!existingPaid && incomingPaid) {
    return { ...existingSan, ...incomingSan };
  }

  return {
    ...incomingSan,
    ...existingSan,
    landing_page_url: existingSan.landing_page_url || incomingSan.landing_page_url,
    referrer: existingSan.referrer || incomingSan.referrer,
  };
}

function hostFromUrl(value) {
  try {
    return new URL(value).hostname.toLowerCase();
  } catch (_) {
    return '';
  }
}

function isSearchReferrer(referrer) {
  const host = hostFromUrl(referrer);
  return host ? SEARCH_ENGINE_HOSTS.test(host) : false;
}

function normalizeUtmSource(value) {
  return cleanAttributionString(value, MAX_LENGTHS.utm_source).toLowerCase();
}

function normalizeUtmMedium(value) {
  return cleanAttributionString(value, MAX_LENGTHS.utm_medium).toLowerCase();
}

function classifyLeadSource(raw) {
  const utmSource = normalizeUtmSource(raw.utm_source);
  const utmMedium = normalizeUtmMedium(raw.utm_medium);

  if (raw.oppref || raw.opref) {
    return { lead_source: 'chatgpt_ads', attribution_status: 'deterministic' };
  }
  if (utmSource === 'openai' || utmSource === 'chatgpt') {
    return { lead_source: 'chatgpt_ads', attribution_status: 'deterministic' };
  }
  if (utmSource === 'google' && /^(cpc|ppc|paid(search)?|display)$/.test(utmMedium)) {
    return { lead_source: 'google_ads', attribution_status: 'deterministic' };
  }
  if (utmSource === 'yelp') {
    return { lead_source: 'yelp', attribution_status: 'deterministic' };
  }
  if (utmMedium === 'organic' || (isSearchReferrer(raw.referrer) && !hasPaidAttributionSignals(raw))) {
    return { lead_source: 'organic_search', attribution_status: 'inferred' };
  }
  if (raw.referrer && !isSearchReferrer(raw.referrer)) {
    const landingHost = hostFromUrl(raw.landing_page_url);
    const refHost = hostFromUrl(raw.referrer);
    if (refHost && refHost !== landingHost) {
      return { lead_source: 'referral', attribution_status: 'inferred' };
    }
  }
  if (utmSource && !['google', 'yelp', 'openai', 'chatgpt'].includes(utmSource)) {
    return { lead_source: 'unknown', attribution_status: 'inferred' };
  }
  if (
    raw.campaign_id
    || raw.ad_group_id
    || raw.ad_id
    || raw.ad_account_id
    || raw.utm_campaign
    || raw.utm_content
    || raw.utm_term
  ) {
    return { lead_source: 'unknown', attribution_status: 'inferred' };
  }
  if (raw.landing_page_url || raw.referrer) {
    return { lead_source: 'direct', attribution_status: 'unattributed' };
  }
  return { lead_source: 'unknown', attribution_status: 'unattributed' };
}

function normalizeWalkthroughAttribution(rawInput, options = {}) {
  const raw = sanitizeAttributionFields(rawInput);
  if (!raw) return null;

  if (options.serverReferer) {
    const serverReferer = cleanAttributionString(options.serverReferer, MAX_LENGTHS.referrer);
    if (serverReferer) raw.referrer = serverReferer;
  }

  const observedAt = options.observedAt instanceof Date
    ? options.observedAt.toISOString()
    : String(options.observedAt || new Date().toISOString());

  const classified = classifyLeadSource(raw);

  return {
    raw,
    normalized: {
      lead_source: classified.lead_source,
      attribution_status: classified.attribution_status,
      captured_at: observedAt,
    },
    provenance: {
      sourceKind: SOURCE_KIND,
      clientSubmitted: true,
      readOnly: true,
      observedAt,
    },
  };
}

function buildAttributionRecord(rawInput, options = {}) {
  const record = normalizeWalkthroughAttribution(rawInput, options);
  if (!record) return null;
  if (record.provenance.sourceKind !== SOURCE_KIND) {
    throw new Error('Attribution provenance must remain first-party');
  }
  return record;
}

module.exports = {
  SOURCE_KIND,
  ATTRIBUTION_FIELD_KEYS,
  MAX_LENGTHS,
  cleanAttributionString,
  sanitizeAttributionFields,
  hasPaidAttributionSignals,
  parseQueryAttribution,
  mergeSessionAttribution,
  normalizeWalkthroughAttribution,
  buildAttributionRecord,
};
