require('dotenv').config();

const axios = require('axios');

const GOOGLE_PLACES_ENDPOINT = 'https://places.googleapis.com/v1/places:searchText';
const GOOGLE_PLACE_DETAILS_ENDPOINT = 'https://places.googleapis.com/v1/places';
const APOLLO_CONTACTS_SEARCH_ENDPOINT = 'https://api.apollo.io/api/v1/contacts/search';
const PROSPEO_SEARCH_ENDPOINT = 'https://api.prospeo.io/search-person';
const DEFAULT_TITLES = ['owner'];

function envStatus() {
  return {
    GOOGLE_PLACES_KEY: Boolean(process.env.GOOGLE_PLACES_KEY),
    APOLLO_API_KEY: {
      present: Boolean(process.env.APOLLO_API_KEY),
      length: process.env.APOLLO_API_KEY ? process.env.APOLLO_API_KEY.length : 0,
    },
    PROSPEO_API_KEY: Boolean(process.env.PROSPEO_API_KEY),
  };
}

function logVerbose(enabled, label, value) {
  if (!enabled) return;
  console.log(`[phoneEnrich] ${label}:`, JSON.stringify(value, null, 2));
}

function baseNotes(notes) {
  return String(notes || '').split('\n\n--- setter notes ---\n')[0].trim();
}

function leadName(lead) {
  return lead.business_name ||
    baseNotes(lead.notes).split('—')[0].trim() ||
    `${lead.first_name || ''} ${lead.last_name || ''}`.trim() ||
    lead.email ||
    'Unknown Lead';
}

function leadWebsite(lead) {
  return lead.website || ((baseNotes(lead.notes) || '').split('—')[1] || '').trim();
}

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    return new URL(raw.startsWith('http') ? raw : `https://${raw}`).hostname.replace(/^www\./, '');
  } catch (err) {
    return raw.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0];
  }
}

function queryFor(lead) {
  const city = lead.city || 'Manchester NH';
  return [leadName(lead), city].filter(Boolean).join(' ');
}

function pickGooglePhone(place) {
  return place?.nationalPhoneNumber || place?.internationalPhoneNumber || null;
}

function pickApolloPhone(contact) {
  return contact?.phone ||
    contact?.sanitized_phone ||
    contact?.direct_phone ||
    contact?.mobile_phone ||
    contact?.organization?.phone ||
    contact?.account?.phone ||
    contact?.phone_numbers?.find(Boolean)?.raw_number ||
    contact?.phone_numbers?.find(Boolean)?.sanitized_number ||
    null;
}

function pickProspeoPhone(data) {
  const person = data?.person || {};
  const company = data?.company || {};
  return person.mobile?.mobile_international ||
    person.mobile?.mobile_national ||
    person.mobile?.mobile ||
    company.phone_hq?.phone_hq_international ||
    company.phone_hq?.phone_hq_national ||
    company.phone_hq?.phone_hq ||
    null;
}

function responsePayload(err) {
  return err.response?.data || { message: err.message };
}

async function googlePlacesAttempt(lead, options) {
  const verbose = Boolean(options.verbose);
  const key = process.env.GOOGLE_PLACES_KEY;
  if (!key) return { source: 'google_places', phone: null, skipped: 'missing_google_places_key' };

  const query = queryFor(lead);
  const searchRes = await axios.post(GOOGLE_PLACES_ENDPOINT, {
    textQuery: query,
    maxResultCount: 3,
  }, {
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': key,
      'X-Goog-FieldMask': 'places.displayName,places.websiteUri,places.nationalPhoneNumber,places.internationalPhoneNumber,places.formattedAddress,places.id',
    },
    timeout: 30000,
  });

  logVerbose(verbose, 'Google Places text search raw response', searchRes.data);
  const places = searchRes.data?.places || [];
  logVerbose(verbose, 'Google Places text search result count', { query, count: places.length });

  if (!places.length) return { source: 'google_places', phone: null, raw: searchRes.data };

  const directPhone = pickGooglePhone(places[0]);
  if (directPhone) return { source: 'google_places', phone: directPhone, raw: searchRes.data };

  const placeId = places[0].id;
  if (!placeId) return { source: 'google_places', phone: null, raw: searchRes.data };

  const detailsRes = await axios.get(`${GOOGLE_PLACE_DETAILS_ENDPOINT}/${placeId}`, {
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': key,
      'X-Goog-FieldMask': 'displayName,websiteUri,nationalPhoneNumber,internationalPhoneNumber,formattedAddress,id',
    },
    timeout: 30000,
  });
  logVerbose(verbose, 'Google Places details raw response', detailsRes.data);
  return { source: 'google_places', phone: pickGooglePhone(detailsRes.data), raw: detailsRes.data };
}

async function apolloAttempt(lead, options) {
  const verbose = Boolean(options.verbose);
  const key = process.env.APOLLO_API_KEY;
  logVerbose(verbose, 'Apollo env check', envStatus().APOLLO_API_KEY);
  logVerbose(verbose, 'Apollo endpoint', { endpoint: APOLLO_CONTACTS_SEARCH_ENDPOINT });
  if (!key) return { source: 'apollo', phone: null, skipped: 'missing_apollo_key' };

  const domain = normalizeDomain(leadWebsite(lead));
  const payload = {
    page: 1,
    per_page: 5,
    person_titles: DEFAULT_TITLES,
    q_keywords: leadName(lead),
    ...(domain ? { q_organization_domains: domain } : {}),
  };

  const res = await axios.post(APOLLO_CONTACTS_SEARCH_ENDPOINT, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-cache',
      'X-Api-Key': key,
    },
    timeout: 30000,
  });

  logVerbose(verbose, 'Apollo contacts search raw response', res.data);
  const contacts = res.data?.contacts || [];
  for (const contact of contacts) {
    const phone = pickApolloPhone(contact);
    if (phone) return { source: 'apollo', phone, raw: res.data };
  }
  return { source: 'apollo', phone: null, raw: res.data };
}

async function prospeoAttempt(lead, options) {
  const verbose = Boolean(options.verbose);
  const key = process.env.PROSPEO_API_KEY;
  if (!key) return { source: 'prospeo', phone: null, skipped: 'missing_prospeo_key' };

  const domain = normalizeDomain(leadWebsite(lead));
  if (!domain) return { source: 'prospeo', phone: null, skipped: 'missing_domain' };

  const res = await axios.post(PROSPEO_SEARCH_ENDPOINT, {
    page: 1,
    filters: {
      company: {
        websites: { include: [domain] },
      },
      person_job_title: {
        include: DEFAULT_TITLES,
      },
    },
  }, {
    headers: {
      'Content-Type': 'application/json',
      'X-KEY': key,
    },
    timeout: 30000,
  });

  logVerbose(verbose, 'Prospeo search-person raw response', res.data);
  const results = res.data?.results || [];
  for (const result of results) {
    const phone = pickProspeoPhone(result);
    if (phone) return { source: 'prospeo', phone, raw: res.data };
  }
  return { source: 'prospeo', phone: null, raw: res.data };
}

async function safeAttempt(name, fn, lead, options) {
  try {
    const result = await fn(lead, options);
    logVerbose(options.verbose, `${name} normalized result`, result);
    return result;
  } catch (err) {
    const payload = responsePayload(err);
    logVerbose(options.verbose, `${name} raw error response`, payload);
    return { source: name, phone: null, error: payload };
  }
}

async function enrichPhoneWaterfall(lead, options = {}) {
  const verbose = Boolean(options.verbose);
  const continueAfterFound = Boolean(options.continueAfterFound || verbose);
  let firstHit = null;
  logVerbose(verbose, 'Env status', envStatus());
  logVerbose(verbose, 'Lead input', {
    id: lead.id,
    name: leadName(lead),
    query: queryFor(lead),
    website: leadWebsite(lead),
    normalized_domain: normalizeDomain(leadWebsite(lead)),
  });

  const chain = [];
  for (const [name, fn] of [
    ['google_places', googlePlacesAttempt],
    ['apollo', apolloAttempt],
    ['prospeo', prospeoAttempt],
  ]) {
    const result = await safeAttempt(name, fn, lead, { verbose });
    chain.push(result);
    if (result.phone) {
      firstHit ||= { phone: result.phone, source: result.source };
      if (!continueAfterFound) return { ...firstHit, chain };
    }
  }
  return { phone: firstHit?.phone || null, source: firstHit?.source || null, chain };
}

async function runCli() {
  const query = process.argv.slice(2).join(' ') || 'All Clean Cleaners LLC Manchester NH';
  const lead = {
    business_name: query.replace(/\s+Manchester\s+NH$/i, ''),
    city: 'Manchester NH',
    notes: `${query.replace(/\s+Manchester\s+NH$/i, '')} — allcleancleaners.com`,
  };
  const result = await enrichPhoneWaterfall(lead, { verbose: true, continueAfterFound: true });
  console.log('[phoneEnrich] Final result:', JSON.stringify(result, null, 2));
}

if (require.main === module) {
  runCli().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = {
  APOLLO_CONTACTS_SEARCH_ENDPOINT,
  enrichPhoneWaterfall,
  envStatus,
};
