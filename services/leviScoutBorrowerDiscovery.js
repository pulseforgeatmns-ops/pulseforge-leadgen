'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');

const CITIES = Object.freeze(['New York', 'Chicago']);
const SOURCE_TYPE = 'property_record_signal';
const NYC_URL = 'https://data.cityofnewyork.us/resource/ipu4-2q9a.json';
const CHICAGO_URL = 'https://data.cityofchicago.org/resource/ydr8-5enu.json';

const SOURCES = Object.freeze({
  nyc: Object.freeze({
    city: 'New York',
    url: NYC_URL,
    permalink(row) {
      const id = encodeURIComponent(String(row.permit_si_no || row.job__ || ''));
      return `${NYC_URL}?permit_si_no=${id}`;
    },
  }),
  chicago: Object.freeze({
    city: 'Chicago',
    url: CHICAGO_URL,
    permalink(row) {
      const id = encodeURIComponent(String(row.permit_ || row.id || ''));
      return `${CHICAGO_URL}?permit_=${id}`;
    },
  }),
});

function fail(code) { throw Object.assign(new Error(code), { code }); }

function text(value) {
  return value == null ? '' : String(value).trim();
}

function calendarDate(value) {
  const raw = text(value);
  const match = /^(\d{4}-\d{2}-\d{2})/.exec(raw);
  return match ? match[1] : null;
}

function isPlacesUrl(url) {
  return /maps\.googleapis\.com|places\.googleapis\.com|serpapi\.com/i.test(String(url || ''));
}

function nycName(row) {
  const person = [row.owner_s_first_name, row.owner_s_last_name].map(text).filter(Boolean).join(' ');
  return person || text(row.owner_s_business_name) || null;
}

function chicagoName(row) {
  return text(row.contact_1_name) || text(row.owner_name) || null;
}

function candidate({ name, company, city, location, permit, permitType, address, url, evidenceDate, work }) {
  const sourceUrl = url;
  const fit = `Open-data building permit ${permit || ''} (${permitType || 'permit'}) at ${address || city}. ${work || ''}`.trim();
  return {
    name,
    company: company || name,
    job_title: 'Real-estate operator',
    email: null,
    phone: null,
    linkedin_url: null,
    service_area_match: location,
    do_not_contact: false,
    notes: [
      'Review state: qualify first.',
      `Fit evidence: ${fit}`,
      `Sources: ${sourceUrl}`,
    ].join('\n'),
    acquisition_metadata: {
      reviewState: 'qualify_first',
      category: 'borrower',
      sourceType: SOURCE_TYPE,
      sourceUrls: [sourceUrl],
      evidenceDate,
      city,
      provider: city === 'New York' ? 'nyc_open_data_permits' : 'chicago_open_data_permits',
    },
  };
}

function mapNyc(row) {
  const name = nycName(row);
  if (!name) return null;
  const address = [row.owner_s_house__, row.house__, row.owner_s_house_street_name, row.street_name, row.borough]
    .map(text).filter(Boolean).join(' ');
  return candidate({
    name,
    company: text(row.owner_s_business_name) || name,
    city: 'New York',
    location: 'New York, NY',
    permit: text(row.permit_si_no || row.job__),
    permitType: text(row.permit_type || row.job_type),
    address,
    url: SOURCES.nyc.permalink(row),
    evidenceDate: calendarDate(row.filing_date || row.issuance_date),
    work: text(row.job_description || row.work_type),
  });
}

function mapChicago(row) {
  const name = chicagoName(row);
  if (!name) return null;
  const address = [row.street_number, row.street_direction, row.street_name, row.suffix].map(text).filter(Boolean).join(' ');
  return candidate({
    name,
    company: name,
    city: 'Chicago',
    location: 'Chicago, IL',
    permit: text(row.permit_ || row.id),
    permitType: text(row.permit_type),
    address,
    url: SOURCES.chicago.permalink(row),
    evidenceDate: calendarDate(row.issue_date || row.application_start_date),
    work: text(row.work_description),
  });
}

async function readJson(fetchImpl, url, limit) {
  if (isPlacesUrl(url)) fail('levi_research_places_forbidden');
  const target = `${url}${url.includes('?') ? '&' : '?'}$limit=${encodeURIComponent(String(limit))}`;
  const response = await fetchImpl(target, { headers: { Accept: 'application/json' } });
  if (!response || response.ok === false) return [];
  const body = typeof response.json === 'function' ? await response.json() : [];
  return Array.isArray(body) ? body : [];
}

async function discover({
  tenantId = TENANT_ID,
  maxCandidates = 20,
  maxProviderRequests = 0,
  fetch: fetchImpl = globalThis.fetch,
  cities = CITIES,
} = {}) {
  if (String(tenantId) !== String(TENANT_ID)) fail('levi_research_tenant_invalid');
  if (!Number.isSafeInteger(maxCandidates) || maxCandidates < 1) fail('levi_research_discovery_contract_invalid');
  if (maxProviderRequests > 0) fail('levi_research_paid_provider_approval_required');
  const wanted = [...cities].filter(city => CITIES.includes(city));
  if (!wanted.length) return [];
  const perCity = Math.max(1, Math.ceil(maxCandidates / wanted.length));
  const out = [];
  for (const city of wanted) {
    const source = city === 'New York' ? SOURCES.nyc : SOURCES.chicago;
    const mapper = city === 'New York' ? mapNyc : mapChicago;
    let rows = [];
    try {
      rows = await readJson(fetchImpl, source.url, perCity);
    } catch (error) {
      if (error && error.code) throw error;
      rows = [];
    }
    let taken = 0;
    for (const row of rows) {
      if (taken >= perCity || out.length >= maxCandidates) break;
      const mapped = mapper(row);
      if (!mapped) continue;
      out.push(mapped);
      taken += 1;
    }
  }
  return out.slice(0, maxCandidates);
}

function citiesOf(rows = []) {
  return [...new Set(rows.map(row => row.acquisition_metadata && row.acquisition_metadata.city).filter(Boolean))];
}

module.exports = {
  CITIES, SOURCE_TYPE, SOURCES, NYC_URL, CHICAGO_URL,
  mapNyc, mapChicago, discover, citiesOf, isPlacesUrl,
};
