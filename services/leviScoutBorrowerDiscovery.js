'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');

const CITIES = Object.freeze(['New York', 'Chicago']);
const SOURCE_TYPE = 'property_record_signal';
const USER_AGENT = 'pulseforge-levi-scout-research-only/1.0';
const LOOKBACK_DAYS = 12;
const DISCOVERY_CAP = 10;
const FETCH_PAGE = 80;
const NYC_URL = 'https://data.cityofnewyork.us/resource/w9ak-ipjd.json';
const CHICAGO_URL = 'https://data.cityofchicago.org/resource/ydr8-5enu.json';
const ENTITY_TOKENS = Object.freeze(['LLC', 'INC', 'CORP', 'HOLDINGS', 'REALTY', 'PROPERTIES']);
const REHAB_LANGUAGE = Object.freeze(['rehab', 'interior', 'dwelling']);

const SOURCES = Object.freeze({
  nyc: Object.freeze({
    city: 'New York',
    url: NYC_URL,
    permalink(row) {
      const id = encodeURIComponent(String(row.job_filing_number || ''));
      return `${NYC_URL}?job_filing_number=${id}`;
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

function isPlacesUrl(url) {
  return /maps\.googleapis\.com|places\.googleapis\.com|serpapi\.com/i.test(String(url || ''));
}

function lookbackDate(now = new Date(), days = LOOKBACK_DAYS) {
  const day = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  day.setUTCDate(day.getUTCDate() - days);
  return day.toISOString().slice(0, 10);
}

function calendarDate(value, now = new Date()) {
  const raw = text(value);
  const match = /^(\d{4}-\d{2}-\d{2})/.exec(raw);
  if (!match) return null;
  const date = match[1];
  const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()))
    .toISOString().slice(0, 10);
  if (date > today) return null;
  return date;
}

function isEntityName(value) {
  const upper = text(value).toUpperCase();
  if (!upper) return false;
  return ENTITY_TOKENS.some(token => upper.includes(token));
}

function hasRehabLanguage(value) {
  const lower = text(value).toLowerCase();
  return REHAB_LANGUAGE.some(token => lower.includes(token));
}

function isSidewalkShedOnly(description, shedFlag) {
  const lower = text(description).toLowerCase();
  const shed = /sidewalk\s+shed/.test(lower) || /^yes$/i.test(text(shedFlag));
  if (!shed) return false;
  return !hasRehabLanguage(lower);
}

function isTuckpointingOnly(description) {
  const lower = text(description).toLowerCase();
  if (!/tuck\s*-?\s*point/.test(lower)) return false;
  return !hasRehabLanguage(lower);
}

function isAlterationOrNewBuilding(jobType) {
  const upper = text(jobType).toUpperCase();
  return upper.includes('ALTERATION') || upper.includes('NEW BUILDING');
}

function isRenovationPermit(permitType) {
  return /renovation/i.test(text(permitType));
}

function withinLookback(date, now) {
  const stamp = calendarDate(date, now);
  if (!stamp) return false;
  return stamp >= lookbackDate(now);
}

function nycAddress(row) {
  const street = [row.house_no, row.street_name].map(text).filter(Boolean).join(' ');
  const borough = text(row.borough);
  return [street, borough, 'New York, NY'].filter(Boolean).join(', ');
}

function chicagoPropertyAddress(row) {
  const street = [row.street_number, row.street_direction, row.street_name, row.suffix].map(text).filter(Boolean).join(' ');
  return street ? `${street}, Chicago, IL` : 'Chicago, IL';
}

function chicagoOwner(row) {
  for (let i = 1; i <= 15; i += 1) {
    const type = text(row[`contact_${i}_type`]).toUpperCase();
    const name = text(row[`contact_${i}_name`]);
    if (!type.startsWith('OWNER')) continue;
    if (!isEntityName(name)) continue;
    return name;
  }
  return null;
}

function fitEvidence(address) {
  return `The operator owns the property at ${address}. Renovation is underway`;
}

function candidate({ name, company, city, location, address, url, evidenceDate }) {
  const fit = fitEvidence(address);
  if (/needs\s*\$/i.test(fit)) return null;
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
      `Sources: ${url}`,
    ].join('\n'),
    acquisition_metadata: {
      reviewState: 'qualify_first',
      category: 'borrower',
      sourceType: SOURCE_TYPE,
      sourceUrls: [url],
      evidenceDate,
      city,
      provider: city === 'New York' ? 'nyc_open_data_permits' : 'chicago_open_data_permits',
    },
  };
}

function mapNyc(row, now = new Date()) {
  const owner = text(row.owner_s_business_name);
  const description = text(row.job_description);
  const evidenceDate = calendarDate(row.first_permit_date, now);
  if (!isEntityName(owner)) return null;
  if (!isAlterationOrNewBuilding(row.job_type)) return null;
  if (!withinLookback(row.first_permit_date, now)) return null;
  if (!hasRehabLanguage(description)) return null;
  if (isSidewalkShedOnly(description, row.sidewalk_shed_work_type_)) return null;
  if (isTuckpointingOnly(description)) return null;
  if (!evidenceDate) return null;
  const address = nycAddress(row);
  return candidate({
    name: owner,
    company: owner,
    city: 'New York',
    location: 'New York, NY',
    address,
    url: SOURCES.nyc.permalink(row),
    evidenceDate,
  });
}

function mapChicago(row, now = new Date()) {
  const owner = chicagoOwner(row);
  const description = text(row.work_description);
  const evidenceDate = calendarDate(row.issue_date, now);
  if (!owner) return null;
  if (!isRenovationPermit(row.permit_type)) return null;
  if (!withinLookback(row.issue_date, now)) return null;
  if (!evidenceDate) return null;
  const address = chicagoPropertyAddress(row);
  return candidate({
    name: owner,
    company: owner,
    city: 'Chicago',
    location: address,
    address,
    url: SOURCES.chicago.permalink(row),
    evidenceDate,
  });
}

function requestHeaders() {
  return { Accept: 'application/json', 'User-Agent': USER_AGENT };
}

function sodaUrl(base, where, limit) {
  const params = new URLSearchParams({
    $limit: String(limit),
    $order: where.includes('first_permit_date') ? 'first_permit_date DESC' : 'issue_date DESC',
    $where: where,
  });
  return `${base}?${params.toString()}`;
}

function likeAny(column, tokens) {
  return tokens.map(token => `upper(${column}) like '%${String(token).toUpperCase()}%'`).join(' OR ');
}

function nycWhere(since) {
  return [
    `first_permit_date >= '${since}'`,
    `(upper(job_type) like 'ALTERATION%' OR upper(job_type) like '%NEW BUILDING%')`,
    `(${likeAny('job_description', REHAB_LANGUAGE)})`,
    `(${likeAny('owner_s_business_name', ENTITY_TOKENS)})`,
  ].join(' AND ');
}

function chicagoOwnerClause() {
  const clauses = [];
  for (let i = 1; i <= 15; i += 1) {
    clauses.push(`(upper(contact_${i}_type) like 'OWNER%' AND (${likeAny(`contact_${i}_name`, ENTITY_TOKENS)}))`);
  }
  return `(${clauses.join(' OR ')})`;
}

function chicagoWhere(since) {
  return [
    `issue_date >= '${since}'`,
    `upper(permit_type) like '%RENOVATION%'`,
    chicagoOwnerClause(),
  ].join(' AND ');
}

async function readJson(fetchImpl, url) {
  if (isPlacesUrl(url)) fail('levi_research_places_forbidden');
  const response = await fetchImpl(url, { headers: requestHeaders() });
  if (!response || response.ok === false) return [];
  const body = typeof response.json === 'function' ? await response.json() : [];
  return Array.isArray(body) ? body : [];
}

function interleave(left, right) {
  const out = [];
  const n = Math.max(left.length, right.length);
  for (let i = 0; i < n; i += 1) {
    if (left[i]) out.push(left[i]);
    if (right[i]) out.push(right[i]);
  }
  return out;
}

async function discover({
  tenantId = TENANT_ID,
  maxCandidates = DISCOVERY_CAP,
  maxProviderRequests = 0,
  fetch: fetchImpl = globalThis.fetch,
  cities = CITIES,
  now = new Date(),
} = {}) {
  if (String(tenantId) !== String(TENANT_ID)) fail('levi_research_tenant_invalid');
  if (!Number.isSafeInteger(maxCandidates) || maxCandidates < 1) fail('levi_research_discovery_contract_invalid');
  if (maxProviderRequests > 0) fail('levi_research_paid_provider_approval_required');
  const wanted = [...cities].filter(city => CITIES.includes(city));
  if (!wanted.length) return [];
  const cap = Math.min(maxCandidates, DISCOVERY_CAP);
  const since = lookbackDate(now);
  const nycWanted = wanted.includes('New York');
  const chiWanted = wanted.includes('Chicago');
  let nycRows = [];
  let chiRows = [];
  try {
    if (nycWanted) {
      nycRows = await readJson(fetchImpl, sodaUrl(
        NYC_URL,
        nycWhere(since),
        FETCH_PAGE
      ));
    }
  } catch (error) {
    if (error && error.code) throw error;
    nycRows = [];
  }
  try {
    if (chiWanted) {
      chiRows = await readJson(fetchImpl, sodaUrl(
        CHICAGO_URL,
        chicagoWhere(since),
        FETCH_PAGE
      ));
    }
  } catch (error) {
    if (error && error.code) throw error;
    chiRows = [];
  }
  const nyc = nycRows.map(row => mapNyc(row, now)).filter(Boolean);
  const chi = chiRows.map(row => mapChicago(row, now)).filter(Boolean);
  return interleave(nyc, chi).slice(0, cap);
}

function citiesOf(rows = []) {
  return [...new Set(rows.map(row => row.acquisition_metadata && row.acquisition_metadata.city).filter(Boolean))];
}

module.exports = {
  CITIES, SOURCE_TYPE, SOURCES, NYC_URL, CHICAGO_URL, USER_AGENT, LOOKBACK_DAYS, DISCOVERY_CAP,
  mapNyc, mapChicago, discover, citiesOf, isPlacesUrl, lookbackDate, fitEvidence,
};
