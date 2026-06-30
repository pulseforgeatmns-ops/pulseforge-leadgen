const { searchSerpApi } = require('../lib/serpapi');

const VERTICAL_SYNONYMS = {
  property_management: ['property management', 'facilities management', 'property manager'],
  commercial_cleaning: ['commercial cleaning', 'janitorial services', 'facility services'],
  cleaning: ['commercial cleaning', 'janitorial services', 'facility services'],
  multi_location_operators: ['multi-location operator', 'multiple locations', 'regional operator'],
  commercial_landscaping: ['commercial landscaping', 'landscape management', 'commercial lawn care'],
  landscaping: ['commercial landscaping', 'landscape management', 'commercial lawn care'],
  franchise_home_services: ['home services franchise', 'franchise owner home services', 'home service franchisor'],
  home_services: ['home services', 'home service company', 'home services franchise'],
};

function parseList(value, fallback = []) {
  const values = Array.isArray(value) ? value : String(value || '').split(/[|,]/);
  const clean = values.map(item => String(item).trim().toLowerCase()).filter(Boolean);
  return clean.length ? [...new Set(clean)] : fallback;
}

function quote(value) {
  return `"${String(value).replace(/"/g, '').trim()}"`;
}

function orGroup(values) {
  return `(${values.map(quote).join(' OR ')})`;
}

function verticalTerms(vertical) {
  const key = String(vertical || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
  return VERTICAL_SYNONYMS[key] || [String(vertical || '').replace(/_/g, ' ').trim()].filter(Boolean);
}

function buildLinkedInQueries({ vertical, geo, titleFilter, titleExclude, sizeSignal } = {}) {
  const titles = parseList(titleFilter, ['owner', 'founder', 'ceo', 'president']);
  const excluded = parseList(titleExclude, ['agency', 'consultant', 'marketing']);
  const verticals = verticalTerms(vertical);
  const sizeTerms = parseList(sizeSignal);
  const suffix = [geo ? quote(geo) : '', ...excluded.map(term => `-${quote(term)}`), sizeTerms.length ? orGroup(sizeTerms) : '']
    .filter(Boolean)
    .join(' ');
  const variants = [];
  const add = (titleTerms, industryTerms) => {
    const query = `site:linkedin.com/in/ ${orGroup(titleTerms)} ${orGroup(industryTerms)} ${suffix}`.replace(/\s+/g, ' ').trim();
    if (!variants.includes(query)) variants.push(query);
  };

  add(titles, verticals);
  if (titles.length > 1) {
    const midpoint = Math.ceil(titles.length / 2);
    add(titles.slice(0, midpoint), verticals);
    add(titles.slice(midpoint), verticals);
  }
  for (const industryTerm of verticals) add(titles, [industryTerm]);
  for (const title of titles) add([title], verticals);
  return variants;
}

function canonicalizeLinkedInUrl(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, '');
    if (host !== 'linkedin.com' && !host.endsWith('.linkedin.com')) return null;
    const match = parsed.pathname.match(/^\/in\/([^/?#]+)/i);
    if (!match) return null;
    return `https://www.linkedin.com/in/${decodeURIComponent(match[1]).toLowerCase()}`;
  } catch (_) {
    return null;
  }
}

function titleCase(value) {
  return String(value || '').replace(/\b[a-z]/g, char => char.toUpperCase());
}

function nameFromSlug(linkedinUrl) {
  const slug = linkedinUrl?.split('/in/')[1] || '';
  const parts = slug.split('-').filter(Boolean);
  while (parts.length > 2 && /^(?:[a-z]*\d[a-z\d]*|[a-f\d]{6,})$/i.test(parts[parts.length - 1])) parts.pop();
  return titleCase(parts.join(' '));
}

function cleanResultTitle(rawTitle) {
  return String(rawTitle || '')
    .replace(/\s*[|–—-]\s*LinkedIn\s*$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseEmployeeCountSignal(text) {
  const source = String(text || '');
  const patterns = [
    /\b\d{1,5}\s*[-–]\s*\d{1,5}\s+employees\b/i,
    /\b\d{1,5}\+\s+employees\b/i,
    /\bteam\s+of\s+\d{1,5}\b/i,
    /\b\d{1,5}\+?\s+team\s+members\b/i,
  ];
  for (const pattern of patterns) {
    const match = source.match(pattern);
    if (match) return match[0].replace(/\s+/g, ' ').trim();
  }
  return null;
}

const ROLE_PATTERN = '(?:co-?owner|owner|co-?founder|founder|chief executive officer|ceo(?:\\s*\\(owner\\))?|president(?:\\s*&\\s*ceo)?|senior vice president|vice president|managing partner|partner|principal|director|general manager)';
const ROLE_RE = new RegExp(`\\b${ROLE_PATTERN}\\b`, 'i');

function parseRoleAndCompany(text) {
  const clean = String(text || '').replace(/\s+/g, ' ').trim();
  if (!clean) return null;
  const match = clean.match(new RegExp(`^(${ROLE_PATTERN})\\s*(?:at|of|,|[-–—])\\s+(.+)$`, 'i'));
  if (!match) return null;
  return { jobTitle: match[1].trim(), company: match[2].trim() };
}

const US_LOCATION_RE = /\b(?:united states|metropolitan area|alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming|AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/i;
const US_STATE_ONLY_RE = /^(?:alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)$/i;

function looksLikeLocation(value) {
  const text = String(value || '').trim();
  if (!text || text.length > 100) return false;
  if (/\b(?:employees|followers|connections)\b/i.test(text)) return false;
  if (/\b(?:licensed|licenses|certifications?|experienced|broker|responsible|specializes?)\b/i.test(text)) return false;
  if (ROLE_RE.test(text) || /\b(?:inc\.?|llc|ltd\.?|company|property management|services)\b/i.test(text)) return false;
  if (US_STATE_ONLY_RE.test(text)) return true;
  if (!US_LOCATION_RE.test(text)) return false;
  return /\b(?:united states|metropolitan area)\b/i.test(text)
    || (text.includes(',') && text.split(/\s+/).length <= 10);
}

function cleanParsedCompany(value) {
  const company = String(value || '').replace(/\s*[|–—-]\s*LinkedIn\s*$/i, '').trim();
  if (!company || /^(?:property management|facilities management|commercial cleaning|commercial landscaping|home services)(?:\s*\.\.\.)?$/i.test(company)) return null;
  return company;
}

function locationFromSnippet(snippet) {
  const text = String(snippet || '').replace(/\s+/g, ' ').trim();
  const labeled = text.match(/\bLocation:\s*([^·|.]{2,100})/i);
  if (labeled) return labeled[1].trim().replace(/\s+-\s*$/, '');
  const chunks = text.split(/\s+[·|]\s+/).map(chunk => chunk.trim()).filter(Boolean);
  const location = chunks.slice(0, 5).find(looksLikeLocation);
  if (location) return location;
  return null;
}

function parseLinkedInResult(item, sourceMeta = {}) {
  const linkedinUrl = canonicalizeLinkedInUrl(item?.link);
  if (!linkedinUrl) return { parsed: false, reason: 'not_linkedin_profile', raw: item };

  const headline = cleanResultTitle(item?.title);
  const segments = headline.split(/\s+[–—-]\s+/).map(value => value.trim()).filter(Boolean);
  let name = segments.shift() || '';
  if (!name || /^linkedin$/i.test(name) || name.length < 3) name = nameFromSlug(linkedinUrl);
  let jobTitle = null;
  let company = null;
  let location = null;
  const remainder = segments.join(' - ');
  const roleCompany = parseRoleAndCompany(remainder);
  if (roleCompany) {
    jobTitle = roleCompany.jobTitle;
    company = roleCompany.company;
    if (segments.length > 1 && looksLikeLocation(segments[segments.length - 1])) {
      location = segments[segments.length - 1];
      company = company.replace(new RegExp(`\\s+[–—-]\\s+${location.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), '').trim();
    }
  } else if (segments.length) {
    if (ROLE_RE.test(segments[0])) {
      jobTitle = segments[0].split(/\s+\|\s+/)[0] || null;
      company = segments[1] || null;
      location = segments.length > 2 && looksLikeLocation(segments.slice(2).join(' - '))
        ? segments.slice(2).join(' - ')
        : null;
    } else {
      company = segments[0] || null;
    }
  }
  const snippetRoleCompany = String(item?.snippet || '')
    .split(/\s+[·|]\s+/)
    .map(parseRoleAndCompany)
    .find(Boolean);
  if (snippetRoleCompany) {
    jobTitle ||= snippetRoleCompany.jobTitle;
    company ||= snippetRoleCompany.company;
  }
  location ||= locationFromSnippet(item?.snippet);
  const nameParts = name.split(/\s+/).filter(Boolean);
  if (!nameParts.length) return { parsed: false, reason: 'missing_name', raw: item };

  return {
    parsed: true,
    record: {
      first_name: nameParts[0],
      last_name: nameParts.slice(1).join(' ') || null,
      name,
      company: cleanParsedCompany(company),
      linkedin_url: linkedinUrl,
      linkedin_headline: headline || item?.title || null,
      linkedin_location: location,
      job_title: jobTitle,
      employee_count_estimate: parseEmployeeCountSignal(`${item?.title || ''} ${item?.snippet || ''}`),
      linkedin_source_query: {
        query: sourceMeta.query || null,
        page: Number(sourceMeta.page) || 1,
        result_rank: Number(item?.position) || Number(sourceMeta.rank) || null,
        timestamp: sourceMeta.timestamp || new Date().toISOString(),
      },
    },
  };
}

async function sourceLinkedInProspects({
  vertical,
  geo,
  titleFilter,
  titleExclude,
  sizeSignal,
  maxRequests = 20,
  pageDepth = 1,
  maxResults = 30,
  search = searchSerpApi,
} = {}) {
  const requestCap = Math.max(1, Math.min(Number(maxRequests) || 20, 100));
  const depth = Math.max(1, Math.min(Number(pageDepth) || 1, 10));
  const resultCap = Math.max(1, Math.min(Number(maxResults) || 30, 100));
  const variants = buildLinkedInQueries({ vertical, geo, titleFilter, titleExclude, sizeSignal });
  const records = [];
  const parseFailures = [];
  const queries = [];
  let requestCount = 0;
  let rawResultCount = 0;

  outer: for (const query of variants) {
    for (let page = 0; page < depth; page++) {
      if (requestCount >= requestCap) break outer;
      const response = await search({ query, page, num: 10 });
      requestCount += Number(response.requestCount) || 0;
      const organicResults = response.organicResults || [];
      rawResultCount += organicResults.length;
      queries.push({
        query,
        page: page + 1,
        result_count: organicResults.length,
        request_count: Number(response.requestCount) || 0,
        duration_ms: response.durationMs || 0,
        error: response.error || null,
      });
      organicResults.forEach((item, index) => {
        const parsed = parseLinkedInResult(item, { query, page: page + 1, rank: item.position || index + 1 });
        if (parsed.parsed) records.push(parsed.record);
        else parseFailures.push({ reason: parsed.reason, title: item.title, link: item.link, query, page: page + 1 });
      });
      if (response.error) break;
    }
  }

  const uniqueRecords = [];
  const seenUrls = new Set();
  let withinRunDeduped = 0;
  for (const record of records) {
    if (seenUrls.has(record.linkedin_url)) {
      withinRunDeduped++;
      continue;
    }
    seenUrls.add(record.linkedin_url);
    uniqueRecords.push(record);
  }
  const cappedCount = Math.max(uniqueRecords.length - resultCap, 0);
  if (cappedCount) uniqueRecords.length = resultCap;

  return {
    records: uniqueRecords,
    queries,
    requestCount,
    rawResultCount,
    parsedCount: records.length,
    parseFailures,
    withinRunDeduped,
    cappedCount,
    requestCap,
    pageDepth: depth,
    resultCap,
  };
}

module.exports = {
  buildLinkedInQueries,
  canonicalizeLinkedInUrl,
  parseEmployeeCountSignal,
  parseLinkedInResult,
  sourceLinkedInProspects,
};
