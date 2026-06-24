/**
 * Pulseforge Lead Engine — Node.js CLI
 * =====================================
 * Pulls leads from Google Custom Search + Prospeo,
 * deduplicates, scores, and exports to CSV and/or Google Sheets.
 *
 * Setup:
 *   npm install axios cheerio csv-writer dotenv googleapis
 *   cp .env.example .env  → fill in your keys
 *   node leadgen.js --industry "cleaning" --location "Manchester NH" --max 25
 */

require('dotenv').config();
const axios = require('axios');
const { randomUUID } = require('crypto');
const { createObjectCsvWriter } = require('csv-writer');
const { google } = require('googleapis');
const pool = require('./db');
const { appendQualifiedScoutLead } = require('./utils/setterSheet');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { recordScoutBaseline } = require('./utils/icpScoring');
const { verifyEmail } = require('./utils/emailVerifier');
const { invalidOutreachEmailReason } = require('./utils/emailGuard');
const { ensureEmailVerificationColumns } = require('./utils/emailVerificationSchema');
const { ensureScoutUnenrichedTable } = require('./utils/scoutUnenrichedSchema');
const { acquireScoutLockWithWait, releaseScoutLock, getActiveScoutLock } = require('./utils/scoutLock');
const { awaitProspeoSlot } = require('./utils/prospeoThrottle');
const { checkProspeoQuota, recordProspeoCall, trip429 } = require('./utils/prospeoBreaker');
const { shouldExcludeProspect, extractEmailDomain } = require('./utils/prospectFilter');
const { normalizeVertical } = require('./utils/normalize');
const { SCOUT_SKIP_REASONS, ensureScoutSkipLogTable, logScoutSkip } = require('./utils/scoutSkipLog');
const { reportAgentRun } = require('./utils/agentObservability');

function normalizeCompanyName(raw) {
  if (!raw || typeof raw !== 'string') return raw;
  return raw
    .split('|')[0]
    .split('—')[0]
    .split(' - ')[0]
    .split(/:\s/)[0]
    .trim();
}

// Write one agent_log row per Scout run so we can answer
// "did Scout run? for which client/industry/location? did it find anything?"
// without trawling Railway stdout. Best-effort — never throws.
async function logScoutRun(status, payload, action = 'scrape') {
  const allowedStatuses = new Set(['success', 'failed', 'pending', 'completed', 'skipped']);
  const safeStatus = allowedStatuses.has(status) ? status : 'skipped';
  try {
    const result = await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
      RETURNING id
    `, ['scout', action, JSON.stringify(payload), safeStatus, CONFIG.clientId]);
    return result.rows[0]?.id || null;
  } catch (err) {
    console.error('[logScoutRun] failed to write:', err.message);
    return null;
  }
}

function makeScoutObservabilityRunId() {
  return `scout-${CONFIG.clientId || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

async function reportScoutRun({ runId, attempts, successes, skipped, errorSample = null }) {
  try {
    return await reportAgentRun({
      agent: 'scout',
      clientId: CONFIG.clientId,
      runId,
      attempts,
      successes,
      skipped,
      errorSample,
    });
  } catch (err) {
    console.error('[Scout] Observability report failed:', err.message);
    return null;
  }
}

async function getScoutSkipSummary(runId) {
  if (!runId) return { total: 0, dbErrorSample: null };
  try {
    const [countResult, dbErrorResult] = await Promise.all([
      pool.query('SELECT COUNT(*)::int AS count FROM scout_skip_log WHERE run_id = $1', [String(runId)]),
      pool.query(`
        SELECT detail
        FROM scout_skip_log
        WHERE run_id = $1 AND skip_reason = $2
        ORDER BY created_at DESC
        LIMIT 1
      `, [String(runId), SCOUT_SKIP_REASONS.DB_ERROR]),
    ]);
    return {
      total: countResult.rows[0]?.count || 0,
      dbErrorSample: dbErrorResult.rows[0]?.detail || null,
    };
  } catch (err) {
    console.error('[Scout] Skip summary lookup failed:', err.message);
    return { total: 0, dbErrorSample: { error: err.message } };
  }
}

async function resolveScoutObservabilityStats(stats, runId, fallbackError = null) {
  const dbErrors = Number(stats?.skipped_breakdown?.[SCOUT_SKIP_REASONS.DB_ERROR] || 0);
  const saved = Number(stats?.saved || 0);
  const skipSummary = await getScoutSkipSummary(runId);
  return {
    attempts: saved + dbErrors,
    successes: saved,
    skipped: skipSummary.total,
    errorSample: skipSummary.dbErrorSample || (fallbackError ? { error: fallbackError.message } : null),
  };
}

function incrementBreakdown(breakdown, reason) {
  breakdown[reason] = (breakdown[reason] || 0) + 1;
}

function scoutDiscoveryMethod(lead) {
  return Array.isArray(lead?.source) && lead.source.includes('google_places') ? 'google_places' : 'serpapi';
}

function scoutCandidateIdentifier(lead, companyName) {
  const email = typeof lead?.email === 'string' && lead.email !== '—' ? lead.email.trim() : '';
  return email || normalizeDomain(lead?.url) || `${companyName || lead?.company || 'unknown'} @ ${CONFIG.location}`;
}

async function persistScoutSkip(runId, lead, skipReason, detail = {}, companyName = null) {
  return logScoutSkip({
    runId, clientId: CONFIG.clientId, vertical: CONFIG.vertical, location: CONFIG.location,
    searchQuery: lead?.search_query || null, discoveryMethod: scoutDiscoveryMethod(lead),
    skipReason, candidateIdentifier: scoutCandidateIdentifier(lead, companyName), detail,
  });
}

async function recordEmailEnrichmentMethod(domain, method, details = {}) {
  const normalizedMethod = method || 'none';
  console.log(`[Enrichment] ${domain}: method=${normalizedMethod}`);
  await logScoutRun(normalizedMethod === 'none' ? 'skipped' : 'success', {
    domain,
    method: normalizedMethod,
    ...details,
  }, 'email_enrichment_method');
}

async function logExcludedProspect({ email, source, exclusion }) {
  try {
    await pool.query(`
      INSERT INTO excluded_prospect_log (email, domain, source, exclusion_reason, exclusion_detail)
      VALUES ($1, $2, $3, $4, $5::jsonb)
    `, [
      email || null,
      extractEmailDomain(email),
      source || 'scout',
      exclusion.reason,
      JSON.stringify(exclusion.detail || {}),
    ]);
  } catch (err) {
    console.error('[Scout] excluded_prospect_log write failed:', err.message);
  }
}


const DOMAIN_BLACKLIST = [
  'indeed.com','glassdoor.com','ziprecruiter.com','thumbtack.com',
  'yelp.com','yellowpages.com','mapquest.com','bbb.org','patch.com',
  'avvo.com','zoominfo.com','inven.ai','prnewswire.com','ofn.org',
  'cbsnews.com','bebee.com','amesburyma.gov',
  'townplanner.com','ccsnh.edu','servpro.com','stanleysteemer.com',
  'angieslist.com','homeadvisor.com','houzz.com','facebook.com',
  'linkedin.com','twitter.com','instagram.com','reddit.com',
  'google.com','bing.com','yahoo.com','amazon.com','wikipedia.org',
  'us.bold.pro','bold.pro','remotebooksonline.com',
  'unionleader.com','bizbuysell.com','procore.com','brixrecruiting.com',
  'windhamnh.gov','lincolnnh.gov','warnernh.gov','portsmouthnh.gov',
  'nh.gov','nh.us','vermont.gov','govinfo.gov',
  'vagaro.com','alignable.com','peerspace.com',
  'tiktok.com','snapchat.com',
  'businessnhmagazine.com','nhmagazine.com','nhpr.org',
  'issuu.com','smugmug.com','aptuitivcdn.com',
  'forbes.com','wmur.com','wokq.com','cnn.com','foxnews.com','nbcnews.com',
  'abcnews.go.com','usatoday.com','apnews.com','reuters.com','bloomberg.com',
  'nytimes.com','washingtonpost.com','wsj.com','npr.org','pbs.org',
  'bostonglobe.com','masslive.com','wcvb.com','nbcboston.com','boston.com',
  'trulia.com','zillow.com','bostonrealtyweb.com',
  'opensecrets.org','novoco.com','dealstream.com',
  'inmyarea.com','businessesforsale.com','veteranownedbusiness.com',
  'nbss.edu','grotonherald.com','speedbagcentral.com',
  'steemer.com','townplanner.com','bizbuysell.com','unionleader.com',
  'sniffspot.com','woofies.com','brixrecruiting.com','procore.com',
  'promatcher.com','afoodieaffair.com','dizscafe.com','redarrowdiner.com',
  'bostonvoyager.com','christopherduffley.com','shoppersmht.com',
  'opensecrets.org','latimes.com','businesswest.com','macaronikid.com',
  'crestmontcapital.com','thebedfordmom.com','mhl.org','usmodernist.org',
  'rackcdn.com','amazonaws.com','whs1959.com','spaindex.com',
  'sentextsolutions.com','londonderrynh.org',
  'nhpr.org','vagaro.com','rocketreach.co','experience.com',
  'nextdoor.com','promatcher.com','bizbuysell.com','turno.com',
  'pmrepublic.com','inmyarea.com','bark.com','expertise.com',
  'thumbtack.com','angi.com','porch.com','homeguide.com',
  'housekeeper.com','manchesterhousecleaning.com','co.uk',
  'legacy.com','dignitymemorial.com','tributearchive.com',
  'everloved.com','echovita.com','obitsarchive.com','obituary.com',
  'obituarieshelp.org','afterall.com',
];

function isBlacklistedDomain(domain) {
  const host = String(domain || '').toLowerCase().replace(/^www\./, '');
  if (!host) return true;
  if (host.endsWith('.gov')) return true;
  return DOMAIN_BLACKLIST.some(blocked => host === blocked || host.endsWith(`.${blocked}`) || host.includes(blocked));
}

const REJECTED_PROSPECT_NAME_PATTERNS = [
  /^Obituary information for/i,
  / Email Formats?$/i,
  /^Condo.*HOA Renovations/i,
];

function rejectedProspectNameReason(name) {
  const clean = String(name || '').trim();
  if (!clean) return null;
  if (REJECTED_PROSPECT_NAME_PATTERNS.some(pattern => pattern.test(clean))) {
    return 'blocked prospect name pattern';
  }
  return null;
}

function preEnrichmentRejectReason(lead) {
  const nameReason = rejectedProspectNameReason(lead?.company);
  if (nameReason) return nameReason;
  const domain = normalizeDomain(lead?.url);
  if (isBlacklistedDomain(domain)) return 'blocked domain';
  return null;
}

// ── CLI ARGS ─────────────────────────────────────────────────────────
const args = parseArgs(process.argv.slice(2));
let CONFIG = {
  industry:    args.industry  || 'cleaning',
  location:    sanitizeQueueLocation(args.location || 'Manchester NH'),
  jobTitle:    args.title     || 'owner',
  maxResults:  parseInt(args.max || '75'),
  minScore:    parseInt(args.minscore || '40'),
  mode:        args.mode      || 'both',     // smb | tech | both
  outputCSV:   args.csv       !== 'false',
  outputSheet: args.sheet     !== 'false',
  sheetId:     args.sheetid   || process.env.GOOGLE_SHEET_ID || '',
  clientId:    getRuntimeClientId(args),
  vertical:    normalizeVertical(args.industry || 'cleaning') || 'unknown',
  // Set from CLIENT_CONFIG.scoring_profile once the client row loads in run().
  // Selects which ICP rubric scoreLead() applies. 'cleaning_buyer' = the
  // commercial-cleaning buyer rubric; null/other = default lead-gen rubric.
  scoringProfile: null,
};
let CLIENT_CONFIG = null;

const GOOGLE_API_KEY    = process.env.GOOGLE_API_KEY;
const GOOGLE_CX         = process.env.GOOGLE_CX;
const PROSPEO_API_KEY   = process.env.PROSPEO_API_KEY;
const SETTER_ICP_THRESHOLD = 70;
// Cleaning pilot (client_id=10) runs a moderate threshold, not 70+. The pilot
// needs volume and Jacob is the human final-filter early on; live scores also
// run lower than the synthetic harness (which assumed full enrichment). Tune
// here as enrichment hit-rates come in.
const CLEANING_SETTER_THRESHOLD = 60;
function getSetterThreshold() {
  return CONFIG.scoringProfile === 'cleaning_buyer' ? CLEANING_SETTER_THRESHOLD : SETTER_ICP_THRESHOLD;
}

// Per-vertical saturation caps. Once a client has accumulated this many
// prospects in a vertical, Scout stops scraping it and rotates to the
// least-saturated queued vertical instead. Keys are normalized (snake_case).
const SATURATION_THRESHOLDS = {
  auto: 50,
  cleaning: 50,
  restaurant: 60,
  fitness: 40,
  salon: 40,
  med_spa: 30,
  landscaping: 30,
  property_management: 40,
  probate_attorney: 40,
  home_services: 30,
  default: 40,
};

const MSHI_SCOUT_CITIES = [
  'Charleston',
  'South Charleston',
  'St. Albans',
  'Dunbar',
  'Nitro',
  'Cross Lanes',
  'Hurricane',
];

const MSHI_PROBATE_ATTORNEY_GEO = [
  { city: 'Charleston', state: 'WV' },
  { city: 'South Charleston', state: 'WV' },
  { city: 'St. Albans', state: 'WV' },
  { city: 'Dunbar', state: 'WV' },
  { city: 'Nitro', state: 'WV' },
  { city: 'Cross Lanes', state: 'WV' },
  { city: 'Hurricane', state: 'WV' },
  { city: 'Teays Valley', state: 'WV' },
  { city: 'Scott Depot', state: 'WV' },
  { city: 'Huntington', state: 'WV' },
  { city: 'Barboursville', state: 'WV' },
  { city: 'Logan', state: 'WV' },
  { city: 'Madison', state: 'WV' },
  { city: 'Hamlin', state: 'WV' },
  { city: 'Fayetteville', state: 'WV' },
  { city: 'Beckley', state: 'WV' },
];

// Manchester pilot cluster for client_id=10. Tightened from the wider ring for
// the pilot — the held-back towns (Nashua, Concord, Derry, Merrimack,
// Litchfield, Hudson, etc.) come back post-pilot. Must stay in sync with
// clients.service_area, which drives Scout's out-of-area cull. Also used by the
// cleaning ICP rubric for geography scoring and by Scout's city rotation.
const CLEANING_AREA_CITIES = [
  'Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn',
];

const CLIENT_SCOUT_PLANS = {
  // Cleaning company (client_id=10). Professional-services offices that BUY
  // commercial cleaning. Law firms and accounting practices run as SEPARATE
  // passes (one vertical per run). Google Places is primary for this client
  // (see getSourcePreference) — SerpAPI underperforms on professional offices.
  10: {
    cities: CLEANING_AREA_CITIES,
    verticals: {
      law_firm: [
        'law firm {city} {state}',
        'law office {city} {state}',
        'attorney {city} {state}',
      ],
      accounting: [
        'accounting firm {city} {state}',
        'cpa firm {city} {state}',
        'tax accountant {city} {state}',
      ],
    },
  },
  2: {
    cities: MSHI_SCOUT_CITIES,
    geoByVertical: {
      probate_attorney: MSHI_PROBATE_ATTORNEY_GEO,
    },
    verticals: {
      property_management: [
        'property management company {city} WV',
      ],
      probate_attorney: [
        'probate attorney {city} {state}',
        'estate attorney {city} {state}',
        'estate planning attorney {city} {state}',
        'trust and estate attorney {city} {state}',
      ],
      renovation_lender: [
        'mortgage broker {city} WV',
      ],
      insurance_restoration: [
        'insurance agency {city} WV',
        'public adjuster {city} WV',
      ],
      home_inspector: [
        'home inspector {city} WV',
      ],
      listing_agent: [
        'real estate agent {city} WV',
      ],
    },
  },
};

function getSaturationThreshold(vertical) {
  const key = normalizeVertical(vertical) || 'unknown';
  return Object.prototype.hasOwnProperty.call(SATURATION_THRESHOLDS, key)
    ? SATURATION_THRESHOLDS[key]
    : SATURATION_THRESHOLDS.default;
}

function sanitizeQueueLocation(value) {
  return String(value == null ? '' : value)
    .replace(/https?:\/\/\S*/gi, ' ')
    .replace(/\bwww\.\S*/gi, ' ')
    .replace(/[^a-zA-Z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeGeoText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function cityFromLocation(location) {
  return sanitizeQueueLocation(location)
    .replace(/\b[A-Z]{2}\b$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function getClientScoutPlan(clientId) {
  return CLIENT_SCOUT_PLANS[Number(clientId)] || null;
}

function getPlannedVerticals(clientId) {
  const plan = getClientScoutPlan(clientId);
  return plan ? Object.keys(plan.verticals || {}) : [];
}

function getPlannedLocations(clientId, fallbackLocation, vertical = CONFIG.vertical) {
  const plan = getClientScoutPlan(clientId);
  const normalizedVertical = normalizeVertical(vertical);
  const geoTargets = Array.isArray(plan?.geoByVertical?.[normalizedVertical])
    ? plan.geoByVertical[normalizedVertical]
    : null;
  if (geoTargets?.length) return geoTargets.map(({ city, state }) => sanitizeQueueLocation(`${city} ${state}`));
  const cities = Array.isArray(plan?.cities) ? plan.cities : [];
  if (!cities.length) return [sanitizeQueueLocation(fallbackLocation)];
  return cities.map(city => sanitizeQueueLocation(`${city} WV`));
}

function getSearchQueriesForTarget() {
  const plan = getClientScoutPlan(CONFIG.clientId);
  const seeds = plan?.verticals?.[CONFIG.vertical];
  if (!Array.isArray(seeds) || seeds.length === 0) {
    return [sanitizeQueueLocation(`${CONFIG.industry} ${CONFIG.location}`)];
  }
  const city = cityFromLocation(CONFIG.location);
  const state = sanitizeQueueLocation(CONFIG.location).split(/\s+/).pop() || CLIENT_CONFIG?.state || '';
  return seeds.map(seed => seed
    .replace(/\{city\}/g, city)
    .replace(/\{state\}/g, state)
    .trim());
}

function locationToIlikePattern(location) {
  const cleaned = sanitizeQueueLocation(location);
  if (!cleaned) return '%';
  return `%${cleaned.split(/\s+/).join('%')}%`;
}

const GENERIC_CONTACT_NAMES = new Set(['there', 'info', 'hello', 'contact', 'admin', 'support', 'sales']);
const GENERIC_EMAIL_PREFIX_RE = /^(?:info|hello|contact|admin|support|sales|office|team|service|customerservice|customer\.?service|no-?reply|noreply|mail|inquir(?:y|ies))[\w.+-]*$/i;

function sanitizeFirstName(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const local = raw.includes('@') ? raw.split('@')[0] : raw;
  const cleaned = local.trim().replace(/^["'`]+|["'`]+$/g, '');
  if (!cleaned) return null;
  const normalized = cleaned.toLowerCase();
  if (GENERIC_CONTACT_NAMES.has(normalized)) return null;
  if (GENERIC_EMAIL_PREFIX_RE.test(normalized)) return null;
  return cleaned;
}


async function enrichWithHunter(domain) {
  const HUNTER_KEY = process.env.HUNTER_API_KEY;
  if (!HUNTER_KEY) {
    console.warn('[WARN] Hunter key not set — skipping Hunter enrichment');
    return null;
  }
  try {
    const res = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key: HUNTER_KEY, limit: 5, type: 'personal' }
    });
    const emails = res.data?.data?.emails || [];
    if (!emails.length) return null;
    const tf = (CONFIG.jobTitle || '').toLowerCase();
    const match = emails.find(e => e.position?.toLowerCase().includes(tf)) || emails[0];
    return {
      contact: (match.first_name || '') + ' ' + (match.last_name || ''),
      email: match.value || null,
      title: match.position || null
    };
  } catch (err) {
    return null;
  }
}


// ─────────────────────────────────────────────────────────────────────
// STEP 2c: Scrape website for contact email (fallback for Places leads)
// ─────────────────────────────────────────────────────────────────────
async function scrapeWebsiteEmail(domain) {
  const pages = [
    `https://${domain}/contact`,
    `https://${domain}/contact-us`,
    `https://${domain}/about`,
    `https://www.${domain}/contact`,
    `https://www.${domain}`
  ];

  for (const url of pages) {
    try {
      const res = await axios.get(url, {
        timeout: 5000,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)' }
      });
      const html = res.data;

      // Extract email from page
      const emailMatch = html.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g);
      if (emailMatch) {
        // Filter out noreply, info@, support@ — prefer personal emails
        const filtered = emailMatch.filter(e =>
          !e.includes('noreply') &&
          !e.includes('no-reply') &&
          !e.includes('example.com') &&
          !e.includes('sentry') &&
          !e.includes('wix') &&
          !e.includes('squarespace') &&
          !e.includes('.png') &&
          !e.includes('.jpg')
        );
        if (filtered.length > 0) {
          return { email: filtered[0], contact: '', title: '' };
        }
      }
    } catch(err) {
      // try next page
    }
  }
  return null;
}

async function fetchProbateWebsiteSignals(domain) {
  if (CONFIG.vertical !== 'probate_attorney' || !domain) return [];

  const urls = [
    `https://${domain}`,
    `https://${domain}/practice-areas`,
    `https://${domain}/services`,
    `https://${domain}/probate`,
    `https://${domain}/estate-planning`,
  ];
  const signals = new Set();

  for (const url of urls) {
    try {
      const res = await axios.get(url, {
        timeout: 4000,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)' },
      });
      const text = String(res.data || '').toLowerCase().replace(/<[^>]*>/g, ' ');
      if (/\bprobate\b/.test(text)) signals.add('probate');
      if (/\bestate\s+sale(s)?\b/.test(text)) signals.add('estate_sale');
      if (/\bexecutor(s)?\b|\bexecutrix\b/.test(text)) signals.add('executor');
      if (/\bestate\s+planning\b/.test(text)) signals.add('estate_planning');
      if (signals.size >= 3) break;
    } catch(err) {
      // try next service page
    }
  }

  return [...signals];
}

// ─────────────────────────────────────────────────────────────────────
// STEP 1: Google Custom Search
// Docs: https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
// Free tier: 100 queries/day. $5 per 1,000 after.
// ─────────────────────────────────────────────────────────────────────
async function searchGoogle(query, numResults = 10) {
  if (!process.env.SERPAPI_KEY) {
    console.warn('[WARN] SerpAPI key not set — skipping search');
    return [];
  }

  const results = [];
  const socialLinks = []; // { key, facebook_url?, instagram_url? } captured from social results
  const pages = Math.ceil(numResults / 10);

  for (let page = 0; page < pages; page++) {
    if (results.length >= numResults) break;
    try {
      const res = await axios.get('https://serpapi.com/search', {
        params: {
          api_key: process.env.SERPAPI_KEY,
          q: query,
          num: 10,
          start: page * 10,
          engine: 'google',
        }
      });

      const remaining = Math.max(numResults - results.length, 0);
      const items = (res.data.organic_results || []).slice(0, remaining);
      for (const item of items) {
        const link = item.link || '';
        const company = normalizeCompanyName(item.title);
        // Capture facebook/instagram profile links so they can be attached to the
        // matching business lead instead of being discarded as junk domains.
        if (/facebook\.com/i.test(link)) {
          socialLinks.push({ key: socialKey(company), facebook_url: link });
          continue;
        }
        if (/instagram\.com/i.test(link)) {
          socialLinks.push({ key: socialKey(company), instagram_url: link });
          continue;
        }
        results.push({
          company,
          url:     extractDomain(item.link),
          snippet: item.snippet,
          source:  ['google'],
        });
        if (results.length >= numResults) break;
      }
    } catch (err) {
      console.error('[SerpAPI] Error:', err.response?.data?.error || err.message);
      break;
    }
  }

  // Attach captured social URLs to the business lead whose name matches.
  for (const lead of results) {
    const key = socialKey(lead.company);
    if (!key) continue;
    for (const social of socialLinks) {
      if (social.key !== key) continue;
      if (social.facebook_url && !lead.facebook_url) lead.facebook_url = social.facebook_url;
      if (social.instagram_url && !lead.instagram_url) lead.instagram_url = social.instagram_url;
    }
  }

      const skipDomains = ['facebook.com','instagram.com','yelp.com','twitter.com','linkedin.com','youtube.com'];
      return results.filter(r => !skipDomains.some(s => r.url.includes(s)));
}

// Normalized key for fuzzy-matching company names across SerpAPI result types
// (e.g. a business's website result and its Facebook page result).
function socialKey(name) {
  return String(name || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

// ─────────────────────────────────────────────────────────────────────
// STEP 2: Prospeo Domain Search
// Docs: https://prospeo.io/api
// Takes a domain → returns contacts with name, title, email
// Starter plan search endpoints: 30 req/min — throttle to ~28/min + backoff on 429
// ─────────────────────────────────────────────────────────────────────
const PROSPEO_RATE_LIMIT_BACKOFF_MS = [1000, 2000, 4000];

function isProspeoRateLimited(err) {
  if (err?.response?.status === 429) return true;
  const body = err?.response?.data;
  const text = typeof body === 'string'
    ? body
    : JSON.stringify(body || err?.message || '');
  return /rate\s*limit/i.test(text);
}

// Decision-maker titles to ask Prospeo for. The cleaning client wants the
// person who can authorize a walkthrough: owner OR office manager (plus the
// usual small-firm principal titles). Other clients keep the single
// CONFIG.jobTitle behavior.
function getProspeoTitleIncludes() {
  if (CONFIG.scoringProfile === 'cleaning_buyer') {
    return ['owner', 'office manager', 'managing partner', 'partner', 'principal', 'founder', 'president'];
  }
  return [CONFIG.jobTitle];
}

async function callProspeoSearchPerson(domain) {
  await awaitProspeoSlot();

  const res = await axios.post('https://api.prospeo.io/search-person',
    {
      page: 1,
      filters: {
        company: {
          websites: { include: [domain] }
        },
        person_job_title: {
          include: getProspeoTitleIncludes()
        }
      }
    },
    {
      headers: {
        'Content-Type': 'application/json',
        'X-KEY': PROSPEO_API_KEY
      }
    }
  );

  const results = res.data?.results || [];
  if (!results.length) return null;

  const match = results[0];
  const person = match.person || {};

  return {
    contact: `${person.first_name || ''} ${person.last_name || ''}`.trim(),
    email: typeof person.email === 'object' ? person.email?.email || null : person.email || null,
    title: person.job_title || null,
  };
}

async function enrichWithProspeo(domain) {
  if (process.env.PROSPEO_ENABLED !== 'true') {
    return null;
  }

  if (!PROSPEO_API_KEY) {
    console.warn('[WARN] Prospeo key not set — skipping enrichment');
    return null;
  }

  const quota = await checkProspeoQuota();
  if (!quota.ok) {
    console.warn(`[Prospeo] Skipped (${quota.reason}): ${quota.count ?? '?'}/${quota.cap}`);
    return null;
  }
  await recordProspeoCall();

  const maxAttempts = 1 + PROSPEO_RATE_LIMIT_BACKOFF_MS.length;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await callProspeoSearchPerson(domain);
    } catch (err) {
      const rateLimited = isProspeoRateLimited(err);
      if (rateLimited) trip429();
      const backoffMs = PROSPEO_RATE_LIMIT_BACKOFF_MS[attempt];

      if (rateLimited && backoffMs != null) {
        await logScoutRun('skipped', {
          domain,
          attempt: attempt + 1,
          backoff_ms: backoffMs,
          error: err.response?.data || err.message,
        }, 'prospeo_rate_limited');
        console.warn(`[Prospeo] ${domain}: rate limited — retry in ${backoffMs}ms (attempt ${attempt + 1})`);
        await new Promise(r => setTimeout(r, backoffMs));
        continue;
      }

      if (rateLimited) {
        await logScoutRun('skipped', {
          domain,
          attempt: attempt + 1,
          exhausted: true,
          error: err.response?.data || err.message,
        }, 'prospeo_rate_limited');
        console.warn(`[Prospeo] ${domain}: rate limit retries exhausted`);
      } else {
        console.warn(`[Prospeo] ${domain}:`, err.response?.data || err.message);
      }
      return null;
    }
  }

  return null;
}


// ─────────────────────────────────────────────────────────────────────
// STEP 1b: Google Places API Search (Phase 4)
// Secondary local business discovery — runs after SerpAPI, not instead of it
// Docs: https://developers.google.com/maps/documentation/places/web-service
// ─────────────────────────────────────────────────────────────────────
const PLACES_TEXTSEARCH = 'https://maps.googleapis.com/maps/api/place/textsearch/json';
const PLACES_DETAILS    = 'https://maps.googleapis.com/maps/api/place/details/json';

async function fetchPlaceDetails(placeId, apiKey) {
  const res = await axios.get(PLACES_DETAILS, {
    params: {
      place_id: placeId,
      fields: 'name,formatted_address,formatted_phone_number,website,place_id,rating,user_ratings_total',
      key: apiKey,
    },
  });
  if (res.data.status !== 'OK') return null;
  return res.data.result;
}

async function searchGooglePlaces(industry, location, numResults = 20) {
  const PLACES_KEY = process.env.GOOGLE_PLACES_KEY;
  if (!PLACES_KEY) {
    console.warn('[WARN] Google Places key not set — skipping Places search');
    return [];
  }

  const leads = [];
  const query = sanitizeQueueLocation(`${industry || ''} ${location || ''}`);

  try {
    const res = await axios.get(PLACES_TEXTSEARCH, {
      params: { query, key: PLACES_KEY },
    });

    const status = res.data.status;
    if (status !== 'OK' && status !== 'ZERO_RESULTS') {
      console.error('[Places] Text Search status:', status, res.data.error_message || '');
      return [];
    }

    const results = (res.data.results || []).slice(0, numResults);

    for (const hit of results) {
      try {
        const details = await fetchPlaceDetails(hit.place_id, PLACES_KEY);
        if (!details?.website) continue;

        const domain = extractDomain(details.website);
        if (!domain || isBlacklistedDomain(domain)) continue;

        leads.push({
          company: normalizeCompanyName(details.name || hit.name || 'Unknown'),
          url: domain,
          phone: details.formatted_phone_number || null,
          address: details.formatted_address || hit.formatted_address || '',
          place_id: details.place_id || hit.place_id,
          google_rating: details.rating ?? hit.rating ?? null,
          google_review_count: details.user_ratings_total ?? hit.user_ratings_total ?? null,
          source: ['google_places'],
          snippet: '',
        });

        await new Promise(r => setTimeout(r, 200));
      } catch (err) {
        // skip individual place errors
      }
    }

    console.log(`[Places] Found ${leads.length} results with websites`);
    return leads;
  } catch (err) {
    console.error('[Places] Error:', err.response?.data || err.message);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────
// CLEANING-COMPANY ICP (client_id=10, scoring_profile='cleaning_buyer')
// The buyer is a commercial-cleaning customer, NOT a lead-gen target. This
// rubric is fully independent of the Pulseforge rubric below — it drops every
// Pulseforge-specific signal (probate weighting, MSHI boosts, tech/web-presence
// scoring) and scores the things that make an office worth a cleaning pitch.
//
// Components (max 100):
//   vertical        0–35  single-tenant professional-services office (law/CPA). Highest weight.
//   geography       0–25  Manchester NH + ring; out-of-area culled separately by service_area gate.
//   contact         0–25  reachable owner/office-manager (name + email/phone). No contact = low.
//   single_tenant   0–10  own space vs. a suite in a managed multi-tenant building. Heuristic — flagged.
//   size            0–5   prefer small offices (walkable site visit). Thin data — flagged.
//
// Returns { total, components, flags } so the test harness and Scout logs can
// show a per-lead breakdown for rubric tuning.
function scoreCleaningLead(lead) {
  const hay = (
    (lead.company || '') + ' ' +
    (lead.url     || '') + ' ' +
    (lead.snippet || '')
  ).toLowerCase();
  const addr = (lead.address || '').toLowerCase();
  const locHay = addr || hay;

  // 1. Vertical match (0–35) — law firms / accounting practices = target.
  const TARGET_VERTICAL = [
    'law firm', 'law office', 'law offices', 'attorney', 'attorneys', 'lawyer',
    'lawyers', 'legal', ' esq', 'llp', 'counsel', 'litigation', 'paralegal',
    'cpa', 'accounting', 'accountant', 'accountants', 'bookkeeping', 'bookkeeper',
    'tax service', 'tax services', 'tax prep', 'tax preparation', 'enrolled agent',
  ];
  // Adjacent single-tenant professional offices — plausible but not the beachhead.
  const ADJACENT_VERTICAL = [
    'insurance agency', 'financial advisor', 'wealth management', 'financial planning',
    'title company', 'real estate office', 'consulting', 'architect', 'engineering firm',
    'dental', 'dentist', 'orthodont', 'medical office', 'physical therapy', 'chiropractic',
  ];
  let vertical = 0;
  if (TARGET_VERTICAL.some(k => hay.includes(k)))        vertical = 35;
  else if (ADJACENT_VERTICAL.some(k => hay.includes(k))) vertical = 18;

  // 2. Geography (0–25) — in-area scores high, out-of-area is culled upstream
  //    by the service_area gate in saveToDatabase.
  let geography = 0;
  if (locHay.includes('manchester')) {
    geography = 25;
  } else if (CLEANING_AREA_CITIES.some(c => c.toLowerCase() !== 'manchester' && locHay.includes(c.toLowerCase()))) {
    geography = 20;
  } else if (locHay.includes(' nh') || locHay.includes('new hampshire')) {
    geography = 8;
  }

  // 3. Reachable decision-maker (0–25) — can we book a walkthrough?
  const hasEmail = !!(lead.email && lead.email !== '—' && String(lead.email).includes('@'));
  const hasPhone = !!(lead.phone && lead.phone !== '');
  const contactName = (lead.contact && lead.contact !== '—') ? String(lead.contact).trim() : '';
  const hasName = /[a-z]/i.test(contactName) && contactName.split(/\s+/).filter(Boolean).length >= 2;
  let contact = 0;
  if (hasName && hasEmail && hasPhone)       contact = 25;
  else if (hasName && (hasEmail || hasPhone)) contact = 18;
  else if (hasEmail && hasPhone)              contact = 12;
  else if (hasEmail || hasPhone)              contact = 8;

  // 4. Single-tenant signal (0–10) — HEURISTIC, address-derived only.
  //    A suite/unit/floor token suggests a unit inside a larger managed
  //    building (property-manager sale — wrong beachhead). A bare street
  //    address suggests the firm occupies its own space. When no address is
  //    available this is genuinely undetectable, so we stay neutral and flag it
  //    rather than guess. This is a known limitation, not a confident signal.
  const MULTI_TENANT = /\b(suite|ste\.?|unit|floor|fl\.?|#\s*\d|room|rm\.?)\b/i;
  let singleTenant = 5;
  let singleTenantBasis = 'undetectable (no address) — neutral';
  if (lead.address) {
    if (MULTI_TENANT.test(lead.address)) {
      singleTenant = 0;
      singleTenantBasis = 'suite/unit in address — likely multi-tenant';
    } else {
      singleTenant = 10;
      singleTenantBasis = 'bare street address — likely single-tenant';
    }
  }

  // 5. Size proxy (0–5) — prefer small offices; thin data defaults to neutral.
  const SMALL_FIRM = [
    'law office of', 'law offices of', 'solo', 'sole practitioner', 'pllc',
    'attorney at law', '& associates', 'and associates', ' p.c.', ' pc',
  ];
  let size = 3;
  let sizeBasis = 'thin data — neutral default';
  if (SMALL_FIRM.some(k => hay.includes(k))) { size = 5; sizeBasis = 'small/solo-firm signals'; }

  // 6. Disqualifier penalty (negative) — actively pushes the WRONG beachhead
  //    below threshold instead of merely zero-awarding it. Two detectable
  //    signals, both narrow on purpose so we don't cull good in-area firms:
  //    (a) multi-office / national firm — they run their own facilities and
  //        have incumbent vendors; not a walkthrough-to-close cleaning buyer.
  //    (b) high-floor / large-suite address — a big managed multi-tenant tower
  //        (property-manager sale). This is DISTINCT from a plain "Suite 3" in
  //        a small building, which stays in the ambiguous middle and is handled
  //        at walkthrough booking. We do NOT chase certainty the data can't give.
  const MULTI_OFFICE = [
    'nationwide', 'national law firm', 'multi-state', 'multistate',
    'regional offices', 'am law', 'offices nationwide', 'offices across',
  ];
  const multiOfficeRe = /\b\d+\s+offices\b|offices\s+(in|across)\s+\d+\s+(states|cities|locations|offices)|\b\d+\s+locations\b|hundreds of (attorneys|lawyers|offices)/i;
  const towerRe = /\bfloor\s+\d+\b|\bfl\.?\s*\d{1,2}\b|\b(suite|ste\.?)\s*\d{3,}\b/i;
  let penalty = 0;
  const penaltyBasis = [];
  if (MULTI_OFFICE.some(k => hay.includes(k)) || multiOfficeRe.test(hay)) {
    penalty += 25;
    penaltyBasis.push('multi-office/national firm');
    if (sizeBasis.startsWith('thin')) { size = 0; sizeBasis = 'large/multi-office signals'; }
  }
  if (lead.address && towerRe.test(lead.address)) {
    penalty += 12;
    penaltyBasis.push('high-floor/large-suite — multi-tenant tower');
  }

  const total = Math.min(100, Math.max(0, vertical + geography + contact + singleTenant + size - penalty));

  const flags = [];
  if (!lead.address)                         flags.push('no_address: geography + single-tenant precision limited');
  if (!lead.address || singleTenant === 5)   flags.push('single_tenant_undetectable');
  if (sizeBasis.startsWith('thin'))          flags.push('size_thin_data');
  if (!hasName)                              flags.push('no_named_contact');
  if (penalty)                               flags.push('disqualifier_penalty: ' + penaltyBasis.join(', '));

  return {
    total,
    components: {
      vertical, geography, contact, single_tenant: singleTenant, size, penalty,
      single_tenant_basis: singleTenantBasis, size_basis: sizeBasis,
      penalty_basis: penaltyBasis.join('; ') || 'none',
    },
    flags,
  };
}

// ─────────────────────────────────────────────────────────────────────
// STEP 3: Score each lead (0–100)
// Factors: vertical (25) + location (20) + contact (20) + web (20) + size (15)
// ─────────────────────────────────────────────────────────────────────
function scoreLead(lead) {
  // Cleaning company uses a fully separate buyer rubric (see scoreCleaningLead).
  if (CONFIG.scoringProfile === 'cleaning_buyer') {
    const r = scoreCleaningLead(lead);
    lead.scoreComponents = r.components;
    lead.scoreFlags = r.flags;
    console.log(`  ICP[cleaning] ${r.total} (vertical:${r.components.vertical} geo:${r.components.geography} contact:${r.components.contact} single_tenant:${r.components.single_tenant} size:${r.components.size} penalty:-${r.components.penalty})${r.flags.length ? ' flags:' + r.flags.join(';') : ''} — ${lead.company}`);
    return r.total;
  }

  const hay = (
    (lead.company || '') + ' ' +
    (lead.url     || '') + ' ' +
    (lead.snippet || '')
  ).toLowerCase();
  const addr = (lead.address || '').toLowerCase();

  // 1. Vertical (0–25)
  const TARGET_VERTICAL = [
    'clean','cleaning','cleaner','restaurant','cafe','diner','eatery',
    'hvac','heating','cooling','air conditioning','salon','hair','spa',
    'beauty','retail','shop','store','boutique','auto','automotive',
    'mechanic','repair'
  ];
  const ADJACENT_VERTICAL = [
    'landscap','lawn','property management','hotel','hospitality',
    'motel','gym','fitness'
  ];
  let vertical = 5;
  if (TARGET_VERTICAL.some(k => hay.includes(k)))    vertical = 25;
  else if (ADJACENT_VERTICAL.some(k => hay.includes(k))) vertical = 15;

  if (CONFIG.clientId === 2) {
    const MSHI_TARGET_VERTICAL = [
      'property management', 'property manager', 'probate', 'estate attorney',
      'estate planning', 'trust and estate', 'estate sale', 'executor',
      'mortgage broker', 'insurance agency', 'public adjuster',
      'home inspector', 'real estate agent', 'realtor',
    ];
    if (MSHI_TARGET_VERTICAL.some(k => hay.includes(k))) vertical = 25;
  }

  // 2. Location (0–20) — addr preferred, falls back to hay for SerpAPI leads
  const NH_SUBURBS = [
    'bedford','goffstown','hooksett','londonderry','auburn','candia',
    'derry','merrimack','nashua','concord'
  ];
  const locHay = addr || hay;
  let location = 0;
  if (CONFIG.clientId === 2) {
    const wvCore = ['charleston', 'south charleston', 'st albans', 'dunbar', 'nitro', 'cross lanes', 'hurricane'];
    const wvAdjacent = ['kanawha', 'putnam', 'cabell', 'logan', 'boone', 'lincoln', 'fayette', 'scott depot', 'teays valley', 'huntington', 'barboursville', 'madison', 'hamlin', 'fayetteville', 'beckley'];
    if (wvCore.some(c => locHay.includes(c) || hay.includes(c))) location = 20;
    else if (wvAdjacent.some(c => locHay.includes(c) || hay.includes(c))) location = 15;
    else if (locHay.includes(' wv') || locHay.includes('west virginia')) location = 8;
  } else if (locHay.includes('manchester'))                      location = 20;
  else if (NH_SUBURBS.some(c => locHay.includes(c)))     location = 15;
  else if (locHay.includes(' nh') || locHay.includes('new hampshire')) location = 8;

  // 3. Contact quality (0–20)
  const hasEmail = lead.email && lead.email !== '—' && lead.email.includes('@');
  const hasPhone = !!(lead.phone && lead.phone !== '');
  let contact = 0;
  if (hasEmail && hasPhone) contact = 20;
  else if (hasEmail)        contact = 12;
  else if (hasPhone)        contact = 8;

  // 4. Web presence (0–20)
  const JUNK_DOMAINS = ['yelp','facebook','google','yellowpages','bbb.org','tripadvisor'];
  const hasRealUrl = lead.url && !JUNK_DOMAINS.some(d => lead.url.includes(d));
  const hasSocial  = /instagram|facebook|social|twitter|tiktok|linkedin/.test(hay);
  let web = 0;
  if (hasRealUrl && hasSocial) web = 20;
  else if (hasRealUrl)         web = 12;

  // 5. Business size signals (0–15)
  const SIZE_STRONG = ['llc','inc','corp','commercial','team','staff',' locations'];
  const hasStrong  = SIZE_STRONG.some(k => hay.includes(k));
  const hasBasic   = hasPhone || !!(lead.address);
  let size = 0;
  if (hasStrong)    size = 15;
  else if (hasBasic) size = 8;

  let clientBoost = 0;
  if (CONFIG.clientId === 2) {
    const targetSignals = [
      'hoa', 'homeowners association', 'landlord', 'property management',
      'property manager', 'probate', 'estate attorney', 'estate planning',
      'trust and estate', 'estate sale', 'executor', 'mortgage broker',
      'insurance agency', 'public adjuster', 'home inspector',
      'real estate agent', 'realtor',
    ];
    const countySignals = ['kanawha', 'putnam', 'cabell', 'logan', 'boone', 'lincoln', 'fayette'];
    if (targetSignals.some(k => hay.includes(k))) clientBoost += 12;
    if (countySignals.some(k => locHay.includes(k) || hay.includes(k))) clientBoost += 8;
    if (['charleston', 'south charleston', 'st albans', 'dunbar', 'nitro', 'cross lanes', 'scott depot', 'teays valley', 'hurricane', 'huntington', 'barboursville', 'logan', 'madison', 'hamlin', 'fayetteville', 'beckley'].some(k => locHay.includes(k) || hay.includes(k))) {
      clientBoost += 5;
    }
  }

  let probateAdjustment = 0;
  if (CONFIG.clientId === 2 && CONFIG.vertical === 'probate_attorney') {
    const siteSignals = Array.isArray(lead.websiteSignals) ? lead.websiteSignals : [];
    const serviceSignals = ['probate', 'estate_sale', 'executor', 'estate_planning'];
    const smallFirmSignals = [
      'solo', 'sole practitioner', 'principal attorney', 'founding attorney',
      'law office of', 'law offices of', 'attorney at law', 'pllc',
    ];
    const bigFirmSignals = [
      'biglaw', 'national law firm', 'international law firm', 'multi-state',
      'multistate', 'offices nationwide', 'nationwide', 'global law firm',
      'am law', 'multiple offices', 'regional offices',
    ];

    if (serviceSignals.some(signal => siteSignals.includes(signal))) probateAdjustment += 12;
    if (smallFirmSignals.some(k => hay.includes(k))) probateAdjustment += 8;
    if (bigFirmSignals.some(k => hay.includes(k))) probateAdjustment -= 20;
    if (/\b(100|200|500)\+?\s+(attorneys|lawyers)\b/i.test(hay)) probateAdjustment -= 20;
    if (/\b(offices|locations)\s+(in|across)\s+\d+\s+states\b/i.test(hay)) probateAdjustment -= 20;
  }

  const total = Math.max(0, vertical + location + contact + web + size + clientBoost + probateAdjustment);
  console.log(`  ICP Score: ${total} (vertical:${vertical} location:${location} contact:${contact} web:${web} size:${size} client:${clientBoost} probate:${probateAdjustment}) — ${lead.company}`);
  return Math.min(total, 100);
}

// ─────────────────────────────────────────────────────────────────────
// STEP 5: Export to CSV
// ─────────────────────────────────────────────────────────────────────
async function exportToCSV(leads, filename) {
  const writer = createObjectCsvWriter({
    path: filename,
    header: [
      { id: 'company',  title: 'Company'  },
      { id: 'url',      title: 'Website'  },
      { id: 'contact',  title: 'Contact'  },
      { id: 'email',    title: 'Email'    },
      { id: 'title',    title: 'Title'    },
      { id: 'type',     title: 'Type'     },
      { id: 'source',   title: 'Sources'  },
      { id: 'score',    title: 'Score'    },
    ]
  });

  await writer.writeRecords(leads.map(l => ({
    ...l,
    source: Array.isArray(l.source) ? l.source.join('+') : l.source,
  })));

  console.log(`[CSV] Exported ${leads.length} leads → ${filename}`);
}

// ─────────────────────────────────────────────────────────────────────
// STEP 6: Push to Google Sheets
// Requires: a service account JSON key (GOOGLE_SERVICE_ACCOUNT_JSON in .env)
// Or: OAuth2 token flow
// Docs: https://developers.google.com/sheets/api/reference/rest
// ─────────────────────────────────────────────────────────────────────
async function pushToGoogleSheets(leads, sheetId) {
  if (!sheetId) {
    console.warn('[Sheets] No Sheet ID configured — skipping');
    return;
  }

  const serviceAccountJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!serviceAccountJson) {
    console.warn('[Sheets] No service account JSON — skipping. Set GOOGLE_SERVICE_ACCOUNT_JSON in .env');
    return;
  }

  const credentials = JSON.parse(serviceAccountJson);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const headers = [['Company','Website','Contact','Email','Title','Type','Sources','Score','Date']];
  const rows = leads.map(l => [
    l.company, l.url, l.contact || '', l.email || '', l.title || '',
    l.type, Array.isArray(l.source) ? l.source.join('+') : l.source,
    l.score, new Date().toISOString().slice(0,10)
  ]);

  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: sheetId,
      range: 'Sheet1!A1',
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [...headers, ...rows] }
    });

    console.log(`[Sheets] Pushed ${leads.length} leads → Sheet ID: ${sheetId}`);
  } catch (err) {
    console.error('[Sheets] Error:', err.message);
  }
}

// ─────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────
async function main({ runId = null } = {}) {
  await ensureScoutSkipLogTable();
  const preSaveStats = { skipped: 0, rejected: 0, skipped_breakdown: {} };
  console.log('\n🔷 Pulseforge Lead Engine');
  console.log('─────────────────────────────────────────');
  console.log(`  Industry : ${CONFIG.industry}`);
  console.log(`  Location : ${CONFIG.location}`);
  console.log(`  Title    : ${CONFIG.jobTitle}`);
  console.log(`  Max      : ${CONFIG.maxResults}`);
  console.log(`  Min score: ${CONFIG.minScore}`);
  console.log('─────────────────────────────────────────\n');

  if (!process.env.GOOGLE_PLACES_KEY) {
    console.warn('[WARN] GOOGLE_PLACES_KEY is not set — Google Places search will be skipped');
  }

  const searchQueries = getSearchQueriesForTarget();
  // Source strategy. Most clients run SerpAPI + Places additively. The cleaning
  // client (cleaning_buyer) makes Google Places PRIMARY and skips SerpAPI:
  // SerpAPI underperforms on professional offices and has been running dry.
  const placesPrimary = CONFIG.scoringProfile === 'cleaning_buyer';
  let leads = [];

  for (const searchQuery of searchQueries) {
    if (placesPrimary) {
      // Places-primary: Google Places only.
      console.log(`[Source] Places-primary (cleaning_buyer) — skipping SerpAPI for "${searchQuery}"`);
      console.log(`[Places] Searching: "${searchQuery}"`);
      const placesLeads = (await searchGooglePlaces(searchQuery, '', Math.min(CONFIG.maxResults, 20))).map(lead => ({ ...lead, search_query: searchQuery }));
      console.log(`[Places] Found ${placesLeads.length} results for "${searchQuery}"`);
      leads = [...leads, ...placesLeads];
      continue;
    }

    const googleQuery = `"${searchQuery}" -indeed -ziprecruiter -thumbtack -glassdoor -yelp -yellowpages -mapquest -bbb -patch -avvo`;
    console.log(`[Google] Searching: ${googleQuery}`);

    // 1. SerpAPI search
    const serpLeads = (await searchGoogle(googleQuery, CONFIG.maxResults)).map(lead => ({ ...lead, search_query: searchQuery }));
    console.log(`[SerpAPI] Found ${serpLeads.length} raw results for "${searchQuery}"`);
    leads = [...leads, ...serpLeads];

    // 1b. Google Places search (additive — secondary local discovery)
    console.log(`[Places] Searching: "${searchQuery}"`);
    const placesLeads = (await searchGooglePlaces(searchQuery, '', Math.min(CONFIG.maxResults, 20))).map(lead => ({ ...lead, search_query: searchQuery }));
    if (placesLeads.length) {
      leads = [...leads, ...placesLeads];
    }
  }

  const uniqueLeads = [];
  const seenDomains = new Set();
  for (const lead of leads) {
    const domain = normalizeDomain(lead.url);
    if (domain && seenDomains.has(domain)) {
      incrementBreakdown(preSaveStats.skipped_breakdown, SCOUT_SKIP_REASONS.DUPLICATE);
      preSaveStats.skipped++;
      await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.DUPLICATE, { match: 'within_run_domain', domain });
      continue;
    }
    if (domain) seenDomains.add(domain);
    uniqueLeads.push(lead);
  }
  leads = uniqueLeads;
  console.log(`[Combined] ${leads.length} unique domains after SerpAPI + Places`);

  // Pre-enrichment blacklist — strip junk domains/names before spending Prospeo/Hunter credits
  const beforePreEnrichment = leads.length;
  const preEnrichmentAccepted = [];
  for (const l of leads) {
    const missingFields = [];
    if (!String(l.company || '').trim()) missingFields.push('company');
    if (!normalizeDomain(l.url)) missingFields.push('domain');
    if (missingFields.length) {
      incrementBreakdown(preSaveStats.skipped_breakdown, SCOUT_SKIP_REASONS.MISSING_REQUIRED_FIELD);
      preSaveStats.skipped++;
      await persistScoutSkip(runId, l, SCOUT_SKIP_REASONS.MISSING_REQUIRED_FIELD, { missing_fields: missingFields, stage: 'pre_enrichment' });
      continue;
    }
    const reason = preEnrichmentRejectReason(l);
    if (reason) {
      console.log(`[Pre-enrichment reject] ${l.company || l.url || 'unknown'}: ${reason}`);
      incrementBreakdown(preSaveStats.skipped_breakdown, SCOUT_SKIP_REASONS.PRE_ENRICHMENT_REJECT);
      preSaveStats.skipped++;
      preSaveStats.rejected++;
      await persistScoutSkip(runId, l, SCOUT_SKIP_REASONS.PRE_ENRICHMENT_REJECT, { reason });
      continue;
    }
    preEnrichmentAccepted.push(l);
  }
  leads = preEnrichmentAccepted;
  console.log(`[Pre-enrichment blacklist] ${leads.length} leads after filtering (${beforePreEnrichment - leads.length} removed)`);

  // 3. Enrich with email waterfall
  console.log(`[Enrichment] Running email waterfall for ${leads.length} domains...`);
  for (let i = 0; i < leads.length; i++) {
    const stillActive = await getClientConfig(CONFIG.clientId);
    if (!stillActive) {
      throw new Error(`[Scout] Client ${CONFIG.clientId} deactivated mid-run — aborting at lead ${i+1}/${leads.length}`);
    }

    const lead = leads[i];
    process.stdout.write(`  [${i+1}/${leads.length}] ${lead.url}...`);
    const rootDomain = lead.url.replace(/^(?:[^.]+\.)+?([^.]+\.[^.]+)$/, (_, d) => d) || lead.url;
    let enrichmentMethod = 'none';
    let enriched = await enrichWithProspeo(rootDomain);
    if (enriched) {
      Object.assign(lead, enriched);
      lead.source = [...(lead.source || []), 'prospeo'];
      enrichmentMethod = 'prospeo';
      process.stdout.write(` ✓ ${enriched.email || 'no email'}\n`);
    } else {
      enriched = await enrichWithHunter(rootDomain);
      if (enriched) {
        Object.assign(lead, enriched);
        lead.source = [...(lead.source || []), 'hunter'];
        enrichmentMethod = 'hunter';
        process.stdout.write(` ✓ [Hunter] ${enriched.email || 'no email'}\n`);
      } else {
        const scraped = await scrapeWebsiteEmail(rootDomain);
        if (scraped) {
          Object.assign(lead, scraped);
          lead.source = [...(lead.source || []), 'scraped'];
          enrichmentMethod = 'scraped';
          process.stdout.write(` ✓ [Scraped] ${scraped.email}\n`);
        } else {
          process.stdout.write(' —\n');
        }
      }
    }
    await recordEmailEnrichmentMethod(rootDomain, enrichmentMethod, {
      source: lead.source || [],
      has_email: Boolean(lead.email),
    });
    // Rate limit: 2 req/sec
    await new Promise(r => setTimeout(r, 1500));
  }

  if (CONFIG.vertical === 'probate_attorney') {
    console.log(`[Probate] Checking website service-line signals for ${leads.length} domains...`);
    for (const lead of leads) {
      const domain = normalizeDomain(lead.url);
      lead.websiteSignals = await fetchProbateWebsiteSignals(domain);
      if (lead.websiteSignals.length) {
        console.log(`  ${domain}: ${lead.websiteSignals.join(', ')}`);
      }
    }
  }

  // 4. Fill missing fields
  leads = leads.map(l => ({
    company: l.company || 'Unknown',
    url:     l.url || '',
    contact: l.contact || '—',
    email:   l.email || '—',
    title:   l.title || '—',
    type:    detectType(l),
    source:  l.source || ['google'],
    score:   scoreLead(l),
    phone:   l.phone || null,
    address: l.address || null,
    google_rating:        l.google_rating ?? null,
    google_review_count:  l.google_review_count ?? null,
    facebook_url:         l.facebook_url || null,
    instagram_url:        l.instagram_url || null,
    websiteSignals:       l.websiteSignals || [],
    search_query:          l.search_query || null,
  }));

  const postEnrichmentAccepted = [];
  for (const l of leads) {
    if (!isBlacklistedDomain(l.url)) { postEnrichmentAccepted.push(l); continue; }
    incrementBreakdown(preSaveStats.skipped_breakdown, SCOUT_SKIP_REASONS.PRE_ENRICHMENT_REJECT);
    preSaveStats.skipped++;
    preSaveStats.rejected++;
    await persistScoutSkip(runId, l, SCOUT_SKIP_REASONS.PRE_ENRICHMENT_REJECT, { reason: 'blocked domain after enrichment', domain: normalizeDomain(l.url) });
  }
  leads = postEnrichmentAccepted;
  console.log("[Blacklist] " + leads.length + " leads after blacklist filter");

  // 5. Filter by min score
  const before = leads.length;
  const scoredLeads = [];
  for (const l of leads) {
    if (l.score >= CONFIG.minScore) { scoredLeads.push(l); continue; }
    incrementBreakdown(preSaveStats.skipped_breakdown, SCOUT_SKIP_REASONS.LOW_SCORE);
    preSaveStats.skipped++;
    preSaveStats.rejected++;
    await persistScoutSkip(runId, l, SCOUT_SKIP_REASONS.LOW_SCORE, { score: l.score, minimum_score: CONFIG.minScore });
  }
  leads = scoredLeads;
  console.log(`\n[Score] Filtered to ${leads.length} leads (removed ${before - leads.length} below ${CONFIG.minScore}%)`);

  // 6. Sort by score desc
  leads.sort((a, b) => b.score - a.score);

  // 7. Print summary table
  console.log('\n─── TOP LEADS ───────────────────────────────────────────────────');
  console.log('  Score  Company                      Email');
  console.log('─────────────────────────────────────────────────────────────────');
  leads.slice(0, 10).forEach(l => {
    const score = String(l.score).padStart(3);
    const co = l.company.padEnd(28).slice(0, 28);
    console.log(`  ${score}    ${co}   ${l.email}`);
  });
  console.log(`\n  Total: ${leads.length} leads\n`);

  // 8. Export
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const csvFile = `./leads-${CONFIG.industry.replace(/ /g,'-')}-${timestamp}.csv`;

  if (CONFIG.outputCSV) await exportToCSV(leads, csvFile);
  if (CONFIG.outputSheet) await pushToGoogleSheets(leads, CONFIG.sheetId);

  const dbStats = await saveToDatabase(leads, { runId, ...preSaveStats });
  console.log('\n✓ Done.\n');
  return { leads_scored: leads.length, ...dbStats };
}

// ─────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────
function extractDomain(url) {
  try {
    return new URL(url).hostname.replace('www.', '');
  } catch {
    return url;
  }
}

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
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

async function findOrCreateCompany({ name, domain, lead }) {
  const existing = await pool.query(
    `SELECT id
       FROM companies
      WHERE client_id = $2
        AND LOWER(TRIM(name)) = LOWER(TRIM($1))
      LIMIT 1`,
    [name, CONFIG.clientId]
  );
  if (existing.rows.length) {
    await pool.query(
      `UPDATE companies
          SET domain = COALESCE(domain, $1),
              website = COALESCE(website, $2),
              updated_at = NOW()
        WHERE id = $3
          AND client_id = $4`,
      [domain, lead.url || null, existing.rows[0].id, CONFIG.clientId]
    );
    return existing.rows[0].id;
  }

  const inserted = await pool.query(
    `INSERT INTO companies (name, domain, website, industry, location, client_id, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT DO NOTHING
     RETURNING id`,
    [name, domain, lead.url || null, CONFIG.industry || null, lead.address || CONFIG.location || null, CONFIG.clientId]
  );
  if (inserted.rows.length) return inserted.rows[0].id;

  const fallback = await pool.query(
    `SELECT id
       FROM companies
      WHERE client_id = $2
        AND LOWER(TRIM(name)) = LOWER(TRIM($1))
      LIMIT 1`,
    [name, CONFIG.clientId]
  );
  return fallback.rows[0]?.id || null;
}

function detectType(lead) {
  const techKeywords = ['saas','software','app','tech','io','platform','ai','cloud','data'];
  const d = (lead.url + lead.company + (lead.snippet || '')).toLowerCase();
  return techKeywords.some(k => d.includes(k)) ? 'tech' : 'smb';
}

function getScoutPreferredChannel() {
  if (CONFIG.clientId === 2 && ['property_management', 'probate_attorney'].includes(CONFIG.vertical)) {
    return 'phone';
  }
  return null;
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith('--')) {
      out[argv[i].slice(2)] = argv[i+1] || true;
      i++;
    }
  }
  return out;
}


// ─────────────────────────────────────────────────────────────────────
// VALIDATION — rejects junk business names before DB insert
//
// Coverage (tested against 90 names across 18 CSV exports, May 2026):
//   Catches: street addresses, zip codes, job listings, generic page
//   words, years, month+year dates, URLs, truncated snippets (...),
//   listicles, news headlines, social media titles, staff pages,
//   classified listings, bare locations, bullet-separated page titles,
//   search result titles (Who is / What is / How to).
//   Rejection rate on historical data: ~46% (41/90).
//
// Known limitations:
//   - News headlines without "..." e.g. "History About to Unfold for Levi's Lovers"
//   - Truncated snippets that drop the trailing "..." e.g. "Should you dry scoop your pre"
//   - "starts with digits" rejects edge cases like "3M" or "24 Hour Fitness"
// ─────────────────────────────────────────────────────────────────────
const JUNK_EXACT = new Set([
  'sitemap','home','contact','index','about','services','products',
  'blog','news','faq','login','register','search','menu','careers',
  'jobs','employment','privacy','terms','404','error','page',
  'manufacturing','retail','consulting','solutions'
]);

function validateProspect(name) {
  if (!name || typeof name !== 'string') return false;
  const n = name.trim();

  const reject = (reason) => { console.log(`Rejected "${n}": ${reason}`); return false; };

  const blockedNameReason = rejectedProspectNameReason(n);
  if (blockedNameReason)
    return reject(blockedNameReason);
  if (n.length < 4)
    return reject('too short');
  if (/^CONTACT:/i.test(n))
    return reject('starts with CONTACT:');
  if (/http/i.test(n))
    return reject('contains URL');
  if (/^\d/.test(n))
    return reject('starts with digits (address)');
  if (/\b\d{5}\b/.test(n))
    return reject('contains zip code');
  if (/\b(Rd|St|Ave|Blvd|Dr|Route|Rte|Unit|Suite|Ste|Hwy|Ln|Ct|Way|Pl|Pkwy)\s+\d/i.test(n))
    return reject('street type followed by number');
  if (/\b(jobs?|employment|hiring|careers?|openings?|positions?|vacanc(?:y|ies))\b/i.test(n))
    return reject('job listing keyword');
  if (JUNK_EXACT.has(n.toLowerCase()))
    return reject('generic web page word');
  if (/\b20[2-9]\d\b/.test(n))
    return reject('contains year');
  if (/\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+20\d\d\b/i.test(n))
    return reject('month + year (job listing date)');
  if (n.endsWith('...'))
    return reject('truncated search snippet');
  if (n.includes('?'))
    return reject('contains question mark');
  if (/^Top\s+\d+/i.test(n))
    return reject('listicle headline');
  if (/^(Who|What|How)\s+(is|to|are)\b/i.test(n))
    return reject('search result title');
  if (/ to (reopen|open|close)\b/i.test(n))
    return reject('news headline');
  if (/,\s*\d/.test(n))
    return reject('embedded address (comma + digits)');
  if (/\bPt\.\s*\d+|\bPart\s+\d+/i.test(n))
    return reject('social media series title');
  if (/\bCore Pt\b|\bRelatable\b|Funny &/i.test(n))
    return reject('social media content');
  if (/\b(Archives|Directory|Lookup)\b/i.test(n))
    return reject('page section title');
  if (n.includes('•'))
    return reject('bullet separator (web page title)');
  if (n.includes('|'))
    return reject('contains pipe separator (SEO-stuffed name)');
  if (n.includes('—'))
    return reject('contains em-dash (SEO-stuffed name)');
  if (/^Find\s/i.test(n))
    return reject('generic search prompt');
  if (/^Top Rated\b/i.test(n))
    return reject('listicle headline (Top Rated)');
  if (/\bTeam Member/i.test(n))
    return reject('staff page');
  if (/\bFor Sale\b/i.test(n))
    return reject('classified listing');
  if (n.length <= 25 && /^[\w\s]+,\s*(NH|MA|CT|VT|ME|RI)\s*$/i.test(n))
    return reject('bare location');
  if (/^(Meet The|Reviews of)\b/i.test(n))
    return reject('web page title pattern');
  // Likely a snippet: longer than 30 chars with no capital letter after the first word
  if (n.length > 30 && !/\s[A-Z]/.test(n))
    return reject('no mid-sentence capitals (likely snippet)');

  // ── NEW RULES ──────────────────────────────────────────────────────────────
  if (/email\s*(&|and)\s*phone/i.test(n))
    return reject('data scraper result (email & phone)');
  if (n.includes('#'))
    return reject('contains hashtag (social media content)');
  if (/^sketch\s+mockup:/i.test(n))
    return reject('sketch mockup label');
  if (n.startsWith('$'))
    return reject('starts with dollar sign (price listing)');
  if (/^About\s+\S/i.test(n))
    return reject('web page "About" title');
  if (/^Contact\s+Us$/i.test(n))
    return reject('"Contact Us" page title');
  if (/^Home\s+(cleaning|services|maintenance|repair|improvement|solutions|care|pros?)\b/i.test(n))
    return reject('generic SEO page title (Home + category)');
  // Two title-cased words with no business indicator = likely a person's name from a scraper
  // Only reject if neither word is a known business keyword
  if (/^[A-Z][a-z]{2,14}\s[A-Z][a-z]{2,14}$/.test(n)) {
    if (CONFIG.clientId === 2 && CONFIG.vertical === 'listing_agent') return true;
    const BIZ_WORDS = /\b(llc|inc|corp|co|company|group|services|solutions|studio|labs|works|consulting|cleaning|plumbing|hvac|landscaping|roofing|electric|construction|contracting|design|media|management|properties|realty|realtor|agency|associates|partners|industries|enterprise|foundation|center|institute|strength|fitness|performance|training|athletics|wellness|health|gym|salon|spa|club|team|law|legal|mortgage|lending|loans?|insurance|inspections?|inspector|estate|auctions?)\b/i;
    if (!BIZ_WORDS.test(n))
      return reject('likely a personal name, not a business');
  }

  return true;
}

// Returns a rejection reason string if the email is invalid, else null.
// Empty/absent emails are not rejections — they just mean "no email".
const EMAIL_PLACEHOLDER_DOMAINS = ['godaddy.com', 'example.com', 'test.com'];
function emailRejection(email) {
  if (typeof email !== 'string' || !email.trim()) return null;
  const guardReason = invalidOutreachEmailReason(email);
  if (guardReason) return guardReason;
  const e = email.trim();
  if (/\s/.test(e)) return 'contains spaces';
  if (e.length < 6) return 'too short';
  if ((e.match(/@/g) || []).length !== 1) return 'invalid @ count';
  if (/\.(webp|png|jpg|gif|svg|pdf)$/i.test(e)) return 'file extension domain';
  if (EMAIL_PLACEHOLDER_DOMAINS.includes(e.split('@')[1].toLowerCase())) return 'placeholder domain';
  return null;
}

async function resolveEmailVerification(email, lead) {
  const result = await verifyEmail(email);
  const doNotContact = ['invalid', 'catchall', 'risky'].includes(result.status);

  return {
    emailVerified: result.valid,
    emailVerificationMethod: result.method || result.vendor || 'mx_lookup',
    verifiedAt: new Date(),
    doNotContact,
    emailStatus: result.status,
    verifierResponse: result.raw,
    verifierCheckedAt: new Date(),
    note: doNotContact
      ? `Email verifier marked ${result.status}${result.reason ? ` (${result.reason})` : ''}; outbound disabled.`
      : null,
    reject: false,
  };
}

function resolveScoutEmailCandidate(lead) {
  const JUNK_EMAILS = ['user@domain.com', 'info@example.com', 'test@test.com', 'admin@domain.com'];
  const rawEmail = typeof lead.email === 'string' ? lead.email.trim() : '';

  if (!rawEmail || rawEmail === '—') {
    return { insertTarget: 'unenriched', reason: 'no_email', email: null };
  }

  const rejectReason = emailRejection(rawEmail) || (JUNK_EMAILS.includes(rawEmail.toLowerCase()) ? 'junk address' : null);
  if (rejectReason) {
    return { insertTarget: 'unenriched', reason: rejectReason, email: rawEmail };
  }

  return {
    insertTarget: 'prospect',
    email: rawEmail,
  };
}

async function resolveScoutEmail(lead, companyName) {
  const candidate = resolveScoutEmailCandidate(lead);
  if (candidate.insertTarget !== 'prospect') return candidate;

  const rawEmail = candidate.email;
  const verification = await resolveEmailVerification(rawEmail, lead);
  if (verification.reject) {
    return {
      insertTarget: 'unenriched',
      reason: verification.rejectReason || 'no_mx_record',
      email: rawEmail,
    };
  }

  return {
    insertTarget: 'prospect',
    email: rawEmail,
    emailVerified: verification.emailVerified,
    emailVerificationMethod: verification.emailVerificationMethod,
    verifiedAt: verification.verifiedAt,
    doNotContact: verification.doNotContact,
    emailStatus: verification.emailStatus,
    verifierResponse: verification.verifierResponse,
    verifierCheckedAt: verification.verifierCheckedAt,
    note: verification.note,
  };
}

async function upsertScoutUnenriched({
  companyName,
  domain,
  websiteUrl,
  discoveryMethod,
  reason,
  email,
}) {
  const notes = [
    reason ? `reason: ${reason}` : null,
    email ? `last_email: ${email}` : null,
  ].filter(Boolean).join(', ') || null;

  if (domain) {
    const existing = await pool.query(
      `SELECT id FROM scout_unenriched
       WHERE client_id = $1 AND LOWER(domain) = LOWER($2)
       LIMIT 1`,
      [CONFIG.clientId, domain]
    );
    if (existing.rows.length) {
      await pool.query(`
        UPDATE scout_unenriched
        SET enrichment_attempts = enrichment_attempts + 1,
            last_attempt_at = NOW(),
            company = COALESCE($1, company),
            website_url = COALESCE($2, website_url),
            vertical = COALESCE($3, vertical),
            location = COALESCE($4, location),
            source = COALESCE($5, source),
            notes = COALESCE($6, notes)
        WHERE id = $7
      `, [
        companyName,
        websiteUrl,
        CONFIG.vertical,
        CONFIG.location,
        discoveryMethod,
        notes,
        existing.rows[0].id,
      ]);
      return 'updated';
    }
  }

  await pool.query(`
    INSERT INTO scout_unenriched (
      client_id, company, website_url, domain, vertical, location, source, notes
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
  `, [
    CONFIG.clientId,
    companyName,
    websiteUrl,
    domain,
    CONFIG.vertical,
    CONFIG.location,
    discoveryMethod,
    notes,
  ]);
  return 'inserted';
}

async function saveToDatabase(leads, {
  runId = null, skipped: initialSkipped = 0, rejected: initialRejected = 0,
  skipped_breakdown: initialBreakdown = {},
} = {}) {
  let saved = 0, skipped = initialSkipped, rejected = initialRejected, unenriched = 0;
  const skippedBreakdown = { ...initialBreakdown };
  let setterQueued = 0, setterSkipped = 0, setterFailed = 0;
  await ensureSetterQueueColumns(pool);
  await ensureCompanyColumns(pool);
  await ensureEmailVerificationColumns();
  await ensureScoutUnenrichedTable();
  await ensureScoutSkipLogTable();
  for (const lead of leads) {
    let companyName = null;
    try {
      companyName = String(lead.company || '').replace(/^CONTACT:\s*/i, '').trim();
      const domain = normalizeDomain(lead.url);
      const discoveryMethod = scoutDiscoveryMethod(lead);
      const websiteUrl = lead.url || null;

      const missingFields = [];
      if (!companyName || companyName.toLowerCase() === 'unknown') missingFields.push('company');
      if (!domain) missingFields.push('domain');
      if (missingFields.length) {
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.MISSING_REQUIRED_FIELD);
        skipped++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.MISSING_REQUIRED_FIELD, { missing_fields: missingFields }, companyName);
        continue;
      }

      if (!validateProspect(companyName)) {
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.INVALID_PROSPECT);
        skipped++; rejected++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.INVALID_PROSPECT, { reason: 'validateProspect rejected candidate' }, companyName);
        continue;
      }
      if ((lead.score || 0) < 40) {
        console.log(`Score too low (${lead.score}): ${companyName}`);
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.LOW_SCORE);
        skipped++; rejected++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.LOW_SCORE, { score: lead.score || 0, minimum_score: 40 }, companyName);
        continue;
      }

    // Dedup: skip if a prospect with the same business name already exists in the same city.
    // City is taken from the first token of CONFIG.location (e.g., "Manchester NH" -> "Manchester").
    // If the existing prospect has no city info recorded, fall back to a name-only match within the client.
      const cityScope = String(CONFIG.location || '').split(/\s+/)[0] || '';
      const dupCheck = await pool.query(
      `SELECT p.id
         FROM prospects p
         LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
        WHERE p.client_id = $2
          AND (
            LOWER(TRIM(c.name)) = LOWER(TRIM($1))
            OR LOWER(TRIM(SPLIT_PART(COALESCE(p.notes, ''), ' — ', 1))) = LOWER(TRIM($1))
          )
           AND (
             $3 = ''
             OR p.service_area_match ILIKE '%' || $3 || '%'
             OR c.location ILIKE '%' || $3 || '%'
             OR p.service_area_match IS NULL
           )`,
      [companyName, CONFIG.clientId, cityScope]
    );
      if (dupCheck.rows.length > 0) {
        console.log(`Duplicate skipped: ${companyName} (${cityScope || 'any city'})`);
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.DUPLICATE);
        skipped++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.DUPLICATE, { match: 'company_name_and_city', city_scope: cityScope || null, existing_prospect_id: dupCheck.rows[0].id }, companyName);
        continue;
      }

      const emailCandidate = resolveScoutEmailCandidate(lead);
      if (emailCandidate.insertTarget === 'unenriched') {
        await upsertScoutUnenriched({
          companyName,
          domain,
          websiteUrl,
          discoveryMethod,
          reason: emailCandidate.reason,
          email: emailCandidate.email,
        });
        await logScoutRun('skipped', {
          company: companyName,
          domain,
          reason: emailCandidate.reason,
          discovery_method: discoveryMethod,
        }, 'scout_skipped_no_email');
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.NO_EMAIL);
        skipped++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.NO_EMAIL, { reason: emailCandidate.reason, email: emailCandidate.email, unenriched_action: 'upserted' }, companyName);
        unenriched++;
        continue;
      }

      const email = emailCandidate.email;

      const exclusion = await shouldExcludeProspect({
        email,
        websiteUrl,
        source: discoveryMethod,
      });
      if (exclusion.excluded) {
        await logExcludedProspect({ email, source: discoveryMethod, exclusion });
        await logScoutRun('skipped', {
          company: companyName,
          email,
          domain: extractEmailDomain(email),
          website_url: websiteUrl,
          discovery_method: discoveryMethod,
          exclusion_reason: exclusion.reason,
          exclusion_detail: exclusion.detail || {},
        }, 'prospect_excluded');
        console.log(`[Scout] Excluded prospect ${email}: ${exclusion.reason}`);
        incrementBreakdown(skippedBreakdown, 'excluded_filter');
        skipped++;
        continue;
      }

      const verification = await resolveEmailVerification(email, lead);
      const emailVerified = verification.emailVerified;
      const emailVerificationMethod = verification.emailVerificationMethod;
      const verifiedAt = verification.verifiedAt;
      const doNotContact = verification.doNotContact;
      const emailStatus = verification.emailStatus;
      const verifierResponse = verification.verifierResponse;
      const verifierCheckedAt = verification.verifierCheckedAt;
      const prospectNote = verification.note;

      const nameParts = (lead.contact && lead.contact !== '—' ? lead.contact : '').trim().split(/\s+/).filter(Boolean);

      // Use contact first name if available and looks like a real person name.
      // Keep generic inbox names null so downstream copy can use company/name fallbacks.
      const looksLikePerson = nameParts.length >= 2 || (nameParts.length === 1 && /^[A-Z][a-z]{2,}$/.test(nameParts[0]));
      const firstName = sanitizeFirstName(looksLikePerson ? nameParts[0] : null);
      const lastName  = firstName ? nameParts.slice(1).join(' ') || null : null;
      const phone = lead.phone || null;
      const addressHay = normalizeGeoText(lead.address || '');
      const domainHay = normalizeGeoText(domain || '');
      const serviceArea = (CLIENT_CONFIG?.service_area || []).find(area => {
        const needle = normalizeGeoText(area);
        return needle && (addressHay.includes(needle) || domainHay.includes(needle));
      }) || null;
      if (serviceArea === null && Array.isArray(CLIENT_CONFIG?.service_area) && CLIENT_CONFIG.service_area.length > 0) {
        console.log(`[Scout] Out-of-area prospect skipped: ${companyName} (${lead.address || 'no address'})`);
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.OUT_OF_AREA);
        skipped++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.OUT_OF_AREA, { address: lead.address || null, candidate_domain: domain, allowed_service_area: CLIENT_CONFIG.service_area }, companyName);
        continue;
      }
      const hasWebsite = !!(domain || websiteUrl);
      const facebookUrl = lead.facebook_url || null;
      const instagramUrl = lead.instagram_url || null;
      const hasFacebook = !!facebookUrl;
      const hasInstagram = !!instagramUrl;
      const googleRating = lead.google_rating ?? null;
      const googleReviewCount = lead.google_review_count ?? null;
      const preferredChannel = getScoutPreferredChannel();
      const companyId = await findOrCreateCompany({ name: companyName, domain, lead });
      if (!companyId) throw new Error(`Unable to link company for ${companyName}`);
      const insert = await pool.query(
      `INSERT INTO prospects (
        company_id, first_name, last_name, email, phone, status, source, icp_score, notes, vertical,
        client_id, service_area_match, discovery_method, has_website, google_review_count, google_rating,
        has_facebook, has_instagram, facebook_url, instagram_url, website_url,
        email_verified, email_verification_method, verified_at, do_not_contact, preferred_channel,
        email_status, verifier_response, verifier_checked_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28::jsonb, $29)
      ON CONFLICT (email) DO NOTHING RETURNING id`,
      [companyId, firstName, lastName, email, phone, 'cold', 'scout', lead.score, prospectNote, CONFIG.vertical, CONFIG.clientId, serviceArea, discoveryMethod, hasWebsite, googleReviewCount, googleRating, hasFacebook, hasInstagram, facebookUrl, instagramUrl, websiteUrl, emailVerified, emailVerificationMethod, verifiedAt, doNotContact, preferredChannel, emailStatus, JSON.stringify(verifierResponse || null), verifierCheckedAt]
    );
      if (!insert.rows.length) {
        incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.INSERT_CONFLICT);
        skipped++;
        await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.INSERT_CONFLICT, { conflict_target: 'prospects.email', email }, companyName);
        continue;
      }
      saved++;
      const prospectId = insert.rows[0].id;

      // Seed icp_score_history with Scout's initial score so dynamic ICP
      // recalculation has a baseline to diff future engagement changes against.
      await recordScoutBaseline(prospectId, lead.score, 'scout_initial').catch(err =>
        console.error(`[Scout] recordScoutBaseline failed for ${companyName}: ${err.message}`)
      );

      try {
        await pool.query(`
          UPDATE prospects
          SET setter_status = 'new',
              setter_visible = true,
              setter_updated_at = NOW()
          WHERE id = $1
            AND client_id = $2
            AND COALESCE(icp_score, 0) >= $3
            AND COALESCE(do_not_contact, false) = false
        `, [prospectId, CONFIG.clientId, getSetterThreshold()]);
        if (CONFIG.clientId === 1 && (lead.score || 0) >= getSetterThreshold()) {
          const handoff = await appendQualifiedScoutLead(lead, CONFIG.industry);
          if (handoff.appended) setterQueued++;
          else setterSkipped++;
        } else {
          setterSkipped++;
        }
      } catch (err) {
        setterFailed++;
        console.error(`[Setter] Handoff failed for ${companyName}: ${err.message}`);
      }
    } catch (err) {
      incrementBreakdown(skippedBreakdown, SCOUT_SKIP_REASONS.DB_ERROR);
      skipped++;
      await persistScoutSkip(runId, lead, SCOUT_SKIP_REASONS.DB_ERROR, { error: err.message, code: err.code || null, constraint: err.constraint || null }, companyName);
      console.error(`[Scout] Database save failed for ${companyName || scoutCandidateIdentifier(lead)}: ${err.message}`);
    }
  }
  console.log(`[DB] Saved ${saved} prospects, ${unenriched} unreachable (scout_unenriched), rejected ${rejected} (junk), skipped ${skipped} (errors/dupes)`);
  console.log(`[Setter] Queued ${setterQueued}, skipped ${setterSkipped}, failed ${setterFailed}`);
  return { saved, skipped, skipped_breakdown: skippedBreakdown, rejected, unenriched, setter_queued: setterQueued, setter_skipped: setterSkipped, setter_failed: setterFailed };
}

async function ensureSetterQueueColumns(pool) {
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS setter_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS setter_visible BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS service_area_match TEXT,
    ADD COLUMN IF NOT EXISTS discovery_method TEXT,
    ADD COLUMN IF NOT EXISTS has_website BOOLEAN,
    ADD COLUMN IF NOT EXISTS google_review_count INTEGER,
    ADD COLUMN IF NOT EXISTS google_rating NUMERIC,
    ADD COLUMN IF NOT EXISTS has_facebook BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_instagram BOOLEAN,
    ADD COLUMN IF NOT EXISTS facebook_url TEXT,
    ADD COLUMN IF NOT EXISTS instagram_url TEXT,
    ADD COLUMN IF NOT EXISTS website_url TEXT,
    ADD COLUMN IF NOT EXISTS preferred_channel TEXT,
    ADD COLUMN IF NOT EXISTS employee_count_estimate TEXT
  `);
}

async function ensureCompanyColumns(pool) {
  await pool.query(`
    ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS domain TEXT
  `);
}

// scout_queue tracks, per client + vertical + location, how many prospects
// have accumulated and whether the vertical is saturated. It drives the
// queue rotation: when a requested vertical is saturated, Scout pulls the
// next least-saturated queued item instead.
async function ensureScoutQueue(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_queue (
      id             SERIAL PRIMARY KEY,
      client_id      INTEGER NOT NULL DEFAULT 1 REFERENCES clients(id),
      industry       TEXT NOT NULL DEFAULT '',
      vertical       TEXT NOT NULL,
      location       TEXT NOT NULL DEFAULT '',
      prospect_count INTEGER NOT NULL DEFAULT 0,
      threshold      INTEGER NOT NULL DEFAULT ${SATURATION_THRESHOLDS.default},
      saturated      BOOLEAN NOT NULL DEFAULT false,
      status         TEXT NOT NULL DEFAULT 'queued',
      last_run_at    TIMESTAMPTZ,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS scout_queue_client_vertical_location_idx
    ON scout_queue (client_id, vertical, location)
  `);
  await pool.query(`
    WITH normalized AS (
      SELECT id,
             client_id,
             LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) AS clean_vertical,
             TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')) AS clean_location
      FROM scout_queue
    ),
    ranked AS (
      SELECT id,
             clean_location,
             ROW_NUMBER() OVER (PARTITION BY client_id, clean_vertical, clean_location ORDER BY id) AS rn
      FROM normalized
      WHERE clean_vertical <> ''
        AND clean_location <> ''
    )
    DELETE FROM scout_queue sq
    USING ranked r
    WHERE sq.id = r.id
      AND r.rn > 1
  `);
  await pool.query(`
    UPDATE scout_queue
    SET vertical = LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')),
        location = TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')),
        updated_at = NOW()
    WHERE location ~* 'https?://|www\\.|[^[:alnum:] ]'
       OR location <> TRIM(location)
       OR vertical <> LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g'))
  `);
  await pool.query(`
    UPDATE scout_queue
    SET threshold = CASE
          WHEN vertical = 'auto' THEN 50
          WHEN vertical = 'cleaning' THEN 50
          WHEN vertical = 'restaurant' THEN 60
          WHEN vertical = 'fitness' THEN 40
          WHEN vertical = 'salon' THEN 40
          WHEN vertical = 'med_spa' THEN 30
          WHEN vertical = 'landscaping' THEN 30
          WHEN vertical = 'property_management' THEN 40
          WHEN vertical = 'probate_attorney' THEN 40
          WHEN vertical = 'home_services' THEN 30
          ELSE 40
        END,
        updated_at = NOW()
    WHERE threshold <> CASE
          WHEN vertical = 'auto' THEN 50
          WHEN vertical = 'cleaning' THEN 50
          WHEN vertical = 'restaurant' THEN 60
          WHEN vertical = 'fitness' THEN 40
          WHEN vertical = 'salon' THEN 40
          WHEN vertical = 'med_spa' THEN 30
          WHEN vertical = 'landscaping' THEN 30
          WHEN vertical = 'property_management' THEN 40
          WHEN vertical = 'probate_attorney' THEN 40
          WHEN vertical = 'home_services' THEN 30
          ELSE 40
        END
  `);
}

// One-time-safe migration: rewrite any non-snake_case vertical values
// (e.g. "home services", "auto repair", NULL) to the normalized form.
async function normalizeExistingVerticals(pool) {
  await pool.query(`
    UPDATE prospects
    SET vertical = LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g'))
    WHERE vertical IS NOT NULL
      AND TRIM(vertical) <> ''
      AND vertical <> LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g'))
  `);
  await pool.query(`
    UPDATE prospects
    SET vertical = 'unknown'
    WHERE vertical IS NULL OR TRIM(vertical) = ''
  `);
}

async function getProspectCount(clientId, vertical, location) {
  const res = await pool.query(
    `SELECT COUNT(*)::int AS count
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1
        AND p.vertical = $2
        AND COALESCE(p.service_area_match, c.location, '') ILIKE $3`,
    [clientId, vertical, locationToIlikePattern(location)]
  );
  return res.rows[0]?.count || 0;
}

// Recompute prospect_count + saturated + status for EVERY queued vertical of a
// client (including zero-count rows) from the live prospects table, so rotation
// picks an accurate "lowest count" item and saturated/status never drift apart.
async function refreshQueueCounts(clientId) {
  await pool.query(`
    UPDATE scout_queue sq
    SET prospect_count = calc.cnt,
        saturated = (calc.cnt >= sq.threshold),
        status = CASE WHEN calc.cnt >= sq.threshold THEN 'saturated' ELSE 'queued' END,
        updated_at = NOW()
    FROM (
      SELECT sq2.id, COALESCE(p.cnt, 0) AS cnt
      FROM scout_queue sq2
      LEFT JOIN (
        SELECT sq3.id,
               COUNT(p.id) FILTER (
                 WHERE COALESCE(p.service_area_match, c.location, '') ILIKE (
                   '%' || REGEXP_REPLACE(TRIM(sq3.location), '\\s+', '%', 'g') || '%'
                 )
               )::int AS cnt
        FROM scout_queue sq3
        LEFT JOIN prospects p
          ON p.client_id = sq3.client_id
         AND p.vertical = sq3.vertical
        LEFT JOIN companies c
          ON c.id = p.company_id
         AND c.client_id = p.client_id
        WHERE sq3.client_id = $1
        GROUP BY sq3.id
      ) p ON p.id = sq2.id
      WHERE sq2.client_id = $1
    ) calc
    WHERE sq.id = calc.id
  `, [clientId]);
}

async function seedExpansionQueueMarkets(clientId) {
  const exists = await pool.query(`SELECT to_regclass('public.scout_expansion_queue') AS table_name`);
  if (!exists.rows[0]?.table_name) return;

  await pool.query(`
    INSERT INTO scout_queue (client_id, industry, vertical, location, prospect_count, threshold, saturated, status, updated_at)
    SELECT DISTINCT
      q.client_id,
      q.vertical,
      LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')),
      TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')) AS clean_location,
      0,
      CASE
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'auto' THEN 50
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'cleaning' THEN 50
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'restaurant' THEN 60
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'fitness' THEN 40
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'salon' THEN 40
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'med_spa' THEN 30
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'landscaping' THEN 30
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'property_management' THEN 40
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'probate_attorney' THEN 40
        WHEN LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) = 'home_services' THEN 30
        ELSE 40
      END,
      false,
      'queued',
      NOW()
    FROM scout_expansion_queue q
    WHERE q.client_id = $1
      AND q.status = 'pending'
      AND q.vertical IS NOT NULL
      AND LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.vertical), '[[:space:].-]+', '_', 'g'), '[^a-zA-Z0-9_]', '', 'g')) <> 'unknown'
      AND TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')) <> ''
    ON CONFLICT (client_id, vertical, location) DO NOTHING
  `, [clientId]);
}

// Seed a row for every active vertical declared on the client so the queue is
// complete before selection/rotation. Existing rows are left untouched.
async function seedClientVerticals(clientId, verticals, location) {
  const list = Array.isArray(verticals) ? verticals : [];
  const seen = new Set();
  for (const raw of list) {
    const vertical = normalizeVertical(raw);
    if (!vertical || vertical === 'unknown' || seen.has(vertical)) continue;
    seen.add(vertical);
    const cleanLocations = getPlannedLocations(clientId, location, vertical);
    const threshold = getSaturationThreshold(vertical);
    for (const cleanLocation of cleanLocations) {
      await pool.query(`
        INSERT INTO scout_queue (client_id, industry, vertical, location, prospect_count, threshold, saturated, status, updated_at)
        VALUES ($1, $2, $3, $4, 0, $5, false, 'queued', NOW())
        ON CONFLICT (client_id, vertical, location) DO NOTHING
      `, [clientId, vertical, vertical, cleanLocation, threshold]);
    }
  }
}

async function upsertQueueItem({ clientId, industry, vertical, location, count, threshold, saturated }) {
  const status = saturated ? 'saturated' : 'queued';
  const cleanLocation = sanitizeQueueLocation(location);
  await pool.query(`
    INSERT INTO scout_queue (client_id, industry, vertical, location, prospect_count, threshold, saturated, status, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
    ON CONFLICT (client_id, vertical, location) DO UPDATE SET
      industry = EXCLUDED.industry,
      prospect_count = EXCLUDED.prospect_count,
      threshold = EXCLUDED.threshold,
      saturated = EXCLUDED.saturated,
      status = EXCLUDED.status,
      updated_at = NOW()
  `, [clientId, industry || vertical, vertical, cleanLocation, count, threshold, saturated, status]);
}

// Next item to scrape: the unsaturated queued vertical with the fewest prospects.
async function pickNextQueueItem(clientId, allowedVerticals = []) {
  const allowed = Array.isArray(allowedVerticals)
    ? allowedVerticals.map(normalizeVertical).filter(v => v && v !== 'unknown')
    : [];
  const params = [clientId];
  let allowedClause = '';
  if (allowed.length) {
    params.push(allowed);
    allowedClause = ` AND vertical = ANY($${params.length})`;
  }
  const res = await pool.query(`
    SELECT industry, vertical, location, prospect_count
    FROM scout_queue
    WHERE client_id = $1 AND saturated = false${allowedClause}
    ORDER BY prospect_count ASC, id ASC
    LIMIT 1
  `, params);
  return res.rows[0] || null;
}

// Decide what Scout should actually scrape this run. Records the requested
// target in the queue; if it is saturated, logs vertical_saturated and rotates
// to the least-saturated queued item. Returns { skip: true } when nothing is
// left to scrape.
async function resolveScoutTarget({ clientId, industry, location, verticals }) {
  await ensureScoutQueue(pool);
  await normalizeExistingVerticals(pool);
  const cleanLocation = sanitizeQueueLocation(location);
  const plannedVerticals = getPlannedVerticals(clientId);
  const activeVerticals = plannedVerticals.length ? plannedVerticals : verticals;
  // Ensure every active vertical for this client exists in the queue before we
  // select/rotate. Client-specific plans are authoritative; other clients keep
  // the requested industry as a fallback.
  await seedClientVerticals(
    clientId,
    plannedVerticals.length ? activeVerticals : [...(activeVerticals || []), industry],
    cleanLocation
  );
  await seedExpansionQueueMarkets(clientId);
  await refreshQueueCounts(clientId);

  const vertical = normalizeVertical(industry) || 'unknown';

  if (plannedVerticals.length && !plannedVerticals.includes(vertical)) {
    console.log(`[Scout] "${vertical}" is not in client ${clientId}'s Scout plan — rotating to planned queue`);
    await logScoutRun('skipped', { vertical, industry, location: cleanLocation }, 'vertical_not_in_scout_plan');
    const next = await pickNextQueueItem(clientId, plannedVerticals);
    if (!next) {
      console.log('[Scout] No planned queue items remain — skipping run');
      return { skip: true, vertical, saturated: true };
    }
    return {
      industry: next.industry || next.vertical,
      location: next.location || cleanLocation,
      vertical: next.vertical,
      saturated: false,
      rotatedFrom: vertical,
    };
  }

  const threshold = getSaturationThreshold(vertical);
  const count = await getProspectCount(clientId, vertical, cleanLocation);
  const saturated = count >= threshold;

  await upsertQueueItem({ clientId, industry, vertical, location: cleanLocation, count, threshold, saturated });

  if (!saturated) {
    return { industry, location: cleanLocation, vertical, saturated: false };
  }

  console.log(`[Scout] "${vertical}" saturated for client ${clientId}: ${count}/${threshold} prospects — rotating queue`);
  await logScoutRun('skipped', { vertical, industry, location: cleanLocation, prospect_count: count, threshold }, 'vertical_saturated');

  const next = await pickNextQueueItem(clientId, plannedVerticals);
  if (!next) {
    console.log('[Scout] No unsaturated queue items remain — skipping run');
    return { skip: true, vertical, saturated: true };
  }

  console.log(`[Scout] Rotated to "${next.vertical}" (${next.prospect_count} prospects, lowest in queue)`);
  return {
    industry: next.industry || next.vertical,
    location: next.location || cleanLocation,
    vertical: next.vertical,
    saturated: false,
    rotatedFrom: vertical,
  };
}

async function run(params = {}) {
  CONFIG.clientId = getRuntimeClientId(params);
  const observabilityRunId = makeScoutObservabilityRunId();
  process.env.ACTIVE_CLIENT_ID = String(CONFIG.clientId);
  CLIENT_CONFIG = await getClientConfig(CONFIG.clientId);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CONFIG.clientId}`);
  // Per-client ICP rubric selector (e.g. 'cleaning_buyer'). Drives scoreLead()
  // dispatch and Scout source strategy. Defaults to the Pulseforge rubric.
  CONFIG.scoringProfile = CLIENT_CONFIG.scoring_profile || null;
  if (params.industry) CONFIG.industry = params.industry;
  if (params.location) CONFIG.location = sanitizeQueueLocation(params.location);
  if (params.jobTitle) CONFIG.jobTitle = params.jobTitle;
  if (params.maxResults) CONFIG.maxResults = parseInt(params.maxResults);

  // Saturation gate + queue rotation. May redirect this run to a different
  // vertical, or skip entirely when every queued vertical is saturated.
  const target = await resolveScoutTarget({
    clientId: CONFIG.clientId,
    industry: CONFIG.industry,
    location: CONFIG.location,
    verticals: CLIENT_CONFIG.verticals,
  });
  if (target.skip) {
    const result = { attempts: 0, successes: 0, skipped: 0, errorSample: null, skipped_run: true, reason: 'saturated', vertical: target.vertical };
    await reportScoutRun({ runId: observabilityRunId, ...result });
    return result;
  }
  CONFIG.industry = target.industry;
  CONFIG.location = target.location;
  CONFIG.vertical = target.vertical;

  const startedAt = Date.now();
  const runContext = {
    industry: CONFIG.industry,
    location: CONFIG.location,
    vertical: CONFIG.vertical,
    job_title: CONFIG.jobTitle,
    max_results: CONFIG.maxResults,
    ...(target.rotatedFrom ? { rotated_from: target.rotatedFrom } : {}),
  };

  const lockMeta = {
    clientId: CONFIG.clientId,
    industry: CONFIG.industry,
    vertical: CONFIG.vertical,
    location: CONFIG.location,
  };

  const lockHolder = await acquireScoutLockWithWait(lockMeta);
  if (!lockHolder) {
    const active = await getActiveScoutLock();
    await logScoutRun('skipped', {
      ...runContext,
      reason: 'scout_lock_timeout',
      wait_ms: 5 * 60 * 1000,
      active_lock: active || null,
    }, 'scout_lock_timeout');
    console.log('[Scout] Global lock timeout — another Scout run is still active');
    const result = { attempts: 0, successes: 0, skipped: 0, errorSample: null, skipped_run: true, reason: 'scout_lock_timeout' };
    await reportScoutRun({ runId: observabilityRunId, ...result });
    return result;
  }

  await logScoutRun('success', {
    ...runContext,
    holder_id: lockHolder,
  }, 'scout_lock_acquired');

  try {
    const runId = await logScoutRun('pending', runContext);
    const stats = await main({ runId });
    const observability = await resolveScoutObservabilityStats(stats, runId);
    await pool.query(
      `UPDATE scout_queue SET last_run_at = NOW(), updated_at = NOW()
       WHERE client_id = $1 AND vertical = $2 AND location = $3`,
      [CONFIG.clientId, CONFIG.vertical, CONFIG.location || '']
    );
    await refreshQueueCounts(CONFIG.clientId);
    await logScoutRun('success', {
      ...runContext,
      duration_ms: Date.now() - startedAt,
      ...(stats || {}),
      observability,
    });
    await reportScoutRun({ runId: observabilityRunId, ...observability });
    return { ...(stats || {}), ...observability };
  } catch (err) {
    await logScoutRun('failed', {
      ...runContext,
      duration_ms: Date.now() - startedAt,
      error: err.message,
    });
    const result = {
      attempts: 1,
      successes: 0,
      skipped: 0,
      errorSample: { error: err.message },
      failed: true,
    };
    await reportScoutRun({ runId: observabilityRunId, ...result });
    return result;
  } finally {
    const released = await releaseScoutLock(lockHolder);
    await logScoutRun(released ? 'success' : 'skipped', {
      ...runContext,
      holder_id: lockHolder,
      released,
    }, 'scout_lock_released');
  }
}

async function runEnrichmentChain(domain, jobTitle = 'owner') {
  const savedTitle = CONFIG.jobTitle;
  CONFIG.jobTitle = jobTitle;
  try {
    let enriched = await enrichWithProspeo(domain);
    if (enriched) {
      enriched.source = ['prospeo'];
      await recordEmailEnrichmentMethod(domain, 'prospeo', { source: enriched.source, has_email: Boolean(enriched.email) });
      return enriched;
    }
    enriched = await enrichWithHunter(domain);
    if (enriched) {
      enriched.source = ['hunter'];
      await recordEmailEnrichmentMethod(domain, 'hunter', { source: enriched.source, has_email: Boolean(enriched.email) });
      return enriched;
    }
    enriched = await scrapeWebsiteEmail(domain);
    if (enriched) {
      enriched.source = ['scraped'];
      await recordEmailEnrichmentMethod(domain, 'scraped', { source: enriched.source, has_email: Boolean(enriched.email) });
      return enriched;
    }
    await recordEmailEnrichmentMethod(domain, 'none');
    return null;
  } finally {
    CONFIG.jobTitle = savedTitle;
  }
}

module.exports = {
  run,
  enrichWithProspeo,
  enrichWithHunter,
  scrapeWebsiteEmail,
  normalizeDomain,
  resolveEmailVerification,
  runEnrichmentChain,
  normalizeVertical,
  scoreCleaningLead,
};

if (require.main === module) {
  run({ client_id: CONFIG.clientId }).catch(err => {
    console.error('[FATAL]', err.message);
    process.exit(1);
  });
}
