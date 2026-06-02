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
const { createObjectCsvWriter } = require('csv-writer');
const { google } = require('googleapis');
const pool = require('./db');
const { appendQualifiedScoutLead } = require('./utils/setterSheet');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { recordScoutBaseline } = require('./utils/icpScoring');
const { validateEmail } = require('./utils/emailValidation');
const { invalidOutreachEmailReason } = require('./utils/emailGuard');
const { ensureEmailVerificationColumns } = require('./utils/emailVerificationSchema');
const { ensureScoutUnenrichedTable } = require('./utils/scoutUnenrichedSchema');
const { acquireScoutLockWithWait, releaseScoutLock, getActiveScoutLock } = require('./utils/scoutLock');
const { awaitProspeoSlot } = require('./utils/prospeoThrottle');

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
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, ['scout', action, JSON.stringify(payload), safeStatus, CONFIG.clientId]);
  } catch (err) {
    console.error('[logScoutRun] failed to write:', err.message);
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
];

function isBlacklistedDomain(domain) {
  const host = String(domain || '').toLowerCase().replace(/^www\./, '');
  if (!host) return true;
  if (host.endsWith('.gov')) return true;
  return DOMAIN_BLACKLIST.some(blocked => host === blocked || host.endsWith(`.${blocked}`) || host.includes(blocked));
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
  vertical:    normalizeVertical(args.industry || 'cleaning'),
};
let CLIENT_CONFIG = null;

const GOOGLE_API_KEY    = process.env.GOOGLE_API_KEY;
const GOOGLE_CX         = process.env.GOOGLE_CX;
const PROSPEO_API_KEY   = process.env.PROSPEO_API_KEY;
const SETTER_ICP_THRESHOLD = 70;

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
  home_services: 30,
  default: 40,
};

// Standardize a free-form industry/vertical label to snake_case.
// e.g. "Home Services" -> "home_services", "auto repair" -> "auto_repair",
// null/empty -> "unknown".
function normalizeVertical(value) {
  const raw = String(value == null ? '' : value).trim();
  if (!raw) return 'unknown';
  const slug = raw.toLowerCase().replace(/[\s\-]+/g, '_').replace(/^_+|_+$/g, '');
  return slug || 'unknown';
}

function getSaturationThreshold(vertical) {
  const key = normalizeVertical(vertical);
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

      const items = res.data.organic_results || [];
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
          include: [CONFIG.jobTitle]
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
  if (!PROSPEO_API_KEY) {
    console.warn('[WARN] Prospeo key not set — skipping enrichment');
    return null;
  }

  const maxAttempts = 1 + PROSPEO_RATE_LIMIT_BACKOFF_MS.length;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await callProspeoSearchPerson(domain);
    } catch (err) {
      const rateLimited = isProspeoRateLimited(err);
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
  const query = `${industry} ${location}`;

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
// STEP 3: Score each lead (0–100)
// Factors: vertical (25) + location (20) + contact (20) + web (20) + size (15)
// ─────────────────────────────────────────────────────────────────────
function scoreLead(lead) {
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

  // 2. Location (0–20) — addr preferred, falls back to hay for SerpAPI leads
  const NH_SUBURBS = [
    'bedford','goffstown','hooksett','londonderry','auburn','candia',
    'derry','merrimack','nashua','concord'
  ];
  const locHay = addr || hay;
  let location = 0;
  if (locHay.includes('manchester'))                      location = 20;
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
    const targetSignals = ['hoa', 'homeowners association', 'landlord', 'property management', 'property manager', 'bank', 'reo', 'foreclosure', 'real estate developer'];
    const countySignals = ['kanawha', 'putnam', 'cabell'];
    if (targetSignals.some(k => hay.includes(k))) clientBoost += 12;
    if (countySignals.some(k => locHay.includes(k) || hay.includes(k))) clientBoost += 8;
    if (['charleston', 'dunbar', 'st albans', 'scott depot', 'teays valley', 'hurricane', 'huntington', 'barboursville'].some(k => locHay.includes(k) || hay.includes(k))) {
      clientBoost += 5;
    }
  }

  const total = vertical + location + contact + web + size + clientBoost;
  console.log(`  ICP Score: ${total} (vertical:${vertical} location:${location} contact:${contact} web:${web} size:${size} client:${clientBoost}) — ${lead.company}`);
  return Math.min(total, 100);
}

// ─────────────────────────────────────────────────────────────────────
// STEP 4: Deduplicate by domain
// ─────────────────────────────────────────────────────────────────────
function deduplicate(leads) {
  const seen = new Set();
  return leads.filter(l => {
    if (seen.has(l.url)) return false;
    seen.add(l.url);
    return true;
  });
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
async function main() {
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

  // Build search query
  const query = `"${CONFIG.industry}" "${CONFIG.location}" "${CONFIG.jobTitle}" -indeed -ziprecruiter -thumbtack -glassdoor -yelp -yellowpages -mapquest -bbb -patch -avvo`;
  console.log(`[Google] Searching: ${query}`);

  // 1. SerpAPI search
  let leads = await searchGoogle(query, CONFIG.maxResults);
  console.log(`[SerpAPI] Found ${leads.length} raw results`);

  // 1b. Google Places search (additive — secondary local discovery)
  console.log(`[Places] Searching: "${CONFIG.industry}" in ${CONFIG.location}`);
  const placesLeads = await searchGooglePlaces(CONFIG.industry, CONFIG.location, 20);
  if (placesLeads.length) {
    leads = [...leads, ...placesLeads];
  }
  leads = deduplicate(leads);
  console.log(`[Combined] ${leads.length} unique domains after SerpAPI + Places`);

  // Pre-enrichment blacklist — strip junk domains before spending Prospeo/Hunter credits
  leads = leads.filter(l => !isBlacklistedDomain(l.url));
  console.log(`[Pre-enrichment blacklist] ${leads.length} leads after filtering`);

  // 3. Enrich with Prospeo
  console.log(`[Prospeo] Enriching ${leads.length} domains...`);
  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    process.stdout.write(`  [${i+1}/${leads.length}] ${lead.url}...`);
    const rootDomain = lead.url.replace(/^(?:[^.]+\.)+?([^.]+\.[^.]+)$/, (_, d) => d) || lead.url;
    let enriched = await enrichWithProspeo(rootDomain);
    if (enriched) {
      Object.assign(lead, enriched);
      lead.source = [...(lead.source || []), 'prospeo'];
      process.stdout.write(` ✓ ${enriched.email || 'no email'}\n`);
    } else {
      enriched = await enrichWithHunter(rootDomain);
      if (enriched) {
        Object.assign(lead, enriched);
        lead.source = [...(lead.source || []), 'hunter'];
        process.stdout.write(` ✓ [Hunter] ${enriched.email || 'no email'}\n`);
      } else {
        const scraped = await scrapeWebsiteEmail(rootDomain);
        if (scraped) {
          Object.assign(lead, scraped);
          lead.source = [...(lead.source || []), 'scraped'];
          process.stdout.write(` ✓ [Scraped] ${scraped.email}\n`);
        } else {
          process.stdout.write(' —\n');
        }
      }
    }
    // Rate limit: 2 req/sec
    await new Promise(r => setTimeout(r, 1500));
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
  }));

  leads = leads.filter(l => !isBlacklistedDomain(l.url));
  console.log("[Blacklist] " + leads.length + " leads after blacklist filter");

  // 5. Filter by min score
  const before = leads.length;
  leads = leads.filter(l => l.score >= CONFIG.minScore);
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

  const dbStats = await saveToDatabase(leads);
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
    const BIZ_WORDS = /\b(llc|inc|corp|co|company|group|services|solutions|studio|labs|works|consulting|cleaning|plumbing|hvac|landscaping|roofing|electric|construction|contracting|design|media|management|properties|realty|agency|associates|partners|industries|enterprise|foundation|center|institute|strength|fitness|performance|training|athletics|wellness|health|gym|salon|spa|club|team)\b/i;
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
  const sources = Array.isArray(lead.source) ? lead.source : [];
  const fromProspeo = sources.includes('prospeo');
  const fromScraped = sources.includes('scraped');

  if (fromProspeo) {
    return {
      emailVerified: true,
      emailVerificationMethod: 'prospeo',
      verifiedAt: new Date(),
      doNotContact: false,
      reject: false,
    };
  }

  const validation = await validateEmail(email);
  if (!validation.valid) {
    return {
      emailVerified: false,
      emailVerificationMethod: validation.reason,
      verifiedAt: null,
      doNotContact: validation.reason === 'no_mx_record',
      reject: validation.reason === 'no_mx_record',
      rejectReason: validation.reason,
    };
  }

  if (validation.isRole && fromScraped) {
    return {
      emailVerified: false,
      emailVerificationMethod: 'mx_lookup_role',
      verifiedAt: new Date(),
      doNotContact: false,
      reject: false,
    };
  }

  return {
    emailVerified: true,
    emailVerificationMethod: 'mx_lookup',
    verifiedAt: new Date(),
    doNotContact: false,
    reject: false,
  };
}

async function resolveScoutEmail(lead, companyName) {
  const JUNK_EMAILS = ['user@domain.com', 'info@example.com', 'test@test.com', 'admin@domain.com'];
  const rawEmail = typeof lead.email === 'string' ? lead.email.trim() : '';

  if (!rawEmail || rawEmail === '—') {
    return { insertTarget: 'unenriched', reason: 'no_email', email: null };
  }

  const rejectReason = emailRejection(rawEmail) || (JUNK_EMAILS.includes(rawEmail.toLowerCase()) ? 'junk address' : null);
  if (rejectReason) {
    return { insertTarget: 'unenriched', reason: rejectReason, email: rawEmail };
  }

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

async function saveToDatabase(leads) {
  let saved = 0, skipped = 0, rejected = 0, unenriched = 0;
  let setterQueued = 0, setterSkipped = 0, setterFailed = 0;
  await ensureSetterQueueColumns(pool);
  await ensureCompanyColumns(pool);
  await ensureEmailVerificationColumns();
  await ensureScoutUnenrichedTable();
  for (const lead of leads) {
    const companyName = lead.company.replace(/^CONTACT:\s*/i, '').trim();

    if (!validateProspect(companyName)) {
      rejected++;
      continue;
    }
    if ((lead.score || 0) < 40) {
      console.log(`Score too low (${lead.score}): ${companyName}`);
      rejected++;
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
      skipped++;
      continue;
    }

    try {
      const domain = normalizeDomain(lead.url);
      const discoveryMethod = Array.isArray(lead.source) && lead.source.includes('google_places')
        ? 'google_places'
        : 'serpapi';
      const websiteUrl = lead.url || null;

      const emailResolution = await resolveScoutEmail(lead, companyName);
      if (emailResolution.insertTarget === 'unenriched') {
        await upsertScoutUnenriched({
          companyName,
          domain,
          websiteUrl,
          discoveryMethod,
          reason: emailResolution.reason,
          email: emailResolution.email,
        });
        await logScoutRun('skipped', {
          company: companyName,
          domain,
          reason: emailResolution.reason,
          discovery_method: discoveryMethod,
        }, 'scout_skipped_no_email');
        unenriched++;
        continue;
      }

      const email = emailResolution.email;
      const emailVerified = emailResolution.emailVerified;
      const emailVerificationMethod = emailResolution.emailVerificationMethod;
      const verifiedAt = emailResolution.verifiedAt;
      const doNotContact = emailResolution.doNotContact;

      const nameParts = (lead.contact && lead.contact !== '—' ? lead.contact : '').trim().split(/\s+/).filter(Boolean);

      // Use contact first name if available and looks like a real person name.
      // Keep generic inbox names null so downstream copy can use company/name fallbacks.
      const looksLikePerson = nameParts.length >= 2 || (nameParts.length === 1 && /^[A-Z][a-z]{2,}$/.test(nameParts[0]));
      const firstName = sanitizeFirstName(looksLikePerson ? nameParts[0] : null);
      const lastName  = firstName ? nameParts.slice(1).join(' ') || null : null;
      const phone = lead.phone || null;
      const serviceArea = (CLIENT_CONFIG?.service_area || []).find(area => {
        const needle = String(area).toLowerCase();
        return (lead.address || '').toLowerCase().includes(needle) || (domain || '').includes(needle);
      }) || null;
      const hasWebsite = !!(domain || websiteUrl);
      const facebookUrl = lead.facebook_url || null;
      const instagramUrl = lead.instagram_url || null;
      const hasFacebook = !!facebookUrl;
      const hasInstagram = !!instagramUrl;
      const googleRating = lead.google_rating ?? null;
      const googleReviewCount = lead.google_review_count ?? null;
      const companyId = await findOrCreateCompany({ name: companyName, domain, lead });
      if (!companyId) throw new Error(`Unable to link company for ${companyName}`);
      const insert = await pool.query(
      `INSERT INTO prospects (
        company_id, first_name, last_name, email, phone, status, source, icp_score, notes, vertical,
        client_id, service_area_match, discovery_method, has_website, google_review_count, google_rating,
        has_facebook, has_instagram, facebook_url, instagram_url, website_url,
        email_verified, email_verification_method, verified_at, do_not_contact
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
      ON CONFLICT (email) DO NOTHING RETURNING id`,
      [companyId, firstName, lastName, email, phone, 'cold', 'scout', lead.score, null, CONFIG.vertical, CONFIG.clientId, serviceArea, discoveryMethod, hasWebsite, googleReviewCount, googleRating, hasFacebook, hasInstagram, facebookUrl, instagramUrl, websiteUrl, emailVerified, emailVerificationMethod, verifiedAt, doNotContact]
    );
      if (!insert.rows.length) {
        skipped++;
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
        `, [prospectId, CONFIG.clientId, SETTER_ICP_THRESHOLD]);
        if (CONFIG.clientId === 1 && (lead.score || 0) >= SETTER_ICP_THRESHOLD) {
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
      skipped++;
    }
  }
  console.log(`[DB] Saved ${saved} prospects, ${unenriched} unreachable (scout_unenriched), rejected ${rejected} (junk), skipped ${skipped} (errors/dupes)`);
  console.log(`[Setter] Queued ${setterQueued}, skipped ${setterSkipped}, failed ${setterFailed}`);
  return { saved, skipped, rejected, unenriched, setter_queued: setterQueued, setter_skipped: setterSkipped, setter_failed: setterFailed };
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
             LOWER(REGEXP_REPLACE(TRIM(vertical), '[\\s\\-]+', '_', 'g')) AS clean_vertical,
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
    SET vertical = LOWER(REGEXP_REPLACE(TRIM(vertical), '[\\s\\-]+', '_', 'g')),
        location = TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')),
        updated_at = NOW()
    WHERE location ~* 'https?://|www\\.|[^[:alnum:] ]'
       OR location <> TRIM(location)
       OR vertical <> LOWER(REGEXP_REPLACE(TRIM(vertical), '[\\s\\-]+', '_', 'g'))
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
    SET vertical = LOWER(REGEXP_REPLACE(TRIM(vertical), '[\\s\\-]+', '_', 'g'))
    WHERE vertical IS NOT NULL
      AND TRIM(vertical) <> ''
      AND vertical <> LOWER(REGEXP_REPLACE(TRIM(vertical), '[\\s\\-]+', '_', 'g'))
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
      LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')),
      TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')) AS clean_location,
      0,
      CASE
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'auto' THEN 50
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'cleaning' THEN 50
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'restaurant' THEN 60
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'fitness' THEN 40
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'salon' THEN 40
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'med_spa' THEN 30
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'landscaping' THEN 30
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'property_management' THEN 40
        WHEN LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) = 'home_services' THEN 30
        ELSE 40
      END,
      false,
      'queued',
      NOW()
    FROM scout_expansion_queue q
    WHERE q.client_id = $1
      AND q.status = 'pending'
      AND q.vertical IS NOT NULL
      AND LOWER(REGEXP_REPLACE(TRIM(q.vertical), '[\\s\\-]+', '_', 'g')) <> 'unknown'
      AND TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.location, 'https?://\\S*', '', 'gi'), '\\mwww\\.\\S*', '', 'gi'), '[^[:alnum:] ]+', ' ', 'g')) <> ''
    ON CONFLICT (client_id, vertical, location) DO NOTHING
  `, [clientId]);
}

// Seed a row for every active vertical declared on the client so the queue is
// complete before selection/rotation. Existing rows are left untouched.
async function seedClientVerticals(clientId, verticals, location) {
  const list = Array.isArray(verticals) ? verticals : [];
  const seen = new Set();
  const cleanLocation = sanitizeQueueLocation(location);
  for (const raw of list) {
    const vertical = normalizeVertical(raw);
    if (!vertical || vertical === 'unknown' || seen.has(vertical)) continue;
    seen.add(vertical);
    const threshold = getSaturationThreshold(vertical);
    await pool.query(`
      INSERT INTO scout_queue (client_id, industry, vertical, location, prospect_count, threshold, saturated, status, updated_at)
      VALUES ($1, $2, $3, $4, 0, $5, false, 'queued', NOW())
      ON CONFLICT (client_id, vertical, location) DO NOTHING
    `, [clientId, vertical, vertical, cleanLocation, threshold]);
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
async function pickNextQueueItem(clientId) {
  const res = await pool.query(`
    SELECT industry, vertical, location, prospect_count
    FROM scout_queue
    WHERE client_id = $1 AND saturated = false
    ORDER BY prospect_count ASC, id ASC
    LIMIT 1
  `, [clientId]);
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
  // Ensure every active vertical for this client exists in the queue before we
  // select/rotate. The requested industry is always included as a fallback.
  await seedClientVerticals(clientId, [...(verticals || []), industry], cleanLocation);
  await seedExpansionQueueMarkets(clientId);
  await refreshQueueCounts(clientId);

  const vertical = normalizeVertical(industry);
  const threshold = getSaturationThreshold(vertical);
  const count = await getProspectCount(clientId, vertical, cleanLocation);
  const saturated = count >= threshold;

  await upsertQueueItem({ clientId, industry, vertical, location: cleanLocation, count, threshold, saturated });

  if (!saturated) {
    return { industry, location: cleanLocation, vertical, saturated: false };
  }

  console.log(`[Scout] "${vertical}" saturated for client ${clientId}: ${count}/${threshold} prospects — rotating queue`);
  await logScoutRun('skipped', { vertical, industry, location: cleanLocation, prospect_count: count, threshold }, 'vertical_saturated');

  const next = await pickNextQueueItem(clientId);
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
  process.env.ACTIVE_CLIENT_ID = String(CONFIG.clientId);
  CLIENT_CONFIG = await getClientConfig(CONFIG.clientId);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CONFIG.clientId}`);
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
    return { skipped: true, reason: 'saturated', vertical: target.vertical };
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
    return { skipped: true, reason: 'scout_lock_timeout' };
  }

  await logScoutRun('success', {
    ...runContext,
    holder_id: lockHolder,
  }, 'scout_lock_acquired');

  try {
    await logScoutRun('pending', runContext);
    const stats = await main();
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
    });
    return stats;
  } catch (err) {
    await logScoutRun('failed', {
      ...runContext,
      duration_ms: Date.now() - startedAt,
      error: err.message,
    });
    throw err;
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
      return enriched;
    }
    enriched = await enrichWithHunter(domain);
    if (enriched) {
      enriched.source = ['hunter'];
      return enriched;
    }
    enriched = await scrapeWebsiteEmail(domain);
    if (enriched) {
      enriched.source = ['scraped'];
      return enriched;
    }
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
};

if (require.main === module) {
  run({ client_id: CONFIG.clientId }).catch(err => {
    console.error('[FATAL]', err.message);
    process.exit(1);
  });
}
