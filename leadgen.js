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

// Write one agent_log row per Scout run so we can answer
// "did Scout run? for which client/industry/location? did it find anything?"
// without trawling Railway stdout. Best-effort — never throws.
async function logScoutRun(status, payload, action = 'scrape') {
  try {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, ['scout', action, JSON.stringify(payload), status, CONFIG.clientId]);
  } catch (err) {
    console.error('[logScoutRun] failed to write:', err.message);
  }
}


const DOMAIN_BLACKLIST = [
  'indeed.com','glassdoor.com','ziprecruiter.com','thumbtack.com',
  'yelp.com','yellowpages.com','mapquest.com','bbb.org','patch.com',
  'avvo.com','zoominfo.com','inven.ai','prnewswire.com','ofn.org',
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
  'forbes.com','wmur.com','wokq.com',
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

// ── CLI ARGS ─────────────────────────────────────────────────────────
const args = parseArgs(process.argv.slice(2));
let CONFIG = {
  industry:    args.industry  || 'cleaning',
  location:    args.location  || 'Manchester NH',
  jobTitle:    args.title     || 'owner',
  maxResults:  parseInt(args.max || '25'),
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
  cleaning: 150,
  restaurant: 150,
  salon: 120,
  fitness: 120,
  hvac: 100,
  roofing: 100,
  auto_repair: 100,
  dental: 80,
  med_spa: 80,
  property_management: 80,
  landscaping: 120,
  home_services: 150,
  home_renovation: 120,
};
const DEFAULT_SATURATION_THRESHOLD = 100;

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
    : DEFAULT_SATURATION_THRESHOLD;
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
  try {
    const res = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key: HUNTER_KEY, limit: 5, type: 'personal' }
    });
    const emails = res.data?.data?.emails || [];
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
        results.push({
          company: item.title.split('|')[0].split('-')[0].trim(),
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
      const skipDomains = ['facebook.com','instagram.com','yelp.com','twitter.com','linkedin.com','youtube.com'];
      return results.filter(r => !skipDomains.some(s => r.url.includes(s)));
}

// ─────────────────────────────────────────────────────────────────────
// STEP 2: Prospeo Domain Search
// Docs: https://prospeo.io/api
// Takes a domain → returns contacts with name, title, email
// Free tier: 50 searches/mo
// ─────────────────────────────────────────────────────────────────────
async function enrichWithProspeo(domain) {
  if (!PROSPEO_API_KEY) {
    console.warn('[WARN] Prospeo key not set — skipping enrichment');
    return null;
  }

  try {
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
      email:   typeof person.email === 'object' ? person.email?.email || null : person.email || null,
      title:   person.job_title || null,
};
  } catch (err) {
    console.warn(`[Prospeo] ${domain}:`, err.response?.data || err.message);
    return null;
  }
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
      fields: 'name,formatted_address,formatted_phone_number,website,place_id',
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
        if (!domain || DOMAIN_BLACKLIST.some(b => domain.includes(b))) continue;

        leads.push({
          company: details.name || hit.name || 'Unknown',
          url: domain,
          phone: details.formatted_phone_number || null,
          address: details.formatted_address || hit.formatted_address || '',
          place_id: details.place_id || hit.place_id,
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
  leads = leads.filter(l => !DOMAIN_BLACKLIST.some(b => l.url && l.url.includes(b)));
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
      enriched = await enrichWithHunter(lead.url);
      if (enriched) {
        Object.assign(lead, enriched);
        lead.source = [...(lead.source || []), 'hunter'];
        process.stdout.write(` ✓ [Hunter] ${enriched.email || 'no email'}\n`);
      } else {
        // For Google Places leads, try scraping the website directly
        if (lead.source?.includes('google_places')) {
          const scraped = await scrapeWebsiteEmail(lead.url);
          if (scraped) {
            Object.assign(lead, scraped);
            lead.source = [...(lead.source || []), 'scraped'];
            process.stdout.write(` ✓ [Scraped] ${scraped.email}\n`);
          } else {
            process.stdout.write(' —\n');
          }
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
  }));

  leads = leads.filter(l => !DOMAIN_BLACKLIST.some(b => l.url && l.url.includes(b)));
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

async function saveToDatabase(leads) {
  let saved = 0, skipped = 0, rejected = 0, setterQueued = 0, setterSkipped = 0, setterFailed = 0;
  await ensureSetterQueueColumns(pool);
  await ensureCompanyColumns(pool);
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
      const JUNK_EMAILS = ['user@domain.com', 'info@example.com', 'test@test.com', 'admin@domain.com'];
      const email = typeof lead.email === 'string' && lead.email.includes('@') && !JUNK_EMAILS.includes(lead.email.toLowerCase()) ? lead.email : null;
      const nameParts = (lead.contact && lead.contact !== '—' ? lead.contact : '').trim().split(/\s+/).filter(Boolean);

      // Use contact first name if available and looks like a real person name.
      // Keep generic inbox names null so downstream copy can use company/name fallbacks.
      const looksLikePerson = nameParts.length >= 2 || (nameParts.length === 1 && /^[A-Z][a-z]{2,}$/.test(nameParts[0]));
      const firstName = sanitizeFirstName(looksLikePerson ? nameParts[0] : null);
      const lastName  = firstName ? nameParts.slice(1).join(' ') || null : null;
      const domain = normalizeDomain(lead.url);
      const phone = lead.phone || null;
      const serviceArea = (CLIENT_CONFIG?.service_area || []).find(area => {
        const needle = String(area).toLowerCase();
        return (lead.address || '').toLowerCase().includes(needle) || (domain || '').includes(needle);
      }) || null;
      const discoveryMethod = Array.isArray(lead.source) && lead.source.includes('google_places') ? 'google_places' : 'serpapi';
      const companyId = await findOrCreateCompany({ name: companyName, domain, lead });
      if (!companyId) throw new Error(`Unable to link company for ${companyName}`);
      const insert = await pool.query(
      'INSERT INTO prospects (company_id, first_name, last_name, email, phone, status, source, icp_score, notes, vertical, client_id, service_area_match, discovery_method) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (email) DO NOTHING RETURNING id',
      [companyId, firstName, lastName, email, phone, 'cold', 'scout', lead.score, null, CONFIG.vertical, CONFIG.clientId, serviceArea, discoveryMethod]
    );
      if (!insert.rows.length) {
        skipped++;
        continue;
      }
      saved++;
      const prospectId = insert.rows[0].id;

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
  console.log(`[DB] Saved ${saved} prospects, rejected ${rejected} (junk), skipped ${skipped} (errors/dupes)`);
  console.log(`[Setter] Queued ${setterQueued}, skipped ${setterSkipped}, failed ${setterFailed}`);
  return { saved, skipped, rejected, setter_queued: setterQueued, setter_skipped: setterSkipped, setter_failed: setterFailed };
}

async function ensureSetterQueueColumns(pool) {
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS setter_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS setter_visible BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS service_area_match TEXT,
    ADD COLUMN IF NOT EXISTS discovery_method TEXT
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
      threshold      INTEGER NOT NULL DEFAULT ${DEFAULT_SATURATION_THRESHOLD},
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

async function getProspectCount(clientId, vertical) {
  const res = await pool.query(
    `SELECT COUNT(*)::int AS count FROM prospects WHERE client_id = $1 AND vertical = $2`,
    [clientId, vertical]
  );
  return res.rows[0]?.count || 0;
}

// Recompute prospect_count + saturated for every queued vertical of a client
// so rotation picks an accurate "lowest count" item.
async function refreshQueueCounts(clientId) {
  await pool.query(`
    UPDATE scout_queue sq
    SET prospect_count = COALESCE(sub.cnt, 0),
        saturated = COALESCE(sub.cnt, 0) >= sq.threshold,
        updated_at = NOW()
    FROM (
      SELECT vertical, COUNT(*)::int AS cnt
      FROM prospects
      WHERE client_id = $1
      GROUP BY vertical
    ) sub
    WHERE sq.client_id = $1 AND sq.vertical = sub.vertical
  `, [clientId]);
}

async function upsertQueueItem({ clientId, industry, vertical, location, count, threshold, saturated }) {
  await pool.query(`
    INSERT INTO scout_queue (client_id, industry, vertical, location, prospect_count, threshold, saturated, status, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued', NOW())
    ON CONFLICT (client_id, vertical, location) DO UPDATE SET
      industry = EXCLUDED.industry,
      prospect_count = EXCLUDED.prospect_count,
      threshold = EXCLUDED.threshold,
      saturated = EXCLUDED.saturated,
      updated_at = NOW()
  `, [clientId, industry || vertical, vertical, location || '', count, threshold, saturated]);
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
async function resolveScoutTarget({ clientId, industry, location }) {
  await ensureScoutQueue(pool);
  await normalizeExistingVerticals(pool);
  await refreshQueueCounts(clientId);

  const vertical = normalizeVertical(industry);
  const threshold = getSaturationThreshold(vertical);
  const count = await getProspectCount(clientId, vertical);
  const saturated = count >= threshold;

  await upsertQueueItem({ clientId, industry, vertical, location, count, threshold, saturated });

  if (!saturated) {
    return { industry, location, vertical, saturated: false };
  }

  console.log(`[Scout] "${vertical}" saturated for client ${clientId}: ${count}/${threshold} prospects — rotating queue`);
  await logScoutRun('skipped', { vertical, industry, location, prospect_count: count, threshold }, 'vertical_saturated');

  const next = await pickNextQueueItem(clientId);
  if (!next) {
    console.log('[Scout] No unsaturated queue items remain — skipping run');
    return { skip: true, vertical, saturated: true };
  }

  console.log(`[Scout] Rotated to "${next.vertical}" (${next.prospect_count} prospects, lowest in queue)`);
  return {
    industry: next.industry || next.vertical,
    location: next.location || location,
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
  if (params.location) CONFIG.location = params.location;
  if (params.jobTitle) CONFIG.jobTitle = params.jobTitle;
  if (params.maxResults) CONFIG.maxResults = parseInt(params.maxResults);

  // Saturation gate + queue rotation. May redirect this run to a different
  // vertical, or skip entirely when every queued vertical is saturated.
  const target = await resolveScoutTarget({
    clientId: CONFIG.clientId,
    industry: CONFIG.industry,
    location: CONFIG.location,
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
  await logScoutRun('running', runContext);
  try {
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
  } catch (err) {
    await logScoutRun('failed', {
      ...runContext,
      duration_ms: Date.now() - startedAt,
      error: err.message,
    });
    throw err;
  }
}

module.exports = { run };

if (require.main === module) {
  run({ client_id: CONFIG.clientId }).catch(err => {
    console.error('[FATAL]', err.message);
    process.exit(1);
  });
}
