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

// ── CLI ARGS ─────────────────────────────────────────────────────────
const args = parseArgs(process.argv.slice(2));
const CONFIG = {
  industry:    args.industry  || 'cleaning',
  location:    args.location  || 'Manchester NH',
  jobTitle:    args.title     || 'owner',
  maxResults:  parseInt(args.max || '25'),
  minScore:    parseInt(args.minscore || '40'),
  mode:        args.mode      || 'both',     // smb | tech | both
  outputCSV:   args.csv       !== 'false',
  outputSheet: args.sheet     !== 'false',
  sheetId:     args.sheetid   || process.env.GOOGLE_SHEET_ID || '',
};

const GOOGLE_API_KEY    = process.env.GOOGLE_API_KEY;
const GOOGLE_CX         = process.env.GOOGLE_CX;
const PROSPEO_API_KEY   = process.env.PROSPEO_API_KEY;

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
      email:   person.email?.email || person.email || null,
      title:   person.job_title || null,
};
  } catch (err) {
    console.warn(`[Prospeo] ${domain}:`, err.response?.data || err.message);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────
// STEP 3: Score each lead (0–100)
// Weights:
//   email verified   +35
//   contact name     +20
//   job title match  +15
//   domain quality   +15
//   both sources     +15
// ─────────────────────────────────────────────────────────────────────
function scoreLead(lead) {
  let score = 0;
  if (lead.email && lead.email !== '—')      score += 35;
  if (lead.contact && lead.contact !== '—')  score += 20;
  if (lead.title && lead.title !== '—')      score += 15;
  if (lead.url && !lead.url.includes('yelp') && !lead.url.includes('facebook')) score += 15;
  if (lead.source?.includes('google') && lead.source?.includes('prospeo'))      score += 15;
  return Math.min(score, 100);
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

  // Build search query
  const query = `"${CONFIG.industry}" business "${CONFIG.location}" "${CONFIG.jobTitle}"`;
  console.log(`[Google] Searching: ${query}`);

  // 1. Google search
  let leads = await searchGoogle(query, CONFIG.maxResults);
  console.log(`[Google] Found ${leads.length} raw results`);

  // 2. Deduplicate first (saves Prospeo credits)
  leads = deduplicate(leads);
  console.log(`[Dedup] ${leads.length} unique domains`);

  // 3. Enrich with Prospeo
  console.log(`[Prospeo] Enriching ${leads.length} domains...`);
  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    process.stdout.write(`  [${i+1}/${leads.length}] ${lead.url}...`);
    const enriched = await enrichWithProspeo(lead.url);
    if (enriched) {
      Object.assign(lead, enriched);
      lead.source = [...(lead.source || []), 'prospeo'];
      process.stdout.write(` ✓ ${enriched.email || 'no email'}\n`);
    } else {
      process.stdout.write(' —\n');
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

  await saveToDatabase(leads);
  console.log('\n✓ Done.\n');
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


async function saveToDatabase(leads) {
  const pool = require('./db');
  let saved = 0, skipped = 0;
  for (const lead of leads) {
    try {
      const email = typeof lead.email === 'string' && lead.email.includes('@') ? lead.email : null;
      const nameParts = (lead.contact || '').trim().split(' ');
      const firstName = nameParts[0] || lead.company.split(' ')[0];
      const lastName = nameParts.slice(1).join(' ') || '';
      const notes = lead.company + ' — ' + lead.url;
      await pool.query(
        'INSERT INTO prospects (first_name, last_name, email, status, source, icp_score, notes) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (email) DO NOTHING',
        [firstName, lastName, email, 'cold', 'scout', lead.score, notes]
      );
      saved++;
    } catch (err) {
      skipped++;
    }
  }
  console.log('[DB] Saved ' + saved + ' prospects, skipped ' + skipped);
  pool.end();
}

main().catch(err => {
  console.error('[FATAL]', err.message);
  process.exit(1);
});
