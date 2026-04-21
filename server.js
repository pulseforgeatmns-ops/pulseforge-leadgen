/**
 * Pulseforge Lead Engine — Express Server
 * =========================================
 * Bridges the dashboard UI with the lead gen logic.
 * Run: node server.js
 * Then open: http://localhost:3000
 */

require('dotenv').config();
const express  = require('express');
const axios    = require('axios');
const cors     = require('cors');
const path     = require('path');
const fs       = require('fs');
const { createObjectCsvWriter } = require('csv-writer');

const app  = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname)); // serves dashboard HTML

// ── CONFIG ────────────────────────────────────────────────────────────
const SERPAPI_KEY   = process.env.SERPAPI_KEY;
const HUNTER_KEY    = process.env.HUNTER_API_KEY;
const SHEET_ID      = process.env.GOOGLE_SHEET_ID || '';

const SKIP_DOMAINS = [
  'facebook.com','instagram.com','twitter.com','linkedin.com',
  'youtube.com','tiktok.com','nextdoor.com','reddit.com',
  'ziprecruiter.com','indeed.com','glassdoor.com','monster.com',
  'careerbuilder.com','simplyhired.com','snagajob.com',
  'yelp.com','yellowpages.com','bbb.org','angieslist.com',
  'thumbtack.com','homeadvisor.com','houzz.com','bark.com',
  'bizbuysell.com','loopnet.com','businessbroker.net',
  'bizquest.com','franchisegator.com',
  'businessinsider.com','forbes.com','inc.com','entrepreneur.com',
  'quora.com','medium.com','nhbr.com',
  'sba.gov','census.gov','us.bold.pro','housekeeper.com',
  'remotebooksonline.com',
];

// ── ROUTES ────────────────────────────────────────────────────────────

// Health / API status check
app.get('/api/status', (req, res) => {
  res.json({
    serpapi:  !!SERPAPI_KEY,
    hunter:   !!HUNTER_KEY,
    sheets:   !!SHEET_ID,
  });
});

// Main search endpoint — SSE stream for real-time log updates
app.get('/api/search', async (req, res) => {
  const { industry = 'cleaning', location = 'Manchester NH', title = 'owner', max = 25, minscore = 40 } = req.query;

  // Set up SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const send = (type, data) => {
    res.write(`data: ${JSON.stringify({ type, ...data })}\n\n`);
  };

  const log = (level, msg) => send('log', { level, msg });

  try {
    log('info', `Starting search: "${industry}" in ${location}`);

    // 1. SerpAPI search
    log('info', 'Querying SerpAPI...');
    let leads = await searchSerpAPI(`"${industry}" business "${location}" "${title}"`, parseInt(max), log);
    log('ok', `Found ${leads.length} raw results`);

    // 2. Filter junk domains
    leads = leads.filter(l => !SKIP_DOMAINS.some(s => l.url.includes(s)));
    log('ok', `${leads.length} results after filtering directories/social`);

    // 3. Deduplicate
    leads = deduplicate(leads);
    log('ok', `${leads.length} unique domains`);

    send('progress', { pct: 30, label: 'Enriching with Hunter...' });

    // 4. Enrich with Hunter
    for (let i = 0; i < leads.length; i++) {
      const lead = leads[i];
      log('info', `[${i+1}/${leads.length}] Enriching ${lead.url}...`);
      const enriched = await enrichWithHunter(lead.url, title);
      if (enriched) {
        Object.assign(lead, enriched);
        lead.source = [...(lead.source || []), 'hunter'];
        log('ok', `✓ ${lead.url} → ${enriched.email}`);
      }
      send('progress', { pct: 30 + Math.round((i+1)/leads.length * 50), label: `Enriching ${i+1}/${leads.length}...` });
      await delay(1200);
    }

    send('progress', { pct: 85, label: 'Scoring & deduplicating...' });

    // 5. Score & fill
    leads = leads.map(l => ({
      id:      Math.random().toString(36).slice(2,8),
      company: l.company || 'Unknown',
      url:     l.url || '',
      contact: l.contact || '—',
      email:   l.email || '—',
      title:   l.title || '—',
      type:    detectType(l),
      source:  l.source || ['google'],
      score:   scoreLead(l),
    }));

    // 6. Filter by min score
    const before = leads.length;
    leads = leads.filter(l => l.score >= parseInt(minscore));
    leads.sort((a, b) => b.score - a.score);

    send('progress', { pct: 100, label: 'Done' });
    log('ok', `Complete — ${leads.length} leads loaded, ${before - leads.length} filtered below ${minscore}% threshold`);

    // 7. Send results
    send('results', { leads });

  } catch (err) {
    log('err', `Fatal error: ${err.message}`);
  }

  res.end();
});

// CSV export
app.post('/api/export/csv', async (req, res) => {
  const { leads } = req.body;
  if (!leads?.length) return res.status(400).json({ error: 'No leads' });

  const timestamp = new Date().toISOString().replace(/[:.]/g,'-').slice(0,19);
  const filename  = `leads-${timestamp}.csv`;
  const filepath  = path.join(__dirname, filename);

  const writer = createObjectCsvWriter({
    path: filepath,
    header: [
      { id: 'company', title: 'Company'  },
      { id: 'url',     title: 'Website'  },
      { id: 'contact', title: 'Contact'  },
      { id: 'email',   title: 'Email'    },
      { id: 'title',   title: 'Title'    },
      { id: 'type',    title: 'Type'     },
      { id: 'source',  title: 'Sources'  },
      { id: 'score',   title: 'Score'    },
    ]
  });

  await writer.writeRecords(leads.map(l => ({
    ...l,
    source: Array.isArray(l.source) ? l.source.join('+') : l.source,
  })));

  res.download(filepath, filename, () => fs.unlinkSync(filepath));
});

// LinkedIn comment posting endpoint — called by n8n on approval
app.post('/api/post-comment', async (req, res) => {
  const { postUrl, comment, authorName } = req.body;

  if (!postUrl || !comment) {
    return res.status(400).json({ error: 'Missing postUrl or comment' });
  }

  try {
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    const fs = require('fs');
    const db = require('./dbClient');

    puppeteer.use(StealthPlugin());

    const SESSION_FILE = './linkedin_session.json';

    const sessionData = process.env.LINKEDIN_SESSION;
    if (!sessionData) {
    return res.status(500).json({ error: 'No LinkedIn session found' });
}
    const cookies = JSON.parse(Buffer.from(sessionData, 'base64').toString('utf8'));

    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });

    const cookies = JSON.parse(fs.readFileSync(SESSION_FILE));
    await page.setCookie(...cookies);

    await page.goto(postUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));

    // Click comment button
    const commentBtn = await page.$('.comment-button') ||
                       await page.$('[aria-label="Comment"]') ||
                       await page.$('.comments-comment-box__text-editor');

    if (!commentBtn) {
      await browser.close();
      return res.status(500).json({ error: 'Comment button not found' });
    }

    await commentBtn.click();
    await new Promise(r => setTimeout(r, 1500 + Math.random() * 1000));

    const commentBox = await page.$('.comments-comment-box__text-editor') ||
                       await page.$('[contenteditable="true"]');

    if (!commentBox) {
      await browser.close();
      return res.status(500).json({ error: 'Comment box not found' });
    }

    await commentBox.click();
    await new Promise(r => setTimeout(r, 500));

    for (const char of comment) {
      await commentBox.type(char, { delay: Math.floor(Math.random() * 80) + 30 });
    }

    await new Promise(r => setTimeout(r, 1000 + Math.random() * 1000));

    const submitBtn = await page.$('.comments-comment-box__submit-button') ||
                      await page.$('button[type="submit"]');

    if (submitBtn) {
      await submitBtn.click();
      await new Promise(r => setTimeout(r, 2000));
    }

    // Log to second brain
    await db.logAgentAction(
      'linkedin_agent',
      'post_comment',
      null,
      postUrl,
      { comment, authorName },
      'success'
    );

    await browser.close();

    console.log(`✓ Comment posted on: ${postUrl}`);
    res.json({ success: true, postUrl, comment });

  } catch (err) {
    console.error('Error posting comment:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── SEARCH ────────────────────────────────────────────────────────────
async function searchSerpAPI(query, numResults, log) {
  if (!SERPAPI_KEY) {
    log('warn', 'SerpAPI key not set — no results');
    return [];
  }

  const results = [];
  const pages   = Math.min(Math.ceil(numResults / 10), 3);

  for (let page = 0; page < pages; page++) {
    try {
      const res = await axios.get('https://serpapi.com/search', {
        params: { api_key: SERPAPI_KEY, q: query, num: 10, start: page * 10, engine: 'google' }
      });
      const items = res.data.organic_results || [];
      for (const item of items) {
        results.push({
          company: item.title.split('|')[0].split('-')[0].trim(),
          url:     extractDomain(item.link),
          snippet: item.snippet || '',
          source:  ['google'],
        });
      }
    } catch (err) {
      log('err', `SerpAPI error: ${err.response?.data?.error || err.message}`);
      break;
    }
  }

  return results;
}

// ── ENRICH ────────────────────────────────────────────────────────────
async function enrichWithHunter(domain, titleFilter) {
  if (!HUNTER_KEY) return null;

  try {
    const res = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key: HUNTER_KEY, limit: 5, type: 'personal' }
    });

    const emails = res.data?.data?.emails || [];
    if (!emails.length) return null;

    const tf    = (titleFilter || '').toLowerCase();
    const match = emails.find(e => e.position?.toLowerCase().includes(tf)) || emails[0];

    return {
      contact: `${match.first_name || ''} ${match.last_name || ''}`.trim(),
      email:   match.value || null,
      title:   match.position || null,
    };
  } catch (err) {
    return null;
  }
}

// ── HELPERS ───────────────────────────────────────────────────────────
function scoreLead(lead) {
  let score = 0;
  if (lead.email && lead.email !== '—')     score += 35;
  if (lead.contact && lead.contact !== '—') score += 20;
  if (lead.title && lead.title !== '—')     score += 15;
  if (lead.url)                             score += 15;
  if (lead.source?.includes('hunter'))      score += 15;
  return Math.min(score, 100);
}

function deduplicate(leads) {
  const seen = new Set();
  return leads.filter(l => {
    if (seen.has(l.url)) return false;
    seen.add(l.url);
    return true;
  });
}

function detectType(lead) {
  const techKeywords = ['saas','software','app','tech','io','platform','ai','cloud','data'];
  const d = (lead.url + lead.company + (lead.snippet || '')).toLowerCase();
  return techKeywords.some(k => d.includes(k)) ? 'tech' : 'smb';
}

function extractDomain(url) {
  try { return new URL(url).hostname.replace('www.', ''); }
  catch { return url; }
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── START ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n🔷 Pulseforge Lead Engine Server`);
  console.log(`─────────────────────────────────`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   SerpAPI : ${SERPAPI_KEY ? '✓' : '✗ not set'}`);
  console.log(`   Hunter  : ${HUNTER_KEY  ? '✓' : '✗ not set'}`);
  console.log(`─────────────────────────────────\n`);
});
