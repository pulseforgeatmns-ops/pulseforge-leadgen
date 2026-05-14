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
const session  = require('express-session');
const pgSession = require('connect-pg-simple')(session);
const pool     = require('./db');
const { createObjectCsvWriter } = require('csv-writer');
const { generateDemoData } = require('./utils/demoData');

const app  = express();
const PORT = 3000;

app.use(session({
  store: new pgSession({ pool, tableName: 'session' }),
  secret: process.env.SESSION_SECRET || 'pulseforge-secret-key',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000 }
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
app.use(express.static(__dirname));

// Recover from malformed JSON bodies so cron routes can still read req.query.secret
app.use((err, req, res, next) => {
  if (err.status === 400 && err.type === 'entity.parse.failed') {
    req.body = {};
    return next();
  }
  next(err);
});

// ── CONFIG ────────────────────────────────────────────────────────────
const SERPAPI_KEY = process.env.SERPAPI_KEY;
const HUNTER_KEY  = process.env.HUNTER_API_KEY;
const SHEET_ID    = process.env.GOOGLE_SHEET_ID || '';

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
  'steemer.com','townplanner.com','bizbuysell.com','unionleader.com',
  'sniffspot.com','woofies.com','brixrecruiting.com','procore.com',
  'promatcher.com','afoodieaffair.com','dizscafe.com','redarrowdiner.com',
  'bostonvoyager.com','christopherduffley.com','shoppersmht.com',
  'opensecrets.org','latimes.com','businesswest.com','macaronikid.com',
  'crestmontcapital.com','thebedfordmom.com','mhl.org','usmodernist.org',
  'rackcdn.com','amazonaws.com','whs1959.com','spaindex.com',
  'sentextsolutions.com','londonderrynh.org'
];

// ── ROUTES ────────────────────────────────────────────────────────────

// Mount route files
app.use('/', require('./routes/webhooks'));
app.use('/', require('./routes/cron'));
app.use('/', require('./routes/api'));
app.use('/', require('./routes/approvals'));
app.use('/client', require('./routes/client'));
app.use('/setter', require('./routes/setter'));

// ── AUTH MIDDLEWARE ───────────────────────────────────────────────────
function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  res.redirect('/login');
}

// Login page
app.get('/login', (req, res) => {
  res.send(`<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Pulseforge — Login</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=JetBrains+Mono:wght@400;500&family=DM+Sans:wght@400;500&display=swap" rel="stylesheet">
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#040810; color:#e8f0fe; font-family:'DM Sans',sans-serif; min-height:100vh; display:flex; align-items:center; justify-content:center; }
body::before { content:''; position:fixed; inset:0; background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(0,0,0,0.015) 3px,rgba(0,0,0,0.015) 4px); pointer-events:none; }
.login-box { width:380px; background:#070d1a; border:1px solid rgba(96,48,177,0.25); border-radius:16px; padding:2.5rem; position:relative; overflow:hidden; }
.login-box::before { content:''; position:absolute; top:0; left:0; right:0; height:2px; background:linear-gradient(90deg,transparent,#8b5cf6,#00d4b4,transparent); }
.logo { font-family:'Bebas Neue',sans-serif; font-size:1.8rem; letter-spacing:4px; margin-bottom:0.25rem; display:flex; align-items:center; gap:10px; }
.logo-dot { width:10px; height:10px; background:#8b5cf6; border-radius:50%; box-shadow:0 0 12px #8b5cf6; }
.subtitle { font-family:'JetBrains Mono',monospace; font-size:0.62rem; color:#6b7fa0; letter-spacing:2px; text-transform:uppercase; margin-bottom:2rem; }
.error { background:rgba(255,59,92,0.1); border:1px solid rgba(255,59,92,0.2); color:#ff3b5c; font-family:'JetBrains Mono',monospace; font-size:0.7rem; padding:0.6rem 0.75rem; border-radius:6px; margin-bottom:1rem; letter-spacing:1px; }
label { font-family:'JetBrains Mono',monospace; font-size:0.6rem; letter-spacing:2px; text-transform:uppercase; color:#6b7fa0; display:block; margin-bottom:0.4rem; }
input { width:100%; background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08); border-radius:6px; padding:0.8rem 1rem; color:#e8f0fe; font-family:'DM Sans',sans-serif; font-size:0.9rem; outline:none; transition:border-color 0.2s; margin-bottom:1.25rem; }
input:focus { border-color:#8b5cf6; background:rgba(139,92,246,0.05); }
button { width:100%; background:#8b5cf6; border:none; border-radius:6px; padding:0.85rem; color:#040810; font-family:'JetBrains Mono',monospace; font-size:0.75rem; font-weight:500; letter-spacing:2px; text-transform:uppercase; cursor:pointer; transition:all 0.2s; }
button:hover { background:#7c3aed; box-shadow:0 0 20px rgba(139,92,246,0.4); }
.version { font-family:'JetBrains Mono',monospace; font-size:0.55rem; color:#3a4a6a; text-align:center; margin-top:1.5rem; letter-spacing:1px; }
</style>
</head>
<body>
<div class="login-box">
  <div class="logo"><div class="logo-dot"></div>PULSEFORGE</div>
  <div class="subtitle">Command Center · Restricted Access</div>
  ${req.query.error ? '<div class="error">⚠ Invalid password — try again</div>' : ''}
  <form method="POST" action="/login">
    <label>Password</label>
    <input type="password" name="password" placeholder="Enter access code" autofocus required>
    <button type="submit">Access System →</button>
  </form>
  <div class="version">Pulseforge v0.6.0 · Phase 6</div>
</div>
</body>
</html>`);
});

app.post('/login', async (req, res) => {
  const { password } = req.body;
  const correct = process.env.DASHBOARD_PASSWORD;
  if (password === correct) {
    req.session.authenticated = true;
    res.redirect('/dashboard');
  } else {
    res.redirect('/login?error=1');
  }
});

app.get('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

// Health / API status check
app.get('/api/status', (req, res) => {
  res.json({
    serpapi: !!SERPAPI_KEY,
    hunter:  !!HUNTER_KEY,
    sheets:  !!SHEET_ID,
  });
});

// Main search endpoint — SSE stream for real-time log updates
app.get('/api/search', async (req, res) => {
  const { industry = 'cleaning', location = 'Manchester NH', title = 'owner', max = 25, minscore = 40 } = req.query;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const send = (type, data) => {
    res.write(`data: ${JSON.stringify({ type, ...data })}\n\n`);
  };

  const log = (level, msg) => send('log', { level, msg });

  try {
    log('info', `Starting search: "${industry}" in ${location}`);

    log('info', 'Querying SerpAPI...');
    let leads = await searchSerpAPI(`"${industry}" business "${location}" "${title}"`, parseInt(max), log);
    log('ok', `Found ${leads.length} raw results`);

    leads = leads.filter(l => !SKIP_DOMAINS.some(s => l.url.includes(s)));
    log('ok', `${leads.length} results after filtering directories/social`);

    leads = deduplicate(leads);
    log('ok', `${leads.length} unique domains`);

    send('progress', { pct: 30, label: 'Enriching with Hunter...' });

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

    const before = leads.length;
    leads = leads.filter(l => l.score >= parseInt(minscore));
    leads.sort((a, b) => b.score - a.score);

    send('progress', { pct: 100, label: 'Done' });
    log('ok', `Complete — ${leads.length} leads loaded, ${before - leads.length} filtered below ${minscore}% threshold`);

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
  const { postUrl, comment: rawComment, authorName } = req.body;

  if (!postUrl || !rawComment) {
    return res.status(400).json({ error: 'Missing postUrl or comment' });
  }

  let mainComment = rawComment;
  let firstCommentUrl = null;
  if (rawComment.startsWith('POST: ')) {
    const parts = rawComment.split('\nFIRST_COMMENT: ');
    mainComment = parts[0].replace(/^POST: /, '').trim();
    firstCommentUrl = parts[1]?.trim() || null;
  }

  try {
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    const db = require('./dbClient');

    puppeteer.use(StealthPlugin());

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
    await page.setCookie(...cookies);

    await page.goto(postUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));

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

    for (const char of mainComment) {
      await commentBox.type(char, { delay: Math.floor(Math.random() * 80) + 30 });
    }

    await new Promise(r => setTimeout(r, 1000 + Math.random() * 1000));

    const submitBtn = await page.$('.comments-comment-box__submit-button') ||
                      await page.$('button[type="submit"]');

    if (submitBtn) {
      await submitBtn.click();
      await new Promise(r => setTimeout(r, 3000));
    }

    if (firstCommentUrl) {
      const replyPosted = await page.evaluate((commentStart) => {
        const items = document.querySelectorAll('.comments-comment-item');
        for (const item of items) {
          const textEl = item.querySelector(
            '.comments-comment-item__main-content, ' +
            '.comments-comment-texteditor, ' +
            '.feed-shared-update-v2__description'
          );
          if (textEl?.innerText?.includes(commentStart)) {
            const replyBtn = item.querySelector(
              'button[aria-label*="Reply"], ' +
              '.comments-comment-action--reply button, ' +
              '.comments-comment-social-bar__reply-action-button'
            );
            if (replyBtn) { replyBtn.click(); return true; }
          }
        }
        return false;
      }, mainComment.slice(0, 25));

      if (replyPosted) {
        await new Promise(r => setTimeout(r, 1500 + Math.random() * 1000));

        const replyBox = await page.$('.comments-comment-box--is-comment-reply [contenteditable="true"]') ||
                         await page.$('.ql-editor[contenteditable="true"]');

        if (replyBox) {
          await replyBox.click();
          await new Promise(r => setTimeout(r, 400));
          for (const char of firstCommentUrl) {
            await replyBox.type(char, { delay: Math.floor(Math.random() * 60) + 20 });
          }
          await new Promise(r => setTimeout(r, 800));

          const replySubmit = await page.$('.comments-comment-box--is-comment-reply .comments-comment-box__submit-button') ||
                              await page.$('.comments-comment-box__submit-button:last-of-type');

          if (replySubmit) {
            await replySubmit.click();
            await new Promise(r => setTimeout(r, 2000));
            console.log(`✓ First reply posted: ${firstCommentUrl}`);
          }
        }
      } else {
        console.warn('[post-comment] Could not find comment to reply to — skipping first reply');
      }
    }

    await db.logAgentAction(
      'linkedin_agent',
      'post_comment',
      null,
      postUrl,
      { comment: mainComment, firstCommentUrl, authorName },
      'success'
    );

    await browser.close();

    console.log(`✓ Comment posted on: ${postUrl}`);
    res.json({ success: true, postUrl, comment: mainComment, firstCommentUrl });

  } catch (err) {
    console.error('Error posting comment:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/dashboard', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});

// ── DEMO MODE — no auth required ──────────────────────────────────
app.get('/demo', (req, res) => {
  const dashPath = path.join(__dirname, 'public', 'dashboard.html');
  let html;
  try { html = fs.readFileSync(dashPath, 'utf8'); }
  catch (e) { return res.status(500).send('Dashboard not found'); }

  const demoData = generateDemoData();

  const demoScript = `<script>
(function() {
  window.DEMO_MODE = true;
  window.DEMO_DATA = ${JSON.stringify(demoData)};
  var _demoCycle = 0;
  var _realFetch = window.fetch.bind(window);

  function demoResp(data) {
    return Promise.resolve({
      ok: true, status: 200,
      json: function() { return Promise.resolve(data); },
      text: function() { return Promise.resolve(JSON.stringify(data)); }
    });
  }

  window.fetch = function(url, opts) {
    if (!window.DEMO_MODE) return _realFetch(url, opts);
    var p = (url || '').split('?')[0];

    if (p === '/api/agent-status')       return demoResp(window.DEMO_DATA.agentStatus);
    if (p === '/api/agent-weekly-stats') return demoResp(window.DEMO_DATA.agentWeeklyStats);
    if (p === '/api/agent-stats')        return demoResp(window.DEMO_DATA.agentStats);
    if (p === '/api/approvals') {
      if (opts && opts.method === 'POST') return demoResp({ success: true });
      return demoResp(window.DEMO_DATA.approvals);
    }
    if (/^\\/api\\/approvals\\//.test(p))  return demoResp({ success: true });
    if (p === '/api/prospects')          return demoResp(window.DEMO_DATA.prospects);
    if (/^\\/api\\/prospects\\/.+\\/touchpoints$/.test(p)) return demoResp(window.DEMO_DATA.touchpoints);
    if (p === '/api/activity') {
      var events = window.DEMO_DATA.activityEvents;
      var slice = [];
      for (var i = 0; i < 8; i++) {
        var ev = events[(_demoCycle + i) % events.length];
        slice.push(Object.assign({}, ev, { time: i === 0 ? 'now' : (i + 1) + 'm' }));
      }
      _demoCycle = (_demoCycle + 1) % events.length;
      return demoResp(slice);
    }
    if (p === '/api/activity-panel')     return demoResp(window.DEMO_DATA.activityPanel);
    if (p === '/api/activity-timeline')  return demoResp(window.DEMO_DATA.activityTimeline);
    if (p === '/api/analytics')          return demoResp(window.DEMO_DATA.analytics);
    if (/^\\/api\\/run\\//.test(p))        return demoResp({ success: false, message: 'Agents are read-only in demo mode.' });
    return demoResp({});
  };
})();
</script>`;

  html = html.replace('</head>', demoScript + '\n</head>');
  res.setHeader('Content-Type', 'text/html');
  res.send(html);
});

// Sketch mockup preview
app.get('/preview', (req, res) => {
  res.redirect('/client/preview');
});

app.get('/mockups', (req, res) => {
  const mockupsDir = path.join(__dirname, 'mockups');
  let files = [];

  try {
    files = fs.readdirSync(mockupsDir)
      .filter(file => file.endsWith('.html'))
      .map(file => {
        const filepath = path.join(mockupsDir, file);
        return { file, mtime: fs.statSync(filepath).mtimeMs };
      })
      .sort((a, b) => b.mtime - a.mtime);
  } catch (err) {
    console.error('[preview] Failed to read mockups:', err.message);
  }

  if (files.length === 1) {
    return res.redirect(`/preview/${encodeURIComponent(files[0].file)}`);
  }

  const rows = files.map(item => `
    <a class="mockup" href="/preview/${encodeURIComponent(item.file)}">
      <span>${item.file.replace(/-/g, ' ').replace(/\.html$/, '')}</span>
      <small>${new Date(item.mtime).toLocaleString()}</small>
    </a>
  `).join('');

  res.send(`<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pulseforge Preview</title>
<style>
* { box-sizing: border-box; }
body { margin: 0; min-height: 100vh; background: #050711; color: #edf2ff; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; display: flex; align-items: center; justify-content: center; padding: 24px; }
.wrap { width: min(760px, 100%); }
h1 { margin: 0 0 8px; font-size: clamp(2rem, 5vw, 4rem); line-height: 1; }
p { color: #8b96b5; margin: 0 0 22px; }
.mockups { display: grid; gap: 10px; }
.mockup { display: flex; justify-content: space-between; gap: 16px; padding: 16px; border: 1px solid rgba(139, 92, 246, 0.28); border-radius: 8px; background: rgba(255,255,255,0.04); color: #edf2ff; text-decoration: none; }
.mockup:hover { border-color: rgba(139, 92, 246, 0.7); background: rgba(139, 92, 246, 0.09); }
small { color: #8b96b5; white-space: nowrap; }
.empty { border: 1px solid rgba(139, 92, 246, 0.28); border-radius: 8px; padding: 18px; color: #8b96b5; background: rgba(255,255,255,0.04); }
</style>
</head>
<body>
<main class="wrap">
  <h1>Mockup Preview</h1>
  <p>${files.length ? 'Choose a generated mockup to preview.' : 'No generated mockups were found yet.'}</p>
  <div class="mockups">${rows || '<div class="empty">Run Sketch to generate a mockup, then refresh this page.</div>'}</div>
</main>
</body>
</html>`);
});

app.get('/preview/:filename', (req, res) => {
  const { filename } = req.params;
  const filepath = path.join(__dirname, 'mockups', filename);
  if (fs.existsSync(filepath)) {
    res.sendFile(filepath);
  } else {
    res.status(404).send('Mockup not found');
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
async function ensureAgentActionsTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS agent_actions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      created_by TEXT DEFAULT 'max',
      action_type TEXT NOT NULL,
      title TEXT NOT NULL,
      description TEXT NOT NULL,
      payload JSONB,
      status TEXT DEFAULT 'pending',
      executed_at TIMESTAMPTZ,
      result TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

app.listen(PORT, async () => {
  await ensureAgentActionsTable().catch(e => console.error('[startup] agent_actions table error:', e.message));
  console.log(`\n🔷 Pulseforge Lead Engine Server`);
  console.log(`─────────────────────────────────`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   SerpAPI : ${SERPAPI_KEY ? '✓' : '✗ not set'}`);
  console.log(`   Hunter  : ${HUNTER_KEY  ? '✓' : '✗ not set'}`);
  console.log(`─────────────────────────────────\n`);
});
