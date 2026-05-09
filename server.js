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
const session = require('express-session');
const pgSession = require('connect-pg-simple')(session);
const bcrypt = require('bcryptjs');
const pool = require('./db');
const { createObjectCsvWriter } = require('csv-writer');

const BREVO_EVENT_MAP = {
  opened:           'email_opened',
  click:            'email_clicked',   // Brevo sends 'click' not 'clicked'
  loaded_by_proxy:  'email_opened',    // Apple Mail Privacy Protection — treat as open
  hard_bounce:      'email_bounced',
  soft_bounce:      'email_soft_bounce',
  unsubscribed:     'email_unsubscribed',
  spam:             'email_spam',
};

async function checkAndUpdateWarmStatus(prospectId, email) {
  try {
    const res = await pool.query(`
      SELECT
        COUNT(CASE WHEN action_type = 'email_opened'
              AND created_at > NOW() - INTERVAL '14 days' THEN 1 END)::int AS opens_14d,
        COUNT(CASE WHEN action_type = 'email_clicked' THEN 1 END)::int AS clicks_all
      FROM touchpoints
      WHERE prospect_id = $1 AND channel = 'email'
    `, [prospectId]);
    const { opens_14d, clicks_all } = res.rows[0];
    const opens  = parseInt(opens_14d  || 0);
    const clicks = parseInt(clicks_all || 0);
    if (clicks >= 1 || opens >= 3) {
      const upd = await pool.query(
        `UPDATE prospects SET status = 'warm', updated_at = NOW()
         WHERE id = $1 AND status = 'cold' RETURNING id`,
        [prospectId]
      );
      if (upd.rows.length > 0) {
        console.log(`[Riley] ${email} upgraded to warm — ${opens} opens / ${clicks} clicks`);
      }
    }
  } catch (err) {
    console.error('[Riley] checkAndUpdateWarmStatus error:', err.message);
  }
}
const { publishBlogPost } = require('./utils/blogPublisher');
const {
  publishToGoogleBusiness,
  publishToFacebookPage,
  publishFayeComment,
  publishToLinkedInPage,
  publishLinkComment,
} = require('./utils/publishPipeline');
const { generateDemoData } = require('./utils/demoData');


const app  = express();
app.use(session({
  store: new pgSession({
    pool: pool,
    tableName: 'session'
  }),
  secret: process.env.SESSION_SECRET || 'pulseforge-secret-key',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000 }
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
const PORT = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname)); // serves dashboard HTML

// Recover from malformed JSON bodies so cron routes can still read req.query.secret
app.use((err, req, res, next) => {
  if (err.status === 400 && err.type === 'entity.parse.failed') {
    req.body = {};
    return next();
  }
  next(err);
});

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

// ── BREVO EMAIL TRACKING WEBHOOKS ─────────────────────────────────────────────
app.post('/webhooks/brevo', (req, res) => {
  res.status(200).json({ ok: true });
  setImmediate(async () => {
    try {
      const payload = req.body || {};
      const event      = payload.event;
      const email      = (payload.email || '').toLowerCase().trim();
      const actionType = BREVO_EVENT_MAP[event];

      if (!actionType || !email) return;

      const prospectRes = await pool.query(
        `SELECT id, status FROM prospects WHERE LOWER(email) = $1 LIMIT 1`,
        [email]
      );
      if (!prospectRes.rows.length) {
        console.warn(`[Riley] No prospect for email: ${email} (event: ${event})`);
        return;
      }
      const prospect = prospectRes.rows[0];

      // Log touchpoint
      const outcomeJson = JSON.stringify({
        event,
        subject:    payload.subject  || null,
        link:       payload.link     || null,
        brevo_id:   payload.id       || null,
        message_id: payload.messageId || null,
        date:       payload.date     || null,
      });
      await pool.query(`
        INSERT INTO touchpoints
          (prospect_id, channel, action_type, content_summary, outcome, sentiment, external_ref)
        VALUES ($1, 'email', $2, $3, $4, 'neutral', $5)
      `, [
        prospect.id, actionType,
        payload.subject || null,
        outcomeJson,
        payload.messageId || null,
      ]);

      // DNC for bounces / spam / unsubscribes
      if (['email_bounced', 'email_spam', 'email_unsubscribed'].includes(actionType)) {
        await pool.query(
          `UPDATE prospects SET do_not_contact = true, updated_at = NOW() WHERE id = $1`,
          [prospect.id]
        );
        console.log(`[Riley] ${email} marked do_not_contact (${event})`);
      }

      // Warm signal check
      if (['email_opened', 'email_clicked'].includes(actionType)) {
        await checkAndUpdateWarmStatus(prospect.id, email);
      }

      // agent_log
      await pool.query(`
        INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
        VALUES ('riley', $1, $2, $3, 'success', NOW())
      `, [
        actionType,
        prospect.id,
        JSON.stringify({ event, email, subject: payload.subject, link: payload.link }),
      ]);

      console.log(`[Riley] Tracked ${event} for ${email}`);
    } catch (err) {
      console.error('[Riley] Webhook error:', err.message);
    }
  });
});

// Auth middleware
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

// Login POST
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

// Logout
app.get('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

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
  const { postUrl, comment: rawComment, authorName } = req.body;

  if (!postUrl || !rawComment) {
    return res.status(400).json({ error: 'Missing postUrl or comment' });
  }

  // Parse delimiter format: POST: {main text}\nFIRST_COMMENT: {url}
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
    const fs = require('fs');
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

    // Post first reply with URL if present
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

    // Log to second brain
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

  // Inject before </head> so fetch is overridden before any dashboard scripts run
  html = html.replace('</head>', demoScript + '\n</head>');
  res.setHeader('Content-Type', 'text/html');
  res.send(html);
});

// API - Agent status for dashboard
app.get('/api/agent-status', requireAuth, async (req, res) => {
  try {
    const [prospects, touchpoints, pending, agentRuns, channels, weeklyTouchpoints] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM prospects'),
      pool.query('SELECT COUNT(*) FROM touchpoints'),
      pool.query('SELECT COUNT(*) FROM pending_comments WHERE status = $1', ['pending']),
      pool.query('SELECT agent_name, COUNT(*) as runs, MAX(ran_at) as last_run FROM agent_log GROUP BY agent_name'),
      pool.query('SELECT channel, COUNT(*) as count FROM pending_comments GROUP BY channel'),
      pool.query('SELECT COUNT(*) FROM touchpoints WHERE created_at > NOW() - INTERVAL \'7 days\'')
    ]);

    // Build agent ring percentages
    const runMap = {};
    agentRuns.rows.forEach(r => { runMap[r.agent_name] = parseInt(r.runs); });

    const totalProspects = parseInt(prospects.rows[0].count);
    const totalTouchpoints = parseInt(touchpoints.rows[0].count);
    const fbPending = channels.rows.find(c => c.channel === 'facebook')?.count || 0;
    const liPending = channels.rows.find(c => c.channel === 'linkedin')?.count || 0;

    const rings = {
      scout: Math.min((runMap['scout_agent'] || 0) / 20, 1),
      link: totalTouchpoints > 0 ? Math.min(parseInt(liPending) / Math.max(runMap['linkedin_agent'] || 1, 1), 1) : 0,
      faye: totalTouchpoints > 0 ? Math.min(parseInt(fbPending) / Math.max(runMap['facebook_agent'] || 1, 1), 1) : 0,
      emmett: Math.min((runMap['email_agent'] || 0) / Math.max(totalProspects, 1), 1),
      max: runMap['max_agent'] ? 1 : 0,
      rex: runMap['rex_agent'] ? 1 : 0
    };

    res.json({
      prospects: totalProspects,
      touchpoints: totalTouchpoints,
      pending: parseInt(pending.rows[0].count),
      weeklyTouchpoints: parseInt(weeklyTouchpoints.rows[0].count),
      agentRuns: runMap,
      rings,
      channels: channels.rows
    });

  } catch (err) {
    console.error('API error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API - Agent status for dashboard
app.get('/api/agent-status', requireAuth, async (req, res) => {
  try {
    const [prospects, touchpoints, pending, agentRuns, channels, weeklyTouchpoints] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM prospects'),
      pool.query('SELECT COUNT(*) FROM touchpoints'),
      pool.query('SELECT COUNT(*) FROM pending_comments WHERE status = $1', ['pending']),
      pool.query('SELECT agent_name, COUNT(*) as runs, MAX(ran_at) as last_run FROM agent_log GROUP BY agent_name'),
      pool.query('SELECT channel, COUNT(*) as count FROM pending_comments GROUP BY channel'),
      pool.query('SELECT COUNT(*) FROM touchpoints WHERE created_at > NOW() - INTERVAL \'7 days\'')
    ]);

    const runMap = {};
    agentRuns.rows.forEach(r => { runMap[r.agent_name] = parseInt(r.runs); });

    const totalProspects = parseInt(prospects.rows[0].count);
    const totalTouchpoints = parseInt(touchpoints.rows[0].count);
    const fbPending = channels.rows.find(c => c.channel === 'facebook')?.count || 0;
    const liPending = channels.rows.find(c => c.channel === 'linkedin')?.count || 0;

    const rings = {
      scout: Math.min((runMap['scout_agent'] || 0) / 20, 1),
      link: totalTouchpoints > 0 ? Math.min(parseInt(liPending) / Math.max(runMap['linkedin_agent'] || 1, 1), 1) : 0,
      faye: totalTouchpoints > 0 ? Math.min(parseInt(fbPending) / Math.max(runMap['facebook_agent'] || 1, 1), 1) : 0,
      emmett: Math.min((runMap['email_agent'] || 0) / Math.max(totalProspects, 1), 1),
      max: runMap['max_agent'] ? 1 : 0,
      rex: runMap['rex_agent'] ? 1 : 0
    };

    res.json({
      prospects: totalProspects,
      touchpoints: totalTouchpoints,
      pending: parseInt(pending.rows[0].count),
      weeklyTouchpoints: parseInt(weeklyTouchpoints.rows[0].count),
      agentRuns: runMap,
      rings,
      channels: channels.rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Get pending comments
app.get('/api/approvals', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, author_name, author_title, post_content, comment, channel, status, created_at
      FROM pending_comments
      WHERE status = 'pending'
      ORDER BY created_at DESC
      LIMIT 50
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Approve or reject a comment
app.post('/api/approvals/:id', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { action } = req.body;
  if (!['approved', 'rejected'].includes(action)) {
    return res.status(400).json({ error: 'Invalid action' });
  }
  try {
    const result = await pool.query(
      'UPDATE pending_comments SET status = $1 WHERE id = $2 RETURNING *',
      [action, id]
    );
    res.json({ success: true, id, action });

    // Fire-and-forget publish pipelines for all approved channels
    const item = result.rows[0];
    if (item && action === 'approved') {
      const publishers = {
        blog:             () => publishBlogPost(item),
        google_business:  () => publishToGoogleBusiness(item),
        facebook_page:    () => publishToFacebookPage(item),
        facebook:         () => publishFayeComment(item),
        linkedin_page:    () => publishToLinkedInPage(item),
        linkedin:         () => publishLinkComment(item),
      };
      const publish = publishers[item.channel];
      if (publish) {
        publish().catch(err =>
          console.error(`[Publisher:${item.channel}] Unhandled error:`, err.message)
        );
      }
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Prospects table
app.get('/api/prospects', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        p.id, p.first_name, p.last_name, p.email, p.phone,
        p.status, p.icp_score, p.notes, p.last_contacted_at,
        c.name as company_name,
        COUNT(t.id)::int as touchpoint_count
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id
      LEFT JOIN touchpoints t ON t.prospect_id = p.id
      WHERE p.do_not_contact = false
      GROUP BY p.id, c.name
      ORDER BY p.icp_score DESC NULLS LAST
      LIMIT 200
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Touchpoints for a single prospect
app.get('/api/prospects/:id/touchpoints', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT channel, action_type, content_summary, outcome, created_at
      FROM touchpoints
      WHERE prospect_id = $1
      ORDER BY created_at ASC
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Agent stats for sparklines
app.get('/api/agent-stats', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END as agent_name,
        COUNT(*) as total_runs,
        MAX(ran_at) as last_run,
        COUNT(CASE WHEN ran_at > NOW() - INTERVAL '7 days' THEN 1 END) as week_runs,
        COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count
      FROM agent_log
      GROUP BY CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END
    `);

    const daily = await pool.query(`
      SELECT
        CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END as agent_name,
        DATE(ran_at) as date, COUNT(*) as count
      FROM agent_log
      WHERE ran_at > NOW() - INTERVAL '7 days'
      GROUP BY CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END, DATE(ran_at)
      ORDER BY date ASC
    `);

    const stats = {};
    result.rows.forEach(r => {
      stats[r.agent_name] = {
        total: parseInt(r.total_runs),
        weekRuns: parseInt(r.week_runs),
        successCount: parseInt(r.success_count),
        lastRun: r.last_run,
        daily: []
      };
    });

    daily.rows.forEach(r => {
      if (stats[r.agent_name]) {
        stats[r.agent_name].daily.push({ date: r.date, count: parseInt(r.count) });
      }
    });

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Agent weekly stats for hover tooltips
app.get('/api/agent-weekly-stats', requireAuth, async (req, res) => {
  try {
    const WEEK = `created_at > NOW() - INTERVAL '7 days'`;
    const WEEK_AL = `ran_at > NOW() - INTERVAL '7 days'`;

    const [logRows, emmettRow, scoutRow, linkRow, fayeRow, ivyRow] = await Promise.all([
      // agent_log — covers Paige, Max, Riley, Cal, Vera, Rex, Sam, Penny, Sketch
      pool.query(`
        SELECT LOWER(REPLACE(agent_name, '_agent', '')) AS agent, action, COUNT(*) AS count
        FROM agent_log
        WHERE ${WEEK_AL} AND status = 'success'
        GROUP BY agent, action
      `),
      // Emmett → touchpoints (emails sent as outbound)
      pool.query(`SELECT COUNT(*) AS count FROM touchpoints WHERE channel = 'email' AND action_type = 'outbound' AND ${WEEK}`),
      // Scout → prospects saved this week via scout
      pool.query(`SELECT COUNT(*) AS count FROM prospects WHERE source = 'scout' AND ${WEEK}`),
      // Link → pending_comments for linkedin
      pool.query(`SELECT COUNT(*) AS count FROM pending_comments WHERE channel = 'linkedin' AND ${WEEK}`),
      // Faye → pending_comments for facebook
      pool.query(`SELECT COUNT(*) AS count FROM pending_comments WHERE channel = 'facebook' AND ${WEEK}`),
      // Ivy → pending_comments for instagram
      pool.query(`SELECT COUNT(*) AS count FROM pending_comments WHERE channel = 'instagram' AND ${WEEK}`),
    ]);

    const raw = {};
    for (const r of logRows.rows) {
      if (!raw[r.agent]) raw[r.agent] = {};
      raw[r.agent][r.action] = parseInt(r.count);
    }
    const pick = (r, ...actions) => actions.reduce((s, a) => s + (r[a] || 0), 0);

    const stats = {
      scout:  { count: parseInt(scoutRow.rows[0].count),                                    label: 'prospects found'   },
      emmett: { count: parseInt(emmettRow.rows[0].count),                                   label: 'emails sent'       },
      link:   { count: parseInt(linkRow.rows[0].count),                                     label: 'drafts generated'  },
      faye:   { count: parseInt(fayeRow.rows[0].count),                                     label: 'drafts generated'  },
      ivy:    { count: parseInt(ivyRow.rows[0].count),                                      label: 'drafts generated'  },
      paige:  { count: pick(raw.paige  || {}, 'generate_content'),                          label: 'posts generated'   },
      max:    { count: pick(raw.max    || {}, 'daily_digest', 'weekly_report'),             label: 'digests sent'      },
      sam:    { count: pick(raw.sam    || {}, 'send_sms', 'batch_sms'),                     label: 'SMS sent'          },
      rex:    { count: pick(raw.rex    || {}, 'weekly_report', 'run'),                      label: 'reports generated' },
      riley:  { count: pick(raw.riley  || {}, 'triage', 'classify_email'),                  label: 'emails triaged'    },
      vera:   { count: pick(raw.vera   || {}, 'analyze_reviews', 'run'),                    label: 'reviews monitored' },
      cal:    { count: pick(raw.cal    || {}, 'initiate_call', 'run'),                      label: 'calls initiated'   },
      penny:     { count: pick(raw.penny     || {}, 'analyze_account', 'run'),              label: 'accounts analyzed' },
      sketch:    { count: pick(raw.sketch    || {}, 'generate_mockup', 'run'),              label: 'mockups generated' },
      analytics: { count: pick(raw.analytics || {}, 'fetch_metrics', 'run'),               label: 'posts analyzed'    },
    };

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Live activity feed
app.get('/api/activity', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT al.agent_name, al.action, al.status, al.ran_at, al.payload,
        p.first_name, p.last_name
      FROM agent_log al
      LEFT JOIN prospects p ON al.prospect_id = p.id
      ORDER BY al.ran_at DESC
      LIMIT 20
    `);

    const agentNameMap = {
      facebook: 'Faye', linkedin: 'Link', emmett: 'Emmett',
      max: 'Max', rex: 'Rex', scout: 'Scout', sketch: 'Sketch', email: 'Emmett'
    };

    const feed = result.rows.map(row => {
      const rawAgent = row.agent_name?.replace('_agent', '') || 'system';
      const agent = agentNameMap[rawAgent] || rawAgent.charAt(0).toUpperCase() + rawAgent.slice(1);
      const minutesAgo = Math.floor((Date.now() - new Date(row.ran_at)) / 60000);
      const timeLabel = minutesAgo < 60 ? `${minutesAgo}m` : minutesAgo < 1440 ? `${Math.floor(minutesAgo/60)}h` : `${Math.floor(minutesAgo/1440)}d`;
      const prospect = row.first_name ? `· ${row.first_name} ${row.last_name}`.trim() : '';
      const actionLabels = {
        generate_comment: `generated a comment draft ${prospect}`,
        daily_digest: 'daily digest sent · jacob@gopulseforge.com',
        weekly_report: 'weekly report dispatched',
        generate_mockup: `generated a mockup ${prospect}`,
        outbound: `sent email sequence ${prospect}`,
        dashboard_trigger: 'triggered from dashboard'
      };
      const label = actionLabels[row.action] || row.action;
      const icons = {
        Faye: { icon: '📣', color: 'fi-t' }, Link: { icon: '💬', color: 'fi-p' },
        Emmett: { icon: '✉️', color: 'fi-o' }, Max: { icon: '🧠', color: 'fi-p' },
        Rex: { icon: '📊', color: 'fi-p' }, Scout: { icon: '🔍', color: 'fi-t' },
        Sketch: { icon: '🎨', color: 'fi-t' }
      };
      const { icon, color } = icons[agent] || { icon: '⚡', color: 'fi-g' };
      return { agent, action: label, icon, color, time: timeLabel, status: row.status };
    });

    res.json(feed);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Activity panel (sequences + timeline)
app.get('/api/activity-panel', requireAuth, async (req, res) => {
  try {
    const [seqResult, timelineResult] = await Promise.all([
      pool.query(`
        SELECT
          p.id, p.first_name, p.last_name, p.notes, p.status,
          c.name as company_name,
          COUNT(t.id)::int as emails_sent,
          MAX(t.created_at) as last_touch,
          COALESCE(eng.open_count,  0)::int as open_count,
          COALESCE(eng.click_count, 0)::int as click_count,
          CASE
            WHEN COUNT(t.id) = 1 THEN MAX(t.created_at) + INTERVAL '4 days'
            WHEN COUNT(t.id) = 2 THEN MAX(t.created_at) + INTERVAL '4 days'
            WHEN COUNT(t.id) = 3 THEN MAX(t.created_at) + INTERVAL '5 days'
            ELSE NULL
          END as next_due_at
        FROM prospects p
        LEFT JOIN companies c ON p.company_id = c.id
        INNER JOIN touchpoints t
          ON t.prospect_id = p.id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
        LEFT JOIN (
          SELECT
            prospect_id,
            COUNT(CASE WHEN action_type = 'email_opened'  THEN 1 END)::int AS open_count,
            COUNT(CASE WHEN action_type = 'email_clicked' THEN 1 END)::int AS click_count
          FROM touchpoints
          WHERE channel = 'email'
          GROUP BY prospect_id
        ) eng ON eng.prospect_id = p.id
        WHERE p.do_not_contact = false
        GROUP BY p.id, c.name, eng.open_count, eng.click_count
        ORDER BY MAX(t.created_at) DESC
        LIMIT 100
      `),
      pool.query(`
        SELECT
          al.id, al.agent_name, al.action, al.status, al.ran_at,
          p.first_name, p.last_name, p.notes as prospect_notes
        FROM agent_log al
        LEFT JOIN prospects p ON al.prospect_id = p.id
        ORDER BY al.ran_at DESC
        LIMIT 50
      `)
    ]);

    const STAGE_LABELS = ['', 'Day 0 sent · next Day 4', 'Day 4 sent · next Day 8', 'Day 8 sent · next Day 13', 'Complete'];
    const sequences = seqResult.rows.map(r => {
      const count = r.emails_sent;
      return {
        id:           r.id,
        business:     r.company_name || (r.notes || '').split('—')[0].trim() || `${r.first_name} ${r.last_name}`.trim(),
        status:       r.status,
        emails_sent:  count,
        stage_label:  STAGE_LABELS[Math.min(count, 4)] || 'Unknown',
        last_touch:   r.last_touch,
        next_due_at:  r.next_due_at,
        overdue:      r.next_due_at ? new Date(r.next_due_at) < new Date() : false,
        complete:     count >= 4,
        open_count:   r.open_count  || 0,
        click_count:  r.click_count || 0,
        has_opened:   (r.open_count  || 0) > 0,
        has_clicked:  (r.click_count || 0) > 0,
      };
    });

    const AGENT_LABELS = {
      scout: { name: 'Scout', icon: '🔍' }, linkedin: { name: 'Link', icon: '💬' },
      facebook: { name: 'Faye', icon: '📣' }, emmett: { name: 'Emmett', icon: '✉️' },
      email: { name: 'Emmett', icon: '✉️' }, max: { name: 'Max', icon: '🧠' },
      rex: { name: 'Rex', icon: '📊' }, riley: { name: 'Riley', icon: '🙋' },
      sketch: { name: 'Sketch', icon: '🎨' }, paige: { name: 'Paige', icon: '✍️' },
      sam: { name: 'Sam', icon: '📱' }, vera: { name: 'Vera', icon: '⭐' },
      cal: { name: 'Cal', icon: '📞' }, ivy: { name: 'Ivy', icon: '📸' },
      penny: { name: 'Penny', icon: '💰' }
    };
    const ACTION_LABELS = {
      generate_comment: 'drafted comment', daily_digest: 'sent daily digest',
      weekly_report: 'sent weekly report', generate_mockup: 'generated mockup',
      outbound: 'sent email', dashboard_trigger: 'triggered from dashboard',
      send_sms: 'sent SMS', generate_content: 'generated content',
      triage: 'triaged inbox', batch_sms: 'ran SMS batch',
      analyze_account: 'analyzed ad account', initiate_call: 'initiated call',
      analyze_reviews: 'analyzed reviews'
    };
    const timeline = timelineResult.rows.map(r => {
      const rawAgent = (r.agent_name || '').replace('_agent', '');
      const agentInfo = AGENT_LABELS[rawAgent] || { name: rawAgent, icon: '⚡' };
      const prospectName = r.first_name ? `${r.first_name} ${r.last_name}`.trim() : null;
      const prospectBiz = prospectName || (r.prospect_notes || '').split('—')[0].trim() || null;
      return {
        id: r.id,
        agent: agentInfo.name,
        icon: agentInfo.icon,
        action: ACTION_LABELS[r.action] || r.action,
        prospect: prospectBiz,
        status: r.status,
        ran_at: r.ran_at
      };
    });

    res.json({ sequences, timeline });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Load more timeline items
app.get('/api/activity-timeline', requireAuth, async (req, res) => {
  const offset = parseInt(req.query.offset) || 0;
  try {
    const result = await pool.query(`
      SELECT al.id, al.agent_name, al.action, al.status, al.ran_at,
        p.first_name, p.last_name, p.notes as prospect_notes
      FROM agent_log al
      LEFT JOIN prospects p ON al.prospect_id = p.id
      ORDER BY al.ran_at DESC
      LIMIT 50 OFFSET $1
    `, [offset]);

    const AGENT_LABELS = {
      scout: { name: 'Scout', icon: '🔍' }, linkedin: { name: 'Link', icon: '💬' },
      facebook: { name: 'Faye', icon: '📣' }, emmett: { name: 'Emmett', icon: '✉️' },
      email: { name: 'Emmett', icon: '✉️' }, max: { name: 'Max', icon: '🧠' },
      rex: { name: 'Rex', icon: '📊' }, riley: { name: 'Riley', icon: '🙋' },
      sketch: { name: 'Sketch', icon: '🎨' }, paige: { name: 'Paige', icon: '✍️' },
      sam: { name: 'Sam', icon: '📱' }, vera: { name: 'Vera', icon: '⭐' },
      cal: { name: 'Cal', icon: '📞' }, ivy: { name: 'Ivy', icon: '📸' },
      penny: { name: 'Penny', icon: '💰' }
    };
    const ACTION_LABELS = {
      generate_comment: 'drafted comment', daily_digest: 'sent daily digest',
      weekly_report: 'sent weekly report', generate_mockup: 'generated mockup',
      outbound: 'sent email', dashboard_trigger: 'triggered from dashboard',
      send_sms: 'sent SMS', generate_content: 'generated content',
      triage: 'triaged inbox', batch_sms: 'ran SMS batch',
      analyze_account: 'analyzed ad account', initiate_call: 'initiated call',
      analyze_reviews: 'analyzed reviews'
    };
    const rows = result.rows.map(r => {
      const rawAgent = (r.agent_name || '').replace('_agent', '');
      const agentInfo = AGENT_LABELS[rawAgent] || { name: rawAgent, icon: '⚡' };
      const prospectName = r.first_name ? `${r.first_name} ${r.last_name}`.trim() : null;
      const prospectBiz = prospectName || (r.prospect_notes || '').split('—')[0].trim() || null;
      return {
        id: r.id, agent: agentInfo.name, icon: agentInfo.icon,
        action: ACTION_LABELS[r.action] || r.action,
        prospect: prospectBiz, status: r.status, ran_at: r.ran_at
      };
    });
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Analytics
app.get('/api/analytics', requireAuth, async (req, res) => {
  try {
    const [vol, reply, icp, agents, funnel, topProspects] = await Promise.all([

      // 1. Outbound volume — email + sms per day, last 30 days
      pool.query(`
        SELECT
          DATE(created_at)::text AS date,
          channel,
          COUNT(*) AS count
        FROM touchpoints
        WHERE channel IN ('email','sms')
          AND action_type = 'outbound'
          AND created_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE(created_at), channel
        ORDER BY date ASC
      `),

      // 2. Reply rate — inbound vs outbound per week, last 8 weeks
      pool.query(`
        SELECT
          DATE_TRUNC('week', created_at)::text AS week,
          action_type,
          COUNT(*) AS count
        FROM touchpoints
        WHERE channel = 'email'
          AND created_at >= NOW() - INTERVAL '56 days'
        GROUP BY DATE_TRUNC('week', created_at), action_type
        ORDER BY week ASC
      `),

      // 3. ICP score distribution
      pool.query(`
        SELECT
          CASE
            WHEN icp_score IS NULL          THEN 'Unknown'
            WHEN icp_score BETWEEN 0  AND 20 THEN '0–20'
            WHEN icp_score BETWEEN 21 AND 40 THEN '21–40'
            WHEN icp_score BETWEEN 41 AND 60 THEN '41–60'
            WHEN icp_score BETWEEN 61 AND 80 THEN '61–80'
            ELSE '81–100'
          END AS bucket,
          COUNT(*) AS count
        FROM prospects
        WHERE do_not_contact = false
        GROUP BY bucket
      `),

      // 4. Agent activity breakdown — last 30 days
      pool.query(`
        SELECT agent_name, COUNT(*) AS count
        FROM agent_log
        WHERE ran_at >= NOW() - INTERVAL '30 days'
          AND agent_name IS NOT NULL
        GROUP BY agent_name
        ORDER BY count DESC
      `),

      // 5. Pipeline funnel
      pool.query(`
        SELECT
          COALESCE(status, 'cold') AS stage,
          COUNT(*) AS count
        FROM prospects
        WHERE do_not_contact = false
        GROUP BY stage
      `),

      // 6. Top 10 prospects by touchpoint count
      pool.query(`
        SELECT
          p.id,
          p.first_name,
          p.last_name,
          p.notes,
          p.status,
          c.name AS company_name,
          COUNT(t.id)::int AS touchpoint_count,
          MAX(t.created_at) AS last_contacted_at
        FROM prospects p
        LEFT JOIN companies c ON p.company_id = c.id
        LEFT JOIN touchpoints t ON t.prospect_id = p.id
        WHERE p.do_not_contact = false
        GROUP BY p.id, c.name
        ORDER BY touchpoint_count DESC
        LIMIT 10
      `)
    ]);

    // Build 30-day date spine for volume chart
    const days = [];
    for (let i = 29; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      days.push(d.toISOString().slice(0, 10));
    }
    const volByDay = {};
    vol.rows.forEach(r => {
      if (!volByDay[r.date]) volByDay[r.date] = { email: 0, sms: 0 };
      volByDay[r.date][r.channel] = parseInt(r.count);
    });
    const outbound_volume = days.map(d => ({
      date: d,
      email: volByDay[d]?.email || 0,
      sms:   volByDay[d]?.sms   || 0
    }));

    // Build reply rate per week
    const weekMap = {};
    reply.rows.forEach(r => {
      if (!weekMap[r.week]) weekMap[r.week] = { outbound: 0, inbound: 0 };
      weekMap[r.week][r.action_type] = parseInt(r.count);
    });
    const reply_rate = Object.entries(weekMap).map(([week, v]) => ({
      week: new Date(week).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      outbound: v.outbound,
      inbound:  v.inbound,
      rate: v.outbound > 0 ? Math.round((v.inbound / v.outbound) * 100) : 0
    }));

    // ICP buckets in fixed order
    const BUCKETS = ['0–20', '21–40', '41–60', '61–80', '81–100', 'Unknown'];
    const icpMap = {};
    icp.rows.forEach(r => { icpMap[r.bucket] = parseInt(r.count); });
    const icp_distribution = BUCKETS.map(b => ({ bucket: b, count: icpMap[b] || 0 }));

    // Agent breakdown — normalize dirty historical names then merge duplicates
    const AGENT_NAME_MAP = {
      faye_agent:              'faye',
      faye_agent1:             'faye',
      facebook_agent:          'faye',
      link_agent:              'link',
      link_agent1:             'link',
      linkedin_agent:          'link',
      cal_agent:               'cal',
      analytics_agent:         'analytics',
      emmett_agent:            'emmett',
      emmett_agent1:           'emmett',
      email_agent:             'emmett',
      scout_agent:             'scout',
      sketch_agent:            'sketch',
      max_agent:               'max',
      rex_agent:               'rex',
      riley_agent:             'riley',
      sam_agent:               'sam',
      vera_agent:              'vera',
      paige_agent:             'paige',
      penny_agent:             'penny',
      ivy_agent:               'ivy',
      facebook_page_publisher: 'paige',
      linkedin_page_publisher: 'paige',
      google_business_publisher: 'paige',
      blog_publisher:          'paige',
    };
    function normalizeAgentName(raw) {
      if (!raw) return 'unknown';
      const lower = raw.toLowerCase();
      if (AGENT_NAME_MAP[lower]) return AGENT_NAME_MAP[lower];
      // strip trailing _agent or _publisher suffix
      return lower.replace(/_(agent|publisher)\d*$/, '').replace(/\d+$/, '');
    }
    const agentTotals = {};
    agents.rows.forEach(r => {
      const name = normalizeAgentName(r.agent_name);
      agentTotals[name] = (agentTotals[name] || 0) + parseInt(r.count);
    });
    const agent_breakdown = Object.entries(agentTotals)
      .map(([agent, count]) => ({ agent, count }))
      .sort((a, b) => b.count - a.count);

    // Funnel — enforce order, include replied/converted if present
    const STAGES = ['cold', 'warm', 'replied', 'converted'];
    const stageMap = {};
    funnel.rows.forEach(r => { stageMap[r.stage] = parseInt(r.count); });
    const total = Object.values(stageMap).reduce((s, v) => s + v, 0);
    const pipeline_funnel = STAGES
      .filter(s => stageMap[s] !== undefined)
      .map(s => ({ stage: s, count: stageMap[s], pct: total > 0 ? Math.round((stageMap[s] / total) * 100) : 0 }));
    // ensure cold always shows even if zero
    if (!pipeline_funnel.find(f => f.stage === 'cold')) {
      pipeline_funnel.unshift({ stage: 'cold', count: 0, pct: 0 });
    }

    // Top prospects
    const top_prospects = topProspects.rows.map(r => ({
      id: r.id,
      name: `${r.first_name} ${r.last_name}`.trim(),
      business: r.company_name || (r.notes || '').split('—')[0].trim() || `${r.first_name} ${r.last_name}`.trim(),
      status: r.status || 'cold',
      touchpoint_count: r.touchpoint_count,
      last_contacted_at: r.last_contacted_at
    }));

    res.json({ outbound_volume, reply_rate, icp_distribution, agent_breakdown, pipeline_funnel, top_prospects });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Content analytics: recent posts with metrics
app.get('/api/analytics/posts', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        pa.id, pa.channel, pa.content_type, pa.post_text,
        pa.platform_post_id, pa.published_at,
        pa.post_day_of_week, pa.post_hour,
        pa.likes, pa.comments, pa.shares, pa.reach, pa.clicks,
        pa.engagement_rate, pa.metrics_fetched_at,
        c.name AS company_name
      FROM post_analytics pa
      LEFT JOIN companies c ON pa.company_id = c.id
      ORDER BY pa.published_at DESC
      LIMIT 100
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Content performance summary by channel/type
app.get('/api/analytics/summary', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        cps.channel, cps.content_type,
        cps.post_count, cps.avg_likes, cps.avg_comments,
        cps.avg_shares, cps.avg_reach, cps.avg_engagement_rate,
        cps.best_day_of_week, cps.best_hour,
        c.name AS company_name
      FROM content_performance_summary cps
      LEFT JOIN companies c ON cps.company_id = c.id
      ORDER BY cps.avg_engagement_rate DESC
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Top posts by engagement rate
app.get('/api/analytics/top-posts', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const result = await pool.query(`
      SELECT
        pa.id, pa.channel, pa.content_type,
        LEFT(pa.post_text, 120) AS post_preview,
        pa.published_at, pa.likes, pa.comments, pa.shares,
        pa.reach, pa.engagement_rate,
        c.name AS company_name
      FROM post_analytics pa
      LEFT JOIN companies c ON pa.company_id = c.id
      WHERE pa.engagement_rate > 0
      ORDER BY pa.engagement_rate DESC
      LIMIT $1
    `, [limit]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Email engagement stats
app.get('/api/analytics/email', requireAuth, async (req, res) => {
  try {
    const [totals, weekTotals, warmUpgraded] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(CASE WHEN action_type = 'outbound'            THEN 1 END)::int AS sent_total,
          COUNT(CASE WHEN action_type = 'email_opened'        THEN 1 END)::int AS opened_total,
          COUNT(CASE WHEN action_type = 'email_clicked'       THEN 1 END)::int AS clicked_total,
          COUNT(CASE WHEN action_type = 'email_bounced'       THEN 1 END)::int AS bounced_total,
          COUNT(CASE WHEN action_type = 'email_unsubscribed'  THEN 1 END)::int AS unsub_total
        FROM touchpoints WHERE channel = 'email'
      `),
      pool.query(`
        SELECT
          COUNT(CASE WHEN action_type = 'outbound'           THEN 1 END)::int AS sent_week,
          COUNT(CASE WHEN action_type = 'email_opened'       THEN 1 END)::int AS opened_week,
          COUNT(CASE WHEN action_type = 'email_clicked'      THEN 1 END)::int AS clicked_week,
          COUNT(CASE WHEN action_type = 'email_bounced'      THEN 1 END)::int AS bounced_week
        FROM touchpoints
        WHERE channel = 'email' AND created_at > NOW() - INTERVAL '7 days'
      `),
      pool.query(`
        SELECT COUNT(*)::int AS count
        FROM prospects
        WHERE status = 'warm'
          AND updated_at > NOW() - INTERVAL '7 days'
          AND EXISTS (
            SELECT 1 FROM touchpoints t
            WHERE t.prospect_id = prospects.id AND t.action_type = 'email_clicked'
          )
      `),
    ]);

    const t  = totals.rows[0];
    const w  = weekTotals.rows[0];

    const pct = (num, den) => den > 0 ? +((num / den) * 100).toFixed(1) : 0;

    res.json({
      sent_total:     t.sent_total,
      sent_week:      w.sent_week,
      open_rate:      pct(t.opened_total, t.sent_total),
      click_rate:     pct(t.clicked_total, t.sent_total),
      bounce_rate:    pct(t.bounced_total, t.sent_total),
      unsub_rate:     pct(t.unsub_total, t.sent_total),
      open_rate_week: pct(w.opened_week, w.sent_week),
      warm_upgraded_week: warmUpgraded.rows[0].count,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Max daily brief (latest from agent_log)
app.get('/api/max-brief', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT payload, ran_at
      FROM agent_log
      WHERE agent_name = 'max' AND action = 'daily_digest'
      ORDER BY ran_at DESC
      LIMIT 1
    `);
    if (!result.rows.length) return res.json({ insights: null, ran_at: null });
    const row = result.rows[0];
    res.json({ insights: row.payload?.insights || null, ran_at: row.ran_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Agent actions (deposited by Max)
app.get('/api/actions', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, created_by, action_type, title, description, payload, status, created_at, executed_at, result
      FROM agent_actions
      WHERE status IN ('pending', 'in_progress')
      ORDER BY created_at DESC
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/actions/:id/dismiss', requireAuth, async (req, res) => {
  try {
    await pool.query(
      `UPDATE agent_actions SET status = 'dismissed', executed_at = NOW() WHERE id = $1`,
      [req.params.id]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/actions/:id/execute', requireAuth, async (req, res) => {
  try {
    await pool.query(
      `UPDATE agent_actions SET status = 'executed', executed_at = NOW(), result = $2 WHERE id = $1`,
      [req.params.id, req.body.result || 'Marked done from dashboard']
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API - Trigger agents
app.post('/api/run/:agent', requireAuth, async (req, res) => {
  const { agent } = req.params;
  const localOnly = ['ivy'];
  if (localOnly.includes(agent)) {
    return res.json({ success: false, message: `${agent} requires local execution — run from your terminal` });
  }
  const agentModules = {
    scout: './leadgen', emmett: './emmettAgent',
    max: './maxAgent', rex: './rexAgent', sketch: './sketchAgent',
    paige: './paigeAgent', faye: './facebookAgent', link: './linkedinAgent',
    sam: './samAgent', vera: './veraAgent', cal: './calAgent', ivy: './ivyAgent',
    penny: './pennyAgent', analytics: './analyticsAgent', riley: './rileyAgent',
  };
  if (!agentModules[agent]) return res.status(400).json({ error: 'Unknown agent' });
  await pool.query(
    `INSERT INTO agent_log (agent_name, action, payload, status, ran_at) VALUES ($1, $2, $3, $4, NOW())`,
    [agent, 'dashboard_trigger', JSON.stringify({ triggered_by: 'dashboard' }), 'pending']
  );
  res.json({ success: true, message: `${agent} triggered successfully` });
  try {
    delete require.cache[require.resolve(agentModules[agent])];
    require(agentModules[agent]);
  } catch (err) {
    console.error(`Agent ${agent} error:`, err.message);
  }
});

// Cron endpoint - protected by secret key
const CRON_MODULES = {
  scout:     './leadgen',
  emmett:    './emmettAgent',
  max:       './maxAgent',
  rex:       './rexAgent',
  sketch:    './sketchAgent',
  paige:     './paigeAgent',
  faye:      './facebookAgent',
  link:      './linkedinAgent',
  sam:       './samAgent',
  vera:      './veraAgent',
  cal:       './calAgent',
  penny:     './pennyAgent',
  analytics: './analyticsAgent',
  riley:     './rileyAgent',
};

function runCronAgent(agent, res) {
  if (!CRON_MODULES[agent]) return res.status(400).json({ error: `Unknown agent: ${agent}` });
  res.json({ success: true, agent });
  try {
    delete require.cache[require.resolve(CRON_MODULES[agent])];
    require(CRON_MODULES[agent]);
  } catch (err) {
    console.error(`[cron] ${agent} error:`, err.message);
  }
}

// POST /cron/:agent — triggered by external cron services with secret in body
app.post('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.body?.secret || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res);
});

// GET /cron/:agent — triggered by Railway cron (which sends GET requests)
app.get('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res);
});

// Approval dashboard
app.get('/approvals', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pulseforge — Comment Approvals</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f8f8f6; color: #1a1a18; padding: 2rem 1rem; max-width: 720px; margin: 0 auto; }
  h1 { font-size: 20px; font-weight: 500; margin-bottom: 4px; }
  .subtitle { font-size: 13px; color: #888780; margin-bottom: 1.5rem; }
  .card { background: #fff; border: 0.5px solid #e0dfd7; border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem; }
  .author { font-size: 13px; font-weight: 500; margin-bottom: 2px; }
  .author-title { font-size: 12px; color: #888780; margin-bottom: 10px; }
  .label { font-size: 11px; color: #888780; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
  .post-content { font-size: 13px; color: #5f5e5a; line-height: 1.5; background: #f8f8f6; border-radius: 8px; padding: 10px; margin-bottom: 12px; }
  .comment-text { font-size: 14px; color: #1a1a18; line-height: 1.6; margin-bottom: 14px; font-style: italic; }
  .url-link { font-size: 12px; color: #7F77DD; word-break: break-all; margin-bottom: 14px; display: block; }
  .btn-row { display: flex; gap: 8px; }
  .btn { padding: 8px 20px; border-radius: 8px; border: 0.5px solid #e0dfd7; font-size: 13px; cursor: pointer; background: #fff; }
  .btn-approve { background: #E1F5EE; color: #0F6E56; border-color: #5DCAA5; }
  .btn-approve:hover { background: #5DCAA5; color: #fff; }
  .btn-reject { background: #f8f8f6; color: #888780; }
  .btn-reject:hover { background: #e0dfd7; }
  .empty { text-align: center; padding: 3rem; color: #888780; font-size: 14px; }
  .done-badge { background: #E1F5EE; color: #0F6E56; font-size: 11px; padding: 2px 8px; border-radius: 99px; }
  .rejected-badge { background: #f1efe8; color: #888780; font-size: 11px; padding: 2px 8px; border-radius: 99px; }
</style>
</head>
<body>
<h1>Comment approvals</h1>
<p class="subtitle">Review and approve LinkedIn & Facebook comment drafts</p>
<div id="drafts">Loading...</div>
<script>
async function load() {
  const res = await fetch('/api/pending-comments');
  const data = await res.json();
  const container = document.getElementById('drafts');
  if (!data.length) { container.innerHTML = '<div class="empty">No pending drafts. Run the LinkedIn agent to generate new ones.</div>'; return; }
  container.innerHTML = data.map(d => \`
    <div class="card" id="card-\${d.id}">
      <p class="author">\${d.author_name || 'Unknown'}</p>
      <p class="author-title">\${d.author_title || ''}</p>
      \${{
        facebook:       '<span style="background:#1877F2;color:#fff;font-size:10px;padding:2px 8px;border-radius:99px;margin-bottom:10px;display:inline-block;">Facebook · Faye</span>',
        facebook_page:  '<span style="background:#1877F2;color:#fff;font-size:10px;padding:2px 8px;border-radius:99px;margin-bottom:10px;display:inline-block;">Facebook Page · Paige</span>',
        google_business:'<span style="background:#34A853;color:#fff;font-size:10px;padding:2px 8px;border-radius:99px;margin-bottom:10px;display:inline-block;">Google Business · Paige</span>',
        google_review:  '<span style="background:#f4b942;color:#1a1a18;font-size:10px;padding:2px 8px;border-radius:99px;margin-bottom:10px;display:inline-block;">Google Review · Vera</span>',
        linkedin:       '<span style="background:#0A66C2;color:#fff;font-size:10px;padding:2px 8px;border-radius:99px;margin-bottom:10px;display:inline-block;">LinkedIn · Link</span>',
      }[d.channel] || \`<span style="background:#888;color:#fff;font-size:10px;padding:2px 8px;border-radius:99px;margin-bottom:10px;display:inline-block;">\${d.channel}</span>\`}
      <p class="label">Post</p>
      <div class="post-content">\${d.post_content || ''}</div>
      <p class="label">Draft comment</p>
      <p class="comment-text">"\${d.comment}"</p>
      \${d.post_url ? \`<a class="url-link" href="\${d.post_url}" target="_blank">\${d.post_url}</a>\` : ''}
      <div class="btn-row">
        <button class="btn btn-approve" onclick="approve('\${d.id}')">Approve</button>
        <button class="btn btn-reject" onclick="reject('\${d.id}')">Reject</button>
      </div>
    </div>
  \`).join('');
}

async function approve(id) {
  await fetch('/api/approve-comment/' + id, { method: 'POST' });
  document.getElementById('card-' + id).querySelector('.btn-row').innerHTML = '<span class="done-badge">Approved — posting...</span>';
}

async function reject(id) {
  await fetch('/api/reject-comment/' + id, { method: 'POST' });
  document.getElementById('card-' + id).querySelector('.btn-row').innerHTML = '<span class="rejected-badge">Rejected</span>';
}

load();
</script>
</body>
</html>`);
});

// Get pending comments API
app.get('/api/pending-comments', async (req, res) => {
  const db = require('./dbClient');
  const comments = await db.getPendingComments();
  res.json(comments);
});

// Approve a comment
app.post('/api/approve-comment/:id', async (req, res) => {
  const db = require('./dbClient');
  const { id } = req.params;
  const comments = await db.getPendingComments();
  const comment = comments.find(c => c.id === id);
  if (!comment) return res.status(404).json({ error: 'Not found' });
  
  await db.updateCommentStatus(id, 'approved');

  // Trigger posting via local server
  const postUrl = comment.post_url;
  const commentText = comment.comment;
  const authorName = comment.author_name;

  if (!postUrl) {
    return res.json({ success: true, message: 'Approved but no URL to post to' });
  }

  try {
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    const fs = require('fs');
    puppeteer.use(StealthPlugin());

    const SESSION_FILE = './linkedin_session.json';
    const sessionData = process.env.LINKEDIN_SESSION;
    if (!sessionData) return res.status(500).json({ error: 'No LinkedIn session' });

    const cookies = JSON.parse(Buffer.from(sessionData, 'base64').toString('utf8'));

    console.log('Launching browser to post comment on:', postUrl);
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
      return res.json({ success: true, message: 'Approved but comment button not found on page' });
    }

    await commentBtn.click();
    await new Promise(r => setTimeout(r, 1500));

    const commentBox = await page.$('.comments-comment-box__text-editor') ||
                       await page.$('[contenteditable="true"]');

    if (!commentBox) {
      await browser.close();
      return res.json({ success: true, message: 'Approved but comment box not found' });
    }

    await commentBox.click();
    for (const char of commentText) {
      await commentBox.type(char, { delay: Math.floor(Math.random() * 80) + 30 });
    }

    await new Promise(r => setTimeout(r, 1000));

    const submitBtn = await page.$('.comments-comment-box__submit-button') ||
                      await page.$('button[type="submit"]');

    if (submitBtn) {
      await submitBtn.click();
      await new Promise(r => setTimeout(r, 2000));
    }

    await db.logAgentAction(
      'linkedin_agent',
      'post_comment',
      null,
      postUrl,
      { comment: commentText, authorName },
      'success'
    );

    await browser.close();
    await db.updateCommentStatus(id, 'posted');
    res.json({ success: true, message: 'Comment posted' });

  } catch (err) {
    console.error('Posting error:', err.message);
    res.json({ success: true, message: 'Approved but posting failed: ' + err.message });
  }
  });

// Reject a comment
app.post('/api/reject-comment/:id', async (req, res) => {
  const db = require('./dbClient');
  const { id } = req.params;
  await db.updateCommentStatus(id, 'rejected');
  res.json({ success: true });
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

// Sketch mockup preview
app.get('/preview/:filename', (req, res) => {
  const { filename } = req.params;
  const filepath = path.join(__dirname, 'mockups', filename);
  if (fs.existsSync(filepath)) {
    res.sendFile(filepath);
  } else {
    res.status(404).send('Mockup not found');
  }
});

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

// ── BLAND.AI WEBHOOK ──────────────────────────────────────────────────
// Receives call completion callbacks. Parses transcript for booked time,
// creates a Google Calendar event if appointment was made, notifies Telegram.
app.post('/webhooks/bland', async (req, res) => {
  res.sendStatus(200); // Acknowledge immediately

  const { call_id, status, duration, transcript, summary, metadata } = req.body || {};
  if (!call_id) return;

  const prospectId  = metadata?.prospect_id;
  const companyName = metadata?.company_name || 'Unknown';

  console.log(`[bland webhook] call_id=${call_id} status=${status} prospect=${prospectId}`);

  try {
    // Update touchpoint outcome
    if (prospectId) {
      await pool.query(`
        UPDATE touchpoints
        SET outcome = $1, payload = payload || $2::jsonb
        WHERE prospect_id = $3
          AND channel = 'call'
          AND external_ref = $4
      `, [
        status || 'completed',
        JSON.stringify({ duration, summary }),
        prospectId,
        call_id,
      ]);
    }

    // Parse transcript for booked appointment using Claude
    const fullText = typeof transcript === 'string'
      ? transcript
      : Array.isArray(transcript) ? transcript.map(t => `${t.user}: ${t.text}`).join('\n') : '';

    if (!fullText || status !== 'completed') return;

    const Anthropic = require('@anthropic-ai/sdk');
    const anthropic = new Anthropic();

    const parseRes = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 200,
      messages: [{
        role: 'user',
        content: `Read this phone call transcript and extract booking information if a discovery call was booked.

Transcript:
${fullText.slice(0, 3000)}

Respond with JSON only — no explanation:
{
  "booked": true/false,
  "agreed_day": "Monday" or null,
  "agreed_time": "2pm" or null,
  "agreed_iso": "ISO 8601 datetime in America/New_York if determinable, else null",
  "confirmed_email": "email if stated, else null",
  "prospect_name": "name if stated, else null"
}`
      }]
    });

    let parsed;
    try {
      const raw = parseRes.content[0].text.trim();
      parsed = JSON.parse(raw.replace(/^```json\n?/, '').replace(/\n?```$/, ''));
    } catch {
      console.log('[bland webhook] Could not parse Claude response');
      return;
    }

    const { createCalendarEvent, notify } = require('./calAgent');

    let calendarCreated = false;
    if (parsed.booked && parsed.agreed_iso) {
      const event = await createCalendarEvent(
        parsed.prospect_name || 'Prospect',
        companyName,
        parsed.agreed_iso
      );
      calendarCreated = !!event;
    }

    const lines = [
      parsed.booked ? `✅ Discovery call BOOKED — Cal` : `📞 Call complete — Cal`,
      ``,
      `Business: ${companyName}`,
      `Outcome: ${status || 'completed'}`,
      duration ? `Duration: ${Math.round(duration / 60)} min` : null,
    ];

    if (parsed.booked) {
      if (parsed.agreed_day || parsed.agreed_time) {
        lines.push(`Agreed time: ${[parsed.agreed_day, parsed.agreed_time].filter(Boolean).join(' ')}`);
      }
      if (parsed.confirmed_email) lines.push(`Email confirmed: ${parsed.confirmed_email}`);
      lines.push(calendarCreated ? `📅 Calendar invite created` : `⚠️ Calendar invite skipped — set GOOGLE_CALENDAR_REFRESH_TOKEN`);
    }

    await notify(lines.filter(l => l !== null).join('\n'));

    await pool.query(
      `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
       VALUES ($1, $2, $3, $4, $5, NOW())`,
      ['cal_agent', 'call_completed', prospectId,
       JSON.stringify({ call_id, booked: parsed.booked, calendar_created: calendarCreated }),
       'success']
    );

  } catch (err) {
    console.error('[bland webhook] Error processing callback:', err.message);
  }
});

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
