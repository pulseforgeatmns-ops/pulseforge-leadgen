const axios = require('axios');
const pool  = require('../db');

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_REPO  = process.env.GITHUB_REPO || 'pulseforgeatmns-ops/freelance-portfolio';
const API_BASE     = `https://api.github.com/repos/${GITHUB_REPO}/contents`;

// ── MARKDOWN ────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str || '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function inline(text) {
  return escHtml(text)
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`(.+?)`/g, '<code>$1</code>');
}

function mdToHtml(md) {
  const lines = md.split('\n');
  const parts = [];
  let listItems = [];

  function flushList() {
    if (!listItems.length) return;
    parts.push('<ul>\n' + listItems.map(i => `  <li>${i}</li>`).join('\n') + '\n</ul>');
    listItems = [];
  }

  for (const raw of lines) {
    const line = raw.trim();
    if (!line)                    { flushList(); continue; }
    if (line.startsWith('### ')) { flushList(); parts.push(`<h3>${inline(line.slice(4))}</h3>`); }
    else if (line.startsWith('## ')) { flushList(); parts.push(`<h2>${inline(line.slice(3))}</h2>`); }
    else if (line.startsWith('# '))  { flushList(); parts.push(`<h1>${inline(line.slice(2))}</h1>`); }
    else if (/^[-*] /.test(line))   { listItems.push(inline(line.slice(2))); }
    else                            { flushList(); parts.push(`<p>${inline(line)}</p>`); }
  }
  flushList();
  return parts.join('\n');
}

function extractTitle(md) {
  const m = md.match(/^#\s+(.+)$/m);
  return m ? m[1].trim() : 'Untitled';
}

function extractExcerpt(md, len = 200) {
  for (const line of md.split('\n')) {
    const t = line.trim();
    if (!t || /^[#\-*]/.test(t)) continue;
    const clean = t.replace(/\*\*/g, '').replace(/\*/g, '');
    return clean.length > len ? clean.slice(0, len) + '…' : clean;
  }
  return '';
}

function slugify(title) {
  return title.toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 60);
}

// ── GITHUB API ──────────────────────────────────────────────────────
function ghHeaders() {
  return {
    Authorization:  `token ${GITHUB_TOKEN}`,
    Accept:         'application/vnd.github.v3+json',
    'User-Agent':   'pulseforge-blog-publisher',
  };
}

async function ghGet(path) {
  try {
    const res = await axios.get(`${API_BASE}/${path}`, { headers: ghHeaders() });
    return res.data;
  } catch (err) {
    if (err.response?.status === 404) return null;
    throw err;
  }
}

async function ghPut(path, content, message, sha) {
  const body = { message, content: Buffer.from(content).toString('base64') };
  if (sha) body.sha = sha;
  await axios.put(`${API_BASE}/${path}`, body, { headers: ghHeaders() });
}

// ── HTML TEMPLATES ──────────────────────────────────────────────────
function fmtDate(dateStr) {
  return new Date(dateStr + 'T12:00:00').toLocaleDateString('en-US', {
    year: 'numeric', month: 'long', day: 'numeric',
  });
}

function buildPostHtml(title, dateStr, category, bodyHtml, excerpt) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${escHtml(title)} | Pulseforge</title>
  <meta name="description" content="${escHtml(excerpt.slice(0, 160))}">
  <link href="https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700;900&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Lato', sans-serif; background: #0d0d1a; color: #e0e0f0; line-height: 1.7; }
    a { color: #9b6dff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    nav { background: #0d0d1a; border-bottom: 1px solid #1e1e3a; padding: 1rem 2rem; display: flex; align-items: center; gap: 2rem; }
    .logo { font-size: 1.25rem; font-weight: 900; color: #fff; letter-spacing: -0.02em; }
    .logo span { color: #6030b1; }
    .nav-link { font-size: 0.875rem; color: #9b6dff; }
    .wrap { max-width: 720px; margin: 0 auto; padding: 3rem 1.5rem 5rem; }
    .post-meta { display: flex; align-items: center; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
    .badge { background: #6030b1; color: #fff; font-size: 0.7rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; padding: 0.3rem 0.75rem; border-radius: 2rem; }
    .post-date { color: #7070a0; font-size: 0.875rem; }
    .wrap > h1 { font-size: clamp(1.75rem, 4vw, 2.5rem); font-weight: 900; color: #fff; line-height: 1.2; margin-bottom: 2rem; }
    .body h1 { display: none; }
    .body h2 { font-size: 1.35rem; font-weight: 700; color: #fff; margin: 2rem 0 0.75rem; }
    .body h3 { font-size: 1.1rem; font-weight: 700; color: #c0b0ff; margin: 1.5rem 0 0.5rem; }
    .body p { margin-bottom: 1.25rem; color: #c8c8e0; }
    .body ul { margin: 0.5rem 0 1.25rem 1.5rem; }
    .body li { margin-bottom: 0.4rem; color: #c8c8e0; }
    .body strong { color: #e0e0f0; }
    .body code { background: #1a1a2e; color: #9b6dff; padding: 0.15em 0.4em; border-radius: 4px; font-size: 0.9em; }
    .cta { margin-top: 3.5rem; background: linear-gradient(135deg, #1a0d3a, #2a1060); border: 1px solid #6030b1; border-radius: 12px; padding: 2rem; text-align: center; }
    .cta h3 { color: #fff; font-size: 1.2rem; font-weight: 700; margin-bottom: 0.5rem; }
    .cta p { color: #a090c0; margin-bottom: 1.25rem; font-size: 0.95rem; }
    .cta a { display: inline-block; background: #6030b1; color: #fff; font-weight: 700; padding: 0.8rem 2rem; border-radius: 8px; font-size: 0.95rem; }
    .cta a:hover { background: #7840d0; text-decoration: none; }
    footer { border-top: 1px solid #1e1e3a; padding: 2rem; text-align: center; color: #50507a; font-size: 0.8rem; }
    footer a { color: #7060a0; }
  </style>
</head>
<body>
  <nav>
    <span class="logo">Pulse<span>forge</span></span>
    <a class="nav-link" href="https://gopulseforge.com">← gopulseforge.com</a>
    <a class="nav-link" href="/blog/index.html">All posts</a>
  </nav>
  <div class="wrap">
    <div class="post-meta">
      <span class="badge">${escHtml(category)}</span>
      <span class="post-date">${fmtDate(dateStr)}</span>
    </div>
    <h1>${escHtml(title)}</h1>
    <div class="body">
      ${bodyHtml}
    </div>
    <div class="cta">
      <h3>Want this for your business?</h3>
      <p>Pulseforge builds automated outreach and content systems for local businesses in New Hampshire. No fluff — just more customers.</p>
      <a href="https://gopulseforge.com/#contact">Book a free call</a>
    </div>
  </div>
  <footer>
    &copy; ${new Date().getFullYear()} <a href="https://gopulseforge.com">Pulseforge</a> &mdash; Automated growth for local business.
  </footer>
</body>
</html>`;
}

function buildIndexHtml(posts) {
  const cards = posts.map(p => `    <article class="card">
      <div class="card-meta">
        <span class="badge">${escHtml(p.category)}</span>
        <span class="post-date">${fmtDate(p.dateStr)}</span>
      </div>
      <h2><a href="${escHtml(p.path)}">${escHtml(p.title)}</a></h2>
      <p class="excerpt">${escHtml(p.excerpt)}</p>
      <a class="more" href="${escHtml(p.path)}">Read more →</a>
    </article>`).join('\n');

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Blog | Pulseforge</title>
  <meta name="description" content="Marketing insights and growth tips for local businesses in New Hampshire.">
  <link href="https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700;900&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Lato', sans-serif; background: #0d0d1a; color: #e0e0f0; line-height: 1.7; }
    a { color: #9b6dff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    nav { background: #0d0d1a; border-bottom: 1px solid #1e1e3a; padding: 1rem 2rem; display: flex; align-items: center; gap: 2rem; }
    .logo { font-size: 1.25rem; font-weight: 900; color: #fff; letter-spacing: -0.02em; }
    .logo span { color: #6030b1; }
    .nav-link { font-size: 0.875rem; color: #9b6dff; }
    .wrap { max-width: 760px; margin: 0 auto; padding: 3rem 1.5rem 5rem; }
    .wrap > h1 { font-size: clamp(1.75rem, 4vw, 2.5rem); font-weight: 900; color: #fff; margin-bottom: 0.4rem; }
    .sub { color: #7070a0; margin-bottom: 2.5rem; }
    .card { border: 1px solid #1e1e3a; border-radius: 12px; padding: 1.75rem; margin-bottom: 1.5rem; transition: border-color 0.2s; }
    .card:hover { border-color: #6030b1; }
    .card-meta { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.75rem; flex-wrap: wrap; }
    .badge { background: #6030b1; color: #fff; font-size: 0.7rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; padding: 0.3rem 0.75rem; border-radius: 2rem; }
    .post-date { color: #7070a0; font-size: 0.875rem; }
    .card h2 { font-size: 1.2rem; font-weight: 700; margin-bottom: 0.6rem; line-height: 1.3; }
    .card h2 a { color: #fff; }
    .card h2 a:hover { color: #9b6dff; text-decoration: none; }
    .excerpt { color: #9090b0; font-size: 0.95rem; margin-bottom: 1rem; }
    .more { color: #9b6dff; font-size: 0.875rem; font-weight: 700; }
    .empty { text-align: center; color: #50507a; padding: 4rem; }
    footer { border-top: 1px solid #1e1e3a; padding: 2rem; text-align: center; color: #50507a; font-size: 0.8rem; }
    footer a { color: #7060a0; }
  </style>
</head>
<body>
  <nav>
    <span class="logo">Pulse<span>forge</span></span>
    <a class="nav-link" href="https://gopulseforge.com">← gopulseforge.com</a>
  </nav>
  <div class="wrap">
    <h1>Pulseforge Blog</h1>
    <p class="sub">Marketing insights and growth tips for local businesses in New Hampshire.</p>
${posts.length ? cards : '    <p class="empty">No posts yet — check back soon.</p>'}
  </div>
  <footer>
    &copy; ${new Date().getFullYear()} <a href="https://gopulseforge.com">Pulseforge</a> &mdash; Automated growth for local business.
  </footer>
</body>
</html>`;
}

// ── PUBLISH ─────────────────────────────────────────────────────────
async function publishBlogPost(item) {
  if (!GITHUB_TOKEN || !GITHUB_REPO) {
    console.warn('[BlogPublisher] GITHUB_TOKEN or GITHUB_REPO not set — skipping');
    return;
  }

  const md      = item.comment || '';
  const title   = extractTitle(md);
  const excerpt = extractExcerpt(md);
  const slug    = slugify(title);
  const dateStr = new Date(item.created_at || Date.now()).toISOString().split('T')[0];
  const postPath = `blog/${dateStr}-${slug}.html`;

  const rawCat  = (item.post_content || '').split('·').pop().trim();
  const category = rawCat ? rawCat.charAt(0).toUpperCase() + rawCat.slice(1) : 'Blog';

  // Strip the H1 from the body — it's rendered as the page <h1> separately
  const mdBody   = md.replace(/^#\s+.+$/m, '').trim();
  const bodyHtml = mdToHtml(mdBody);
  const postHtml = buildPostHtml(title, dateStr, category, bodyHtml, excerpt);

  // Commit post file (pass sha if it already exists so GitHub accepts the update)
  const existing = await ghGet(postPath);
  await ghPut(postPath, postHtml, `Add blog post: ${title}`, existing?.sha);
  console.log(`[BlogPublisher] Published: ${postPath}`);

  // Rebuild index from all approved blog posts in the DB
  const { rows } = await pool.query(`
    SELECT comment, post_content, created_at
    FROM pending_comments
    WHERE channel = 'blog' AND status = 'approved'
    ORDER BY created_at DESC
  `);

  const posts = rows.map(r => {
    const t   = extractTitle(r.comment || '');
    const ds  = new Date(r.created_at).toISOString().split('T')[0];
    const cat = (r.post_content || '').split('·').pop().trim();
    return {
      title:    t,
      excerpt:  extractExcerpt(r.comment || ''),
      dateStr:  ds,
      category: cat ? cat.charAt(0).toUpperCase() + cat.slice(1) : 'Blog',
      path:     `blog/${ds}-${slugify(t)}.html`,
    };
  });

  const indexHtml  = buildIndexHtml(posts);
  const currentIdx = await ghGet('blog/index.html');
  await ghPut('blog/index.html', indexHtml, `Update blog index (${posts.length} post${posts.length !== 1 ? 's' : ''})`, currentIdx?.sha);
  console.log(`[BlogPublisher] Index updated — ${posts.length} post${posts.length !== 1 ? 's' : ''}`);
}

module.exports = { publishBlogPost };
