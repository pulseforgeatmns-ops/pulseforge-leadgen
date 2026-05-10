const express = require('express');
const router = express.Router();

router.get('/approvals', (req, res) => {
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

router.get('/api/pending-comments', async (req, res) => {
  const db = require('../dbClient');
  const comments = await db.getPendingComments();
  res.json(comments);
});

router.post('/api/approve-comment/:id', async (req, res) => {
  const db = require('../dbClient');
  const { id } = req.params;
  const comments = await db.getPendingComments();
  const comment = comments.find(c => c.id === id);
  if (!comment) return res.status(404).json({ error: 'Not found' });

  await db.updateCommentStatus(id, 'approved');

  const postUrl = comment.post_url;
  const commentText = comment.comment;
  const authorName = comment.author_name;

  if (!postUrl) {
    return res.json({ success: true, message: 'Approved but no URL to post to' });
  }

  try {
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

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

router.post('/api/reject-comment/:id', async (req, res) => {
  const db = require('../dbClient');
  const { id } = req.params;
  await db.updateCommentStatus(id, 'rejected');
  res.json({ success: true });
});

module.exports = router;
