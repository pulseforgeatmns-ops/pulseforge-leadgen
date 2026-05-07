'use strict';

const axios = require('axios');
const fs    = require('fs');
const path  = require('path');
const pool  = require('../db');

const GBP_BASE = 'https://mybusiness.googleapis.com/v4';

// ── SHARED HELPERS ────────────────────────────────────────────────────────────

async function updateStatus(id, status) {
  await pool.query('UPDATE pending_comments SET status = $1 WHERE id = $2', [status, id]);
}

async function logResult(channel, action, itemId, status, details) {
  try {
    await pool.query(
      `INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
       VALUES ($1, $2, $3, $4, NOW())`,
      [`${channel}_publisher`, action, JSON.stringify({ id: itemId, ...details }), status]
    );
  } catch (_) {}
}

function parseComment(raw) {
  if (!raw || !raw.startsWith('POST: ')) return { main: raw || '', firstUrl: null };
  const parts = raw.split('\nFIRST_COMMENT: ');
  return {
    main:     parts[0].replace(/^POST: /, '').trim(),
    firstUrl: parts[1]?.trim() || null,
  };
}

// ── 1. GOOGLE BUSINESS PROFILE ────────────────────────────────────────────────

async function getGoogleAccessToken() {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id:     process.env.GOOGLE_CLIENT_ID,
    client_secret: process.env.GOOGLE_CLIENT_SECRET,
    refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
    grant_type:    'refresh_token',
  });
  return res.data.access_token;
}

async function resolveGBPLocation(token) {
  if (process.env.GBP_ACCOUNT_ID && process.env.GBP_LOCATION_ID) {
    return `accounts/${process.env.GBP_ACCOUNT_ID}/locations/${process.env.GBP_LOCATION_ID}`;
  }
  const acctRes = await axios.get(`${GBP_BASE}/accounts`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const account = acctRes.data.accounts?.[0];
  if (!account) throw new Error('No GBP accounts accessible');
  const locsRes = await axios.get(`${GBP_BASE}/${account.name}/locations`, {
    headers: { Authorization: `Bearer ${token}` },
    params: { readMask: 'name,title' },
  });
  const loc = locsRes.data.locations?.[0];
  if (!loc) throw new Error('No GBP locations found');
  return loc.name;
}

async function publishToGoogleBusiness(item) {
  const needed = ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET', 'GOOGLE_REFRESH_TOKEN'];
  if (needed.some(k => !process.env[k])) {
    console.warn('[GBP Publisher] Missing Google OAuth credentials — skipping');
    return;
  }
  try {
    const token    = await getGoogleAccessToken();
    const location = await resolveGBPLocation(token);
    const text     = (item.comment || '').trim();

    await axios.post(
      `${GBP_BASE}/${location}/localPosts`,
      { languageCode: 'en-US', summary: text, topicType: 'STANDARD' },
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } }
    );

    await updateStatus(item.id, 'posted');
    await logResult('google_business', 'publish_post', item.id, 'success', { location, chars: text.length });
    console.log(`[GBP Publisher] Posted to ${location}`);
  } catch (err) {
    console.error('[GBP Publisher] Failed:', err.response?.data || err.message);
    await logResult('google_business', 'publish_post', item.id, 'error', { error: err.message });
  }
}

// ── 2. FACEBOOK PAGE (GRAPH API) ──────────────────────────────────────────────

async function publishToFacebookPage(item) {
  const pageId    = process.env.FACEBOOK_PAGE_ID;
  const pageToken = process.env.FACEBOOK_PAGE_ACCESS_TOKEN;
  if (!pageId || !pageToken) {
    console.warn('[FB Page Publisher] FACEBOOK_PAGE_ID or FACEBOOK_PAGE_ACCESS_TOKEN not set — skipping');
    return;
  }
  try {
    const text = (item.comment || '').trim();
    await axios.post(`https://graph.facebook.com/${pageId}/feed`, null, {
      params: { message: text, access_token: pageToken },
    });
    await updateStatus(item.id, 'posted');
    await logResult('facebook_page', 'publish_post', item.id, 'success', { pageId, chars: text.length });
    console.log(`[FB Page Publisher] Posted to page ${pageId}`);
  } catch (err) {
    console.error('[FB Page Publisher] Failed:', err.response?.data || err.message);
    await logResult('facebook_page', 'publish_post', item.id, 'error', { error: err.message });
  }
}

// ── 3. FAYE (FACEBOOK COMMENT VIA PUPPETEER) ─────────────────────────────────

function loadFacebookCookies() {
  if (process.env.FACEBOOK_SESSION) {
    return JSON.parse(Buffer.from(process.env.FACEBOOK_SESSION, 'base64').toString('utf8'));
  }
  const sessionFile = path.join(__dirname, '..', 'facebook_session.json');
  if (fs.existsSync(sessionFile)) {
    return JSON.parse(fs.readFileSync(sessionFile, 'utf8'));
  }
  return null;
}

async function publishFayeComment(item) {
  if (!item.post_url) {
    console.warn('[Faye Publisher] No post_url on item — skipping');
    return;
  }
  const cookies = loadFacebookCookies();
  if (!cookies) {
    console.warn('[Faye Publisher] No Facebook session found (FACEBOOK_SESSION env or facebook_session.json) — skipping');
    await logResult('facebook', 'post_comment', item.id, 'error', { error: 'no_session' });
    return;
  }

  let browser;
  try {
    const puppeteer     = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

    browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    await page.setCookie(...cookies);

    await page.goto(item.post_url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));

    if (page.url().includes('login')) {
      console.warn('[Faye Publisher] Facebook session expired');
      await updateStatus(item.id, 'session_expired');
      await logResult('facebook', 'post_comment', item.id, 'error', { error: 'session_expired' });
      return;
    }

    // Click the comment trigger
    const trigger = await page.$('[data-testid="UFI2CommentLink/root"]') ||
                    await page.$('[aria-label="Leave a comment"]') ||
                    await page.$('[aria-label="Comment"]');
    if (trigger) {
      await trigger.click();
      await new Promise(r => setTimeout(r, 1500 + Math.random() * 1000));
    }

    // Find the editable comment box
    const commentBox = await page.$('[aria-label="Write a comment…"]') ||
                       await page.$('[contenteditable="true"][role="textbox"]');
    if (!commentBox) {
      console.warn('[Faye Publisher] Comment box not found');
      await updateStatus(item.id, 'error');
      await logResult('facebook', 'post_comment', item.id, 'error', { error: 'comment_box_not_found', url: item.post_url });
      return;
    }

    await commentBox.click();
    await new Promise(r => setTimeout(r, 400));
    for (const char of (item.comment || '')) {
      await commentBox.type(char, { delay: Math.floor(Math.random() * 60) + 20 });
    }
    await new Promise(r => setTimeout(r, 800 + Math.random() * 700));
    await page.keyboard.press('Enter');
    await new Promise(r => setTimeout(r, 2500));

    await updateStatus(item.id, 'posted');
    await logResult('facebook', 'post_comment', item.id, 'success', { url: item.post_url });
    console.log(`[Faye Publisher] Comment posted on ${item.post_url}`);
  } catch (err) {
    console.error('[Faye Publisher] Error:', err.message);
    await logResult('facebook', 'post_comment', item.id, 'error', { error: err.message });
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

// ── 4. LINKEDIN PAGE (BUFFER API) ─────────────────────────────────────────────

async function publishToLinkedInPage(item) {
  const token     = process.env.BUFFER_ACCESS_TOKEN;
  const channelId = process.env.BUFFER_CHANNEL_ID;
  if (!token || !channelId) {
    console.warn('[LinkedIn Page Publisher] BUFFER_ACCESS_TOKEN or BUFFER_CHANNEL_ID not set — skipping');
    return;
  }
  const { main } = parseComment(item.comment || '');
  try {
    await axios.post('https://api.bufferapp.com/1/updates/create.json', null, {
      params: {
        access_token:    token,
        'profile_ids[]': channelId,
        text:            main,
      },
    });
    await updateStatus(item.id, 'posted');
    await logResult('linkedin_page', 'publish_post', item.id, 'success', { channelId, chars: main.length });
    console.log(`[LinkedIn Page Publisher] Posted via Buffer to channel ${channelId}`);
  } catch (err) {
    console.error('[LinkedIn Page Publisher] Failed:', err.response?.data || err.message);
    await logResult('linkedin_page', 'publish_post', item.id, 'error', { error: err.message });
  }
}

// ── 5. LINK (LINKEDIN COMMENT VIA PUPPETEER) ──────────────────────────────────

function loadLinkedInCookies() {
  if (process.env.LINKEDIN_SESSION) {
    return JSON.parse(Buffer.from(process.env.LINKEDIN_SESSION, 'base64').toString('utf8'));
  }
  const sessionFile = path.join(__dirname, '..', 'linkedin_session.json');
  if (fs.existsSync(sessionFile)) {
    return JSON.parse(fs.readFileSync(sessionFile, 'utf8'));
  }
  return null;
}

async function publishLinkComment(item) {
  if (!item.post_url) {
    console.warn('[Link Publisher] No post_url on item — skipping');
    return;
  }
  const cookies = loadLinkedInCookies();
  if (!cookies) {
    console.warn('[Link Publisher] No LinkedIn session found (LINKEDIN_SESSION env or linkedin_session.json) — skipping');
    await logResult('linkedin', 'post_comment', item.id, 'error', { error: 'no_session' });
    return;
  }

  const { main: mainComment, firstUrl: firstCommentUrl } = parseComment(item.comment || '');

  let browser;
  try {
    const puppeteer     = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

    browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    await page.setCookie(...cookies);

    await page.goto(item.post_url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));

    if (page.url().includes('/login') || page.url().includes('/checkpoint')) {
      console.warn('[Link Publisher] LinkedIn session expired');
      await updateStatus(item.id, 'session_expired');
      await logResult('linkedin', 'post_comment', item.id, 'error', { error: 'session_expired' });
      return;
    }

    // Click comment button to open the box
    const commentBtn = await page.$('.comment-button') ||
                       await page.$('[aria-label="Comment"]') ||
                       await page.$('.comments-comment-box__text-editor');
    if (commentBtn) {
      await commentBtn.click();
      await new Promise(r => setTimeout(r, 1500 + Math.random() * 1000));
    }

    const commentBox = await page.$('.comments-comment-box__text-editor') ||
                       await page.$('[contenteditable="true"]');
    if (!commentBox) {
      console.warn('[Link Publisher] Comment box not found');
      await updateStatus(item.id, 'error');
      await logResult('linkedin', 'post_comment', item.id, 'error', { error: 'comment_box_not_found', url: item.post_url });
      return;
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

    // Post first reply URL if delimiter was present
    if (firstCommentUrl) {
      const replyPosted = await page.evaluate((commentStart) => {
        const items = document.querySelectorAll('.comments-comment-item');
        for (const el of items) {
          const textEl = el.querySelector(
            '.comments-comment-item__main-content, ' +
            '.comments-comment-texteditor, ' +
            '.feed-shared-update-v2__description'
          );
          if (textEl?.innerText?.includes(commentStart)) {
            const replyBtn = el.querySelector(
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
            console.log(`[Link Publisher] First reply posted: ${firstCommentUrl}`);
          }
        }
      } else {
        console.warn('[Link Publisher] Could not find posted comment to reply to — skipping first reply');
      }
    }

    await updateStatus(item.id, 'posted');
    await logResult('linkedin', 'post_comment', item.id, 'success', { url: item.post_url, firstCommentUrl });
    console.log(`[Link Publisher] Comment posted on ${item.post_url}`);
  } catch (err) {
    console.error('[Link Publisher] Error:', err.message);
    await logResult('linkedin', 'post_comment', item.id, 'error', { error: err.message });
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

module.exports = {
  publishToGoogleBusiness,
  publishToFacebookPage,
  publishFayeComment,
  publishToLinkedInPage,
  publishLinkComment,
};
