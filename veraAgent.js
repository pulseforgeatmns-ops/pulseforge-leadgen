require('dotenv').config();
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const pool = require('./db');

const AGENT_NAME = 'vera';
const GBP_BASE   = 'https://mybusiness.googleapis.com/v4';
const DASHBOARD  = process.env.DASHBOARD_URL || 'https://openclaw-main-production-945e.up.railway.app';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const STAR_NUM = { ONE: 1, TWO: 2, THREE: 3, FOUR: 4, FIVE: 5 };

// ── CREDENTIALS ────────────────────────────────────────────────────────
function credentialsConfigured() {
  return !!(
    process.env.GOOGLE_CLIENT_ID &&
    process.env.GOOGLE_CLIENT_SECRET &&
    process.env.GOOGLE_REFRESH_TOKEN
  );
}

// ── OAUTH TOKEN REFRESH ────────────────────────────────────────────────
async function getAccessToken() {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id:     process.env.GOOGLE_CLIENT_ID,
    client_secret: process.env.GOOGLE_CLIENT_SECRET,
    refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
    grant_type:    'refresh_token',
  });
  return res.data.access_token;
}

// ── SCHEMA MIGRATION ───────────────────────────────────────────────────
async function ensureSchema() {
  await pool.query(
    `ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_review_check TIMESTAMPTZ`
  );
}

// ── GBP API HELPERS ────────────────────────────────────────────────────
async function gbpGet(token, path, params = {}) {
  const res = await axios.get(`${GBP_BASE}/${path}`, {
    headers: { Authorization: `Bearer ${token}` },
    params,
  });
  return res.data;
}

async function fetchAccounts(token) {
  const data = await gbpGet(token, 'accounts');
  return data.accounts || [];
}

async function fetchLocations(token, accountName) {
  const data = await gbpGet(token, `${accountName}/locations`, {
    readMask: 'name,title,websiteUri',
  });
  return data.locations || [];
}

async function fetchReviews(token, locationName) {
  const data = await gbpGet(token, `${locationName}/reviews`, {
    orderBy: 'updateTime desc',
    pageSize: 20,
  });
  return data.reviews || [];
}

// ── COMPANY MATCHING ───────────────────────────────────────────────────
async function findCompany(locationTitle) {
  const res = await pool.query(
    `SELECT id, name, industry, location, last_review_check
     FROM companies
     WHERE LOWER(TRIM(name)) = LOWER(TRIM($1))
     LIMIT 1`,
    [locationTitle]
  );
  return res.rows[0] || null;
}

// ── RESPONSE DRAFTING ──────────────────────────────────────────────────
async function draftResponse(review, companyName, industry, location) {
  const stars      = STAR_NUM[review.starRating] || 3;
  const reviewText = review.comment || '(no written review)';
  const reviewer   = review.reviewer?.displayName || 'a customer';

  let toneGuide;
  if (stars >= 4) {
    toneGuide = 'warm, grateful, and personal. If the review mentions something specific, reference it. End by inviting them back.';
  } else if (stars === 3) {
    toneGuide = 'appreciative but briefly addressing any concern mentioned. Keep it professional and forward-looking.';
  } else {
    toneGuide = 'calm and empathetic — never defensive. Acknowledge their experience, apologize sincerely, and offer to make it right offline. Include a contact placeholder like [phone or email].';
  }

  const msg = await anthropic.messages.create({
    model:      'claude-sonnet-4-6',
    max_tokens: 220,
    system: `You are the owner of ${companyName}, a ${industry || 'local business'}${location ? ' in ' + location : ''}. Write a genuine Google review response. Sound like the business owner wrote it — not a marketing agency. 2–4 sentences max. No exclamation mark spam. No hollow openers like "Thank you for your feedback!"`,
    messages: [{
      role:    'user',
      content: `${stars}-star review from ${reviewer}:\n"${reviewText}"\n\nTone: ${toneGuide}\n\nWrite the response:`,
    }],
  });

  return msg.content[0].text.trim();
}

// ── TELEGRAM NOTIFICATION ──────────────────────────────────────────────
async function notifyTelegram(companyName, review, draft) {
  const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
  const CHAT_ID   = process.env.TELEGRAM_CHAT_ID;
  if (!BOT_TOKEN || !CHAT_ID) return;

  const stars      = STAR_NUM[review.starRating] || 3;
  const starEmoji  = '⭐'.repeat(stars);
  const urgent     = stars <= 2;
  const reviewer   = review.reviewer?.displayName || 'Anonymous';
  const snippet    = (review.comment || '').slice(0, 100);

  const text = [
    `${urgent ? '🚨 URGENT' : '⭐ New'} Google review — ${companyName}`,
    '',
    `${starEmoji} ${reviewer}:`,
    snippet || '(no written comment)',
    '',
    'Draft response:',
    draft.slice(0, 200),
    '',
    `→ Approve: ${DASHBOARD}/approvals`,
  ].join('\n');

  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      chat_id: CHAT_ID,
      text,
    });
  } catch (err) {
    console.error('[Vera] Telegram failed:', err.response?.data?.description || err.message);
  }
}

// ── AGENT LOG ──────────────────────────────────────────────────────────
async function logRun(status, details) {
  try {
    await pool.query(
      `INSERT INTO agent_log (agent_name, action, payload, status) VALUES ($1, $2, $3, $4)`,
      [AGENT_NAME, 'review_check', JSON.stringify(details), status]
    );
  } catch (_) {}
}

// ── MAIN ───────────────────────────────────────────────────────────────
async function main() {
  console.log('[Vera] Starting Google Business review check...');

  if (!credentialsConfigured()) {
    console.warn('[Vera] GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN not set — skipping');
    return;
  }

  await ensureSchema();

  let token;
  try {
    token = await getAccessToken();
  } catch (err) {
    console.error('[Vera] OAuth token refresh failed:', err.response?.data || err.message);
    await logRun('error', { error: 'token_refresh_failed' });
    return;
  }

  let accounts;
  try {
    accounts = await fetchAccounts(token);
  } catch (err) {
    console.error('[Vera] Failed to fetch GBP accounts:', err.response?.data || err.message);
    await logRun('error', { error: 'fetch_accounts_failed' });
    return;
  }

  if (!accounts.length) {
    console.warn('[Vera] No GBP accounts accessible — token may be missing business.manage scope');
    await logRun('success', { accounts: 0, new_reviews: 0, drafted: 0 });
    return;
  }

  let totalNew = 0, totalDrafted = 0;

  for (const account of accounts) {
    let locations;
    try {
      locations = await fetchLocations(token, account.name);
    } catch (err) {
      console.warn(`[Vera] Failed to fetch locations for ${account.name}:`, err.message);
      continue;
    }

    for (const location of locations) {
      const title      = location.title || location.name;
      const company    = await findCompany(title);
      // Default lookback: 7 days if Vera has never run for this location
      const lastCheck  = company?.last_review_check
        ? new Date(company.last_review_check)
        : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

      let reviews;
      try {
        reviews = await fetchReviews(token, location.name);
      } catch (err) {
        console.warn(`[Vera] Failed to fetch reviews for ${title}:`, err.message);
        continue;
      }

      const newReviews = reviews.filter(r => {
        const t = new Date(r.updateTime || r.createTime);
        return t > lastCheck;
      });

      console.log(`[Vera] ${title}: ${newReviews.length} new review(s)`);
      totalNew += newReviews.length;

      for (const review of newReviews) {
        const stars      = STAR_NUM[review.starRating] || 3;
        const starEmoji  = '⭐'.repeat(stars);
        const reviewer   = review.reviewer?.displayName || 'Anonymous';
        const snippet    = (review.comment || '').slice(0, 100);
        const postContent = `${starEmoji} ${reviewer}: ${snippet}`;

        let draft;
        try {
          draft = await draftResponse(review, title, company?.industry, company?.location);
        } catch (err) {
          console.error(`[Vera] Claude draft failed (${reviewer}):`, err.message);
          continue;
        }

        try {
          await pool.query(
            `INSERT INTO pending_comments
               (author_name, author_title, post_content, comment, post_url, channel)
             VALUES ($1, $2, $3, $4, $5, 'google_review')`,
            [
              title,
              `${stars} star${stars !== 1 ? 's' : ''}`,
              postContent,
              draft,
              review.name || '',
            ]
          );
        } catch (err) {
          console.error('[Vera] Failed to save pending comment:', err.message);
          continue;
        }

        await notifyTelegram(title, review, draft);
        totalDrafted++;
      }

      // Stamp last_review_check on matched company
      if (company && newReviews.length > 0) {
        await pool.query(
          `UPDATE companies SET last_review_check = NOW() WHERE id = $1`,
          [company.id]
        );
      }
    }
  }

  await logRun('success', { accounts: accounts.length, new_reviews: totalNew, drafted: totalDrafted });
  console.log(`[Vera] Done — ${totalNew} new reviews, ${totalDrafted} drafts saved.`);
}

main().catch(async err => {
  console.error('[Vera] Fatal:', err.message);
  await logRun('error', { error: err.message });
  process.exit(1);
});
