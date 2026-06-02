require('dotenv').config();
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const pool = require('./db');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

const AGENT_NAME = 'vera';
const GBP_ACCOUNT_MGMT_BASE = 'https://mybusinessaccountmanagement.googleapis.com/v1';
const GBP_BUSINESS_INFO_BASE = 'https://mybusinessbusinessinformation.googleapis.com/v1';
const GBP_REVIEWS_BASE = 'https://mybusiness.googleapis.com/v4';
const DASHBOARD  = process.env.DASHBOARD_URL || 'https://openclaw-main-production-945e.up.railway.app';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;

const STAR_NUM = { ONE: 1, TWO: 2, THREE: 3, FOUR: 4, FIVE: 5 };
const GBP_THROTTLE_MS = 6500;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
let lastThrottledGbpAt = 0;

async function waitForGbpThrottle() {
  if (!lastThrottledGbpAt) return;
  const elapsed = Date.now() - lastThrottledGbpAt;
  if (elapsed < GBP_THROTTLE_MS) {
    await sleep(GBP_THROTTLE_MS - elapsed);
  }
}

function markGbpThrottle() {
  lastThrottledGbpAt = Date.now();
}

async function gbpGet(token, baseUrl, path, params = {}) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const res = await axios.get(`${baseUrl}/${path}`, {
        headers: { Authorization: `Bearer ${token}` },
        params,
      });
      return res.data;
    } catch (err) {
      if (err.response?.status === 429) {
        if (attempt === 0) {
          console.warn(`[Vera] GBP rate limit (429) on ${path} — retrying in 60s`);
          await sleep(60000);
          continue;
        }
        console.warn(`[Vera] GBP rate limit (429) on ${path} after retry — skipping`);
      }
      throw err;
    }
  }
}

async function fetchAccounts(token) {
  const data = await gbpGet(token, GBP_ACCOUNT_MGMT_BASE, 'accounts');
  return data.accounts || [];
}

async function fetchLocations(token, accountName) {
  await waitForGbpThrottle();
  try {
    const data = await gbpGet(token, GBP_BUSINESS_INFO_BASE, `${accountName}/locations`, {
      readMask: 'name,title,websiteUri',
    });
    return data.locations || [];
  } finally {
    markGbpThrottle();
  }
}

async function fetchReviews(token, locationName) {
  await waitForGbpThrottle();
  try {
    const data = await gbpGet(token, GBP_REVIEWS_BASE, `${locationName}/reviews`, {
      orderBy: 'updateTime desc',
      pageSize: 20,
    });
    return data.reviews || [];
  } finally {
    markGbpThrottle();
  }
}

// ── COMPANY MATCHING ───────────────────────────────────────────────────
async function findCompany(locationTitle) {
  const res = await pool.query(
    `SELECT id, name, industry, location, last_review_check
     FROM companies
     WHERE LOWER(TRIM(name)) = LOWER(TRIM($1))
       AND client_id = $2
     LIMIT 1`,
    [locationTitle, CLIENT_ID]
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
    system: CLIENT_ID === 2
      ? `This is Mountain State Home Innovations — a locally owned WV contractor run by Brad and Dustin. They do all work themselves. Communication and customer satisfaction are their core values. Always thank the reviewer for the specific service they mention. Never be generic. Sign off as Brad & Dustin, MSHI.`
      : `You are the owner of ${companyName}, a ${industry || 'local business'}${location ? ' in ' + location : ''}. Write a genuine Google review response. Sound like the business owner wrote it — not a marketing agency. 2–4 sentences max. No exclamation mark spam. No hollow openers like "Thank you for your feedback!"`,
    messages: [{
      role:    'user',
      content: `${stars}-star review from ${reviewer}:\n"${reviewText}"\n\nTone: ${toneGuide}\n\nWrite the response:`,
    }],
  });

  return msg.content[0].text.trim();
}

// ── AGENT LOG ──────────────────────────────────────────────────────────
async function logRun(status, details) {
  try {
    await pool.query(
      `INSERT INTO agent_log (agent_name, action, payload, status, client_id) VALUES ($1, $2, $3, $4, $5)`,
      [AGENT_NAME, 'review_check', JSON.stringify(details), status, CLIENT_ID]
    );
  } catch (_) {}
}

async function notifyNegativeReview(title, reviewer, stars, snippet) {
  await pool.query(`
    INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
    VALUES ('vera', 'negative_review', $1, $2, $3, 'pending', $4)
  `, [
    `Negative review needs owner response: ${title}`,
    `${reviewer} left a ${stars}-star review. Brad and Dustin should reply personally.`,
    JSON.stringify({ reviewer, stars, snippet, client_id: CLIENT_ID }),
    CLIENT_ID,
  ]);

  if (!process.env.BREVO_API_KEY) return;
  await axios.post('https://api.brevo.com/v3/smtp/email', {
    sender: { name: 'Vera — Pulseforge', email: 'jacob@gopulseforge.com' },
    to: [{ email: CLIENT_CONFIG?.email || 'mshomeinnovations@gmail.com', name: 'Brad & Dustin' }],
    subject: `MSHI review needs your response: ${stars} stars`,
    textContent: `${reviewer} left a ${stars}-star review for ${title}.\n\n${snippet || '(no written review)'}\n\nVera did not draft a response because MSHI negative reviews are owner-handled.`
  }, {
    headers: { 'api-key': process.env.BREVO_API_KEY, 'Content-Type': 'application/json' }
  }).catch(err => console.error('[Vera] Negative review notification failed:', err.response?.data || err.message));
}

// ── MAIN ───────────────────────────────────────────────────────────────
async function main() {
  console.log('[Vera] Starting Google Business review check...');
  CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);

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
    await logRun('failed', { error: 'token_refresh_failed' });
    return;
  }

  let accounts;
  try {
    accounts = await fetchAccounts(token);
  } catch (err) {
    console.error('[Vera] Failed to fetch GBP accounts:', err.response?.data || err.message);
    await logRun('failed', { error: 'fetch_accounts_failed' });
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

        if (CLIENT_ID === 2 && stars < 4) {
          await notifyNegativeReview(title, reviewer, stars, snippet);
          continue;
        }

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
               (author_name, author_title, post_content, comment, post_url, channel, client_id)
             VALUES ($1, $2, $3, $4, $5, 'google_review', $6)`,
            [
              title,
              `${stars} star${stars !== 1 ? 's' : ''}`,
              postContent,
              draft,
              review.name || '',
              CLIENT_ID,
            ]
          );
        } catch (err) {
          console.error('[Vera] Failed to save pending comment:', err.message);
          continue;
        }

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

module.exports = { run: main };

if (require.main === module) {
  main().catch(async err => {
    console.error('[Vera] Fatal:', err.message);
    await logRun('failed', { error: err.message });
    process.exit(1);
  });
}
