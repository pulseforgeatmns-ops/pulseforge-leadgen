'use strict';
require('dotenv').config();

const axios = require('axios');
const pool  = require('./db');

const AGENT_NAME = 'analytics';

// ── SCHEMA ────────────────────────────────────────────────────────────────────

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_analytics (
      id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      pending_id         UUID UNIQUE REFERENCES pending_comments(id),
      company_id         UUID REFERENCES companies(id),
      channel            TEXT NOT NULL,
      content_type       TEXT,
      post_text          TEXT,
      platform_post_id   TEXT,
      published_at       TIMESTAMPTZ DEFAULT NOW(),
      post_day_of_week   SMALLINT,
      post_hour          SMALLINT,
      likes              INT DEFAULT 0,
      comments           INT DEFAULT 0,
      shares             INT DEFAULT 0,
      reach              INT DEFAULT 0,
      clicks             INT DEFAULT 0,
      engagement_rate    NUMERIC(6,4) DEFAULT 0,
      metrics_fetched_at TIMESTAMPTZ,
      created_at         TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS content_performance_summary (
      id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      company_id          UUID REFERENCES companies(id),
      channel             TEXT NOT NULL,
      content_type        TEXT NOT NULL,
      post_count          INT DEFAULT 0,
      avg_likes           NUMERIC(8,2) DEFAULT 0,
      avg_comments        NUMERIC(8,2) DEFAULT 0,
      avg_shares          NUMERIC(8,2) DEFAULT 0,
      avg_reach           NUMERIC(8,2) DEFAULT 0,
      avg_engagement_rate NUMERIC(6,4) DEFAULT 0,
      best_day_of_week    SMALLINT,
      best_hour           SMALLINT,
      updated_at          TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (company_id, channel, content_type)
    )
  `);
}

// ── GOOGLE ACCESS TOKEN ───────────────────────────────────────────────────────

async function getGoogleAccessToken() {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id:     process.env.GOOGLE_CLIENT_ID,
    client_secret: process.env.GOOGLE_CLIENT_SECRET,
    refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
    grant_type:    'refresh_token',
  });
  return res.data.access_token;
}

// ── FACEBOOK METRICS ──────────────────────────────────────────────────────────

async function fetchFBPageMetrics() {
  const pageToken = process.env.FACEBOOK_PAGE_ACCESS_TOKEN;
  if (!pageToken) {
    console.log('[Analytics] FACEBOOK_PAGE_ACCESS_TOKEN not set — skipping FB metrics');
    return;
  }

  const { rows } = await pool.query(`
    SELECT id, platform_post_id
    FROM post_analytics
    WHERE channel = 'facebook_page'
      AND platform_post_id IS NOT NULL
      AND metrics_fetched_at IS NULL
    LIMIT 50
  `);

  if (!rows.length) {
    console.log('[Analytics] No unanalyzed Facebook posts');
    return;
  }

  let updated = 0;
  for (const row of rows) {
    try {
      const res = await axios.get(`https://graph.facebook.com/${row.platform_post_id}`, {
        params: {
          fields: 'likes.summary(true),comments.summary(true),shares',
          access_token: pageToken,
        },
      });
      const d = res.data;
      const likes    = d.likes?.summary?.total_count    || 0;
      const comments = d.comments?.summary?.total_count || 0;
      const shares   = d.shares?.count                  || 0;

      // Also pull reach via insights
      let reach = 0;
      try {
        const ins = await axios.get(`https://graph.facebook.com/${row.platform_post_id}/insights`, {
          params: {
            metric: 'post_impressions_unique',
            access_token: pageToken,
          },
        });
        reach = ins.data?.data?.[0]?.values?.[0]?.value || 0;
      } catch (_) {}

      const engagement_rate = reach > 0
        ? ((likes + comments + shares) / reach * 100).toFixed(4)
        : 0;

      await pool.query(`
        UPDATE post_analytics
        SET likes = $1, comments = $2, shares = $3, reach = $4,
            engagement_rate = $5, metrics_fetched_at = NOW()
        WHERE id = $6
      `, [likes, comments, shares, reach, engagement_rate, row.id]);
      updated++;
    } catch (err) {
      console.warn(`[Analytics] FB post ${row.platform_post_id} metrics failed:`, err.response?.data?.error?.message || err.message);
    }
    await new Promise(r => setTimeout(r, 300));
  }
  console.log(`[Analytics] Facebook metrics updated: ${updated}/${rows.length}`);
}

// ── BUFFER / LINKEDIN METRICS ─────────────────────────────────────────────────

async function fetchBufferMetrics() {
  const token = process.env.BUFFER_ACCESS_TOKEN;
  if (!token) {
    console.log('[Analytics] BUFFER_ACCESS_TOKEN not set — skipping Buffer metrics');
    return;
  }

  const { rows } = await pool.query(`
    SELECT id, platform_post_id
    FROM post_analytics
    WHERE channel = 'linkedin_page'
      AND platform_post_id IS NOT NULL
      AND metrics_fetched_at IS NULL
    LIMIT 50
  `);

  if (!rows.length) {
    console.log('[Analytics] No unanalyzed LinkedIn posts');
    return;
  }

  let updated = 0;
  for (const row of rows) {
    try {
      const query = `query {
  post(id: ${JSON.stringify(row.platform_post_id)}) {
    id
    statistics {
      likes
      comments
      shares
      impressions
      clicks
    }
  }
}`;
      const res = await axios.post(
        'https://api.buffer.com',
        { query },
        { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } }
      );

      const stats = res.data?.data?.post?.statistics;
      if (!stats) {
        await pool.query(
          `UPDATE post_analytics SET metrics_fetched_at = NOW() WHERE id = $1`,
          [row.id]
        );
        updated++;
        continue;
      }

      const likes    = stats.likes      || 0;
      const comments = stats.comments   || 0;
      const shares   = stats.shares     || 0;
      const reach    = stats.impressions || 0;
      const clicks   = stats.clicks     || 0;
      const engagement_rate = reach > 0
        ? ((likes + comments + shares + clicks) / reach * 100).toFixed(4)
        : 0;

      await pool.query(`
        UPDATE post_analytics
        SET likes = $1, comments = $2, shares = $3, reach = $4, clicks = $5,
            engagement_rate = $6, metrics_fetched_at = NOW()
        WHERE id = $7
      `, [likes, comments, shares, reach, clicks, engagement_rate, row.id]);
      updated++;
    } catch (err) {
      console.warn(`[Analytics] Buffer post ${row.platform_post_id} metrics failed:`, err.response?.data || err.message);
    }
    await new Promise(r => setTimeout(r, 300));
  }
  console.log(`[Analytics] Buffer/LinkedIn metrics updated: ${updated}/${rows.length}`);
}

// ── SUMMARY REBUILD ───────────────────────────────────────────────────────────

async function rebuildSummary() {
  // Aggregate post_analytics into content_performance_summary
  const { rows } = await pool.query(`
    SELECT
      company_id,
      channel,
      content_type,
      COUNT(*)                          AS post_count,
      ROUND(AVG(likes), 2)              AS avg_likes,
      ROUND(AVG(comments), 2)           AS avg_comments,
      ROUND(AVG(shares), 2)             AS avg_shares,
      ROUND(AVG(reach), 2)              AS avg_reach,
      ROUND(AVG(engagement_rate), 4)    AS avg_engagement_rate,
      MODE() WITHIN GROUP (ORDER BY post_day_of_week) AS best_day_of_week,
      MODE() WITHIN GROUP (ORDER BY post_hour)        AS best_hour
    FROM post_analytics
    WHERE content_type IS NOT NULL
    GROUP BY company_id, channel, content_type
  `);

  for (const r of rows) {
    await pool.query(`
      INSERT INTO content_performance_summary
        (company_id, channel, content_type, post_count, avg_likes, avg_comments,
         avg_shares, avg_reach, avg_engagement_rate, best_day_of_week, best_hour, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (company_id, channel, content_type)
      DO UPDATE SET
        post_count          = EXCLUDED.post_count,
        avg_likes           = EXCLUDED.avg_likes,
        avg_comments        = EXCLUDED.avg_comments,
        avg_shares          = EXCLUDED.avg_shares,
        avg_reach           = EXCLUDED.avg_reach,
        avg_engagement_rate = EXCLUDED.avg_engagement_rate,
        best_day_of_week    = EXCLUDED.best_day_of_week,
        best_hour           = EXCLUDED.best_hour,
        updated_at          = NOW()
    `, [
      r.company_id, r.channel, r.content_type,
      r.post_count, r.avg_likes, r.avg_comments,
      r.avg_shares, r.avg_reach, r.avg_engagement_rate,
      r.best_day_of_week, r.best_hour,
    ]);
  }

  console.log(`[Analytics] Summary rebuilt — ${rows.length} channel/type combinations`);
}

// ── MARK NULLS ────────────────────────────────────────────────────────────────

async function markUnfetchableAsAttempted() {
  // GBP and Puppeteer channels have no API metrics — mark them as fetched so they don't pile up
  await pool.query(`
    UPDATE post_analytics
    SET metrics_fetched_at = NOW()
    WHERE channel IN ('google_business', 'facebook', 'linkedin', 'blog')
      AND metrics_fetched_at IS NULL
  `);
}

// ── LOGGING ───────────────────────────────────────────────────────────────────

async function logRun(status, payload) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
    VALUES ($1, $2, $3, $4, NOW())
  `, [AGENT_NAME, 'fetch_metrics', JSON.stringify(payload), status]);
}

// ── MAIN ──────────────────────────────────────────────────────────────────────

async function run() {
  console.log('\nAnalytics agent running...\n');
  try {
    await ensureSchema();
    console.log('[Analytics] Schema ready');

    await markUnfetchableAsAttempted();
    await fetchFBPageMetrics();
    await fetchBufferMetrics();
    await rebuildSummary();

    await logRun('success', { ran_at: new Date().toISOString() });
    console.log('\nAnalytics agent complete.');
  } catch (err) {
    console.error('Analytics agent error:', err.message);
    await logRun('error', { error: err.message }).catch(() => {});
  }
}

run();
