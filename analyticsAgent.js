'use strict';
require('dotenv').config();

const axios = require('axios');
const { randomUUID } = require('crypto');
const pool  = require('./db');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { reportAgentRun } = require('./utils/agentObservability');

const AGENT_NAME = 'analytics';
const CLIENT_ID = getRuntimeClientId();
let bufferMetricsSchemaPromise;

function makeRunId() {
  return `${AGENT_NAME}-${CLIENT_ID || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

function errorSampleFrom(err, row = null) {
  return {
    post_analytics_id: row?.id || null,
    platform_post_id: row?.platform_post_id || null,
    error: err.response?.data || err.message,
  };
}

async function reportAnalyticsRun({ runId, attempts, successes, skipped = 0, errorSample = null }) {
  try {
    return await reportAgentRun({
      agent: AGENT_NAME,
      clientId: CLIENT_ID,
      runId,
      attempts,
      successes,
      skipped,
      errorSample,
    });
  } catch (err) {
    console.error('[Analytics] Observability report failed:', err.message);
    return null;
  }
}

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
      impressions        INT DEFAULT 0,
      clicks             INT DEFAULT 0,
      engagement_rate    NUMERIC(6,4) DEFAULT 0,
      metrics_fetched_at TIMESTAMPTZ,
      buffer_metrics_updated_at TIMESTAMPTZ,
      client_id          INTEGER REFERENCES clients(id) DEFAULT 1,
      created_at         TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`ALTER TABLE post_analytics ADD COLUMN IF NOT EXISTS client_id INTEGER REFERENCES clients(id) DEFAULT 1`);
  await pool.query(`ALTER TABLE post_analytics ADD COLUMN IF NOT EXISTS buffer_metrics_updated_at TIMESTAMPTZ`);
  await pool.query(`ALTER TABLE post_analytics ADD COLUMN IF NOT EXISTS impressions INT DEFAULT 0`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS content_performance_summary (
      id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      company_id          UUID REFERENCES companies(id),
      channel             TEXT NOT NULL,
      content_type        TEXT NOT NULL,
      post_count          INT DEFAULT 0,
      measured_count      INT DEFAULT 0,
      avg_likes           NUMERIC(8,2) DEFAULT 0,
      avg_comments        NUMERIC(8,2) DEFAULT 0,
      avg_shares          NUMERIC(8,2) DEFAULT 0,
      avg_reach           NUMERIC(8,2) DEFAULT 0,
      avg_impressions     NUMERIC(8,2) DEFAULT 0,
      avg_engagement_rate NUMERIC(6,4) DEFAULT 0,
      best_day_of_week    SMALLINT,
      best_hour           SMALLINT,
      updated_at          TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (company_id, channel, content_type)
    )
  `);
  await pool.query(`ALTER TABLE content_performance_summary ADD COLUMN IF NOT EXISTS measured_count INT DEFAULT 0`);
  await pool.query(`ALTER TABLE content_performance_summary ADD COLUMN IF NOT EXISTS avg_impressions NUMERIC(8,2) DEFAULT 0`);
}

function ensureBufferMetricsSchema() {
  if (!bufferMetricsSchemaPromise) {
    bufferMetricsSchemaPromise = pool.query(`
      ALTER TABLE post_analytics
        ADD COLUMN IF NOT EXISTS buffer_metrics_updated_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS impressions INT DEFAULT 0
    `).catch(err => {
      bufferMetricsSchemaPromise = null;
      throw err;
    });
  }
  return bufferMetricsSchemaPromise;
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
    SELECT id, platform_post_id, channel
    FROM post_analytics
    WHERE channel = 'facebook_page'
      AND client_id = $1
      AND platform_post_id IS NOT NULL
      AND published_at > NOW() - INTERVAL '14 days'
      AND (
        metrics_fetched_at IS NULL
        OR metrics_fetched_at < NOW() - INTERVAL '6 hours'
      )
  `, [CLIENT_ID]);

  if (!rows.length) {
    console.log('[Analytics] No Facebook posts due for metrics refresh');
    return;
  }

  let updated = 0;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const stillActive = await getClientConfig(CLIENT_ID);
    if (!stillActive) {
      throw new Error(`[Analytics] Client ${CLIENT_ID} deactivated mid-run — aborting at FB metric row ${i + 1}/${rows.length} after ${updated} updated`);
    }

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

function buildBufferMetricsQuery(postId) {
  return `query {
  post(input: { id: ${JSON.stringify(postId)} }) {
    id
    text
    metrics {
      type
      name
      value
      unit
    }
    metricsUpdatedAt
  }
}`;
}

function numericMetric(value) {
  if (value == null) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function metricLookup(metrics) {
  if (!Array.isArray(metrics)) return null;
  const lookup = new Map();
  for (const metric of metrics) {
    if (!metric?.type || lookup.has(metric.type)) continue;
    lookup.set(metric.type, metric);
  }
  return lookup;
}

function metricValue(lookup, type) {
  if (!lookup?.has(type)) return null;
  return numericMetric(lookup.get(type)?.value);
}

function chooseShareMetric(lookup) {
  const reposts = metricValue(lookup, 'reposts');
  const shares = metricValue(lookup, 'shares');
  if (reposts != null) return { type: 'reposts', value: reposts };
  if (shares != null) return { type: 'shares', value: shares };
  return { type: null, value: null };
}

function mapBufferPostMetrics(metrics) {
  const lookup = metricLookup(metrics);
  if (!lookup) return null;

  const impressions = metricValue(lookup, 'impressions');
  const likes = metricValue(lookup, 'reactions');
  const comments = metricValue(lookup, 'comments');
  const shareMetric = chooseShareMetric(lookup);
  const engagementValues = [likes, comments, shareMetric.value].filter(value => value != null);
  const engagement_rate = impressions > 0 && engagementValues.length
    ? (engagementValues.reduce((sum, value) => sum + value, 0) / impressions * 100).toFixed(4)
    : null;

  return {
    likes,
    comments,
    shares: shareMetric.value,
    impressions,
    clicks: null,
    engagement_rate,
    buffer_engagement_rate: metricValue(lookup, 'engagementRate'),
    share_metric_type: shareMetric.type,
  };
}

function parseBufferPostResponse(data) {
  const post = data?.data?.post;
  if (!post) {
    throw new Error('Buffer response missing data.post');
  }

  const metrics = post.metrics;
  if (!Array.isArray(metrics)) {
    throw new Error('Buffer response data.post.metrics is missing or is not an array');
  }
  if (metrics.length === 0) {
    throw new Error('Buffer response data.post.metrics is empty');
  }

  return { post, metrics };
}

async function fetchBufferMetrics(options = {}) {
  const token = process.env.BUFFER_ACCESS_TOKEN;
  if (!token) {
    console.log('[Analytics] BUFFER_ACCESS_TOKEN not set — skipping Buffer metrics');
    return { attempts: 0, successes: 0, skipped: 0, errorSample: null };
  }
  await ensureBufferMetricsSchema();

  const params = [CLIENT_ID];
  let filter = '';
  if (options.postAnalyticsId) {
    params.push(options.postAnalyticsId);
    filter += ` AND id = $${params.length}`;
  }
  if (options.platformPostId) {
    params.push(options.platformPostId);
    filter += ` AND platform_post_id = $${params.length}`;
  }
  if (options.channel) {
    params.push(options.channel);
    filter += ` AND channel = $${params.length}`;
  }
  const { rows } = await pool.query(`
    SELECT id, platform_post_id, channel
    FROM post_analytics
    WHERE channel IN ('linkedin_page', 'linkedin_personal')
      AND client_id = $1
      AND platform_post_id IS NOT NULL
      AND published_at > NOW() - INTERVAL '14 days'
      AND (
        metrics_fetched_at IS NULL
        OR metrics_fetched_at < NOW() - INTERVAL '6 hours'
      )
      ${filter}
  `, params);

  if (!rows.length) {
    console.log('[Analytics] No LinkedIn posts due for metrics refresh');
    return { attempts: 0, successes: 0, skipped: 0, errorSample: null };
  }

  let updated = 0;
  let errorSample = null;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const stillActive = await getClientConfig(CLIENT_ID);
    if (!stillActive) {
      throw new Error(`[Analytics] Client ${CLIENT_ID} deactivated mid-run — aborting at Buffer metric row ${i + 1}/${rows.length} after ${updated} updated`);
    }

    try {
      const query = buildBufferMetricsQuery(row.platform_post_id);
      const res = await axios.post(
        'https://api.buffer.com',
        { query },
        { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } }
      );

      const gqlErrors = res.data?.errors;
      if (gqlErrors?.length) {
        throw new Error(JSON.stringify(gqlErrors));
      }

      const { post, metrics } = parseBufferPostResponse(res.data);

      if (options.logRawMetrics) {
        console.log('[Analytics] Raw Buffer metrics payload:', JSON.stringify({
          platform_post_id: row.platform_post_id,
          metricsUpdatedAt: post?.metricsUpdatedAt || null,
          metrics,
        }, null, 2));
      }
      const mapped = mapBufferPostMetrics(metrics);

      await pool.query(`
        UPDATE post_analytics
        SET likes = $1, comments = $2, shares = $3, impressions = $4, clicks = $5,
            engagement_rate = $6, metrics_fetched_at = NOW(),
            buffer_metrics_updated_at = $7
        WHERE id = $8
      `, [
        mapped.likes,
        mapped.comments,
        mapped.shares,
        mapped.impressions,
        mapped.clicks,
        mapped.engagement_rate,
        post?.metricsUpdatedAt || null,
        row.id,
      ]);
      if (options.logRawMetrics) {
        console.log('[Analytics] Buffer metric mapping:', JSON.stringify({
          platform_post_id: row.platform_post_id,
          mapped_reactions_to_likes: mapped.likes,
          mapped_share_metric_type: mapped.share_metric_type,
          mapped_share_metric_value: mapped.shares,
          computed_engagement_rate: mapped.engagement_rate,
          buffer_engagement_rate: mapped.buffer_engagement_rate,
        }, null, 2));
      }
      updated++;
    } catch (err) {
      errorSample = errorSample || errorSampleFrom(err, row);
      console.error(
        `[Analytics] Buffer post ${row.platform_post_id} (${row.channel}) metrics failed:`,
        err.response?.data || err.message
      );
    }
    await new Promise(r => setTimeout(r, 300));
  }
  console.log(`[Analytics] Buffer/LinkedIn metrics updated: ${updated}/${rows.length}`);
  return { attempts: rows.length, successes: updated, skipped: 0, errorSample };
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
      COUNT(*) FILTER (WHERE metrics_fetched_at IS NOT NULL) AS measured_count,
      ROUND(AVG(likes), 2)              AS avg_likes,
      ROUND(AVG(comments), 2)           AS avg_comments,
      ROUND(AVG(shares), 2)             AS avg_shares,
      ROUND(AVG(reach), 2)              AS avg_reach,
      ROUND(AVG(impressions), 2)        AS avg_impressions,
      ROUND(AVG(engagement_rate) FILTER (WHERE metrics_fetched_at IS NOT NULL), 4) AS avg_engagement_rate,
      MODE() WITHIN GROUP (ORDER BY post_day_of_week) AS best_day_of_week,
      MODE() WITHIN GROUP (ORDER BY post_hour)        AS best_hour
    FROM post_analytics
    WHERE content_type IS NOT NULL
      AND client_id = $1
    GROUP BY company_id, channel, content_type
  `, [CLIENT_ID]);

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const stillActive = await getClientConfig(CLIENT_ID);
    if (!stillActive) {
      throw new Error(`[Analytics] Client ${CLIENT_ID} deactivated mid-run — aborting at summary row ${i + 1}/${rows.length}`);
    }

    await pool.query(`
      INSERT INTO content_performance_summary
        (company_id, channel, content_type, post_count, measured_count, avg_likes, avg_comments,
         avg_shares, avg_reach, avg_impressions, avg_engagement_rate, best_day_of_week, best_hour, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
      ON CONFLICT (company_id, channel, content_type)
      DO UPDATE SET
        post_count          = EXCLUDED.post_count,
        measured_count      = EXCLUDED.measured_count,
        avg_likes           = EXCLUDED.avg_likes,
        avg_comments        = EXCLUDED.avg_comments,
        avg_shares          = EXCLUDED.avg_shares,
        avg_reach           = EXCLUDED.avg_reach,
        avg_impressions     = EXCLUDED.avg_impressions,
        avg_engagement_rate = EXCLUDED.avg_engagement_rate,
        best_day_of_week    = EXCLUDED.best_day_of_week,
        best_hour           = EXCLUDED.best_hour,
        updated_at          = NOW()
    `, [
      r.company_id, r.channel, r.content_type,
      r.post_count, r.measured_count, r.avg_likes, r.avg_comments,
      r.avg_shares, r.avg_reach, r.avg_impressions, r.avg_engagement_rate,
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
      AND client_id = $1
      AND metrics_fetched_at IS NULL
  `, [CLIENT_ID]);
}

// ── LOGGING ───────────────────────────────────────────────────────────────────

async function logRun(status, payload) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [AGENT_NAME, 'fetch_metrics', JSON.stringify({ ...payload, client_id: CLIENT_ID }), status, CLIENT_ID]);
}

// ── MAIN ──────────────────────────────────────────────────────────────────────

async function run() {
  const runId = makeRunId();
  let attempts = 0;
  let successes = 0;
  let skipped = 0;
  let errorSample = null;

  console.log('\nAnalytics agent running...\n');
  try {
    const clientConfig = await getClientConfig(CLIENT_ID);
    if (!clientConfig) throw new Error(`Active client not found: ${CLIENT_ID}`);
    await ensureSchema();
    console.log('[Analytics] Schema ready');

    await markUnfetchableAsAttempted();
    await fetchFBPageMetrics();
    const bufferStats = await fetchBufferMetrics();
    attempts = bufferStats?.attempts || 0;
    successes = bufferStats?.successes || 0;
    skipped = bufferStats?.skipped || 0;
    errorSample = bufferStats?.errorSample || null;
    await rebuildSummary();

    await logRun('success', { ran_at: new Date().toISOString(), attempts, successes, skipped });
    await reportAnalyticsRun({ runId, attempts, successes, skipped, errorSample });
    console.log('\nAnalytics agent complete.');
    return { attempts, successes, skipped, errorSample };
  } catch (err) {
    errorSample = errorSample || errorSampleFrom(err);
    console.error('Analytics agent error:', err.message);
    await logRun('failed', { error: err.message }).catch(() => {});
    await reportAnalyticsRun({ runId, attempts, successes, skipped, errorSample });
    return { attempts, successes, skipped, errorSample, failed: true };
  }
}

module.exports = {
  run,
  fetchBufferMetrics,
  buildBufferMetricsQuery,
  mapBufferPostMetrics,
  parseBufferPostResponse,
};

if (require.main === module) {
  run().catch(err => {
    console.error('[Analytics] Fatal error:', err.message);
    process.exit(1);
  });
}
