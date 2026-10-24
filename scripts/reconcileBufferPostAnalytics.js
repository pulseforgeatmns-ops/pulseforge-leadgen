'use strict';

require('dotenv').config();

const axios = require('axios');
const pool = require('../db');
const { fetchBufferMetrics } = require('../analyticsAgent');

const BUFFER_API_URL = 'https://api.buffer.com';
const CLIENT_ID = Number(process.env.CLIENT_ID || 1);
const LOOKBACK_DAYS = Number(process.env.BUFFER_RECONCILE_LOOKBACK_DAYS || 30);

function usage() {
  console.log('Usage: node scripts/reconcileBufferPostAnalytics.js [--dry-run|--apply]');
}

function parseArgs(argv) {
  const apply = argv.includes('--apply');
  const dryRun = argv.includes('--dry-run') || !apply;
  if (argv.includes('--help')) {
    usage();
    process.exit(0);
  }
  return { apply, dryRun };
}

function authHeaders() {
  if (!process.env.BUFFER_ACCESS_TOKEN) {
    throw new Error('BUFFER_ACCESS_TOKEN is required');
  }
  return {
    Authorization: `Bearer ${process.env.BUFFER_ACCESS_TOKEN}`,
    'Content-Type': 'application/json',
  };
}

async function bufferQuery(query, variables = {}) {
  const res = await axios.post(BUFFER_API_URL, { query, variables }, { headers: authHeaders() });
  if (res.data?.errors?.length) {
    throw new Error(JSON.stringify(res.data.errors));
  }
  return res.data?.data;
}

async function getOrganizationId() {
  const data = await bufferQuery(`query {
  account {
    organizations { id name }
  }
}`);
  const organization = data?.account?.organizations?.[0];
  if (!organization?.id) {
    throw new Error('Buffer account has no organization available');
  }
  return organization.id;
}

async function getChannels(organizationId) {
  const data = await bufferQuery(`query($organizationId: OrganizationId!) {
  channels(input: { organizationId: $organizationId }) {
    id
    name
    displayName
    service
    type
    serviceId
    isDisconnected
  }
}`, { organizationId });
  return data?.channels || [];
}

function findChannelIds(channels) {
  const linkedinPersonal = process.env.BUFFER_LINKEDIN_PERSONAL_ID;
  const linkedinPage = process.env.BUFFER_CHANNEL_ID;
  const facebookPage = process.env.BUFFER_FACEBOOK_PAGE_CHANNEL_ID
    || channels.find(channel =>
      channel.service === 'facebook'
      && channel.type === 'page'
      && (!process.env.FACEBOOK_PAGE_ID || channel.serviceId === process.env.FACEBOOK_PAGE_ID)
    )?.id;

  const required = {
    linkedin_personal: linkedinPersonal,
    linkedin_page: linkedinPage,
    facebook_page: facebookPage,
  };
  for (const [channel, id] of Object.entries(required)) {
    if (!id) throw new Error(`Missing Buffer channel id for ${channel}`);
  }
  return required;
}

function cleanText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

function metricMap(metrics) {
  return Object.fromEntries((metrics || []).map(metric => [metric.type, metric.value]));
}

async function fetchSentPostsForChannel(organizationId, channel, channelId, since) {
  const query = `query($organizationId: OrganizationId!, $channelIds: [ChannelId!], $after: String) {
  posts(
    input: {
      organizationId: $organizationId,
      filter: { channelIds: $channelIds, status: [sent] },
      sort: [{ field: dueAt, direction: desc }]
    },
    first: 100,
    after: $after
  ) {
    edges {
      node {
        id
        status
        createdAt
        dueAt
        sentAt
        text
        channelId
        metrics { type name value unit }
        metricsUpdatedAt
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}`;

  const posts = [];
  let after = null;
  for (let page = 0; page < 20; page++) {
    const data = await bufferQuery(query, {
      organizationId,
      channelIds: [channelId],
      after,
    });
    const result = data?.posts;
    const edges = result?.edges || [];
    for (const edge of edges) {
      const node = edge.node;
      const publishedAt = node.sentAt || node.dueAt || node.createdAt;
      if (!publishedAt) continue;
      const publishedDate = new Date(publishedAt);
      if (publishedDate >= since) {
        posts.push({
          channel,
          platform_post_id: node.id,
          published_at: publishedAt,
          post_text: cleanText(node.text),
          metrics: metricMap(node.metrics),
          metrics_updated_at: node.metricsUpdatedAt || null,
        });
      }
    }

    if (!result?.pageInfo?.hasNextPage) break;
    after = result.pageInfo.endCursor;
    const oldest = edges
      .map(edge => new Date(edge.node.sentAt || edge.node.dueAt || edge.node.createdAt))
      .filter(date => Number.isFinite(date.getTime()))
      .sort((a, b) => a - b)[0];
    if (oldest && oldest < since) break;
  }
  return posts;
}

function groupCounts(posts) {
  return posts.reduce((counts, post) => {
    counts[post.channel] = (counts[post.channel] || 0) + 1;
    return counts;
  }, {});
}

async function getMappedRows(posts) {
  if (!posts.length) return [];
  const ids = posts.map(post => post.platform_post_id);
  const { rows } = await pool.query(`
    SELECT id, platform_post_id, channel, published_at, impressions, reach, shares
    FROM post_analytics
    WHERE client_id = $1
      AND platform_post_id = ANY($2::text[])
    ORDER BY published_at
  `, [CLIENT_ID, ids]);
  return rows;
}

function summarize(posts, mappedRows) {
  const mapped = new Map(mappedRows.map(row => [row.platform_post_id, row]));
  const wouldInsert = posts.filter(post => !mapped.has(post.platform_post_id));
  return {
    found: groupCounts(posts),
    alreadyMapped: groupCounts(posts.filter(post => mapped.has(post.platform_post_id))),
    toInsert: groupCounts(wouldInsert),
    wouldInsert,
  };
}

function printSummary(label, summary) {
  console.log(label);
  console.log(JSON.stringify({
    found: summary.found,
    already_mapped: summary.alreadyMapped,
    to_insert: summary.toInsert,
    total_found: Object.values(summary.found).reduce((sum, count) => sum + count, 0),
    total_to_insert: summary.wouldInsert.length,
  }, null, 2));
  for (const post of summary.wouldInsert) {
    console.log([
      post.published_at.slice(0, 10),
      post.channel,
      post.platform_post_id,
      post.post_text.slice(0, 60),
    ].join('\t'));
  }
}

async function insertMissingPosts(posts) {
  const inserted = [];
  await pool.query('BEGIN');
  try {
    for (const post of posts) {
      const publishedAt = new Date(post.published_at);
      const result = await pool.query(`
        INSERT INTO post_analytics
          (channel, post_text, platform_post_id, published_at, post_day_of_week, post_hour, client_id)
        SELECT $1, $2, $3, $4, $5, $6, $7
        WHERE NOT EXISTS (
          SELECT 1
          FROM post_analytics
          WHERE client_id = $7
            AND platform_post_id = $3
        )
        RETURNING id, platform_post_id, channel
      `, [
        post.channel,
        post.post_text.slice(0, 2000),
        post.platform_post_id,
        post.published_at,
        publishedAt.getUTCDay(),
        publishedAt.getUTCHours(),
        CLIENT_ID,
      ]);
      inserted.push(...result.rows);
    }
    await pool.query('COMMIT');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  }
  return inserted;
}

async function spotCheckRecovered() {
  const { rows } = await pool.query(`
    SELECT
      published_at::date AS date,
      platform_post_id,
      impressions,
      reach,
      shares,
      metrics_fetched_at IS NOT NULL AS metrics_populated,
      LEFT(REGEXP_REPLACE(COALESCE(post_text, ''), '\\s+', ' ', 'g'), 60) AS text60
    FROM post_analytics
    WHERE client_id = $1
      AND channel = 'linkedin_personal'
      AND published_at::date BETWEEN DATE '2026-06-27' AND DATE '2026-07-06'
    ORDER BY published_at DESC
  `, [CLIENT_ID]);
  return rows;
}

async function main() {
  const { apply, dryRun } = parseArgs(process.argv.slice(2));
  const since = new Date(Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000);
  const organizationId = await getOrganizationId();
  const channelIds = findChannelIds(await getChannels(organizationId));

  const posts = [];
  for (const [channel, channelId] of Object.entries(channelIds)) {
    posts.push(...await fetchSentPostsForChannel(organizationId, channel, channelId, since));
  }

  const mappedRows = await getMappedRows(posts);
  const summary = summarize(posts, mappedRows);
  printSummary(dryRun ? 'DRY RUN' : 'APPLY PLAN', summary);

  if (!apply) return;

  const inserted = await insertMissingPosts(summary.wouldInsert);
  console.log(`Inserted rows: ${inserted.length}`);

  let metricStats = { attempts: 0, successes: 0, skipped: 0, errorSample: null };
  if (inserted.length) {
    metricStats = await fetchBufferMetrics({
      postAnalyticsIds: inserted.map(row => row.id),
      includeFacebookPage: true,
      lookbackDays: LOOKBACK_DAYS + 1,
    });
  }
  console.log(`Metrics populated: ${metricStats.successes}/${metricStats.attempts}`);
  if (metricStats.errorSample) {
    console.log('Metric error sample:', JSON.stringify(metricStats.errorSample, null, 2));
  }

  console.log('Recovered Jun 27-Jul 6 LinkedIn personal rows:');
  console.log(JSON.stringify(await spotCheckRecovered(), null, 2));

  const afterMappedRows = await getMappedRows(posts);
  const afterSummary = summarize(posts, afterMappedRows);
  console.log(`Idempotency check - rows that would insert now: ${afterSummary.wouldInsert.length}`);
}

main()
  .catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end().catch(() => {});
  });
