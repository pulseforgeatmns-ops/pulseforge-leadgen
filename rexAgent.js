require('dotenv').config();
const pool = require('./db');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const { randomUUID } = require('crypto');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { reportAgentRun } = require('./utils/agentObservability');

const client = new Anthropic();
const AGENT_NAME = 'rex';
const CLIENT_ID = getRuntimeClientId();
let lastSendReportError = null;

function makeRunId() {
  return `${AGENT_NAME}-${CLIENT_ID || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

function captureErrorSample(err) {
  if (!err) return null;
  if (err.response?.data) return err.response.data;
  return {
    message: err.message,
    code: err.code || null,
    status: err.response?.status || null,
  };
}

async function reportRexRun({ runId, attempts, successes, skipped = 0, errorSample = null }) {
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
    console.error('[Rex] Observability report failed:', err.message);
    return null;
  }
}

async function getWeeklyData() {
  const weekAgo = "NOW() - INTERVAL '7 days'";

  // Agent activity this week
  const agentActivity = await pool.query(`
    SELECT agent_name, action, status, COUNT(*) as count
    FROM agent_log
    WHERE ran_at > ${weekAgo}
    GROUP BY agent_name, action, status
    ORDER BY agent_name, count DESC
  `);

  // Touchpoints by channel this week
  const touchpointsByChannel = await pool.query(`
    SELECT channel, action_type, COUNT(*) as count
    FROM touchpoints
    WHERE created_at > ${weekAgo}
    GROUP BY channel, action_type
    ORDER BY count DESC
  `);

  // Touchpoints by channel all time
  const allTimeTouchpoints = await pool.query(`
    SELECT channel, COUNT(*) as count
    FROM touchpoints
    GROUP BY channel
    ORDER BY count DESC
  `);

  // Prospect pipeline breakdown
  const pipeline = await pool.query(`
    SELECT status, COUNT(*) as count
    FROM prospects
    GROUP BY status
    ORDER BY count DESC
  `);

  // Pending comments by channel
  const pendingByChannel = await pool.query(`
    SELECT channel, status, COUNT(*) as count
    FROM pending_comments
    GROUP BY channel, status
    ORDER BY channel, count DESC
  `);

  // Approved vs rejected comments
  const approvalRate = await pool.query(`
    SELECT 
      status,
      COUNT(*) as count
    FROM pending_comments
    WHERE status IN ('approved', 'rejected')
    GROUP BY status
  `);

  // Most active days this week
  const activityByDay = await pool.query(`
    SELECT 
      DATE(ran_at) as day,
      COUNT(*) as actions
    FROM agent_log
    WHERE ran_at > ${weekAgo}
    GROUP BY DATE(ran_at)
    ORDER BY day DESC
  `);

  // Email sequence performance
  const emailStats = await pool.query(`
    SELECT 
      content_summary as sequence_step,
      COUNT(*) as sent,
      outcome
    FROM touchpoints
    WHERE channel = 'email'
    GROUP BY content_summary, outcome
    ORDER BY sent DESC
  `);

  // Prospects with most touchpoints (most nurtured)
  const mostNurtured = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email, p.status,
      COUNT(t.id) as touch_count,
      MAX(t.created_at) as last_touch
    FROM prospects p
    JOIN touchpoints t ON t.prospect_id = p.id
    GROUP BY p.id, p.first_name, p.last_name, p.email, p.status
    ORDER BY touch_count DESC
    LIMIT 5
  `);

  // Content performance this week
  const contentPerf = await pool.query(`
    SELECT channel, content_type,
           COUNT(*) AS posts_published,
           COUNT(*) FILTER (WHERE metrics_fetched_at IS NOT NULL) AS measured_count,
           ROUND(AVG(engagement_rate) FILTER (WHERE metrics_fetched_at IS NOT NULL), 4) AS avg_engagement,
           MAX(engagement_rate) AS best_engagement
    FROM post_analytics
    WHERE published_at > ${weekAgo}
    GROUP BY channel, content_type
    ORDER BY avg_engagement DESC NULLS LAST
    LIMIT 10
  `).catch(() => ({ rows: [] }));

  // Top post this week
  const topPost = await pool.query(`
    SELECT channel, content_type, post_text,
           likes, comments, shares, reach, engagement_rate
    FROM post_analytics
    WHERE published_at > ${weekAgo}
      AND engagement_rate > 0
    ORDER BY engagement_rate DESC
    LIMIT 1
  `).catch(() => ({ rows: [] }));

  // Worst post this week
  const worstPost = await pool.query(`
    SELECT channel, content_type, post_text, engagement_rate
    FROM post_analytics
    WHERE published_at > ${weekAgo}
      AND metrics_fetched_at IS NOT NULL
    ORDER BY engagement_rate ASC
    LIMIT 1
  `).catch(() => ({ rows: [] }));

  // Email funnel this week
  const emailFunnel = await pool.query(`
    SELECT
      COUNT(CASE WHEN action_type = 'outbound'           THEN 1 END)::int AS sent,
      COUNT(CASE WHEN action_type = 'email_opened'       THEN 1 END)::int AS opened,
      COUNT(CASE WHEN action_type = 'email_clicked'      THEN 1 END)::int AS clicked,
      COUNT(CASE WHEN action_type = 'email_bounced'      THEN 1 END)::int AS bounced,
      COUNT(CASE WHEN action_type = 'email_soft_bounce'  THEN 1 END)::int AS soft_bounced,
      COUNT(CASE WHEN action_type = 'email_unsubscribed' THEN 1 END)::int AS unsubscribed,
      COUNT(CASE WHEN action_type = 'email_spam'         THEN 1 END)::int AS spam
    FROM touchpoints
    WHERE channel = 'email'
      AND client_id = $1
      AND created_at > ${weekAgo}
  `, [CLIENT_ID]).catch(() => ({ rows: [{}] }));

  const emailReplyMetrics = await pool.query(`
    SELECT
      (
        SELECT COUNT(*)::int
        FROM agent_log
        WHERE action = 'email_sent'
          AND client_id = $1
          AND ran_at > ${weekAgo}
      ) AS sent,
      (
        SELECT COUNT(*)::int
        FROM agent_log r
        WHERE r.action = 'email_reply_received'
          AND r.client_id = $1
          AND r.ran_at > ${weekAgo}
          AND EXISTS (
            SELECT 1
            FROM agent_log s
            WHERE s.prospect_id = r.prospect_id
              AND s.client_id = r.client_id
              AND s.action = 'email_sent'
              AND s.ran_at > ${weekAgo}
          )
      ) AS replies
  `, [CLIENT_ID]).catch(() => ({ rows: [{ sent: 0, replies: 0 }] }));

  const warmSignalsSent = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM agent_log
    WHERE action = 'warm_signal_text_sent'
      AND client_id = $1
      AND ran_at > ${weekAgo}
  `, [CLIENT_ID]).catch(() => ({ rows: [{ count: 0 }] }));

  const discoveryCallsBooked = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM prospects
    WHERE booked_at > ${weekAgo}
      AND setter_status = 'booked'
      AND client_id = $1
  `, [CLIENT_ID]).catch(() => ({ rows: [{ count: 0 }] }));

  const emailRejections = await pool.query(`
    SELECT
      COALESCE(payload->>'reason', 'unknown') AS reason,
      COUNT(*)::int AS count
    FROM agent_log
    WHERE agent_name = 'scout'
      AND action = 'email_rejected'
      AND client_id = $1
      AND ran_at > ${weekAgo}
    GROUP BY 1
    ORDER BY count DESC
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Email engagement by sequence day
  const emailByDay = await pool.query(`
    SELECT
      content_summary AS subject,
      COUNT(*)::int AS sent
    FROM touchpoints
    WHERE channel = 'email' AND action_type = 'outbound'
      AND created_at > ${weekAgo}
    GROUP BY content_summary
    ORDER BY sent DESC
    LIMIT 10
  `).catch(() => ({ rows: [] }));

  // DNC additions this week
  const dncAdded = await pool.query(`
    SELECT COUNT(*)::int AS count FROM prospects
    WHERE do_not_contact = true AND updated_at > ${weekAgo}
  `).catch(() => ({ rows: [{ count: 0 }] }));

  // Email performance by vertical
  const emailByVertical = await pool.query(`
    SELECT
      co.industry,
      COUNT(CASE WHEN t.action_type = 'outbound'      THEN 1 END)::int AS sent,
      COUNT(CASE WHEN t.action_type = 'email_opened'  THEN 1 END)::int AS opened,
      COUNT(CASE WHEN t.action_type = 'email_clicked' THEN 1 END)::int AS clicked
    FROM touchpoints t
    JOIN prospects p ON t.prospect_id = p.id
    JOIN companies co ON p.company_id = co.id
    WHERE t.channel = 'email' AND t.created_at > ${weekAgo}
    GROUP BY co.industry
    ORDER BY opened DESC
    LIMIT 8
  `).catch(() => ({ rows: [] }));

  const slotTestVerdict = await getSlotTestVerdict();

  return {
    weekOf: new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }),
    agentActivity: agentActivity.rows,
    touchpointsByChannel: touchpointsByChannel.rows,
    allTimeTouchpoints: allTimeTouchpoints.rows,
    pipeline: pipeline.rows,
    pendingByChannel: pendingByChannel.rows,
    approvalRate: approvalRate.rows,
    activityByDay: activityByDay.rows,
    emailStats: emailStats.rows,
    mostNurtured: mostNurtured.rows,
    contentPerf: contentPerf.rows.map(formatContentPerfRow),
    topPost: topPost.rows[0] || null,
    worstPost: worstPost.rows[0] || null,
    emailFunnel:     emailFunnel.rows[0] || {},
    emailReplyMetrics: emailReplyMetrics.rows[0] || { sent: 0, replies: 0 },
    warmSignalsSent: warmSignalsSent.rows[0]?.count || 0,
    discoveryCallsBooked: discoveryCallsBooked.rows[0]?.count || 0,
    emailRejections: emailRejections.rows,
    emailByDay:      emailByDay.rows,
    dncAdded:        dncAdded.rows[0]?.count || 0,
    emailByVertical: emailByVertical.rows,
    slotTestVerdict,
  };
}

async function generateReport(data) {
  const dataString = JSON.stringify(data, null, 2);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 1500,
    messages: [{
      role: 'user',
      content: `You are Rex, the reporting agent for Pulseforge — an AI marketing system for local small businesses.

Here is this week's performance data from the second brain:

${dataString}

Generate a weekly performance report with these sections:

1. WEEK IN REVIEW — 2-3 sentences summarizing overall system activity and health
2. CHANNEL PERFORMANCE — how each channel (LinkedIn, Facebook, email) performed this week, what's working and what's lagging
3. EMAIL PERFORMANCE — use emailFunnel, emailReplyMetrics, emailByVertical, emailByDay data. Treat reply rate (emailReplyMetrics.replies / emailReplyMetrics.sent) as the primary engagement metric — do NOT emphasize click rate (sequences are reply-driven; click rate is N/A). Report open rate, reply rate, bounce rate, warmSignalsSent, discoveryCallsBooked, and emailRejections breakdown; which vertical has highest open rate; which subject lines drove the most sends; DNC additions this week; note if tracking data is still accumulating if tables are empty
4. CONTENT PERFORMANCE — which content types and channels are getting the most engagement; call out top and worst posts if data is available
5. PIPELINE HEALTH — state of the prospect pipeline, who's moving and who's stalled
6. APPROVAL RATE — comment and content approval patterns, anything worth noting
7. TRENDS — patterns emerging from the data that weren't visible last week
8. RECOMMENDATIONS FOR NEXT WEEK — 3 specific, actionable things to do differently or double down on
9. NORTH STAR METRIC — one single number or fact that best represents this week's performance

Be analytical and direct. Use plain text. Back every claim with a specific number from the data. Write like a sharp analyst delivering a board update — confident, data-driven, no fluff.`
    }]
  });

  return message.content[0].text;
}

function formatContentPerfRow(row) {
  const postsPublished = Number(row.posts_published || 0);
  const measuredCount = Number(row.measured_count || 0);
  const avgEngagement = row.avg_engagement == null ? null : Number(row.avg_engagement);
  const hasMeasuredAvg = measuredCount > 0 && Number.isFinite(avgEngagement);

  return {
    ...row,
    posts_published: postsPublished,
    measured_count: measuredCount,
    avg_engagement: hasMeasuredAvg ? avgEngagement : null,
    avg_engagement_display: hasMeasuredAvg
      ? `${avgEngagement.toFixed(4)} (${measuredCount} of ${postsPublished} measured)`
      : `0 of ${postsPublished} measured`,
  };
}

function parseStats(stats) {
  if (!stats) return null;
  if (typeof stats === 'object') return stats;
  try {
    return JSON.parse(stats);
  } catch (_) {
    return null;
  }
}

function engagementRate(stats) {
  const impressions = Number(stats?.impressions || 0);
  if (impressions <= 0) return null;
  return (
    Number(stats?.reactions || 0) +
    Number(stats?.comments || 0) +
    Number(stats?.saves || 0)
  ) / impressions;
}

function buildSlotTestVerdict(pairs) {
  const slot1Wins = pairs.filter(pair => pair.winner === 1).length;
  const slot2Wins = pairs.filter(pair => pair.winner === 2).length;
  const count = pairs.length;
  if (count < 6) {
    return `Slot test: ${count}/10 pairs collected, not yet conclusive (Slot 1 leading ${slot1Wins}-${slot2Wins}).`;
  }

  const leader = slot1Wins >= slot2Wins ? 1 : 2;
  const leaderWins = Math.max(slot1Wins, slot2Wins);
  if (leaderWins / count >= 0.7) {
    return `Slot test: Slot ${leader} wins (${slot1Wins}-${slot2Wins}). Recommend defaulting new posts to Slot ${leader}.`;
  }

  return `Slot test: inconclusive (${slot1Wins}-${slot2Wins} across ${count} pairs). Slot timing not a meaningful lever. Recommend closing the test.`;
}

async function getSlotTestVerdict() {
  try {
    const res = await pool.query(`
      SELECT id, format, slot, stats, posted_at, created_at
      FROM pending_comments
      WHERE client_id = $1
        AND channel = 'linkedin_personal'
        AND slot IN (1, 2)
        AND format IS NOT NULL
        AND stats IS NOT NULL
      ORDER BY format, COALESCE(posted_at, created_at), created_at
    `, [CLIENT_ID]);

    const byFormat = new Map();
    for (const row of res.rows) {
      if (!byFormat.has(row.format)) byFormat.set(row.format, { 1: [], 2: [] });
      byFormat.get(row.format)[Number(row.slot)].push(row);
    }

    const pairs = [];
    for (const group of byFormat.values()) {
      const count = Math.min(group[1].length, group[2].length);
      for (let i = 0; i < count; i++) {
        const slot1Rate = engagementRate(parseStats(group[1][i].stats));
        const slot2Rate = engagementRate(parseStats(group[2][i].stats));
        if (slot1Rate == null || slot2Rate == null) continue;
        pairs.push({
          winner: slot1Rate > slot2Rate ? 1 : slot2Rate > slot1Rate ? 2 : null,
          slot1Rate,
          slot2Rate,
        });
      }
    }

    return buildSlotTestVerdict(pairs.slice(0, 10));
  } catch (err) {
    console.warn('[Rex] Slot test verdict skipped:', err.message);
    return 'Slot test: 0/10 pairs collected, not yet conclusive (Slot 1 leading 0-0).';
  }
}

async function sendReport(reportText, data) {
  lastSendReportError = null;
  const totalActions = data.agentActivity.reduce((a, b) => a + parseInt(b.count), 0);
  const totalProspects = data.pipeline.reduce((a, b) => a + parseInt(b.count), 0);
  const pendingCount = data.pendingByChannel
    .filter(p => p.status === 'pending')
    .reduce((a, b) => a + parseInt(b.count), 0);

  const subject = `Pulseforge Weekly Report — Week of ${data.weekOf}`;

  const body = `PULSEFORGE WEEKLY PERFORMANCE REPORT
Week of ${data.weekOf}
${'═'.repeat(50)}

${reportText}

${data.slotTestVerdict || 'Slot test: 0/10 pairs collected, not yet conclusive (Slot 1 leading 0-0).'}

${'═'.repeat(50)}
${formatFunnelMetrics(data)}

${'═'.repeat(50)}
RAW NUMBERS THIS WEEK

Agent actions: ${totalActions}
Total prospects: ${totalProspects}
Pending approvals: ${pendingCount}

Channel breakdown:
${data.allTimeTouchpoints.map(c => `  ${c.channel}: ${c.count} total touchpoints`).join('\n') || '  No touchpoints yet'}

Pipeline:
${data.pipeline.map(p => `  ${p.status}: ${p.count} prospects`).join('\n') || '  No prospects yet'}

${'═'.repeat(50)}
Pulseforge · gopulseforge.com
Rex runs every Sunday. Reply to adjust reporting cadence.`;

  try {
    const response = await axios.post('https://api.brevo.com/v3/smtp/email', {
      sender: { name: 'Rex — Pulseforge', email: 'jacob@gopulseforge.com' },
      to: [{ email: 'jacob@gopulseforge.com', name: 'Jake Maynard' }],
      subject,
      textContent: body
    }, {
      headers: {
        'api-key': process.env.BREVO_API_KEY,
        'Content-Type': 'application/json'
      }
    });

    console.log('Report sent — Message ID:', response.data.messageId);
    return true;
  } catch (err) {
    lastSendReportError = captureErrorSample(err);
    console.error('Failed to send report:', lastSendReportError);
    return false;
  }
}

async function logAgentRun(status) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [AGENT_NAME, 'weekly_report', JSON.stringify({ week: new Date().toISOString(), client_id: CLIENT_ID }), status, CLIENT_ID]);
}

function pct(num, den) {
  return den > 0 ? +((Number(num || 0) / Number(den || 0)) * 100).toFixed(1) : 0;
}

function formatRejectionBreakdown(rejections = []) {
  if (!rejections.length) return '  None this week';
  return rejections.map(r => `  ${r.reason}: ${r.count}`).join('\n');
}

function formatFunnelMetrics(data) {
  const f = data.emailFunnel || {};
  const sent = Number(f.sent || 0);
  const opened = Number(f.opened || 0);
  const bounced = Number(f.bounced || 0) + Number(f.soft_bounced || 0);
  const replySent = Number(data.emailReplyMetrics?.sent || 0);
  const replies = Number(data.emailReplyMetrics?.replies || 0);

  return `FUNNEL METRICS
Open rate: ${pct(opened, sent)}% (${opened}/${sent})
Click rate: N/A (reply-driven sequences)
Reply rate: ${pct(replies, replySent)}% (${replies}/${replySent}) — primary engagement metric
Bounce rate: ${pct(bounced, sent)}% (${bounced}/${sent})
Warm signals generated this week: ${data.warmSignalsSent ?? 0}
Discovery calls booked this week: ${data.discoveryCallsBooked ?? 0}

Email rejection breakdown (Scout):
${formatRejectionBreakdown(data.emailRejections)}`;
}

function money(value) {
  return `$${Number(value || 0).toLocaleString()}`;
}

function marketName(row) {
  return row.name || row.business_name || `Client ${row.client_id}`;
}

async function getCrossMarketExecutiveData() {
  const weekAgo = "NOW() - INTERVAL '7 days'";
  const [marketOverview, hotSignals] = await Promise.all([
    pool.query(`
      WITH prospect_stats AS (
        SELECT
          client_id,
          COUNT(*)::int AS total_prospects,
          COUNT(*) FILTER (WHERE status = 'warm')::int AS warm_count,
          COUNT(*) FILTER (WHERE status = 'cold' AND COALESCE(do_not_contact, false) = false)::int AS cold_count,
          COUNT(*) FILTER (WHERE source = 'scout' AND created_at > ${weekAgo})::int AS scout_new_week,
          COUNT(*) FILTER (WHERE booked_at > ${weekAgo})::int AS booked_calls,
          COALESCE(SUM(mrr_value) FILTER (WHERE setter_status = 'closed'), 0)::numeric AS mrr
        FROM prospects
        GROUP BY client_id
      ),
      touchpoint_stats AS (
        SELECT
          client_id,
          COUNT(*) FILTER (WHERE channel = 'email' AND action_type = 'outbound' AND created_at > ${weekAgo})::int AS emails_sent_week,
          COUNT(*) FILTER (WHERE channel = 'email' AND action_type = 'email_opened' AND created_at > ${weekAgo})::int AS emails_opened_week,
          COUNT(*) FILTER (WHERE channel = 'email' AND action_type IN ('email_clicked', 'click') AND created_at > ${weekAgo})::int AS email_clicks_week,
          COUNT(*) FILTER (WHERE channel = 'email' AND action_type IN ('email_bounced', 'email_soft_bounce') AND created_at > ${weekAgo})::int AS email_bounces_week,
          COUNT(*) FILTER (WHERE channel = 'phone' AND created_at > ${weekAgo})::int AS setter_calls_week
        FROM touchpoints
        GROUP BY client_id
      ),
      post_stats AS (
        SELECT
          client_id,
          COUNT(*) FILTER (WHERE published_at > ${weekAgo})::int AS posts_published_week
        FROM post_analytics
        GROUP BY client_id
      )
      SELECT
        c.id AS client_id,
        c.name,
        c.business_name,
        COALESCE(ps.total_prospects, 0)::int AS total_prospects,
        COALESCE(ps.warm_count, 0)::int AS warm_count,
        COALESCE(ps.cold_count, 0)::int AS cold_count,
        COALESCE(ps.scout_new_week, 0)::int AS scout_new_week,
        COALESCE(ps.booked_calls, 0)::int AS booked_calls,
        COALESCE(ps.mrr, 0)::numeric AS mrr,
        COALESCE(ts.emails_sent_week, 0)::int AS emails_sent_week,
        COALESCE(ts.emails_opened_week, 0)::int AS emails_opened_week,
        COALESCE(ts.email_clicks_week, 0)::int AS email_clicks_week,
        COALESCE(ts.email_bounces_week, 0)::int AS email_bounces_week,
        COALESCE(ts.setter_calls_week, 0)::int AS setter_calls_week,
        COALESCE(pos.posts_published_week, 0)::int AS posts_published_week
      FROM clients c
      LEFT JOIN prospect_stats ps ON ps.client_id = c.id
      LEFT JOIN touchpoint_stats ts ON ts.client_id = c.id
      LEFT JOIN post_stats pos ON pos.client_id = c.id
      WHERE COALESCE(c.active, true) = true
      ORDER BY c.name ASC
    `),
    pool.query(`
      SELECT
        p.id,
        p.first_name,
        p.last_name,
        p.email,
        p.status,
        p.icp_score,
        p.notes,
        c.name AS client_name,
        co.name AS company_name
      FROM prospects p
      JOIN clients c ON c.id = p.client_id
      LEFT JOIN companies co ON co.id = p.company_id AND co.client_id = p.client_id
      WHERE COALESCE(p.is_hot, false) = true
      ORDER BY p.updated_at DESC NULLS LAST, p.created_at DESC
      LIMIT 20
    `).catch(() => ({ rows: [] })),
  ]);

  return {
    markets: marketOverview.rows.map(row => ({
      ...row,
      open_rate: pct(row.emails_opened_week, row.emails_sent_week),
      bounce_rate: pct(row.email_bounces_week, row.emails_sent_week),
      warm_signals_week: Number(row.emails_opened_week || 0) + Number(row.email_clicks_week || 0),
    })),
    hotSignals: hotSignals.rows,
  };
}

function formatCrossMarketExecutiveSummary(data) {
  const markets = data.markets || [];
  const hotSignals = data.hotSignals || [];
  const weekOf = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' });
  const topOpen = [...markets].sort((a, b) => b.open_rate - a.open_rate)[0];
  const topWarm = [...markets].sort((a, b) => b.warm_signals_week - a.warm_signals_week)[0];
  const thinnestCold = [...markets].sort((a, b) => Number(a.cold_count || 0) - Number(b.cold_count || 0))[0];
  const flags = [];

  for (const m of markets) {
    const name = marketName(m);
    if (m.bounce_rate > 10) flags.push(`${name}: ${m.bounce_rate}% bounce rate`);
    if (Number(m.setter_calls_week || 0) === 0) flags.push(`${name}: 0 setter calls logged`);
    if (Number(m.scout_new_week || 0) < 5) flags.push(`${name}: ${m.scout_new_week || 0} new Scout prospects this week`);
  }

  const marketLines = markets.length
    ? markets.map(m => {
      const name = marketName(m);
      return `${name}: ${m.total_prospects || 0} prospects, ${m.warm_count || 0} warm, ${m.emails_sent_week || 0} emails sent, ${m.open_rate}% open rate, ${m.booked_calls || 0} booked calls, ${money(m.mrr)} MRR.`;
    }).join('\n')
    : 'No active client markets found.';

  const hotLines = hotSignals.length
    ? hotSignals.map(p => {
      const prospect = `${p.first_name || ''} ${p.last_name || ''}`.trim() || p.email || 'Unknown prospect';
      const company = p.company_name || String(p.notes || '').split('—')[0].trim() || 'unknown company';
      return `${p.client_name}: ${prospect} at ${company}${p.icp_score != null ? ` (ICP ${p.icp_score})` : ''}`;
    }).join('\n')
    : 'No hot prospects currently flagged across markets.';

  return `REX WEEKLY EXECUTIVE SUMMARY
Generated ${weekOf}

MARKET OVERVIEW
${marketLines}

TOP PERFORMER
Highest open rate: ${topOpen ? `${marketName(topOpen)} at ${topOpen.open_rate}%` : 'n/a'}.
Most warm signals this week: ${topWarm ? `${marketName(topWarm)} with ${topWarm.warm_signals_week} opens/clicks` : 'n/a'}.

PIPELINE HEALTH
Thinnest cold prospect pool: ${thinnestCold ? `${marketName(thinnestCold)} with ${thinnestCold.cold_count || 0} cold prospects. Scout should prioritize this market if volume is below target.` : 'n/a'}

SETTER ACTIVITY
${markets.length ? markets.map(m => `${marketName(m)}: ${m.setter_calls_week || 0} calls logged this week`).join('\n') : 'No setter activity data found.'}

CONTENT
${markets.length ? markets.map(m => `${marketName(m)}: ${m.posts_published_week || 0} posts published this week`).join('\n') : 'No content publishing data found.'}

HOT SIGNALS
${hotLines}

FLAGS
${flags.length ? flags.join('\n') : 'No cross-market flags tripped this week.'}`;
}

async function logExecutiveSummary(summary) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [AGENT_NAME, 'executive_summary', JSON.stringify({ summary, generated_at: new Date().toISOString() }), 'success', CLIENT_ID]);
}

async function generateAndLogExecutiveSummary() {
  console.log('Generating cross-market executive summary...');
  const data = await getCrossMarketExecutiveData();
  const summary = formatCrossMarketExecutiveSummary(data);
  await logExecutiveSummary(summary);
  return summary;
}

async function run() {
  const runId = makeRunId();
  let attempts = 0;
  let successes = 0;
  let errorSample = null;

  console.log('\nRex agent running...\n');
  const clientConfig = await getClientConfig(CLIENT_ID);
  if (!clientConfig) throw new Error(`Active client not found: ${CLIENT_ID}`);
  if (CLIENT_ID !== 1) {
    console.log('Rex weekly reporting is enabled only for Pulseforge client_id=1.');
    await reportRexRun({ runId, attempts, successes, skipped: 0, errorSample });
    return;
  }

  try {
    attempts = 1;
    console.log('Pulling weekly data...');
    const data = await getWeeklyData();

    console.log('Generating report with Claude...');
    const report = await generateReport(data);

    console.log('\n--- REPORT PREVIEW ---');
    console.log(report);
    console.log('--- END PREVIEW ---\n');

    console.log('Sending report...');
    const sent = await sendReport(report, data);
    successes = sent ? 1 : 0;

    if (!sent) {
      errorSample = lastSendReportError || 'sendReport returned false';
      await logAgentRun('failed').catch(() => {});
      await reportRexRun({ runId, attempts, successes, skipped: 0, errorSample });
      console.log('\nRex failed: weekly report send did not complete.');
      return;
    }

    await logAgentRun('success');
    await generateAndLogExecutiveSummary();
    await reportRexRun({ runId, attempts, successes, skipped: 0, errorSample });
    console.log('\nRex complete.');
  } catch (err) {
    errorSample = errorSample || captureErrorSample(err);
    console.error('Rex error:', err.message);
    await logAgentRun('failed').catch(() => {});
    await reportRexRun({ runId, attempts, successes, skipped: 0, errorSample });
  }

}

module.exports = { run };

if (require.main === module) {
  run().catch(err => {
    console.error('[Rex] Fatal error:', err.message);
    process.exit(1);
  });
}
