require('dotenv').config();
const pool = require('./db');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

const client = new Anthropic();
const AGENT_NAME = 'max';
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;

async function getSystemSnapshot() {
  // Prospect breakdown by status
  const prospectStats = await pool.query(`
    SELECT status, COUNT(*) as count
    FROM prospects
    WHERE client_id = $1
    GROUP BY status
    ORDER BY count DESC
  `, [CLIENT_ID]);

  // Recent touchpoints (last 7 days)
  const recentTouchpoints = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email,
      t.channel, t.action_type, t.content_summary,
      t.created_at
    FROM touchpoints t
    JOIN prospects p ON t.prospect_id = p.id
    WHERE t.created_at > NOW() - INTERVAL '7 days'
      AND t.client_id = $1
      AND p.client_id = $1
    ORDER BY t.created_at DESC
    LIMIT 20
  `, [CLIENT_ID]);

  // Prospects with no touchpoints (never contacted)
  const untouched = await pool.query(`
    SELECT p.first_name, p.last_name, p.email, p.icp_score, p.status
    FROM prospects p
    WHERE NOT EXISTS (
      SELECT 1 FROM touchpoints t WHERE t.prospect_id = p.id
    )
    AND p.do_not_contact = false
    AND p.email IS NOT NULL
    AND p.client_id = $1
    ORDER BY p.icp_score DESC
    LIMIT 10
  `, [CLIENT_ID]);

  // Prospects touched but gone cold (no activity in 14+ days)
  const cold = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email, p.status,
      MAX(t.created_at) as last_touch,
      COUNT(t.id) as touch_count
    FROM prospects p
    JOIN touchpoints t ON t.prospect_id = p.id AND t.client_id = p.client_id
    WHERE p.do_not_contact = false
      AND p.client_id = $1
    GROUP BY p.id, p.first_name, p.last_name, p.email, p.status
    HAVING MAX(t.created_at) < NOW() - INTERVAL '14 days'
    ORDER BY last_touch ASC
    LIMIT 10
  `, [CLIENT_ID]);

  // Pending comments awaiting approval
  const pending = await pool.query(`
    SELECT channel, COUNT(*) as count
    FROM pending_comments
    WHERE status = 'pending' AND client_id = $1
    GROUP BY channel
  `, [CLIENT_ID]);

  // Channel performance
  const channelStats = await pool.query(`
    SELECT channel, COUNT(*) as total
    FROM touchpoints
    WHERE client_id = $1
    GROUP BY channel
    ORDER BY total DESC
  `, [CLIENT_ID]);

  // Recent posts published (last 2 days) with engagement data
  const recentPosts = await pool.query(`
    SELECT channel, content_type, engagement_rate, likes, comments, shares, reach,
           metrics_fetched_at
    FROM post_analytics
    WHERE client_id = $1 AND published_at > NOW() - INTERVAL '2 days'
    ORDER BY published_at DESC
    LIMIT 10
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Best performing content type per channel (all time)
  const bestContentTypes = await pool.query(`
    SELECT channel, content_type, avg_engagement_rate, post_count
    FROM content_performance_summary
    WHERE post_count >= 2
    ORDER BY channel, avg_engagement_rate DESC
  `).catch(() => ({ rows: [] }));

  // Channel posting frequency: posts in last 7 days vs expected (4 channels × 1/week)
  const postFreq = await pool.query(`
    SELECT channel, COUNT(*) AS posts_this_week
    FROM post_analytics
    WHERE client_id = $1 AND published_at > NOW() - INTERVAL '7 days'
    GROUP BY channel
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Prospects clicked a link today — high priority signals
  const clickedToday = await pool.query(`
    SELECT DISTINCT p.id, p.first_name, p.last_name, c.name as company_name
    FROM touchpoints t
    JOIN prospects p ON t.prospect_id = p.id
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE t.action_type = 'email_clicked'
      AND t.client_id = $1
      AND p.client_id = $1
      AND t.created_at > NOW() - INTERVAL '1 day'
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Warm upgrades today
  const warmToday = await pool.query(`
    SELECT COUNT(*)::int AS count FROM prospects
    WHERE status = 'warm'
      AND client_id = $1
      AND updated_at > NOW() - INTERVAL '1 day'
      AND EXISTS (
        SELECT 1 FROM touchpoints t WHERE t.prospect_id = prospects.id
          AND t.client_id = prospects.client_id
          AND t.action_type = 'email_clicked'
      )
  `, [CLIENT_ID]).catch(() => ({ rows: [{ count: 0 }] }));

  // Email open rate this week
  const emailStats = await pool.query(`
    SELECT
      COUNT(CASE WHEN action_type = 'outbound'      THEN 1 END)::int AS sent,
      COUNT(CASE WHEN action_type = 'email_opened'  THEN 1 END)::int AS opened,
      COUNT(CASE WHEN action_type = 'email_clicked' THEN 1 END)::int AS clicked
    FROM touchpoints
    WHERE channel = 'email' AND client_id = $1 AND created_at > NOW() - INTERVAL '7 days'
  `, [CLIENT_ID]).catch(() => ({ rows: [{ sent: 0, opened: 0, clicked: 0 }] }));

  const contentQuality = await pool.query(`
    SELECT
      ROUND(AVG(COALESCE(NULLIF(payload->>'total', '')::int, NULLIF(payload->'scores'->>'total', '')::int))::numeric, 1) AS avg_score,
      COUNT(*) FILTER (WHERE payload->>'regenerated' = 'true')::int AS regenerated_count,
      COUNT(*)::int AS total_scored,
      MODE() WITHIN GROUP (ORDER BY NULLIF(payload->>'weak_dimension', '')) AS most_common_weakness
    FROM agent_log
    WHERE agent_name = 'paige'
      AND action = 'content_scored'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '7 days'
  `, [CLIENT_ID]).catch(() => ({ rows: [{ avg_score: null, regenerated_count: 0, total_scored: 0, most_common_weakness: null }] }));

  const closerMetrics = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE p.booked_at >= date_trunc('week', NOW()))::int AS booked_week,
      COUNT(*) FILTER (WHERE p.closer_status IN ('showed','closed') OR p.setter_status = 'closed')::int AS showed,
      COUNT(*) FILTER (WHERE p.booked_at IS NOT NULL)::int AS booked_total,
      COUNT(*) FILTER (WHERE p.setter_status = 'closed')::int AS closed,
      COALESCE(SUM(p.mrr_value) FILTER (
        WHERE p.setter_status = 'closed'
          AND p.closed_at >= date_trunc('month', NOW())
      ), 0)::numeric AS mrr_this_month,
      COALESCE(SUM(c.commission_amt) FILTER (WHERE c.status = 'pending'), 0)::numeric AS pending_commissions
    FROM prospects p
    LEFT JOIN commissions c ON c.prospect_id = p.id
    WHERE p.client_id = $1
  `, [CLIENT_ID]).catch(() => ({ rows: [{
    booked_week: 0,
    showed: 0,
    booked_total: 0,
    closed: 0,
    mrr_this_month: 0,
    pending_commissions: 0,
  }] }));

  return {
    prospectStats: prospectStats.rows,
    recentTouchpoints: recentTouchpoints.rows,
    untouched: untouched.rows,
    cold: cold.rows,
    pending: pending.rows,
    channelStats: channelStats.rows,
    recentPosts: recentPosts.rows,
    bestContentTypes: bestContentTypes.rows,
    postFreq: postFreq.rows,
    clickedToday:  clickedToday.rows,
    warmToday:     warmToday.rows[0]?.count || 0,
    emailStats:    emailStats.rows[0],
    contentQuality: contentQuality.rows[0],
    closerMetrics: closerMetrics.rows[0],
    client: CLIENT_CONFIG,
  };
}

async function generateInsights(snapshot) {
  if (CLIENT_ID === 2) {
    const closer = snapshot.closerMetrics || {};
    const newLeads = snapshot.prospectStats.reduce((sum, r) => sum + parseInt(r.count || 0), 0);
    const cities = [...new Set(snapshot.untouched
      .map(p => (p.notes || '').match(/\b(Charleston|Dunbar|St Albans|Scott Depot|Teays Valley|Hurricane|Huntington|Barboursville)\b/i)?.[0])
      .filter(Boolean))].join(', ') || 'your service area';
    const outbound = snapshot.emailStats?.sent || 0;
    const replies = snapshot.recentTouchpoints.filter(t => ['reply', 'inbound', 'email_reply'].includes(t.action_type)).length;
    const pending = snapshot.pending.reduce((sum, p) => sum + parseInt(p.count || 0), 0);
    const actionItems = [];
    if (pending) actionItems.push(`${pending} post${pending === 1 ? '' : 's'} pending your approval`);
    if (snapshot.clickedToday.length) actionItems.push(`${snapshot.clickedToday.length} warm lead${snapshot.clickedToday.length === 1 ? '' : 's'} clicked an email`);
    if (!actionItems.length) actionItems.push('No urgent items right now');

    return `Good morning Brad & Dustin — here's your MSHI update for ${new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}

LEADS: ${newLeads} total leads in the system; newest focus is ${cities}
OUTREACH: ${outbound} emails sent in the last 7 days, ${replies} replies detected
REVIEWS: No new review activity flagged in this snapshot
CONTENT: ${pending} posts pending your approval
CLOSER: ${closer.booked_week || 0} booked calls this week, $${Number(closer.mrr_this_month || 0).toLocaleString()} MRR closed this month
ACTION NEEDED: ${actionItems.join('; ')}

— Pulseforge`;
  }

  const dataString = JSON.stringify(snapshot, null, 2);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-5',
    max_tokens: 1000,
    messages: [{
      role: 'user',
      content: `Today's date is ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}. You are Max, the manager agent for Pulseforge — an AI marketing system for local small businesses.

Here is today's second brain snapshot:

${dataString}

Known fixes already implemented (do not flag these as issues):
- Duplicate send deduplication is fixed — touchpoints are logged correctly
- Email copy has been completely rewritten with operator-angle messaging
- All 5 Brevo vertical sequences updated with new copy
- Riley inbound triage agent is now live and monitoring the inbox
- RUN buttons on dashboard are working

Generate a concise daily digest with:
1. SYSTEM STATUS — brief overview of what's happening across all agents
2. TOP PRIORITIES — the 3 most important actions to take today, ranked; if any prospects are in clickedToday, flag them as URGENT: "🔥 [Business] clicked a link in your email — Cal or Sam should follow up today"
3. CONTENT INTELLIGENCE — which content types and channels are performing above or below average based on post_analytics; flag any channel that hasn't posted in 7+ days; note if performance data is still accumulating if the tables are empty; email open rate this week and Paige content quality if data available
4. WARM SIGNALS — any prospects showing signs of interest worth flagging; include email opens/clicks from today
5. CLOSER METRICS — include booked calls this week, show rate, close rate, MRR closed this month, and pending commission liability
6. RECOMMENDATIONS — 2-3 strategic suggestions based on the data patterns
7. WATCH LIST — anything that needs attention or looks off

Be direct, specific, and actionable. Use plain text, no markdown. Keep each section to 2-4 sentences max. Write like a sharp operations manager giving a morning briefing.`
    }]
  });

  return message.content[0].text;
}

async function sendDigest(digestText, snapshot) {
  const pendingSummary = snapshot.pending
    .map(p => `${p.count} ${p.channel}`)
    .join(', ') || 'none';
  const contentQuality = snapshot.contentQuality || {};
  const closer = snapshot.closerMetrics || {};
  const bookedTotal = Number(closer.booked_total || 0);
  const showed = Number(closer.showed || 0);
  const closed = Number(closer.closed || 0);
  const closerBlock = `CLOSER METRICS
Booked calls this week: ${closer.booked_week || 0}
Show rate: ${bookedTotal ? ((showed / bookedTotal) * 100).toFixed(1) : 0}%
Close rate: ${showed ? ((closed / showed) * 100).toFixed(1) : 0}%
MRR closed this month: $${Number(closer.mrr_this_month || 0).toLocaleString()}
Pending commission liability: $${Number(closer.pending_commissions || 0).toLocaleString()}`;
  const contentQualityBlock = `CONTENT QUALITY (last 7 days):
Avg score: ${contentQuality.avg_score ?? 'n/a'}/30
Posts regenerated: ${contentQuality.regenerated_count || 0}/${contentQuality.total_scored || 0}
Most common weakness: ${contentQuality.most_common_weakness || 'none'}`;

  const toEmail = CLIENT_CONFIG?.max_email || 'jacob@gopulseforge.com';
  const toName = CLIENT_ID === 2 ? 'Brad & Dustin' : 'Jake Maynard';
  const subject = `${CLIENT_ID === 2 ? 'MSHI' : 'Pulseforge'} Daily Digest — ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}`;

  const body = CLIENT_ID === 2 ? `${digestText}

Pulseforge · gopulseforge.com` : `PULSEFORGE DAILY DIGEST
${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}
${'─'.repeat(50)}

${digestText}

${'─'.repeat(50)}
${closerBlock}

${'─'.repeat(50)}
${contentQualityBlock}

${'─'.repeat(50)}
QUICK STATS
Prospects in system: ${snapshot.prospectStats.reduce((a, b) => a + parseInt(b.count), 0)}
Pending approvals: ${pendingSummary}
Touchpoints this week: ${snapshot.recentTouchpoints.length}
Untouched prospects: ${snapshot.untouched.length}
Gone cold (14+ days): ${snapshot.cold.length}

${'─'.repeat(50)}
Pulseforge · gopulseforge.com
To adjust digest frequency reply to this email.`;

  try {
    const response = await axios.post('https://api.brevo.com/v3/smtp/email', {
      sender: { name: 'Max — Pulseforge', email: 'jacob@gopulseforge.com' },
      to: [{ email: toEmail, name: toName }],
      subject,
      textContent: body
    }, {
      headers: {
        'api-key': process.env.BREVO_API_KEY,
        'Content-Type': 'application/json'
      }
    });

    console.log('Digest sent — Message ID:', response.data.messageId);
    return true;
  } catch (err) {
    console.error('Failed to send digest:', err.response?.data || err.message);
    return false;
  }
}

async function logAgentRun(insights) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [AGENT_NAME, 'daily_digest', JSON.stringify({ insights: insights.slice(0, 2000), client_id: CLIENT_ID }), 'success', CLIENT_ID]);
}

async function createActions(snapshot) {
  const actions = [];

  // Clicked-not-warm: prospects who clicked but aren't warm yet
  for (const p of snapshot.clickedToday) {
    actions.push({
      action_type: 'follow_up_clicked',
      title: `Follow up with ${p.first_name} ${p.last_name}`,
      description: `${p.first_name} at ${p.company_name || 'their company'} clicked a link in your email today. Cal or Sam should reach out now while they're hot.`,
      payload: { prospect_id: p.id, first_name: p.first_name, last_name: p.last_name, company: p.company_name },
    });
  }

  // Agents idle 48h+
  try {
    const idleAgents = await pool.query(`
      SELECT agent_name, MAX(ran_at) as last_run
      FROM agent_log
      WHERE agent_name NOT IN ('riley')
        AND client_id = $1
      GROUP BY agent_name
      HAVING MAX(ran_at) < NOW() - INTERVAL '48 hours'
      ORDER BY last_run ASC
    `, [CLIENT_ID]);
    for (const a of idleAgents.rows) {
      actions.push({
        action_type: 'agent_idle',
        title: `${a.agent_name} hasn't run in 48+ hours`,
        description: `${a.agent_name} last ran ${new Date(a.last_run).toLocaleDateString()}. Check the cron schedule or run it manually from the dashboard.`,
        payload: { agent_name: a.agent_name, last_run: a.last_run },
      });
    }
  } catch (_) {}

  // >10 untouched prospects
  if (snapshot.untouched.length >= 10) {
    actions.push({
      action_type: 'untouched_backlog',
      title: `${snapshot.untouched.length} prospects never contacted`,
      description: `There are ${snapshot.untouched.length} prospects with no touchpoints. Run Emmett or trigger outreach to start working the pipeline.`,
      payload: { count: snapshot.untouched.length },
    });
  }

  // Cold 14d+ prospects
  if (snapshot.cold.length >= 5) {
    actions.push({
      action_type: 'cold_prospects',
      title: `${snapshot.cold.length} prospects gone cold (14+ days)`,
      description: `${snapshot.cold.length} prospects haven't been touched in 14+ days. Consider a re-engagement sequence or clean them out of the pipeline.`,
      payload: { count: snapshot.cold.length },
    });
  }

  // >5 pending approvals
  const totalPending = snapshot.pending.reduce((acc, p) => acc + parseInt(p.count), 0);
  if (totalPending >= 5) {
    const breakdown = snapshot.pending.map(p => `${p.count} ${p.channel}`).join(', ');
    actions.push({
      action_type: 'pending_approvals',
      title: `${totalPending} posts waiting for approval`,
      description: `Pending queue has ${totalPending} items (${breakdown}). Review and approve to keep the content calendar moving.`,
      payload: { total: totalPending, breakdown: snapshot.pending },
    });
  }

  if (actions.length === 0) return;

  for (const action of actions) {
    try {
      await pool.query(`
        INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
        VALUES ('max', $1, $2, $3, $4, 'pending', $5)
        ON CONFLICT ON CONSTRAINT agent_actions_action_type_payload_key DO NOTHING
      `, [action.action_type, action.title, action.description, JSON.stringify(action.payload), CLIENT_ID]);
    } catch (_) {
      // Fallback if constraint doesn't exist — just insert
      try {
        await pool.query(`
          INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
          SELECT 'max', $1, $2, $3, $4, 'pending', $5
          WHERE NOT EXISTS (
            SELECT 1 FROM agent_actions
            WHERE action_type = $1 AND status = 'pending' AND client_id = $5
              AND created_at > NOW() - INTERVAL '24 hours'
          )
        `, [action.action_type, action.title, action.description, JSON.stringify(action.payload), CLIENT_ID]);
      } catch (e2) {
        console.error('[Max] createActions insert error:', e2.message);
      }
    }
  }
  console.log(`[Max] Deposited ${actions.length} action(s) into agent_actions`);
}

async function run() {
  console.log('\nMax agent running...\n');

  try {
    CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
    if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);
    console.log('Reading second brain...');
    const snapshot = await getSystemSnapshot();

    console.log('Generating insights with Claude...');
    const insights = await generateInsights(snapshot);

    console.log('\n--- DIGEST PREVIEW ---');
    console.log(insights);
    console.log('--- END PREVIEW ---\n');

    console.log('Sending digest...');
    await sendDigest(insights, snapshot);

    await logAgentRun(insights);

    console.log('Creating action items...');
    await createActions(snapshot);

    console.log('\nMax complete.');
  } catch (err) {
    console.error('Max error:', err.message);
  }

}

run();
