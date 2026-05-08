require('dotenv').config();
const pool = require('./db');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');

const client = new Anthropic();
const AGENT_NAME = 'max';

async function getSystemSnapshot() {
  // Prospect breakdown by status
  const prospectStats = await pool.query(`
    SELECT status, COUNT(*) as count
    FROM prospects
    GROUP BY status
    ORDER BY count DESC
  `);

  // Recent touchpoints (last 7 days)
  const recentTouchpoints = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email,
      t.channel, t.action_type, t.content_summary,
      t.created_at
    FROM touchpoints t
    JOIN prospects p ON t.prospect_id = p.id
    WHERE t.created_at > NOW() - INTERVAL '7 days'
    ORDER BY t.created_at DESC
    LIMIT 20
  `);

  // Prospects with no touchpoints (never contacted)
  const untouched = await pool.query(`
    SELECT p.first_name, p.last_name, p.email, p.icp_score, p.status
    FROM prospects p
    WHERE NOT EXISTS (
      SELECT 1 FROM touchpoints t WHERE t.prospect_id = p.id
    )
    AND p.do_not_contact = false
    AND p.email IS NOT NULL
    ORDER BY p.icp_score DESC
    LIMIT 10
  `);

  // Prospects touched but gone cold (no activity in 14+ days)
  const cold = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email, p.status,
      MAX(t.created_at) as last_touch,
      COUNT(t.id) as touch_count
    FROM prospects p
    JOIN touchpoints t ON t.prospect_id = p.id
    WHERE p.do_not_contact = false
    GROUP BY p.id, p.first_name, p.last_name, p.email, p.status
    HAVING MAX(t.created_at) < NOW() - INTERVAL '14 days'
    ORDER BY last_touch ASC
    LIMIT 10
  `);

  // Pending comments awaiting approval
  const pending = await pool.query(`
    SELECT channel, COUNT(*) as count
    FROM pending_comments
    WHERE status = 'pending'
    GROUP BY channel
  `);

  // Channel performance
  const channelStats = await pool.query(`
    SELECT channel, COUNT(*) as total
    FROM touchpoints
    GROUP BY channel
    ORDER BY total DESC
  `);

  // Recent posts published (last 2 days) with engagement data
  const recentPosts = await pool.query(`
    SELECT channel, content_type, engagement_rate, likes, comments, shares, reach,
           metrics_fetched_at
    FROM post_analytics
    WHERE published_at > NOW() - INTERVAL '2 days'
    ORDER BY published_at DESC
    LIMIT 10
  `).catch(() => ({ rows: [] }));

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
    WHERE published_at > NOW() - INTERVAL '7 days'
    GROUP BY channel
  `).catch(() => ({ rows: [] }));

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
  };
}

async function generateInsights(snapshot) {
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
2. TOP PRIORITIES — the 3 most important actions to take today, ranked
3. CONTENT INTELLIGENCE — which content types and channels are performing above or below average based on post_analytics; flag any channel that hasn't posted in 7+ days; note if performance data is still accumulating if the tables are empty
4. WARM SIGNALS — any prospects showing signs of interest worth flagging
5. RECOMMENDATIONS — 2-3 strategic suggestions based on the data patterns
6. WATCH LIST — anything that needs attention or looks off

Be direct, specific, and actionable. Use plain text, no markdown. Keep each section to 2-4 sentences max. Write like a sharp operations manager giving a morning briefing.`
    }]
  });

  return message.content[0].text;
}

async function sendDigest(digestText, snapshot) {
  const pendingSummary = snapshot.pending
    .map(p => `${p.count} ${p.channel}`)
    .join(', ') || 'none';

  const subject = `Pulseforge Daily Digest — ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}`;

  const body = `PULSEFORGE DAILY DIGEST
${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}
${'─'.repeat(50)}

${digestText}

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
      to: [{ email: 'jacob@gopulseforge.com', name: 'Jake Maynard' }],
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
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
    VALUES ($1, $2, $3, $4, NOW())
  `, [AGENT_NAME, 'daily_digest', JSON.stringify({ insights: insights.slice(0, 200) }), 'success']);
}

async function run() {
  console.log('\nMax agent running...\n');

  try {
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

    console.log('\nMax complete.');
  } catch (err) {
    console.error('Max error:', err.message);
  }

}

run();