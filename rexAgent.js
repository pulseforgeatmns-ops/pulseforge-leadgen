require('dotenv').config();
const pool = require('./db');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');

const client = new Anthropic();
const AGENT_NAME = 'rex_agent';

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
    mostNurtured: mostNurtured.rows
  };
}

async function generateReport(data) {
  const dataString = JSON.stringify(data, null, 2);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-5',
    max_tokens: 1500,
    messages: [{
      role: 'user',
      content: `You are Rex, the reporting agent for Pulseforge — an AI marketing system for local small businesses.

Here is this week's performance data from the second brain:

${dataString}

Generate a weekly performance report with these sections:

1. WEEK IN REVIEW — 2-3 sentences summarizing overall system activity and health
2. CHANNEL PERFORMANCE — how each channel (LinkedIn, Facebook, email) performed this week, what's working and what's lagging
3. PIPELINE HEALTH — state of the prospect pipeline, who's moving and who's stalled
4. APPROVAL RATE — comment and content approval patterns, anything worth noting
5. TRENDS — patterns emerging from the data that weren't visible last week
6. RECOMMENDATIONS FOR NEXT WEEK — 3 specific, actionable things to do differently or double down on
7. NORTH STAR METRIC — one single number or fact that best represents this week's performance

Be analytical and direct. Use plain text. Back every claim with a specific number from the data. Write like a sharp analyst delivering a board update — confident, data-driven, no fluff.`
    }]
  });

  return message.content[0].text;
}

async function sendReport(reportText, data) {
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
    console.error('Failed to send report:', err.response?.data || err.message);
    return false;
  }
}

async function logAgentRun(status) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
    VALUES ($1, $2, $3, $4, NOW())
  `, [AGENT_NAME, 'weekly_report', JSON.stringify({ week: new Date().toISOString() }), status]);
}

async function run() {
  console.log('\nRex agent running...\n');

  try {
    console.log('Pulling weekly data...');
    const data = await getWeeklyData();

    console.log('Generating report with Claude...');
    const report = await generateReport(data);

    console.log('\n--- REPORT PREVIEW ---');
    console.log(report);
    console.log('--- END PREVIEW ---\n');

    console.log('Sending report...');
    await sendReport(report, data);

    await logAgentRun('success');
    console.log('\nRex complete.');
  } catch (err) {
    console.error('Rex error:', err.message);
    await logAgentRun('error').catch(() => {});
  }

  pool.end();
}

run();