const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');
const router = express.Router();
const pool = require('../db');
const { normalizeClientId } = require('../utils/clientContext');
const { runScoutExpansionCron } = require('../scoutExpansion');

const anthropic = new Anthropic();

const CRON_MODULES = {
  scout:     '../leadgen',
  emmett:    '../emmettAgent',
  max:       '../maxAgent',
  rex:       '../rexAgent',
  sketch:    '../sketchAgent',
  paige:     '../paigeAgent',
  enrich_prospects: '../enrichProspects',
  faye:      '../facebookAgent',
  link:      '../linkedinAgent',
  sam:       '../samAgent',
  vera:      '../veraAgent',
  cal:       '../calAgent',
  cal_batch: '../calBatchAgent',
  penny:     '../pennyAgent',
  analytics: '../analyticsAgent',
  riley:       '../rileyAgent',
  warm_signal: '../warmSignalAgent',
  setter_handoff: '../setterHandoffAgent',
  sync_setter:    '../syncSetterLeadList',
  handoff_utility: '../setterHandoffAgent',
  mira_transcription: '../miraTranscriptionAgent',
  mira_classifier: '../miraClassifierAgent',
};

function runCronAgent(agent, res, query = {}) {
  if (!CRON_MODULES[agent]) return res.status(400).json({ error: `Unknown agent: ${agent}` });
  const clientId = normalizeClientId(query.client_id || query.clientId);
  res.json({ success: true, agent, client_id: clientId });
  try {
    delete require.cache[require.resolve(CRON_MODULES[agent])];
    process.env.ACTIVE_CLIENT_ID = String(clientId);
    const mod = require(CRON_MODULES[agent]);
    if (agent === 'scout' && typeof mod.run === 'function') {
      mod.run({
        industry: query.industry,
        location: query.location,
        maxResults: query.maxResults || query.max || query.limit,
        client_id: clientId,
      }).catch(err => {
        console.error(`[cron] scout run error:`, err.message);
      });
    } else if ((agent === 'setter_handoff' || agent === 'handoff_utility') && typeof mod.run === 'function') {
      mod.run({ lookbackDays: query.lookbackDays, client_id: clientId }).catch(err => {
        console.error(`[cron] ${agent} run error:`, err.message);
      });
    } else if (typeof mod.run === 'function') {
      mod.run({ client_id: clientId }).catch(err => {
        console.error(`[cron] ${agent} run error:`, err.message);
      });
    }
  } catch (err) {
    console.error(`[cron] ${agent} error:`, err.message);
  }
}

async function handleScoutExpansionCron(req, res) {
  const secret = req.body?.secret || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const clientId = normalizeClientId(req.query.client_id || req.query.clientId);
  try {
    const result = await runScoutExpansionCron(clientId);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[cron] scoutExpansion error:', err.message);
    res.status(500).json({ error: err.message });
  }
}

function extractClaudeText(message) {
  return (message.content || [])
    .filter(block => block && block.type === 'text' && typeof block.text === 'string')
    .map(block => block.text)
    .join('\n')
    .trim();
}

function parsePulseHealthJson(text) {
  try {
    return JSON.parse(text);
  } catch (_) {
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Claude response was not valid JSON');
    return JSON.parse(match[0]);
  }
}

async function handlePulseHealthCron(req, res) {
  const secret = req.body?.secret || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS monitor_state (
        id BIGSERIAL PRIMARY KEY,
        snapshot JSONB NOT NULL,
        digest TEXT,
        alert_level TEXT,
        checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const [
      sendsByAction,
      failuresByVertical,
      failureSpike,
      warmProspects,
      sentTodayByClient,
      recentErrors,
      previousSnapshot,
    ] = await Promise.all([
      pool.query(`
        SELECT client_id, action, COUNT(*)::int AS count
        FROM agent_log
        WHERE action IN ('email_sent', 'email_failed')
          AND ran_at >= NOW() - INTERVAL '24 hours'
        GROUP BY client_id, action
        ORDER BY client_id, action
      `),
      pool.query(`
        SELECT COALESCE(payload->>'vertical', 'unknown') AS vertical, COUNT(*)::int AS count
        FROM agent_log
        WHERE action = 'email_failed'
          AND ran_at >= NOW() - INTERVAL '24 hours'
        GROUP BY COALESCE(payload->>'vertical', 'unknown')
        ORDER BY count DESC, vertical
      `),
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE action = 'email_failed')::int AS email_failed_count,
          COUNT(*) FILTER (
            WHERE action = 'email_failed'
              AND (
                payload->>'error' ILIKE '%Connection timeout%'
                OR error_msg ILIKE '%Connection timeout%'
              )
          )::int AS timeout_count
        FROM agent_log
        WHERE ran_at >= NOW() - INTERVAL '2 hours'
          AND (
            action = 'email_failed'
            OR payload->>'error' ILIKE '%Connection timeout%'
            OR error_msg ILIKE '%Connection timeout%'
          )
      `),
      pool.query(`
        SELECT
          p.id,
          p.client_id,
          p.first_name,
          p.last_name,
          p.email,
          p.phone,
          p.vertical,
          p.icp_score,
          p.status,
          p.created_at,
          c.name AS company_name
        FROM prospects p
        LEFT JOIN companies c ON c.id = p.company_id
        WHERE p.status = 'warm'
        ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
        LIMIT 25
      `),
      pool.query(`
        SELECT client_id, COUNT(*)::int AS count
        FROM agent_log
        WHERE action = 'email_sent'
          AND DATE(ran_at) = CURRENT_DATE
        GROUP BY client_id
        ORDER BY client_id
      `),
      pool.query(`
        SELECT id, agent_name, action, payload, status, error_msg, ran_at, client_id
        FROM agent_log
        WHERE payload ? 'error'
          AND action != 'email_sent'
          AND ran_at >= NOW() - INTERVAL '24 hours'
        ORDER BY ran_at DESC
        LIMIT 15
      `),
      pool.query(`
        SELECT snapshot, digest, alert_level, checked_at
        FROM monitor_state
        ORDER BY checked_at DESC
        LIMIT 1
      `),
    ]);

    const metrics = {
      sends_by_action: sendsByAction.rows,
      failures_by_vertical: failuresByVertical.rows,
      failure_spike: failureSpike.rows[0] || { email_failed_count: 0, timeout_count: 0 },
      warm_prospects: warmProspects.rows,
      sent_today_by_client: sentTodayByClient.rows,
      recent_errors: recentErrors.rows,
      checked_at: new Date().toISOString(),
    };
    const previous = previousSnapshot.rows[0] || null;

    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 500,
      system: `You are the Pulseforge health monitor. Terse, signal-dense, no preamble.
Report to Jacob who wants the truth and one action item — not reassurance.
Historical context: email failures are logged with action='email_failed'.
If you see Connection timeouts only in rows where action='email_sent',
those are pre-fix stale data and must NOT be flagged as current failures.
Only count action='email_failed' rows as real failures.
Classify alert_level as: critical (zero sends in window, or any
Connection timeout), warn (failure spike, vertical drift, cap near 40),
or ok.
Respond with valid JSON only:
{ "alert_level": "ok|warn|critical", "digest": "one-line status",
  "report": "full 8-line report matching the PULSE HEALTH format" }`,
      messages: [
        {
          role: 'user',
          content: JSON.stringify({ current: metrics, previous }),
        },
      ],
    });

    const result = parsePulseHealthJson(extractClaudeText(message));
    const alertLevel = ['ok', 'warn', 'critical'].includes(result.alert_level)
      ? result.alert_level
      : 'warn';
    const digest = String(result.digest || 'Pulse health check completed.');

    await pool.query(
      `INSERT INTO monitor_state (snapshot, digest, alert_level)
       VALUES ($1, $2, $3)`,
      [metrics, digest, alertLevel]
    );

    await pool.query(
      `INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
       VALUES ($1, $2, $3, $4, NOW(), $5)`,
      [
        'pulse_health',
        'health_check',
        { alert_level: alertLevel, digest },
        alertLevel === 'critical' ? 'failed' : 'success',
        1,
      ]
    );

    return res.status(200).json({ ok: true, alert_level: alertLevel, digest });
  } catch (err) {
    console.error('[cron] pulse-health error:', err.message);
    try {
      await pool.query(
        `INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
         VALUES ($1, $2, $3, $4, $5, NOW(), $6)`,
        ['pulse_health', 'health_check', { error: err.message }, 'failed', err.message, 1]
      );
    } catch (logErr) {
      console.error('[cron] pulse-health agent_log error:', logErr.message);
    }
    return res.status(500).json({ ok: false, error: err.message });
  }
}

router.post('/cron/scoutExpansion', handleScoutExpansionCron);
router.get('/cron/scoutExpansion', handleScoutExpansionCron);
router.post('/cron/pulse-health', handlePulseHealthCron);
router.get('/cron/pulse-health', handlePulseHealthCron);

router.post('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.body?.secret || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res, req.query);
});

router.get('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res, req.query);
});

module.exports = router;
