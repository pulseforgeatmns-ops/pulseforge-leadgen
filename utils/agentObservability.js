require('dotenv').config();

const axios = require('axios');
const pool = require('../db');
const { sendOpsAlertSMS } = require('../samAgent');

const ALERT_COOLDOWN_INTERVAL = '2 hours';
const ALERT_COOLDOWN_MS = 2 * 60 * 60 * 1000;
const MAX_ERROR_SAMPLE_CHARS = 1200;

function normalizeCount(value, field) {
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) {
    throw new Error(`${field} must be a non-negative number`);
  }
  return Math.trunc(number);
}

function normalizeErrorSample(errorSample) {
  if (errorSample === undefined || errorSample === null || errorSample === '') return null;

  if (typeof errorSample === 'string') {
    return errorSample.slice(0, MAX_ERROR_SAMPLE_CHARS);
  }

  try {
    const serialized = JSON.stringify(errorSample);
    if (!serialized) return String(errorSample).slice(0, MAX_ERROR_SAMPLE_CHARS);
    return JSON.parse(serialized.slice(0, MAX_ERROR_SAMPLE_CHARS));
  } catch (_err) {
    return String(errorSample).slice(0, MAX_ERROR_SAMPLE_CHARS);
  }
}

function classifyRun(attempts, successes) {
  if (attempts === 0) return 'idle';
  if (successes > 0) return 'working';
  if (attempts > 0 && successes === 0) return 'stranded';
  return 'working';
}

function formatErrorSample(errorSample) {
  if (errorSample === null || errorSample === undefined) return 'none';
  if (typeof errorSample === 'string') return errorSample;
  try {
    return JSON.stringify(errorSample);
  } catch (_err) {
    return String(errorSample);
  }
}

function buildAlertBody({ agent, clientId, runId, attempts, errorSample, timestamp }) {
  return [
    'Pulseforge stranded-agent alert',
    '',
    `Agent: ${agent}`,
    `Client ID: ${clientId ?? 'none'}`,
    `Run ID: ${runId}`,
    `Attempts: ${attempts}`,
    `Timestamp: ${timestamp}`,
    '',
    `Error sample: ${formatErrorSample(errorSample)}`,
  ].join('\n');
}

async function ensureAgentObservabilitySchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS agent_run_health (
      id BIGSERIAL PRIMARY KEY,
      agent TEXT NOT NULL,
      client_id INTEGER,
      run_id TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      successes INTEGER NOT NULL DEFAULT 0,
      skipped INTEGER NOT NULL DEFAULT 0,
      state TEXT NOT NULL CHECK (state IN ('idle', 'working', 'stranded')),
      error_sample JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_agent_run_health_agent_client_created
    ON agent_run_health (agent, client_id, created_at DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS agent_alert_dispatches (
      id BIGSERIAL PRIMARY KEY,
      agent TEXT NOT NULL,
      client_id INTEGER,
      run_id TEXT NOT NULL,
      channel TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      error_sample JSONB,
      sent BOOLEAN NOT NULL DEFAULT FALSE,
      suppressed BOOLEAN NOT NULL DEFAULT FALSE,
      failure_reason TEXT,
      sent_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_agent_alert_dispatches_cooldown
    ON agent_alert_dispatches (agent, client_id, sent_at DESC)
    WHERE sent = TRUE
  `);
}

async function persistRunHealth(record) {
  const result = await pool.query(`
    INSERT INTO agent_run_health (
      agent,
      client_id,
      run_id,
      attempts,
      successes,
      skipped,
      state,
      error_sample
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
    RETURNING *
  `, [
    record.agent,
    record.clientId,
    record.runId,
    record.attempts,
    record.successes,
    record.skipped,
    record.state,
    record.errorSample === null ? null : JSON.stringify(record.errorSample),
  ]);
  return result.rows[0];
}

async function getLastSentAlert(agent, clientId) {
  const result = await pool.query(`
    SELECT id, run_id, channel, sent_at
    FROM agent_alert_dispatches
    WHERE agent = $1
      AND client_id IS NOT DISTINCT FROM $2
      AND sent = TRUE
      AND sent_at > NOW() - INTERVAL '2 hours'
    ORDER BY sent_at DESC
    LIMIT 1
  `, [agent, clientId]);
  return result.rows[0] || null;
}

async function recordAlertDispatch({
  agent,
  clientId,
  runId,
  channel,
  attempts,
  errorSample,
  sent,
  suppressed = false,
  failureReason = null,
}) {
  const result = await pool.query(`
    INSERT INTO agent_alert_dispatches (
      agent,
      client_id,
      run_id,
      channel,
      attempts,
      error_sample,
      sent,
      suppressed,
      failure_reason,
      sent_at
    )
    VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, CASE WHEN $7 THEN NOW() ELSE NULL END)
    RETURNING *
  `, [
    agent,
    clientId,
    runId,
    channel,
    attempts,
    errorSample === null ? null : JSON.stringify(errorSample),
    sent,
    suppressed,
    failureReason,
  ]);
  return result.rows[0];
}

async function sendOpsAlertEmail(subject, body) {
  if (!process.env.BREVO_API_KEY) {
    return { success: false, error: 'BREVO_API_KEY not configured' };
  }
  if (!process.env.OPS_ALERT_EMAIL) {
    return { success: false, error: 'OPS_ALERT_EMAIL not configured' };
  }

  const payload = {
    sender: {
      name: process.env.OPS_ALERT_FROM_NAME || 'Pulseforge Observability',
      email: process.env.OPS_ALERT_FROM_EMAIL || process.env.FROM_EMAIL || 'jacob@gopulseforge.com',
    },
    to: [{ email: process.env.OPS_ALERT_EMAIL, name: process.env.OPS_ALERT_NAME || 'Pulseforge Ops' }],
    subject,
    textContent: body,
    htmlContent: `<html><body style="font-family:Arial,sans-serif;font-size:15px;line-height:1.5;white-space:pre-wrap;">${escapeHtml(body)}</body></html>`,
  };

  try {
    const response = await axios.post('https://api.brevo.com/v3/smtp/email', payload, {
      headers: { 'api-key': process.env.BREVO_API_KEY, 'Content-Type': 'application/json' },
      timeout: 15000,
    });
    return {
      success: true,
      messageId: response.data?.messageId || response.data?.messageID || null,
      brevoResponse: response.data || null,
    };
  } catch (err) {
    return { success: false, error: err.response?.data || err.message };
  }
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function dispatchAlert({ agent, clientId, runId, attempts, errorSample }, options = {}) {
  if (!agent) throw new Error('agent is required');
  if (!runId) throw new Error('runId is required');
  const normalizedAgent = String(agent);
  const normalizedAttempts = normalizeCount(attempts, 'attempts');
  const normalizedErrorSample = normalizeErrorSample(errorSample);

  await ensureAgentObservabilitySchema();

  const lastAlert = await getLastSentAlert(normalizedAgent, clientId);
  if (lastAlert) {
    await recordAlertDispatch({
      agent: normalizedAgent,
      clientId,
      runId,
      channel: 'suppressed',
      attempts: normalizedAttempts,
      errorSample: normalizedErrorSample,
      sent: false,
      suppressed: true,
      failureReason: `cooldown active until ${new Date(new Date(lastAlert.sent_at).getTime() + ALERT_COOLDOWN_MS).toISOString()}`,
    });
    return { sent: false, suppressed: true, reason: 'cooldown', lastAlert };
  }

  const timestamp = new Date().toISOString();
  const body = buildAlertBody({
    agent: normalizedAgent,
    clientId,
    runId,
    attempts: normalizedAttempts,
    errorSample: normalizedErrorSample,
    timestamp,
  });
  const subject = `[Pulseforge] ${normalizedAgent} stranded for client ${clientId ?? 'none'}`;

  if (normalizedAgent === 'sam') {
    const emailResult = await sendOpsAlertEmail(subject, body);
    await recordAlertDispatch({
      agent: normalizedAgent,
      clientId,
      runId,
      channel: 'email',
      attempts: normalizedAttempts,
      errorSample: normalizedErrorSample,
      sent: emailResult.success,
      failureReason: emailResult.success ? null : formatErrorSample(emailResult.error),
    });
    return { sent: emailResult.success, channel: 'email', result: emailResult };
  }

  const smsResult = await sendOpsAlertSMS(body, { to: options.smsToOverride });
  if (smsResult.sent) {
    await recordAlertDispatch({
      agent: normalizedAgent,
      clientId,
      runId,
      channel: 'sms',
      attempts: normalizedAttempts,
      errorSample: normalizedErrorSample,
      sent: true,
    });
    return { sent: true, channel: 'sms', result: smsResult };
  }

  const emailResult = await sendOpsAlertEmail(subject, body);
  await recordAlertDispatch({
    agent: normalizedAgent,
    clientId,
    runId,
    channel: 'email',
    attempts: normalizedAttempts,
    errorSample: normalizedErrorSample,
    sent: emailResult.success,
    failureReason: emailResult.success
      ? null
      : `sms failed: ${smsResult.reason || 'unknown'}; email failed: ${formatErrorSample(emailResult.error)}`,
  });

  return {
    sent: emailResult.success,
    channel: 'email',
    fallback: true,
    smsResult,
    result: emailResult,
  };
}

async function reportAgentRun({ agent, clientId, runId, attempts, successes, skipped, errorSample }, options = {}) {
  if (!agent) throw new Error('agent is required');
  if (!runId) throw new Error('runId is required');

  const normalizedAttempts = normalizeCount(attempts, 'attempts');
  const normalizedSuccesses = normalizeCount(successes, 'successes');
  const normalizedSkipped = normalizeCount(skipped, 'skipped');
  const normalizedErrorSample = normalizeErrorSample(errorSample);
  const state = classifyRun(normalizedAttempts, normalizedSuccesses);

  await ensureAgentObservabilitySchema();

  const runRecord = await persistRunHealth({
    agent: String(agent),
    clientId,
    runId: String(runId),
    attempts: normalizedAttempts,
    successes: normalizedSuccesses,
    skipped: normalizedSkipped,
    state,
    errorSample: normalizedErrorSample,
  });

  if (state !== 'stranded') {
    return { state, runRecord, alert: null };
  }

  const alert = await dispatchAlert({
    agent: String(agent),
    clientId,
    runId: String(runId),
    attempts: normalizedAttempts,
    errorSample: normalizedErrorSample,
  }, options);

  return { state, runRecord, alert };
}

module.exports = {
  ALERT_COOLDOWN_INTERVAL,
  ensureAgentObservabilitySchema,
  reportAgentRun,
  dispatchAlert,
};
