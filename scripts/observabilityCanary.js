require('dotenv').config();

const pool = require('../db');
const { reportAgentRun } = require('../utils/agentObservability');

function requireEnv(name) {
  if (!process.env[name]) throw new Error(`${name} is required for the observability canary`);
}

function preflight() {
  requireEnv('DATABASE_URL');
  requireEnv('TWILIO_ACCOUNT_SID');
  requireEnv('TWILIO_AUTH_TOKEN');
  if (!process.env.TWILIO_FROM && !process.env.TWILIO_PHONE_NUMBER) {
    throw new Error('TWILIO_FROM or TWILIO_PHONE_NUMBER is required for the observability canary');
  }
  requireEnv('OPS_ALERT_PHONE');
  requireEnv('BREVO_API_KEY');
  requireEnv('OPS_ALERT_EMAIL');
}

function makeRunId(label) {
  return `synthetic-canary-${label}-${new Date().toISOString()}`;
}

async function fire(label, payload, options = {}) {
  const result = await reportAgentRun({
    runId: makeRunId(label),
    skipped: 0,
    errorSample: `${label}: synthetic stranded-agent proof`,
    ...payload,
  }, options);

  const alert = result.alert;
  const outcome = !alert
    ? 'silence'
    : alert.suppressed
      ? 'suppressed'
      : alert.channel || 'unknown';

  console.log(JSON.stringify({
    case: label,
    agent: payload.agent,
    clientId: payload.clientId,
    state: result.state,
    outcome,
    fallback: Boolean(alert?.fallback),
    sent: Boolean(alert?.sent),
    suppressed: Boolean(alert?.suppressed),
    failure: alert?.smsResult?.reason || alert?.result?.error || alert?.reason || null,
  }, null, 2));

  return result;
}

async function run() {
  preflight();

  const baseClientId = Number(process.env.CANARY_BASE_CLIENT_ID)
    || (700000 + Math.floor(Date.now() % 100000));
  const invalidSmsDestination = process.env.CANARY_INVALID_SMS_TO || 'not-a-phone';

  console.log(`[observability-canary] baseClientId=${baseClientId}`);
  console.log('[observability-canary] CASE A expects real SMS to OPS_ALERT_PHONE');
  await fire('case-a-normal-stranded', {
    agent: 'synthetic_canary',
    clientId: baseClientId,
    attempts: 5,
    successes: 0,
  });

  console.log('[observability-canary] CASE B expects real EMAIL to OPS_ALERT_EMAIL');
  await fire('case-b-sam-stranded', {
    agent: 'sam',
    clientId: baseClientId + 1,
    attempts: 5,
    successes: 0,
  });

  console.log('[observability-canary] CASE C expects forced SMS failure and real EMAIL fallback');
  await fire('case-c-sms-failover', {
    agent: 'synthetic_canary',
    clientId: baseClientId + 2,
    attempts: 5,
    successes: 0,
    errorSample: {
      synthetic: true,
      injectedFailure: `SMS destination overridden to ${invalidSmsDestination}`,
    },
  }, { smsToOverride: invalidSmsDestination });

  console.log('[observability-canary] CASE D expects idle record and silence');
  await fire('case-d-idle', {
    agent: 'synthetic_canary',
    clientId: baseClientId + 3,
    attempts: 0,
    successes: 0,
    skipped: 7,
    errorSample: null,
  });

  console.log('[observability-canary] CASE E expects first SMS, second cooldown suppression');
  await fire('case-e-cooldown-first', {
    agent: 'synthetic_canary',
    clientId: baseClientId + 4,
    attempts: 5,
    successes: 0,
  });
  await fire('case-e-cooldown-second', {
    agent: 'synthetic_canary',
    clientId: baseClientId + 4,
    attempts: 5,
    successes: 0,
  });
}

run()
  .catch(err => {
    console.error('[observability-canary] failed:', err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end().catch(() => {});
  });
