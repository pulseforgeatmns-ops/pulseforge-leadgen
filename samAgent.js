require('dotenv').config();
const pool = require('./db');
const db = require('./dbClient');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

const AGENT_NAME = 'sam';
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;

const ACCOUNT_SID    = process.env.TWILIO_ACCOUNT_SID;
const AUTH_TOKEN     = process.env.TWILIO_AUTH_TOKEN;
const FROM_NUMBER    = process.env.TWILIO_FROM || process.env.TWILIO_PHONE_NUMBER;
const NOTIFY_PHONE   = process.env.NOTIFY_PHONE || process.env.JACOB_PHONE;
const MARKET_LABELS = {
  1: 'Manchester NH',
  2: 'Charleston WV',
  5: 'Nashville TN',
};

function getTwilioClient() {
  if (!ACCOUNT_SID || !AUTH_TOKEN || !FROM_NUMBER) {
    console.warn('Sam: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, or TWILIO_FROM/TWILIO_PHONE_NUMBER not set — SMS disabled');
    return null;
  }
  const twilio = require('twilio');
  return twilio(ACCOUNT_SID, AUTH_TOKEN);
}

// ── MESSAGE TEMPLATES ─────────────────────────────────────────────────

const MESSAGES = {
  // 3+ emails sent, no reply logged yet
  sequence_no_reply: (p) =>
    `Hey ${p.first_name}, I've sent a few notes your way — just wanted to check if anything landed or if you had questions. Happy to keep it brief. - Jacob at Pulseforge`,

  // prospect.status flipped to 'warm'
  warm_signal: (p) =>
    `Hey ${p.first_name}, wanted to reach out directly — I think there's a real fit here and I'd love to connect when you have 10 minutes. - Jacob at Pulseforge`,

  // 14+ days since last contact, still no reply
  re_engagement: (p) => {
    const sender = CLIENT_CONFIG?.sender_name || 'Jacob at Pulseforge';
    const raw = (p.first_name || '').trim();
    const greeting = !raw || /^there$/i.test(raw) ? 'Hey,' : `Hey ${raw},`;
    return `${greeting} just checking back in — if timing was off before, I'm still happy to put something together for you. - ${sender}`;
  },
};

// ── DEDUP CHECK ───────────────────────────────────────────────────────

async function alreadyContactedBySMS(prospectId) {
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1
      AND client_id = $2
      AND channel = 'sms'
      AND created_at > NOW() - INTERVAL '7 days'
    LIMIT 1
  `, [prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

// ── CORE SEND ─────────────────────────────────────────────────────────

async function sendSMS(prospectId, messageOverride = null) {
  const twilioClient = getTwilioClient();
  if (!twilioClient) return { sent: false, reason: 'twilio_not_configured' };

  const res = await pool.query(
    `SELECT p.*, c.name as company_name
     FROM prospects p
     LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
     WHERE p.id = $1 AND p.client_id = $2`,
    [prospectId, CLIENT_ID]
  );
  const prospect = res.rows[0];

  if (!prospect) return { sent: false, reason: 'prospect_not_found' };
  if (!prospect.phone) return { sent: false, reason: 'no_phone' };
  if (prospect.do_not_contact) return { sent: false, reason: 'do_not_contact' };

  const alreadySent = await alreadyContactedBySMS(prospectId);
  if (alreadySent) {
    console.log(`  ↷ Skipping ${prospect.first_name} ${prospect.last_name} — SMS sent within last 7 days`);
    return { sent: false, reason: 'recent_sms' };
  }

  const body = messageOverride || MESSAGES.warm_signal(prospect);

  try {
    await twilioClient.messages.create({ body, from: FROM_NUMBER, to: prospect.phone });

    await db.logTouchpoint(prospectId, 'sms', 'outbound', body, 'sent', 'neutral', AGENT_NAME);
    await db.logAgentAction(AGENT_NAME, 'send_sms', prospectId, null, { phone: prospect.phone, trigger: 'direct' }, 'success');

    console.log(`  ✓ SMS sent to ${prospect.first_name} ${prospect.last_name} (${prospect.phone})`);
    return { sent: true, prospectId };
  } catch (err) {
    await db.logAgentAction(AGENT_NAME, 'send_sms', prospectId, null, { phone: prospect.phone }, 'failed', err.message);
    console.error(`  ✗ SMS failed for ${prospect.first_name} ${prospect.last_name}: ${err.message}`);
    return { sent: false, reason: err.message };
  }
}

// ── TRIGGER QUERIES ───────────────────────────────────────────────────

// Trigger 1: 3+ emails sent in sequence, no inbound reply logged
// NOTE: relies on outbound email touchpoints logged by Emmett.
// Upgrade to track actual opens once Brevo open webhooks are wired up.
async function getSequenceNoReply() {
  const res = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.phone
    FROM prospects p
    WHERE p.phone IS NOT NULL AND p.phone != ''
      AND p.client_id = $1
      AND p.do_not_contact = false
      AND (
        SELECT COUNT(*) FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
      ) >= 3
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type = 'inbound'
      )
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'sms'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
    LIMIT 10
  `, [CLIENT_ID]);
  return res.rows;
}

// Trigger 2: status recently changed to 'warm'
async function getWarmProspects() {
  const res = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.phone
    FROM prospects p
    WHERE p.status = 'warm'
      AND p.client_id = $1
      AND p.phone IS NOT NULL AND p.phone != ''
      AND p.do_not_contact = false
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'sms'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
    LIMIT 10
  `, [CLIENT_ID]);
  return res.rows;
}

// Trigger 3: in sequence 14+ days, no reply, due for re-engagement
async function getReEngagement() {
  const res = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.phone
    FROM prospects p
    WHERE p.phone IS NOT NULL AND p.phone != ''
      AND p.client_id = $1
      AND p.do_not_contact = false
      AND p.status = 'cold'
      AND EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id AND t.channel = 'email'
      )
      AND (
        SELECT MAX(t.created_at) FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id
      ) < NOW() - INTERVAL '14 days'
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'sms'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
    LIMIT 10
  `, [CLIENT_ID]);
  return res.rows;
}

function parseLogPayload(payload) {
  if (!payload) return {};
  return typeof payload === 'string' ? JSON.parse(payload) : payload;
}

function marketLabel(clientId = CLIENT_ID) {
  return MARKET_LABELS[Number(clientId)] || 'Unknown market';
}

function isWarmSignalSmsWindowOpen(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const hour = Number(parts.find(p => p.type === 'hour')?.value || 0);
  const minute = Number(parts.find(p => p.type === 'minute')?.value || 0);
  const minutes = (hour * 60) + minute;
  return minutes >= (8 * 60 + 30) && minutes <= (22 * 60 + 30);
}

function buildWarmSignalNotification(payload, prospect) {
  const company = payload.company || prospect.company_name || 'Unknown company';
  const firstName = payload.first_name || prospect.first_name || 'there';
  const opens = Number(payload.total_opens || prospect.total_opens || 0);
  const subject = payload.last_email_subject || payload.subject || prospect.last_email_subject || 'Unknown subject';
  const icpScore = Number(payload.icp_score || prospect.icp_score || 0);
  const email = payload.email || prospect.email || 'No email';

  return `🔥 Warm signal — ${company} (${marketLabel(payload.client_id || prospect.client_id)})
${firstName} opened your email ${opens}x
Subject: ${subject}
ICP: ${icpScore} | ${email}`;
}

async function completeWarmSignalAction(actionId, result) {
  await pool.query(
    `UPDATE agent_actions
     SET status = 'executed',
         executed_at = NOW(),
         result = $2
     WHERE id = $1`,
    [actionId, result]
  );
}

async function hasWarmSignalSmsSentToday(prospectId, clientId = CLIENT_ID) {
  const res = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'sms_sent'
      AND prospect_id = $2
      AND client_id = $3
      AND DATE(ran_at) = CURRENT_DATE
    LIMIT 1
  `, [AGENT_NAME, prospectId, clientId]);
  return res.rows.length > 0;
}

async function hasWarmSignalSmsQueuedToday(prospectId, clientId = CLIENT_ID) {
  const res = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'sms_queued'
      AND prospect_id = $2
      AND client_id = $3
      AND DATE(ran_at) = CURRENT_DATE
    LIMIT 1
  `, [AGENT_NAME, prospectId, clientId]);
  return res.rows.length > 0;
}

const WARM_SMS_LOG_ACTION = {
  skipped_dnc:  'sms_skipped_dnc',
  skipped_gate: 'sms_skipped_gate',
  queued:       'sms_queued',
  sent:         'sms_sent',
  failed:       'sms_sent',
};

async function logWarmSignalSms(action, prospect, message, status, extra = {}, errorMsg = null) {
  await db.logAgentAction(
    AGENT_NAME,
    WARM_SMS_LOG_ACTION[status] || 'sms_sent',
    prospect.id,
    null,
    {
      prospect_id: prospect.id,
      company: action.company || prospect.company_name,
      message,
      timestamp: new Date().toISOString(),
      trigger: action.trigger || 'warm_signal',
      source_action_id: action.action_id || null,
      client_id: prospect.client_id || CLIENT_ID,
      ...extra,
    },
    status === 'failed' ? 'failed' : 'success',
    errorMsg
  );
}

const OPEN_TRIGGER_LABELS = new Set(['qualifying_opens', '2+ opens']);

async function processWarmSignalActions(twilioClient, dailyLimit, sentCount) {
  const { logSignalDroppedNoProspect, qualifyingOpenSignal } = require('./rileyAgent');
  const res = await pool.query(`
    SELECT id, payload, client_id
    FROM agent_actions
    WHERE action_type = 'warm_signal'
      AND status = 'pending'
      AND client_id = $1
    ORDER BY created_at ASC
    LIMIT 25
  `, [CLIENT_ID]);

  if (!res.rows.length) return { processed: 0, sent: 0, queued: 0, skipped_dnc: 0 };
  console.log(`Warm-signal actions: ${res.rows.length} pending`);

  let sent = 0;
  let queued = 0;
  let skippedDnc = 0;

  for (const row of res.rows) {
    if (sentCount + sent >= dailyLimit) break;
    const payload = { ...parseLogPayload(row.payload), action_id: row.id };
    const prospectId = payload.prospect_id;
    if (!prospectId) {
      await completeWarmSignalAction(row.id, 'Skipped: missing prospect_id');
      continue;
    }

    const prospectRes = await pool.query(`
      SELECT
        p.id,
        p.client_id,
        p.first_name,
        p.email,
        p.icp_score,
        p.do_not_contact,
        c.name AS company_name,
        COALESCE(eng.total_opens, 0)::int AS total_opens,
        outbound.content_summary AS last_email_subject
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      LEFT JOIN LATERAL (
        SELECT COUNT(*)::int AS total_opens
        FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type IN ('open', 'email_opened')
      ) eng ON TRUE
      LEFT JOIN LATERAL (
        SELECT content_summary
        FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type IN ('outbound', 'email_warm', 'send')
        ORDER BY t.created_at DESC
        LIMIT 1
      ) outbound ON TRUE
      WHERE p.id = $1
        AND p.client_id = $2
      LIMIT 1
    `, [prospectId, payload.client_id || row.client_id || CLIENT_ID]);
    const prospect = prospectRes.rows[0];
    if (!prospect) {
      await logSignalDroppedNoProspect({
        prospect_id: prospectId,
        client_id: payload.client_id || row.client_id || CLIENT_ID,
        source: AGENT_NAME,
        trigger: payload.trigger || 'warm_signal',
        payload: { source_action_id: row.id, action_type: 'warm_signal' },
      });
      await completeWarmSignalAction(row.id, 'Skipped: prospect not found');
      continue;
    }

    const message = buildWarmSignalNotification(payload, prospect);
    if (prospect.do_not_contact) {
      await logWarmSignalSms(payload, prospect, message, 'skipped_dnc', { reason: 'do_not_contact' });
      await completeWarmSignalAction(row.id, 'Skipped: do_not_contact');
      skippedDnc++;
      continue;
    }

    // Re-verify the open gate for opens-triggered actions. Pending rows deposited
    // before the gate tightened may no longer qualify; skip them rather than send.
    if (OPEN_TRIGGER_LABELS.has(payload.trigger)) {
      const gate = await qualifyingOpenSignal(prospect.id, prospect.client_id);
      if (!gate.qualifies) {
        const spreadFmt = Number(gate.spread_minutes).toFixed(1);
        const reason = `open_gate_unmet: ${gate.total_opens} opens, spread ${spreadFmt}m`;
        await logWarmSignalSms(payload, prospect, message, 'skipped_gate', {
          reason,
          total_opens: gate.total_opens,
          spread_minutes: gate.spread_minutes,
        });
        await completeWarmSignalAction(row.id, `Skipped: ${reason}`);
        continue;
      }
    }

    if (await hasWarmSignalSmsSentToday(prospect.id, prospect.client_id)) {
      await completeWarmSignalAction(row.id, 'Skipped: sms already sent today');
      continue;
    }

    if (!isWarmSignalSmsWindowOpen()) {
      if (!(await hasWarmSignalSmsQueuedToday(prospect.id, prospect.client_id))) {
        await logWarmSignalSms(payload, prospect, message, 'queued', { reason: 'outside_quiet_hours' });
      }
      queued++;
      continue;
    }

    if (!NOTIFY_PHONE) {
      await logWarmSignalSms(payload, prospect, message, 'failed', { reason: 'notify_phone_missing' }, 'NOTIFY_PHONE/JACOB_PHONE is not configured');
      await completeWarmSignalAction(row.id, 'Failed: NOTIFY_PHONE/JACOB_PHONE is not configured');
      continue;
    }

    try {
      await twilioClient.messages.create({ body: message, from: FROM_NUMBER, to: NOTIFY_PHONE });
      await logWarmSignalSms(payload, prospect, message, 'sent', { to: NOTIFY_PHONE });
      await completeWarmSignalAction(row.id, 'Sent warm-signal SMS notification');
      sent++;
    } catch (err) {
      await logWarmSignalSms(payload, prospect, message, 'failed', { to: NOTIFY_PHONE }, err.message);
      await completeWarmSignalAction(row.id, `Failed: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, 1500));
  }

  return { processed: res.rows.length, sent, queued, skipped_dnc: skippedDnc };
}

async function completeReengagementTrigger(triggerId, status = 'completed') {
  await pool.query(
    `UPDATE agent_log SET status = $1 WHERE id = $2 AND agent_name = 'max' AND action = 'reengagement_trigger'`,
    [status, triggerId]
  );
}

// Max re-engagement triggers — run before normal SMS triggers
async function processReengagementTriggers(sendFn, dailyLimit, sentCount) {
  const res = await pool.query(`
    SELECT id, prospect_id, payload
    FROM agent_log
    WHERE agent_name = 'max'
      AND action = 'reengagement_trigger'
      AND status = 'pending'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '24 hours'
    ORDER BY ran_at ASC
  `, [CLIENT_ID]);

  if (!res.rows.length) return { processed: 0, sent: 0 };

  console.log(`Max re-engagement triggers: ${res.rows.length} pending`);
  let sent = 0;

  for (const row of res.rows) {
    if (sentCount + sent >= dailyLimit) break;

    const payload = parseLogPayload(row.payload);
    const prospectId = row.prospect_id || payload.prospect_id;
    if (!prospectId) {
      await completeReengagementTrigger(row.id);
      continue;
    }

    const prospectRes = await pool.query(
      `SELECT id, first_name, last_name, phone, do_not_contact
       FROM prospects WHERE id = $1 AND client_id = $2`,
      [prospectId, CLIENT_ID]
    );
    const prospect = prospectRes.rows[0];
    if (!prospect) {
      await completeReengagementTrigger(row.id);
      continue;
    }

    const result = await sendFn(prospectId, MESSAGES.re_engagement(prospect));
    if (result.sent) sent++;
    await completeReengagementTrigger(row.id);
    await new Promise(r => setTimeout(r, 1500));
  }

  return { processed: res.rows.length, sent };
}

// ── RUN ───────────────────────────────────────────────────────────────

async function run() {
  const HOLIDAYS_2026 = [
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-05-25',
    '2026-07-04', '2026-09-07', '2026-11-11', '2026-11-26', '2026-12-25'
  ];
  const today = new Date().toISOString().split('T')[0];
  if (HOLIDAYS_2026.includes(today)) {
    console.log(`Holiday detected (${today}) — skipping run`);
    return;
  }

  console.log('\nSam agent running...\n');
  CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);

  const twilioClient = getTwilioClient();
  if (!twilioClient) {
    console.log('Sam: no Twilio credentials — exiting');
    return;
  }

  let sent = 0;
  const dailyLimit = 20;

  const warmSignalActions = await processWarmSignalActions(twilioClient, dailyLimit, sent);
  sent += warmSignalActions.sent;
  console.log(`Warm-signal actions: ${warmSignalActions.sent} SMS sent, ${warmSignalActions.queued} queued, ${warmSignalActions.skipped_dnc} skipped DNC (${warmSignalActions.processed} action(s) processed)\n`);

  const maxReengage = await processReengagementTriggers(sendSMS, dailyLimit, sent);
  sent += maxReengage.sent;
  console.log(`Max re-engagement: ${maxReengage.sent} SMS sent (${maxReengage.processed} trigger(s) processed)\n`);

  // Trigger 1: sequence, no reply
  const noReply = await getSequenceNoReply();
  console.log(`Trigger 1 (3+ emails, no reply): ${noReply.length} prospects`);
  for (const p of noReply) {
    if (sent >= dailyLimit) break;
    const result = await sendSMS(p.id, MESSAGES.sequence_no_reply(p));
    if (result.sent) sent++;
    await new Promise(r => setTimeout(r, 1500));
  }

  // Trigger 2: warm prospects
  const warm = await getWarmProspects();
  console.log(`\nTrigger 2 (warm status): ${warm.length} prospects`);
  for (const p of warm) {
    if (sent >= dailyLimit) break;
    const result = await sendSMS(p.id, MESSAGES.warm_signal(p));
    if (result.sent) sent++;
    await new Promise(r => setTimeout(r, 1500));
  }

  // Trigger 3: re-engagement
  const reEngage = await getReEngagement();
  console.log(`\nTrigger 3 (14+ days, re-engagement): ${reEngage.length} prospects`);
  for (const p of reEngage) {
    if (sent >= dailyLimit) break;
    const result = await sendSMS(p.id, MESSAGES.re_engagement(p));
    if (result.sent) sent++;
    await new Promise(r => setTimeout(r, 1500));
  }

  await pool.query(
    `INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id) VALUES ($1, $2, $3, $4, NOW(), $5)`,
    [AGENT_NAME, 'batch_sms', JSON.stringify({
      sent,
      triggers: {
        warmSignalActions,
        maxReengage: maxReengage,
        noReply: noReply.length,
        warm: warm.length,
        reEngage: reEngage.length,
      },
      client_id: CLIENT_ID,
    }), 'success', CLIENT_ID]
  );

  console.log(`\nSam complete — ${sent} SMS sent.`);
}

module.exports = { sendSMS, run };

if (require.main === module) {
  run().catch(err => {
    console.error('[Sam] Fatal error:', err.message);
    process.exit(1);
  });
}
