require('dotenv').config();
const pool = require('./db');
const db = require('./dbClient');

const AGENT_NAME = 'sam';

const ACCOUNT_SID    = process.env.TWILIO_ACCOUNT_SID;
const AUTH_TOKEN     = process.env.TWILIO_AUTH_TOKEN;
const FROM_NUMBER    = process.env.TWILIO_PHONE_NUMBER;

function getTwilioClient() {
  if (!ACCOUNT_SID || !AUTH_TOKEN || !FROM_NUMBER) {
    console.warn('Sam: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, or TWILIO_PHONE_NUMBER not set — SMS disabled');
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
  re_engagement: (p) =>
    `Hey ${p.first_name}, just checking back in — if timing was off before, I'm still happy to put something together for you. - Jacob at Pulseforge`,
};

// ── DEDUP CHECK ───────────────────────────────────────────────────────

async function alreadyContactedBySMS(prospectId) {
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1
      AND channel = 'sms'
      AND created_at > NOW() - INTERVAL '7 days'
    LIMIT 1
  `, [prospectId]);
  return res.rows.length > 0;
}

// ── CORE SEND ─────────────────────────────────────────────────────────

async function sendSMS(prospectId, messageOverride = null) {
  const twilioClient = getTwilioClient();
  if (!twilioClient) return { sent: false, reason: 'twilio_not_configured' };

  const res = await pool.query(
    `SELECT p.*, c.name as company_name
     FROM prospects p
     LEFT JOIN companies c ON p.company_id = c.id
     WHERE p.id = $1`,
    [prospectId]
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
    await db.logAgentAction(AGENT_NAME, 'send_sms', prospectId, null, { phone: prospect.phone }, 'error', err.message);
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
      AND p.do_not_contact = false
      AND (
        SELECT COUNT(*) FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
      ) >= 3
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'email'
          AND t.action_type = 'inbound'
      )
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'sms'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
    LIMIT 10
  `);
  return res.rows;
}

// Trigger 2: status recently changed to 'warm'
async function getWarmProspects() {
  const res = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.phone
    FROM prospects p
    WHERE p.status = 'warm'
      AND p.phone IS NOT NULL AND p.phone != ''
      AND p.do_not_contact = false
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'sms'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
    LIMIT 10
  `);
  return res.rows;
}

// Trigger 3: in sequence 14+ days, no reply, due for re-engagement
async function getReEngagement() {
  const res = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.phone
    FROM prospects p
    WHERE p.phone IS NOT NULL AND p.phone != ''
      AND p.do_not_contact = false
      AND p.status = 'cold'
      AND EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.channel = 'email'
      )
      AND (
        SELECT MAX(t.created_at) FROM touchpoints t
        WHERE t.prospect_id = p.id
      ) < NOW() - INTERVAL '14 days'
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'sms'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
    LIMIT 10
  `);
  return res.rows;
}

// ── RUN ───────────────────────────────────────────────────────────────

async function run() {
  console.log('\nSam agent running...\n');

  const twilioClient = getTwilioClient();
  if (!twilioClient) {
    console.log('Sam: no Twilio credentials — exiting');
    return;
  }

  let sent = 0;
  const dailyLimit = 20;

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
    `INSERT INTO agent_log (agent_name, action, payload, status, ran_at) VALUES ($1, $2, $3, $4, NOW())`,
    [AGENT_NAME, 'batch_sms', JSON.stringify({ sent, triggers: { noReply: noReply.length, warm: warm.length, reEngage: reEngage.length } }), 'success']
  );

  console.log(`\nSam complete — ${sent} SMS sent.`);
}

module.exports = { sendSMS };

run().catch(console.error);
