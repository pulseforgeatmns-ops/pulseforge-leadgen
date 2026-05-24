require('dotenv').config();
const { google } = require('googleapis');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const fs = require('fs');
const path = require('path');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const twilio = require('twilio');

const AGENT_NAME = 'riley';
const CLIENT_ID = getRuntimeClientId();
const CREDENTIALS_PATH = './gmail_credentials.json';
const TOKEN_PATH = './gmail_token.json';
const DOWNLOADS_CREDENTIALS_PATH = path.join(process.env.HOME || '.', 'Downloads', 'riley_credentials.json');
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify'
];

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER;
const JACOB_PHONE = process.env.JACOB_PHONE;
const NOTIFICATION_ACTION = 'sms_notification';
const PRICE_TERMS = ['price', 'cost', 'how much', 'interested'];

// ── AUTH ──────────────────────────────────────────────────────────────
function parseJsonSource(label, raw) {
  if (!raw) return null;
  try {
    return JSON.parse(String(raw).trim());
  } catch (err) {
    throw new Error(`${label} is not valid JSON: ${err.message}`);
  }
}

function readFileIfPresent(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : null;
}

function loadOAuthCredentials() {
  const rawCreds = process.env.GMAIL_CREDENTIALS
    || readFileIfPresent(CREDENTIALS_PATH)
    || readFileIfPresent(DOWNLOADS_CREDENTIALS_PATH);

  if (rawCreds) {
    const credentials = parseJsonSource('GMAIL_CREDENTIALS', rawCreds);
    const credKeys = credentials.installed || credentials.web;
    if (!credKeys?.client_id || !credKeys?.client_secret) {
      throw new Error('GMAIL_CREDENTIALS must contain installed or web OAuth client JSON');
    }
    console.log('[Riley] Auth credentials: Gmail OAuth JSON');
    return credKeys;
  }

  if (process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
    console.warn('[Riley] Auth credentials: fallback GOOGLE_CLIENT_ID/SECRET');
    return {
      client_id: process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
    };
  }

  throw new Error('Missing Gmail OAuth credentials. Set GMAIL_CREDENTIALS to the full JSON string.');
}

async function getAuthClient() {
  const { client_id, client_secret } = loadOAuthCredentials();
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, 'http://localhost:3001');
  const rawToken = process.env.GMAIL_TOKEN
    || readFileIfPresent(TOKEN_PATH);

  if (rawToken) {
    try {
      const token = parseJsonSource('GMAIL_TOKEN', rawToken);
      oAuth2Client.setCredentials(token);
      await oAuth2Client.getAccessToken();
      console.log('[Riley] Auth token: parsed and refreshed');
      return oAuth2Client;
    } catch (err) {
      const authError = err.response?.data?.error || err.message || '';
      if (!authError.includes('invalid_grant') && !authError.includes('invalid_client')) throw err;
      if (!process.stdin.isTTY) {
        throw new Error(`Stored GMAIL_TOKEN is invalid or expired (${authError}). Regenerate it with getRileyToken.js and update Railway/local env.`);
      }
      console.warn('[Riley] Stored token is invalid or expired — falling through to re-auth');
    }
  }

  // Path 3: Interactive re-auth (local only — will not work on Railway)
  if (!process.stdin.isTTY) {
    throw new Error('Missing GMAIL_TOKEN. Regenerate it with getRileyToken.js and update Railway/local env.');
  }
  console.log('[Riley] Auth path: interactive re-auth');
  const authUrl = oAuth2Client.generateAuthUrl({ access_type: 'offline', scope: SCOPES });
  console.log('\n🔐 Authorize Riley by visiting this URL:\n');
  console.log(authUrl);
  console.log('\nAfter authorizing, paste the code here:');

  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  return new Promise((resolve) => {
    rl.question('Code: ', async (code) => {
      rl.close();
      const { tokens } = await oAuth2Client.getToken(code);
      oAuth2Client.setCredentials(tokens);
      fs.writeFileSync(TOKEN_PATH, JSON.stringify(tokens));
      console.log('✓ Token saved to gmail_token.json');
      resolve(oAuth2Client);
    });
  });
}

// ── READ INBOX ────────────────────────────────────────────────────────
function decodeGmailBody(data) {
  if (!data) return '';
  return Buffer.from(String(data).replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
}

function extractPlainText(payload) {
  if (!payload) return '';
  if (payload.mimeType === 'text/plain' && payload.body?.data) return decodeGmailBody(payload.body.data);
  for (const part of payload.parts || []) {
    const text = extractPlainText(part);
    if (text) return text;
  }
  if (payload.body?.data) return decodeGmailBody(payload.body.data);
  return '';
}

async function getUnreadEmails(auth) {
  const gmail = google.gmail({ version: 'v1', auth });
  const res = await gmail.users.messages.list({
    userId: 'me',
    q: 'in:inbox is:unread -from:me -from:warmupinbox.com -from:amazonses.com -from:dmarc -from:noreply-dmarc -from:linkedin.com -from:google.com -from:brevo.com -from:mailer-daemon -from:calendly.com -from:notifications -subject:"Unlock" -subject:"Supercharge" -subject:"Optimize" -subject:"Reimagine" -subject:"Curious About Your Approach" -subject:"Quick Question About Your"',
    maxResults: 20
  });

  const messages = res.data.messages || [];
  const emails = [];

  for (const msg of messages) {
    const full = await gmail.users.messages.get({ userId: 'me', id: msg.id, format: 'full' });
    const headers = full.data.payload.headers;
    const subject = headers.find(h => h.name === 'Subject')?.value || '(no subject)';
    const from = headers.find(h => h.name === 'From')?.value || '';
    const date = headers.find(h => h.name === 'Date')?.value || '';

    const body = extractPlainText(full.data.payload);

    emails.push({ id: msg.id, subject, from, date, body: body.slice(0, 1000) });
  }

  return emails;
}

// ── CLASSIFY ──────────────────────────────────────────────────────────
async function classifyEmail(email) {
  const prompt = `You are Riley, an inbound email triage agent for Pulseforge, an AI marketing agency run by Jacob Maynard in Manchester NH.

Classify this email into exactly one category:
- warm: genuine interest, wants more info, asks about pricing, or asks for next steps
- not_now: busy, not right time, maybe later, polite decline
- unsubscribe: remove me, stop emailing, not interested, unsubscribe
- auto_reply: out of office, automated response, vacation, away message
- wrong_person: not the decision maker, wrong company, wrong person

Email:
From: ${email.from}
Subject: ${email.subject}
Body: ${email.body}

Respond with JSON only: { "classification": "warm|not_now|unsubscribe|auto_reply|wrong_person", "reason": "...", "suggested_reply": "..." }
For suggested_reply: write a short, warm, human reply from Jacob if classification is warm or not_now. Leave blank for others.`;

  const response = await anthropic.messages.create({
    model: 'claude-opus-4-5',
    max_tokens: 500,
    messages: [{ role: 'user', content: prompt }]
  });

  try {
    const text = response.content[0].text;
    const match = text.match(/\{[\s\S]*\}/);
    if (match) {
      const parsed = JSON.parse(match[0]);
      const allowed = new Set(['warm', 'not_now', 'unsubscribe', 'auto_reply', 'wrong_person']);
      if (parsed.classification === 'interested') parsed.classification = 'warm';
      if (parsed.classification === 'out_of_office') parsed.classification = 'auto_reply';
      if (!allowed.has(parsed.classification)) parsed.classification = 'not_now';
      return parsed;
    }
    return { classification: 'not_now', reason: 'no json found', suggested_reply: '' };
  } catch(e) {
    return { classification: 'not_now', reason: 'parse error: ' + e.message, suggested_reply: '' };
  }
}

// ── PROSPECT MATCHING ─────────────────────────────────────────────────
async function findProspectByEmail(fromEmail) {
  const pool = require('./db');
  const emailMatch = fromEmail.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
  if (!emailMatch) return null;
  const email = emailMatch[0].toLowerCase();
  const res = await pool.query(
    `SELECT p.*, c.name AS company_name
     FROM prospects p
     LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
     WHERE LOWER(p.email) = $1 AND p.client_id = $2
     LIMIT 1`,
    [email, CLIENT_ID]
  );
  return res.rows[0] || null;
}

async function hasOutboundEmail(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1
    FROM touchpoints
    WHERE prospect_id = $1
      AND client_id = $2
      AND channel = 'email'
      AND action_type = 'outbound'
    LIMIT 1
  `, [prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

async function updateProspectFromReply(prospect, classification) {
  const pool = require('./db');
  if (classification === 'warm') {
    await pool.query(
      'UPDATE prospects SET status = $1, updated_at = NOW() WHERE id = $2 AND client_id = $3',
      ['warm', prospect.id, CLIENT_ID]
    );
    console.log(`  [Riley] ${prospect.email} upgraded to warm`);
  }
  if (classification === 'unsubscribe') {
    await pool.query(
      'UPDATE prospects SET do_not_contact = true, updated_at = NOW() WHERE id = $1 AND client_id = $2',
      [prospect.id, CLIENT_ID]
    );
    console.log(`  [Riley] ${prospect.email} marked do_not_contact`);
  }
  if (classification === 'not_now') {
    await pool.query(`
      UPDATE prospects
      SET notes = CONCAT_WS(E'\n', NULLIF(notes, ''), $1),
          updated_at = NOW()
      WHERE id = $2 AND client_id = $3
    `, ['Riley: follow up in 30 days', prospect.id, CLIENT_ID]);
    console.log(`  [Riley] ${prospect.email} noted for 30-day follow-up`);
  }
}

async function logInboundTouchpoint(prospect, email, classification) {
  const pool = require('./db');
  const actionType = classification === 'unsubscribe'
    ? 'unsubscribed'
    : classification === 'auto_reply'
      ? 'auto_reply'
      : 'inbound_reply';
  const sentiment = classification === 'warm'
    ? 'positive'
    : 'neutral';
  const outcome = {
    from: email.from,
    classification,
  };
  if (classification === 'not_now') outcome.note = 'follow up in 30 days';

  await pool.query(
    'INSERT INTO touchpoints (prospect_id, channel, action_type, content_summary, outcome, sentiment, created_at, client_id) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)',
    [
      prospect.id,
      'email',
      actionType,
      email.subject,
      JSON.stringify(outcome),
      sentiment,
      CLIENT_ID
    ]
  );
}

async function depositInterestedAction(prospect, email, suggestedReply) {
  const pool = require('./db');
  const bizName = prospect.notes?.split('—')[0]?.trim() || prospect.email;
  await pool.query(
    'INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, created_at, client_id) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)',
    [
      'riley',
      'reply_required',
      'Warm reply from ' + bizName,
      prospect.email + ' replied to your outreach. Suggested reply below — approve or edit before sending.',
      JSON.stringify({ prospect_id: prospect.id, email: prospect.email, subject: email.subject, from: email.from, body: email.body, suggested_reply: suggestedReply }),
      'pending',
      CLIENT_ID
    ]
  );
  console.log('  [Riley] Action deposited for Max');
}

function companyName(prospect) {
  return prospect.company_name || prospect.company || prospect.notes?.split('—')[0]?.trim() || prospect.email || 'their company';
}

function firstName(prospect) {
  return prospect.first_name || 'there';
}

function snippet(text) {
  return String(text || '').replace(/\s+/g, ' ').trim().slice(0, 100);
}

function hasPricingIntent(body) {
  const lower = String(body || '').toLowerCase();
  return PRICE_TERMS.some(term => lower.includes(term));
}

function getTwilioClient() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !TWILIO_PHONE_NUMBER || !JACOB_PHONE) return null;
  return twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
}

async function recentlyNotified(prospectId) {
  if (!prospectId) return false;
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = $2
      AND prospect_id = $3
      AND client_id = $4
      AND ran_at > NOW() - INTERVAL '24 hours'
    LIMIT 1
  `, [AGENT_NAME, NOTIFICATION_ACTION, prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

async function logNotification(prospectId, notificationType, body, status, extra = {}) {
  await db.logAgentAction(
    AGENT_NAME,
    NOTIFICATION_ACTION,
    prospectId,
    null,
    { notification_type: notificationType, to: JACOB_PHONE || null, body, client_id: CLIENT_ID, ...extra },
    status
  );
}

async function notifyJacob(prospect, notificationType, body, extra = {}) {
  try {
    if (!prospect?.id) return;
    if (await recentlyNotified(prospect.id)) {
      console.log(`  [Riley] SMS skipped for ${prospect.email || prospect.id} — recent notification exists`);
      return;
    }

    if (process.env.TWILIO_A2P_APPROVED !== 'true') {
      console.log(`  [Riley] SMS would send (${notificationType}): ${body}`);
      await logNotification(prospect.id, notificationType, body, 'skipped', { reason: 'twilio_a2p_not_approved', ...extra });
      return;
    }

    const twilioClient = getTwilioClient();
    if (!twilioClient) {
      console.log(`  [Riley] SMS would send (${notificationType}) but Twilio/JACOB_PHONE is not configured: ${body}`);
      await logNotification(prospect.id, notificationType, body, 'skipped', { reason: 'twilio_not_configured', ...extra });
      return;
    }

    await twilioClient.messages.create({ body, from: TWILIO_PHONE_NUMBER, to: JACOB_PHONE });
    await logNotification(prospect.id, notificationType, body, 'success', extra);
    console.log(`  [Riley] SMS sent to Jacob (${notificationType})`);
  } catch (err) {
    console.error(`  [Riley] SMS failed (${notificationType}):`, err.message);
    await logNotification(prospect?.id, notificationType, body, 'failed', { error: err.message, ...extra }).catch(() => {});
  }
}

async function notifyWarmReply(prospect, email, classification) {
  if (classification !== 'warm') return;
  const name = firstName(prospect);
  const company = companyName(prospect);
  const bodyText = snippet(email.body);
  if (hasPricingIntent(email.body)) {
    await notifyJacob(
      prospect,
      'pricing_reply',
      `💰 ${name} at ${company} is asking about pricing. Reply now.`,
      { email: prospect.email, classification }
    );
    return;
  }
  await notifyJacob(
    prospect,
    'warm_reply',
    `🔥 Warm reply from ${name} at ${company}: '${bodyText}' — check your inbox`,
    { email: prospect.email, classification }
  );
}

async function processHotSignalNotifications() {
  const pool = require('./db');
  try {
    const res = await pool.query(`
      SELECT al.prospect_id, al.payload, p.first_name, p.email, p.notes, c.name AS company_name
      FROM agent_log al
      JOIN prospects p ON p.id = al.prospect_id AND p.client_id = al.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE al.agent_name = 'riley'
        AND al.action = 'hot_prospect_alert'
        AND al.client_id = $1
        AND al.ran_at > NOW() - INTERVAL '24 hours'
      ORDER BY al.ran_at DESC
      LIMIT 20
    `, [CLIENT_ID]);

    for (const row of res.rows) {
      const payload = typeof row.payload === 'string' ? JSON.parse(row.payload || '{}') : (row.payload || {});
      const prospect = { id: row.prospect_id, first_name: row.first_name, email: row.email, notes: row.notes, company_name: row.company_name };
      const opens = payload.open_count || payload.opens || 2;
      await notifyJacob(
        prospect,
        'hot_prospect_alert',
        `⚡ Hot prospect: ${firstName(prospect)} at ${companyName(prospect)} opened ${opens} times. Call them.`,
        { source_action: 'hot_prospect_alert', open_count: opens }
      );
    }
  } catch (err) {
    console.error('[Riley] Hot signal notification scan failed:', err.message);
  }
}

async function processBookedCallNotifications() {
  const pool = require('./db');
  try {
    const res = await pool.query(`
      SELECT t.prospect_id, t.content_summary, p.first_name, p.email, p.notes, c.name AS company_name
      FROM touchpoints t
      JOIN prospects p ON p.id = t.prospect_id AND p.client_id = t.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE t.client_id = $1
        AND t.action_type = 'booked'
        AND t.created_at > NOW() - INTERVAL '24 hours'
      ORDER BY t.created_at DESC
      LIMIT 20
    `, [CLIENT_ID]);

    for (const row of res.rows) {
      const prospect = { id: row.prospect_id, first_name: row.first_name, email: row.email, notes: row.notes, company_name: row.company_name };
      await notifyJacob(
        prospect,
        'booked_call',
        `📅 ${firstName(prospect)} at ${companyName(prospect)} booked a call. Check the closer pipeline.`,
        { source_action: 'booked', summary: row.content_summary }
      );
    }
  } catch (err) {
    console.error('[Riley] Booked call notification scan failed:', err.message);
  }
}

async function processEventNotifications() {
  await processHotSignalNotifications();
  await processBookedCallNotifications();
}

// ── MARK READ ─────────────────────────────────────────────────────────
async function markAsRead(auth, messageId) {
  const gmail = google.gmail({ version: 'v1', auth });
  await gmail.users.messages.modify({
    userId: 'me',
    id: messageId,
    requestBody: { removeLabelIds: ['UNREAD'] }
  });
}

async function safeMarkAsRead(auth, messageId) {
  try {
    await markAsRead(auth, messageId);
  } catch (err) {
    console.error(`[Riley] Failed to mark Gmail message ${messageId} read:`, err.message);
  }
}

// ── MAIN ──────────────────────────────────────────────────────────────
async function run() {
  console.log('\n🤝 Riley — Inbound Triage Agent');
  console.log('─────────────────────────────────\n');
  const clientConfig = await getClientConfig(CLIENT_ID);
  if (!clientConfig) throw new Error(`Active client not found: ${CLIENT_ID}`);
  if (CLIENT_ID !== 1) {
    console.log('Riley currently monitors jacob@gopulseforge.com only. MSHI triage is manual until forwarding or a second OAuth is configured.');
    return;
  }

  let emails = [];
  let auth = null;
  let gmailFailed = false;
  try {
    auth = await getAuthClient();
    emails = await getUnreadEmails(auth);
  } catch (err) {
    gmailFailed = true;
    console.error('[Riley] Gmail processing failed:', err.message);
    await db.logAgentAction(AGENT_NAME, 'triage', null, null, { error: err.message, client_id: CLIENT_ID }, 'failed').catch(() => {});
  }

  if (!emails.length) {
    if (!gmailFailed) {
      console.log('No unread emails to process.');
      await db.logAgentAction(AGENT_NAME, 'triage', null, null, { emails_processed: 0 }, 'success');
    }
    await processEventNotifications();
    return;
  }

  console.log(`Found ${emails.length} unread emails. Classifying...\n`);

  let stats = { warm: 0, not_now: 0, unsubscribe: 0, auto_reply: 0, wrong_person: 0 };

  for (const email of emails) {
    console.log(`Processing: ${email.subject} — ${email.from}`);

    try {
      const prospect = await findProspectByEmail(email.from);
      if (!prospect) {
        console.log('  [Riley] No prospect match — skipping classification for ' + email.from);
        await safeMarkAsRead(auth, email.id);
        continue;
      }

      const hasOutbound = await hasOutboundEmail(prospect.id);
      if (!hasOutbound) {
        console.log(`  [Riley] ${prospect.email} has no Emmett outbound touchpoint — skipping`);
        await safeMarkAsRead(auth, email.id);
        continue;
      }

      const result = await classifyEmail(email);
      console.log(`  → ${result.classification}: ${result.reason}`);

      stats[result.classification] = (stats[result.classification] || 0) + 1;

      await db.logAgentAction(
        AGENT_NAME,
        'reply_classified',
        prospect.id,
        null,
        {
          prospect_id: prospect.id,
          email: prospect.email,
          from: email.from,
          subject: email.subject,
          classification: result.classification,
          reason: result.reason,
          suggested_reply: result.suggested_reply,
          client_id: CLIENT_ID
        },
        'success'
      );

      await updateProspectFromReply(prospect, result.classification);
      await logInboundTouchpoint(prospect, email, result.classification);
      if (result.classification === 'warm') {
        await depositInterestedAction(prospect, email, result.suggested_reply);
        await notifyWarmReply(prospect, email, result.classification);
      }

      await safeMarkAsRead(auth, email.id);
      await new Promise(r => setTimeout(r, 1000));
    } catch (err) {
      console.error(`  [Riley] Failed to process ${email.from}:`, err.message);
      await db.logAgentAction(
        AGENT_NAME,
        'reply_classified',
        null,
        null,
        { from: email.from, subject: email.subject, error: err.message, client_id: CLIENT_ID },
        'failed',
        err.message
      ).catch(() => {});
    }
  }

  console.log('\n─── RILEY SUMMARY ───────────────────────────────');
  Object.entries(stats).forEach(([k, v]) => { if (v > 0) console.log(`  ${k}: ${v}`); });
  console.log('─────────────────────────────────────────────────\n');

  await db.logAgentAction(AGENT_NAME, 'triage', null, null, { ...stats, emails_processed: emails.length }, 'success');
  await processEventNotifications();

  console.log('Riley complete.');
}

module.exports = { run, getAuthClient };

if (require.main === module) {
  run().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
