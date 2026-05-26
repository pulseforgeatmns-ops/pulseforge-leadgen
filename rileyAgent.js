require('dotenv').config();
const { google } = require('googleapis');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const fs = require('fs');
const path = require('path');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

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

async function getTotalEmailOpens(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT COUNT(*)::int AS total_opens
    FROM touchpoints
    WHERE prospect_id = $1
      AND client_id = $2
      AND channel = 'email'
      AND action_type IN ('open', 'email_opened')
  `, [prospectId, CLIENT_ID]);
  return res.rows[0]?.total_opens || 0;
}

function triageBucket(classification, email) {
  if (classification === 'warm' && hasPricingIntent(email?.body)) return 'hot';
  if (classification === 'warm') return 'warm';
  if (classification === 'unsubscribe') return 'unsubscribe';
  return 'cold';
}

function triageTrigger(classification) {
  if (classification === 'unsubscribe') return 'reply';
  if (classification === 'warm' || classification === 'not_now' || classification === 'wrong_person') return 'reply';
  return 'reply';
}

function recommendedTriageAction(classification, suggestedReply) {
  if (classification === 'warm') return suggestedReply ? 'Review and send the suggested reply.' : 'Reply personally and move this prospect into warm follow-up.';
  if (classification === 'unsubscribe') return 'Do not contact again. Prospect has been marked DNC.';
  if (classification === 'not_now') return 'Follow up in 30 days unless they reply sooner.';
  if (classification === 'wrong_person') return 'Find the right decision maker before sending more outreach.';
  if (classification === 'auto_reply') return 'No action needed unless the auto-reply includes a return date.';
  return 'Review the reply and decide the next step.';
}

function signalTimestampFromEmail(email) {
  const parsed = email?.date ? new Date(email.date) : null;
  if (parsed && !Number.isNaN(parsed.getTime())) return parsed.toISOString();
  return new Date().toISOString();
}

async function logTriageAction(prospect, email, classification, suggestedReply) {
  const totalOpens = await getTotalEmailOpens(prospect.id);
  await db.logAgentAction(
    AGENT_NAME,
    'triage',
    prospect.id,
    null,
    {
      action: 'triage',
      prospect_name: `${prospect.first_name || ''} ${prospect.last_name || ''}`.trim() || firstName(prospect),
      company: companyName(prospect),
      email: prospect.email,
      vertical: prospect.vertical || null,
      icp_score: Number(prospect.icp_score || 0),
      total_opens: totalOpens,
      triage_bucket: triageBucket(classification, email),
      trigger: triageTrigger(classification),
      recommended_action: recommendedTriageAction(classification, suggestedReply),
      signal_timestamp: signalTimestampFromEmail(email),
      classification,
      subject: email.subject,
      from: email.from,
      client_id: CLIENT_ID,
    },
    'success'
  );
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

async function getWarmSignalActionContext(prospectId, clientId = CLIENT_ID, fallback = {}) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT
      p.id,
      p.client_id,
      p.first_name,
      p.last_name,
      p.email,
      p.vertical,
      p.icp_score,
      p.notes,
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
  `, [prospectId, clientId]);

  const row = res.rows[0] || {};
  const company =
    row.company_name ||
    String(row.notes || '').split('—')[0].trim() ||
    fallback.company ||
    fallback.email ||
    'Unknown company';
  return {
    prospect_id: row.id || prospectId,
    client_id: row.client_id || clientId,
    company,
    first_name: row.first_name || fallback.first_name || 'there',
    email: row.email || fallback.email || null,
    vertical: row.vertical || fallback.vertical || null,
    icp_score: Number(row.icp_score || fallback.icp_score || 0),
    total_opens: Number(row.total_opens || fallback.total_opens || 0),
    last_email_subject: fallback.last_email_subject || fallback.subject || row.last_email_subject || 'Unknown subject',
  };
}

async function depositWarmSignalAction({ prospect_id, client_id = CLIENT_ID, trigger, signal_timestamp = null, subject = null, total_opens = null, email = null, company = null }) {
  if (!prospect_id) return false;
  const pool = require('./db');
  const ctx = await getWarmSignalActionContext(prospect_id, client_id, {
    subject,
    total_opens,
    email,
    company,
  });
  const payload = {
    prospect_id: ctx.prospect_id,
    client_id: ctx.client_id,
    company: ctx.company,
    first_name: ctx.first_name,
    email: ctx.email,
    vertical: ctx.vertical,
    icp_score: ctx.icp_score,
    total_opens: total_opens != null ? Number(total_opens) : ctx.total_opens,
    last_email_subject: subject || ctx.last_email_subject,
    trigger,
    signal_timestamp: signal_timestamp || new Date().toISOString(),
  };

  const title = `Warm signal — ${payload.company}`;
  const description = `${payload.company} triggered a warm signal (${trigger || 'signal'}). Sam should send the warm-signal SMS notification.`;
  const inserted = await pool.query(`
    INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, created_at, client_id)
    SELECT $1, $2, $3, $4, $5, 'pending', NOW(), $6
    WHERE NOT EXISTS (
      SELECT 1
      FROM agent_actions
      WHERE action_type = $2
        AND status = 'pending'
        AND client_id = $6
        AND payload->>'prospect_id' = $7
        AND created_at >= NOW() - INTERVAL '24 hours'
    )
    RETURNING id
  `, [
    AGENT_NAME,
    'warm_signal',
    title,
    description,
    JSON.stringify(payload),
    payload.client_id,
    String(payload.prospect_id),
  ]);
  if (inserted.rows.length) {
    console.log(`  [Riley] Warm signal action deposited for Sam: ${payload.email || payload.prospect_id}`);
    return true;
  }
  console.log(`  [Riley] Warm signal action already pending for ${payload.email || payload.prospect_id}`);
  return false;
}

async function notifyWarmReply(prospect, email, classification) {
  if (classification !== 'warm') return;
  await depositWarmSignalAction({
    prospect_id: prospect.id,
    client_id: CLIENT_ID,
    trigger: 'reply',
    signal_timestamp: signalTimestampFromEmail(email),
    subject: email.subject,
    email: prospect.email,
    company: companyName(prospect),
  });
}

async function processHotSignalActions() {
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
      await depositWarmSignalAction({
        prospect_id: row.prospect_id,
        client_id: CLIENT_ID,
        trigger: '2+ opens',
        signal_timestamp: payload.signal_timestamp || new Date().toISOString(),
        total_opens: opens,
        email: row.email,
        company: companyName(prospect),
        subject: payload.subject || null,
      });
    }
  } catch (err) {
    console.error('[Riley] Hot signal action scan failed:', err.message);
  }
}

async function processEventNotifications() {
  await processHotSignalActions();
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
    await db.logAgentAction(AGENT_NAME, 'triage_summary', null, null, { error: err.message, client_id: CLIENT_ID }, 'failed').catch(() => {});
  }

  if (!emails.length) {
    if (!gmailFailed) {
      console.log('No unread emails to process.');
      await db.logAgentAction(AGENT_NAME, 'triage_summary', null, null, { emails_processed: 0 }, 'success');
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

      await logTriageAction(prospect, email, result.classification, result.suggested_reply);

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
        'triage_failed',
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

  await db.logAgentAction(AGENT_NAME, 'triage_summary', null, null, { ...stats, emails_processed: emails.length }, 'success');
  await processEventNotifications();

  console.log('Riley complete.');
}

module.exports = { run, getAuthClient, depositWarmSignalAction };

if (require.main === module) {
  run().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
