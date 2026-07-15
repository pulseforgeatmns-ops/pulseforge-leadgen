require('dotenv').config();
const { randomUUID } = require('crypto');
const { google } = require('googleapis');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const fs = require('fs');
const path = require('path');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { recalculateICP } = require('./utils/icpScoring');
const { reportAgentRun } = require('./utils/agentObservability');
const { OPEN_SOURCE, ensureOpenSignalSchema } = require('./utils/openSignalGate');
const { resolveVerticalTier } = require('./utils/verticalTiers');
const { safeIngestNormalizedSignal, safeIngestRileyReplySignal } = require('./utils/maxSignalIngestion');

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
const TOKEN_REFRESH_WINDOW_MS = 5 * 60 * 1000;
const RETURN_DATE_MODEL = 'claude-haiku-4-5-20251001';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const PRICE_TERMS = ['price', 'cost', 'how much', 'interested'];
const BOUNCE_PATTERNS = [
  'undeliverable',
  'delivery failure',
  'returned mail',
  'mail delivery failed',
  'rejection warning',
  'rejected',
];
const AUTORESPONDER_PATTERNS = [
  'out of office',
  'out of the office',
  'automatic reply',
  'auto-reply',
  'autoreply',
  'vacation response',
  'on leave',
  'paternity leave',
  'maternity leave',
];
const DEFLECTION_PATTERNS = [
  "this inbox isn't monitored",
  'inbox is not monitored',
  'do not reply',
];
const VALID_REPLY_BUCKETS = new Set([
  'interested',
  'not_now',
  'negative',
  'unsubscribe',
  'wrong_person',
  'out_of_office',
  'unknown',
]);

function makeRunId() {
  return `${AGENT_NAME}-${CLIENT_ID || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

async function reportRileyRun({ runId, attempts, successes, skipped = 0, errorSample = null }) {
  try {
    return await reportAgentRun({
      agent: AGENT_NAME,
      clientId: CLIENT_ID,
      runId,
      attempts,
      successes,
      skipped,
      errorSample,
    });
  } catch (err) {
    console.error('[Riley] Observability report failed:', err.message);
    return null;
  }
}
const LEGACY_REPLY_BUCKETS = {
  warm: 'interested',
  auto_reply: 'out_of_office',
};
let inboundNoiseConfig = null;

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

function persistGmailToken(token) {
  if (!token?.refresh_token && !token?.access_token) return;
  try {
    fs.writeFileSync(TOKEN_PATH, JSON.stringify(token, null, 2));
    console.log('[Riley] Gmail token refreshed and saved to gmail_token.json');
  } catch (err) {
    console.warn('[Riley] Gmail token refreshed but could not be saved:', err.message);
  }
}

function tokenExpiresSoon(token) {
  const expiry = Number(token?.expiry_date || token?.expires_at || 0);
  if (!expiry) return true;
  return expiry <= Date.now() + TOKEN_REFRESH_WINDOW_MS;
}

function loadRileyEnvToken() {
  if (!process.env.RILEY_ACCESS_TOKEN && !process.env.RILEY_REFRESH_TOKEN) return null;
  return {
    access_token: process.env.RILEY_ACCESS_TOKEN || null,
    refresh_token: process.env.RILEY_REFRESH_TOKEN || null,
    expiry_date: Number(process.env.RILEY_TOKEN_EXPIRY || process.env.RILEY_ACCESS_TOKEN_EXPIRY || 0) || null,
    token_type: 'Bearer',
  };
}

async function refreshRileyAccessToken({ client_id, client_secret, refresh_token }) {
  if (!refresh_token) {
    throw new Error('RILEY_REFRESH_TOKEN is missing');
  }

  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id,
      client_secret,
      refresh_token,
      grant_type: 'refresh_token',
    }),
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const details = data.error_description || data.error || res.statusText;
    throw new Error(`Google token refresh failed: ${details}`);
  }

  const expiryDate = Date.now() + Number(data.expires_in || 3600) * 1000;
  process.env.RILEY_ACCESS_TOKEN = data.access_token;
  process.env.RILEY_TOKEN_EXPIRY = String(expiryDate);
  return {
    access_token: data.access_token,
    refresh_token,
    expiry_date: expiryDate,
    token_type: data.token_type || 'Bearer',
    scope: data.scope,
  };
}

async function logTokenRefreshFailed(err) {
  const message = err?.message || String(err);
  console.error('[Riley] Gmail token refresh failed:', message);
  await db.logAgentAction(
    AGENT_NAME,
    'token_refresh_failed',
    null,
    null,
    { error: message, client_id: CLIENT_ID },
    'failed',
    message
  ).catch(logErr => console.error('[Riley] Failed to log token_refresh_failed:', logErr.message));
}

async function ensureFreshRileyToken(token, credentials, options = {}) {
  if (!tokenExpiresSoon(token)) return token;
  if (!token?.refresh_token) {
    await logTokenRefreshFailed(new Error('RILEY_REFRESH_TOKEN is missing'));
    return null;
  }

  console.log('[Riley] Gmail access token expired/expiring soon — refreshing before inbox read');
  try {
    const refreshed = await refreshRileyAccessToken({
      client_id: credentials.client_id,
      client_secret: credentials.client_secret,
      refresh_token: token.refresh_token,
    });
    if (options.persist) persistGmailToken(refreshed);
    console.log('[Riley] Gmail access token refreshed for this run');
    return refreshed;
  } catch (err) {
    await logTokenRefreshFailed(err);
    return null;
  }
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
  const credentials = loadOAuthCredentials();
  const { client_id, client_secret } = credentials;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, 'http://localhost:3001');
  const rileyEnvToken = loadRileyEnvToken();
  if (rileyEnvToken) {
    const token = await ensureFreshRileyToken(rileyEnvToken, credentials);
    if (!token) return null;
    oAuth2Client.setCredentials(token);
    console.log('[Riley] Auth token: RILEY_ACCESS_TOKEN/RILEY_REFRESH_TOKEN');
    return oAuth2Client;
  }

  const rawToken = process.env.GMAIL_TOKEN
    || readFileIfPresent(TOKEN_PATH);

  if (rawToken) {
    try {
      const token = parseJsonSource('GMAIL_TOKEN', rawToken);
      let latestToken = { ...token };
      oAuth2Client.on('tokens', (tokens) => {
        latestToken = {
          ...latestToken,
          ...tokens,
          refresh_token: tokens.refresh_token || latestToken.refresh_token,
        };
        persistGmailToken(latestToken);
      });
      oAuth2Client.setCredentials(token);
      if (token.refresh_token && tokenExpiresSoon(token)) {
        latestToken = await ensureFreshRileyToken(token, credentials, { persist: true });
        if (!latestToken) return null;
        oAuth2Client.setCredentials(latestToken);
      } else if (!token.refresh_token) {
        await oAuth2Client.getAccessToken();
      }
      console.log('[Riley] Auth token: parsed and refreshed');
      return oAuth2Client;
    } catch (err) {
      const authError = err.response?.data?.error || err.message || '';
      if (!authError.includes('invalid_grant') && !authError.includes('invalid_client')) throw err;
      if (!process.stdin.isTTY) {
        throw new Error(`Stored GMAIL_TOKEN is invalid or expired (${authError}). Regenerate it with getRileyToken.js and set RILEY_ACCESS_TOKEN/RILEY_REFRESH_TOKEN in Railway/local env.`);
      }
      console.warn('[Riley] Stored token is invalid or expired — falling through to re-auth');
    }
  }

  // Path 3: Interactive re-auth (local only — will not work on Railway)
  if (!process.stdin.isTTY) {
    throw new Error('Missing Riley Gmail tokens. Regenerate them with getRileyToken.js and set RILEY_ACCESS_TOKEN/RILEY_REFRESH_TOKEN in Railway/local env.');
  }
  console.log('[Riley] Auth path: interactive re-auth');
  const authUrl = oAuth2Client.generateAuthUrl({ access_type: 'offline', prompt: 'consent', scope: SCOPES });
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

async function getInboundEmails(auth, options = {}) {
  const gmail = google.gmail({ version: 'v1', auth });
  const query = options.query || 'in:inbox -label:"Riley/Processed" -from:me -from:warmupinbox.com -from:amazonses.com -from:dmarc -from:noreply-dmarc -from:linkedin.com -from:google.com -from:brevo.com -from:mailer-daemon -from:calendly.com -from:notifications -subject:"Unlock" -subject:"Supercharge" -subject:"Optimize" -subject:"Reimagine" -subject:"Curious About Your Approach" -subject:"Quick Question About Your"';
  const res = await gmail.users.messages.list({
    userId: 'me',
    q: query,
    maxResults: options.maxResults || 20
  });

  const messages = res.data.messages || [];
  const emails = [];

  for (const msg of messages) {
    const full = await gmail.users.messages.get({ userId: 'me', id: msg.id, format: 'full' });
    const headers = full.data.payload.headers;
    const headerMap = Object.fromEntries(headers.map(h => [String(h.name || '').toLowerCase(), h.value || '']));
    const subject = headers.find(h => h.name === 'Subject')?.value || '(no subject)';
    const from = headers.find(h => h.name === 'From')?.value || '';
    const date = headers.find(h => h.name === 'Date')?.value || '';

    const body = extractPlainText(full.data.payload);

    emails.push({
      id: msg.id,
      threadId: full.data.threadId || msg.threadId || null,
      subject,
      from,
      date,
      headers: headerMap,
      body: body.slice(0, 1000),
    });
  }

  return emails;
}

async function getUnreadEmails(auth) {
  return getInboundEmails(auth);
}

// ── CLASSIFY ──────────────────────────────────────────────────────────
function loadInboundNoiseConfig() {
  if (inboundNoiseConfig) return inboundNoiseConfig;
  const configPath = path.join(__dirname, 'config', 'inboundNoise.json');
  const parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  inboundNoiseConfig = {
    vendor_domains: (parsed.vendor_domains || []).map(normalizeDomain).filter(Boolean),
    system_domains: (parsed.system_domains || []).map(normalizeDomain).filter(Boolean),
    dmarc_senders: (parsed.dmarc_senders || []).map(normalizeEmailAddress).filter(Boolean),
    dmarc_sender_domains: (parsed.dmarc_sender_domains || []).map(normalizeDomain).filter(Boolean),
  };
  return inboundNoiseConfig;
}

function normalizeEmailAddress(value) {
  const text = String(value || '').trim().toLowerCase();
  const emailMatch = text.match(/<\s*([^>]+)\s*>/) || text.match(/[\w.+-]+@[\w-]+(?:\.[\w-]+)+/);
  return (emailMatch?.[1] || emailMatch?.[0] || text).replace(/^<|>$/g, '').trim().toLowerCase();
}

function normalizeDomain(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .split('/')[0]
    .split(':')[0];
}

function emailDomain(email) {
  const normalized = normalizeEmailAddress(email);
  const parts = normalized.split('@');
  return parts.length === 2 ? normalizeDomain(parts[1]) : '';
}

function domainMatches(domain, configuredDomain) {
  const normalized = normalizeDomain(domain);
  const configured = normalizeDomain(configuredDomain);
  return Boolean(normalized && configured && (normalized === configured || normalized.endsWith(`.${configured}`)));
}

function getMessageHeader(message, name) {
  const target = String(name || '').toLowerCase();
  const headers = message?.headers || {};
  if (Array.isArray(headers)) {
    const found = headers.find(h => String(h.name || '').toLowerCase() === target);
    return found?.value || '';
  }
  return headers[target] || headers[name] || '';
}

function extractMessageIds(value) {
  return [...String(value || '').matchAll(/<([^>]+)>/g)]
    .map(match => normalizeMessageId(match[1]))
    .filter(Boolean);
}

function normalizeMessageId(value) {
  return String(value || '').trim().replace(/^<|>$/g, '').toLowerCase();
}

function stripReplyPrefixes(subject) {
  return String(subject || '')
    .replace(/^\s*((re|fw|fwd)\s*:\s*)+/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractOriginalSubject(subject) {
  let clean = String(subject || '')
    .replace(/^\s*(undeliverable|returned mail|mail delivery failure|delivery status notification|failure notice)\s*:\s*/i, '')
    .replace(/\s+/g, ' ')
    .trim();

  // Known gap: "Automatic reply: Subject" without a Re: marker won't currently strip.
  // If that appears in real data, add a targeted prefix rule.
  if (subjectContains(clean, AUTORESPONDER_PATTERNS) || subjectContains(clean, DEFLECTION_PATTERNS)) {
    const marker = clean.match(/\s+re\s*:\s*/i);
    if (marker) clean = clean.slice(marker.index + marker[0].length).trim();
  }

  return stripReplyPrefixes(clean);
}

function normalizeSubject(subject) {
  return extractOriginalSubject(subject).toLowerCase();
}

function subjectStartsReply(subject) {
  return /^\s*re\s*:/i.test(String(subject || ''));
}

function subjectContains(subject, patterns) {
  const lower = String(subject || '').toLowerCase().replace(/[‘’]/g, "'");
  return patterns.some(pattern => lower.includes(pattern));
}

function noiseMatch(message) {
  const config = loadInboundNoiseConfig();
  const sender = normalizeEmailAddress(message.from || getMessageHeader(message, 'From'));
  const domain = emailDomain(sender);
  const subject = String(message.subject || '');

  if (config.dmarc_senders.includes(sender)) return { matched: true, reason: 'dmarc' };
  if (config.dmarc_sender_domains.some(d => domainMatches(domain, d))) return { matched: true, reason: 'dmarc' };
  if (/report domain:/i.test(subject)) return { matched: true, reason: 'dmarc' };
  if (config.vendor_domains.some(d => domainMatches(domain, d))) return { matched: true, reason: 'vendor' };
  if (config.system_domains.some(d => domainMatches(domain, d))) return { matched: true, reason: 'system' };

  return { matched: false };
}

function firstProspectMatch(rows) {
  return rows.find(row => row.prospect_id)?.prospect_id || null;
}

async function findSentByMessageId(messageIds) {
  if (!messageIds.length) return null;
  const pool = require('./db');
  const res = await pool.query(`
    SELECT prospect_id
    FROM agent_log
    WHERE agent_name = 'emmett'
      AND action = 'email_sent'
      AND client_id = $1
      AND LOWER(TRIM(BOTH '<>' FROM COALESCE(payload->>'message_id', ''))) = ANY($2::text[])
    ORDER BY ran_at DESC
  `, [CLIENT_ID, messageIds]);
  return firstProspectMatch(res.rows);
}

async function findSentProspectBySender(senderEmail) {
  const sender = normalizeEmailAddress(senderEmail);
  if (!sender) return null;
  const pool = require('./db');
  const res = await pool.query(`
    SELECT p.id AS prospect_id
    FROM prospects p
    WHERE p.client_id = $1
      AND LOWER(TRIM(p.email)) = $2
      AND EXISTS (
        SELECT 1
        FROM agent_log al
        WHERE al.agent_name = 'emmett'
          AND al.action = 'email_sent'
          AND al.prospect_id = p.id
          AND al.client_id = p.client_id
      )
    ORDER BY p.updated_at DESC NULLS LAST, p.created_at DESC NULLS LAST
  `, [CLIENT_ID, sender]);
  return firstProspectMatch(res.rows);
}

async function findProspectDirectBySender(senderEmail) {
  const sender = normalizeEmailAddress(senderEmail);
  if (!sender) return null;
  const pool = require('./db');
  const res = await pool.query(`
    SELECT id AS prospect_id
    FROM prospects
    WHERE client_id = $1
      AND LOWER(TRIM(email)) = $2
    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
  `, [CLIENT_ID, sender]);
  return firstProspectMatch(res.rows);
}

async function findSentBySubject(subject, { requireReply = false } = {}) {
  if (requireReply && !subjectStartsReply(subject)) return null;
  const cleanSubject = normalizeSubject(subject);
  if (!cleanSubject) return null;
  const pool = require('./db');
  const res = await pool.query(`
    SELECT prospect_id
    FROM agent_log
    WHERE agent_name = 'emmett'
      AND action = 'email_sent'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '90 days'
      AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(payload->>'subject', ''), '^\\s*((re|fw|fwd)\\s*:\\s*)+', '', 'i'))) = $2
    ORDER BY ran_at DESC
  `, [CLIENT_ID, cleanSubject]);
  return firstProspectMatch(res.rows);
}

async function findSentProspectByDomain(senderEmail) {
  const domain = emailDomain(senderEmail);
  if (!domain) return null;
  const pool = require('./db');
  const res = await pool.query(`
    SELECT p.id AS prospect_id
    FROM prospects p
    WHERE p.client_id = $1
      AND LOWER(SPLIT_PART(TRIM(p.email), '@', 2)) = $2
      AND EXISTS (
        SELECT 1
        FROM agent_log al
        WHERE al.agent_name = 'emmett'
          AND al.action = 'email_sent'
          AND al.prospect_id = p.id
          AND al.client_id = p.client_id
          AND al.ran_at >= NOW() - INTERVAL '90 days'
      )
    ORDER BY p.updated_at DESC NULLS LAST, p.created_at DESC NULLS LAST
  `, [CLIENT_ID, domain]);
  return firstProspectMatch(res.rows);
}

async function findLooseStatusProspect(message) {
  const sender = message.from || getMessageHeader(message, 'From');
  return await findProspectDirectBySender(sender)
    || await findSentBySubject(message.subject)
    || await findSentProspectByDomain(sender);
}

function defaultAutoResponderUntil() {
  const date = new Date();
  date.setDate(date.getDate() + 30);
  return date.toISOString().slice(0, 10);
}

function isValidFutureDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value || ''))) return false;
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return false;
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  return date > today;
}

function extractClaudeText(message) {
  return (message.content || [])
    .filter(block => block && block.type === 'text' && typeof block.text === 'string')
    .map(block => block.text)
    .join('\n')
    .trim();
}

function parseJson(text) {
  try {
    return JSON.parse(text);
  } catch (_) {
    const match = String(text || '').match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Classifier response was not valid JSON');
    return JSON.parse(match[0]);
  }
}

async function logReturnDateParse(prospectId, payload, status = 'success', errorMsg = null) {
  if (!prospectId) return;
  await db.logAgentAction(
    AGENT_NAME,
    'auto_responder_return_date_parsed',
    prospectId,
    null,
    { ...payload, client_id: CLIENT_ID },
    status,
    errorMsg
  ).catch(err => console.error('[Riley] Failed to log return-date parse:', err.message));
}

async function parseReturnDate(messageBody, options = {}) {
  const body = String(messageBody || '').slice(0, 2000);
  const today = new Date().toISOString().slice(0, 10);
  const prompt = `Below is an automatic out-of-office reply email. Extract the date the sender returns to work, if explicitly stated.

Rules:
- Return ONLY a JSON object: {"return_date": "YYYY-MM-DD"} or {"return_date": null}
- If no return date is explicitly stated, return null
- If a range is given, use the LAST date (return date)
- Do not infer or guess dates. Only extract what's stated.
- Current date for relative references: ${today}

Email body:
${body}`;

  try {
    const response = await anthropic.messages.create({
      model: RETURN_DATE_MODEL,
      max_tokens: 120,
      messages: [{ role: 'user', content: prompt }],
    });
    const parsed = parseJson(extractClaudeText(response));
    const rawDate = parsed.return_date || null;
    const validFuture = isValidFutureDate(rawDate);
    const returnDate = validFuture ? rawDate : null;
    if (options.logHaiku) {
      await logReturnDateParse(options.prospectId, {
        return_date: returnDate,
        raw_return_date: rawDate,
        model: RETURN_DATE_MODEL,
        input_tokens: response.usage?.input_tokens || null,
        output_tokens: response.usage?.output_tokens || null,
      });
    }
    return {
      returnDate,
      autoResponderUntil: returnDate || defaultAutoResponderUntil(),
      parsed: validFuture,
      error: null,
    };
  } catch (err) {
    if (options.logHaiku) {
      await logReturnDateParse(options.prospectId, {
        return_date: null,
        model: RETURN_DATE_MODEL,
      }, 'failed', err.message);
    }
    console.warn('[Riley] Return-date parse failed; defaulting auto-responder window:', err.message);
    return {
      returnDate: null,
      autoResponderUntil: defaultAutoResponderUntil(),
      parsed: false,
      error: err.message,
    };
  }
}

async function classifyInbound(message, options = {}) {
  const sender = message.from || getMessageHeader(message, 'From');
  const noise = noiseMatch(message);
  if (noise.matched) {
    return { tier: 4, action: 'inbound_noise', reason: noise.reason };
  }

  const inReplyToIds = extractMessageIds(getMessageHeader(message, 'In-Reply-To'));
  const inReplyToProspectId = await findSentByMessageId(inReplyToIds);
  if (inReplyToProspectId) {
    return { tier: 1, action: 'reply_classified', prospectId: inReplyToProspectId, reason: 'in_reply_to' };
  }

  const senderProspectId = await findSentProspectBySender(sender);
  if (senderProspectId) {
    return { tier: 1, action: 'reply_classified', prospectId: senderProspectId, reason: 'sender_match' };
  }

  const subjectProspectId = await findSentBySubject(message.subject, { requireReply: true });
  if (subjectProspectId) {
    return { tier: 1, action: 'reply_classified', prospectId: subjectProspectId, reason: 'subject_match' };
  }

  const subject = message.subject || '';
  const isBounce = subjectContains(subject, BOUNCE_PATTERNS);
  const isAutoResponder = subjectContains(subject, AUTORESPONDER_PATTERNS);
  const isDeflection = subjectContains(subject, DEFLECTION_PATTERNS);

  if (isBounce || isAutoResponder || isDeflection) {
    const prospectId = await findLooseStatusProspect(message);
    if (prospectId) {
      if (isBounce) {
        return {
          tier: 2,
          action: 'prospect_status_updated',
          prospectId,
          newStatus: 'bounced',
          reason: 'bounce_detected',
        };
      }
      if (isDeflection) {
        return {
          tier: 2,
          action: 'prospect_status_updated',
          prospectId,
          newStatus: 'do_not_email',
          reason: 'deflection_detected',
        };
      }

      const parsed = await parseReturnDate(message.body || '', {
        prospectId,
        logHaiku: Boolean(options.logHaiku),
      });
      return {
        tier: 2,
        action: 'prospect_status_updated',
        prospectId,
        newStatus: 'auto_responder',
        autoResponderUntil: parsed.autoResponderUntil,
        reason: parsed.parsed ? 'oof_detected_parsed' : 'oof_detected',
      };
    }

    if (isBounce) {
      return {
        tier: 2,
        action: 'inbound_unmatched_bounce',
        reason: 'bounce_detected_no_prospect_match',
      };
    }
    if (isDeflection) {
      return {
        tier: 2,
        action: 'inbound_unmatched_deflection',
        reason: 'deflection_detected_no_prospect_match',
      };
    }
    return {
      tier: 2,
      action: 'inbound_unmatched_autoresponder',
      reason: 'oof_detected_no_prospect_match',
    };
  }

  return { tier: 3, action: 'inbound_unidentified', reason: 'no_match' };
}

function normalizeReplyClassification(rawClassification) {
  const normalized = String(rawClassification || '').trim().toLowerCase();
  const bucket = LEGACY_REPLY_BUCKETS[normalized] || normalized;
  if (VALID_REPLY_BUCKETS.has(bucket)) return bucket;
  console.warn(`[Riley] Invalid reply classification bucket "${rawClassification}" — using unknown`);
  return 'unknown';
}

async function classifyReply(email) {
  const prompt = `You are Riley, an inbound email triage agent for Pulseforge, an AI marketing agency run by Jacob Maynard in Manchester NH.

Classify this email into exactly one category:
- interested: genuine interest, wants more info, asks about pricing, or asks for next steps
- not_now: busy, not right time, maybe later, polite decline
- negative: clear rejection, no interest, already has a provider, hostile or dismissive reply
- unsubscribe: remove me, stop emailing, unsubscribe, do not contact
- wrong_person: not the decision maker, wrong company, wrong person
- out_of_office: out of office, automated response, vacation, away message
- unknown: unclear, spam, unrelated, or cannot classify confidently

Email:
From: ${email.from}
Subject: ${email.subject}
Body: ${email.body}

Respond with JSON only: { "classification": "interested|not_now|negative|unsubscribe|wrong_person|out_of_office|unknown", "reason": "...", "suggested_reply": "..." }
For suggested_reply: write a short, warm, human reply from Jacob if classification is interested or not_now. Leave blank for others.`;

  console.log('[Riley] Classifying raw reply:', JSON.stringify({
    from: email.from,
    subject: email.subject,
    body: email.body,
  }));

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 500,
    messages: [{ role: 'user', content: prompt }]
  });

  try {
    const text = response.content[0].text;
    const match = text.match(/\{[\s\S]*\}/);
    if (match) {
      const parsed = JSON.parse(match[0]);
      parsed.classification = normalizeReplyClassification(parsed.classification);
      console.log('[Riley] Classification result:', JSON.stringify(parsed));
      return parsed;
    }
    const fallback = { classification: 'unknown', reason: 'no json found', suggested_reply: '' };
    console.log('[Riley] Classification result:', JSON.stringify(fallback));
    return fallback;
  } catch(e) {
    const fallback = { classification: 'unknown', reason: 'parse error: ' + e.message, suggested_reply: '' };
    console.log('[Riley] Classification result:', JSON.stringify(fallback));
    return fallback;
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

async function findProspectById(prospectId) {
  if (!prospectId) return null;
  const pool = require('./db');
  const res = await pool.query(
    `SELECT p.*, c.name AS company_name
     FROM prospects p
     LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
     WHERE p.id = $1 AND p.client_id = $2
     ORDER BY p.updated_at DESC NULLS LAST, p.created_at DESC NULLS LAST`,
    [prospectId, CLIENT_ID]
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
  await ensureOpenSignalSchema(pool);
  const res = await pool.query(`
    SELECT COUNT(*)::int AS total_opens
    FROM email_events ee
    WHERE ee.prospect_id = $1
      AND ee.client_id = $2
      AND ee.event_type IN ('opened', 'open')
      AND ee.open_source = $3::open_source
      AND (
        EXISTS (
          SELECT 1
          FROM email_events sent
          WHERE sent.client_id = ee.client_id
            AND sent.prospect_id = ee.prospect_id
            AND sent.event_type IN ('sent', 'delivered')
            AND (
              (ee.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ee.brevo_message_id)
              OR (
                ee.brevo_message_id IS NULL
                AND LOWER(sent.recipient_email) = LOWER(ee.recipient_email)
                AND sent.subject_line IS NOT DISTINCT FROM ee.subject_line
                AND sent.event_at <= ee.event_at
              )
            )
        )
        OR EXISTS (
          SELECT 1
          FROM agent_log al
          WHERE al.client_id = ee.client_id
            AND al.prospect_id = ee.prospect_id
            AND al.agent_name = 'emmett'
            AND al.action = 'email_sent'
            AND (
              (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
              OR (
                ee.brevo_message_id IS NULL
                AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                AND al.ran_at <= ee.event_at
              )
            )
        )
      )
  `, [prospectId, CLIENT_ID, OPEN_SOURCE.HUMAN]);
  return res.rows[0]?.total_opens || 0;
}

async function prospectExists(prospectId, clientId = CLIENT_ID) {
  if (!prospectId) return false;
  const pool = require('./db');
  const res = await pool.query(
    'SELECT 1 FROM prospects WHERE id = $1 AND client_id = $2 LIMIT 1',
    [prospectId, clientId]
  );
  return res.rows.length > 0;
}

async function logSignalDroppedNoProspect({ prospect_id, client_id = CLIENT_ID, source = AGENT_NAME, trigger = null, payload = {} }) {
  const pool = require('./db');
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ($1, 'signal_dropped_no_prospect', NULL, $2, 'skipped', NOW(), $3)
  `, [
    source,
    JSON.stringify({
      prospect_id: prospect_id || null,
      trigger,
      reason: 'prospect_not_found',
      client_id,
      ...payload,
    }),
    client_id,
  ]);
}

function triageBucket(classification, email) {
  if (classification === 'interested' && hasPricingIntent(email?.body)) return 'hot';
  if (classification === 'interested') return 'warm';
  if (classification === 'unsubscribe') return 'unsubscribe';
  return 'cold';
}

function triageTrigger(classification) {
  if (classification === 'unsubscribe') return 'reply';
  if (classification === 'interested' || classification === 'not_now' || classification === 'wrong_person' || classification === 'negative') return 'reply';
  return 'reply';
}

function recommendedTriageAction(classification, suggestedReply) {
  if (classification === 'interested') return suggestedReply ? 'Review and send the suggested reply.' : 'Reply personally and move this prospect into warm follow-up.';
  if (classification === 'unsubscribe') return 'Do not contact again. Prospect has been marked DNC.';
  if (classification === 'not_now') return 'Follow up in 30 days unless they reply sooner.';
  if (classification === 'negative') return 'Do not continue this thread unless Jacob chooses to respond personally.';
  if (classification === 'wrong_person') return 'Find the right decision maker before sending more outreach.';
  if (classification === 'out_of_office') return 'No action needed unless the auto-reply includes a return date.';
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

async function logReplyClassified(prospect, email, result) {
  await db.logAgentAction(
    AGENT_NAME,
    'reply_classified',
    prospect.id,
    null,
    {
      gmail_message_id: email.id,
      from: email.from,
      subject: email.subject,
      body: email.body,
      classification: result.classification,
      reason: result.reason || '',
      suggested_reply: result.suggested_reply || '',
      human_review_required: result.classification === 'interested',
      client_id: CLIENT_ID,
    },
    'success'
  );
}

async function logReplyClassifiedSkipped(prospect, email, reason) {
  await db.logAgentAction(
    AGENT_NAME,
    'reply_classified',
    prospect?.id || null,
    null,
    {
      gmail_message_id: email.id,
      from: email.from,
      subject: email.subject,
      body: email.body,
      classification: 'unknown',
      reason,
      skipped: true,
      client_id: CLIENT_ID,
    },
    'skipped'
  );
}

function gmailThreadLink(email) {
  const thread = email?.threadId || email?.id || '';
  return thread ? `https://mail.google.com/mail/u/0/#inbox/${encodeURIComponent(thread)}` : null;
}

async function logInboundUnidentified(email, reason = 'no_match') {
  await db.logAgentAction(
    AGENT_NAME,
    'inbound_unidentified',
    null,
    null,
    {
      gmail_message_id: email.id,
      gmail_thread_id: email.threadId || null,
      gmail_thread_link: gmailThreadLink(email),
      sender: normalizeEmailAddress(email.from),
      from: email.from,
      subject: email.subject,
      snippet: snippet(email.body),
      reason,
      client_id: CLIENT_ID,
    },
    'completed'
  );
}

async function logInboundNoise(email, reason = 'noise') {
  await db.logAgentAction(
    AGENT_NAME,
    'inbound_noise',
    null,
    null,
    {
      gmail_message_id: email.id,
      gmail_thread_id: email.threadId || null,
      gmail_thread_link: gmailThreadLink(email),
      sender: normalizeEmailAddress(email.from),
      from: email.from,
      subject: email.subject,
      snippet: snippet(email.body),
      reason,
      client_id: CLIENT_ID,
    },
    'completed'
  );
}

async function updateProspectFromReply(prospect, classification) {
  const pool = require('./db');
  if (classification === 'interested') {
    await pool.query(
      `UPDATE prospects SET status = 'warm', updated_at = NOW()
       WHERE id = $1 AND client_id = $2 AND status <> 'closed'`,
      [prospect.id, CLIENT_ID]
    );
    console.log(`  [Riley] ${prospect.email} upgraded to warm`);
  }
  if (classification === 'unsubscribe') {
    await pool.query(
      `UPDATE prospects
       SET do_not_contact = true,
           status = CASE WHEN status = 'closed' THEN status ELSE 'dead' END,
           updated_at = NOW()
       WHERE id = $1 AND client_id = $2`,
      [prospect.id, CLIENT_ID]
    );
    console.log(`  [Riley] ${prospect.email} marked do_not_contact + dead`);
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

async function updateProspectStatus(prospectId, newStatus, autoResponderUntil = null) {
  const pool = require('./db');
  const updated = await pool.query(`
    UPDATE prospects
    SET status = $1,
        auto_responder_until = CASE
          WHEN $1 = 'auto_responder' THEN $2::timestamp
          WHEN $1 IN ('bounced', 'do_not_email') THEN NULL
          ELSE auto_responder_until
        END,
        updated_at = NOW()
    WHERE id = $3
      AND client_id = $4
    RETURNING id, status, auto_responder_until
  `, [newStatus, autoResponderUntil, prospectId, CLIENT_ID]);

  const row = updated.rows[0] || null;
  await db.logAgentAction(
    AGENT_NAME,
    'prospect_status_updated',
    prospectId,
    null,
    {
      prospect_id: prospectId,
      new_status: newStatus,
      auto_responder_until: row?.auto_responder_until || autoResponderUntil || null,
      matched: Boolean(row),
      client_id: CLIENT_ID,
    },
    'completed'
  );

  return row;
}

async function logInboundTouchpoint(prospect, email, classification) {
  const pool = require('./db');
  const actionType = classification === 'unsubscribe'
    ? 'unsubscribed'
    : classification === 'out_of_office'
      ? 'out_of_office'
      : 'inbound_reply';
  const sentiment = classification === 'interested'
    ? 'positive'
    : classification === 'negative' || classification === 'unsubscribe'
      ? 'negative'
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
  if (!(await prospectExists(prospect?.id, CLIENT_ID))) {
    await logSignalDroppedNoProspect({
      prospect_id: prospect?.id,
      trigger: 'reply_required',
      payload: { email: prospect?.email || email?.from || null, subject: email?.subject || null },
    });
    console.warn(`  [Riley] Dropped reply_required signal for missing prospect_id=${prospect?.id || 'none'}`);
    return false;
  }

  const pool = require('./db');
  const bizName = prospect.notes?.split('—')[0]?.trim() || prospect.email;
  await pool.query(
    'INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, created_at, client_id) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)',
    [
      'riley',
      'reply_required',
      'Warm reply from ' + bizName,
      prospect.email + ' replied to your outreach. Suggested reply below — approve or edit before sending.',
      JSON.stringify({
        prospect_id: prospect.id,
        email: prospect.email,
        subject: email.subject,
        from: email.from,
        body: email.body,
        suggested_reply: suggestedReply,
        human_review_required: true,
      }),
      'pending',
      CLIENT_ID
    ]
  );
  console.log('  [Riley] Human review action deposited for Max');
  return true;
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
  await ensureOpenSignalSchema(pool);
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
      FROM email_events ee
      WHERE ee.prospect_id = p.id
        AND ee.client_id = p.client_id
        AND ee.event_type IN ('opened', 'open')
        AND ee.open_source = $3::open_source
        AND (
          EXISTS (
            SELECT 1
            FROM email_events sent
            WHERE sent.client_id = ee.client_id
              AND sent.prospect_id = ee.prospect_id
              AND sent.event_type IN ('sent', 'delivered')
              AND (
                (ee.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ee.brevo_message_id)
                OR (
                  ee.brevo_message_id IS NULL
                  AND LOWER(sent.recipient_email) = LOWER(ee.recipient_email)
                  AND sent.subject_line IS NOT DISTINCT FROM ee.subject_line
                  AND sent.event_at <= ee.event_at
                )
              )
          )
          OR EXISTS (
            SELECT 1
            FROM agent_log al
            WHERE al.client_id = ee.client_id
              AND al.prospect_id = ee.prospect_id
              AND al.agent_name = 'emmett'
              AND al.action = 'email_sent'
              AND (
                (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
                OR (
                  ee.brevo_message_id IS NULL
                  AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                  AND al.ran_at <= ee.event_at
                )
              )
          )
        )
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
  `, [prospectId, clientId, OPEN_SOURCE.HUMAN]);

  const row = res.rows[0];
  if (!row) return null;
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
  if (!ctx) {
    await logSignalDroppedNoProspect({
      prospect_id,
      client_id,
      trigger: trigger || 'warm_signal',
      payload: { email, company, subject, total_opens },
    });
    console.warn(`  [Riley] Dropped warm signal for missing prospect_id=${prospect_id}`);
    return false;
  }
  const clientConfig = await getClientConfig(ctx.client_id);
  const tier = resolveVerticalTier(ctx.vertical, clientConfig);
  if (!tier.warm_eligible) {
    await pool.query(
      `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
       VALUES ($1, 'warm_signal_tier_blocked', $2, $3::jsonb, 'skipped', NOW(), $4)`,
      [AGENT_NAME, ctx.prospect_id, JSON.stringify({ trigger, raw_vertical: ctx.vertical || null, normalized_vertical: tier.vertical, tier: tier.tier }), ctx.client_id]
    );
    console.log(`  [Riley] Warm signal blocked by tier ${tier.tier}: ${ctx.email || ctx.prospect_id}`);
    return false;
  }
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

// Riley owns ICP recalculation for every inbound email signal it processes
// (opens, clicks, replies, bounces, unsubscribes). The Brevo webhook handler
// calls this for open/click/bounce/unsubscribe events; run() calls it for
// classified replies. Best-effort — a recalc failure never blocks triage.
async function recalcICPAfterEmailEvent(prospectId, clientId = CLIENT_ID, eventType = 'email_event') {
  if (!prospectId) return;
  try {
    await recalculateICP(prospectId, { clientId, reason: `riley:${eventType}` });
  } catch (err) {
    console.error(`[Riley] recalcICPAfterEmailEvent (${eventType}) failed for ${prospectId}:`, err.message);
  }
}

async function notifyWarmReply(prospect, email, classification) {
  if (classification !== 'interested') return false;
  await depositWarmSignalAction({
    prospect_id: prospect.id,
    client_id: CLIENT_ID,
    trigger: 'reply',
    signal_timestamp: signalTimestampFromEmail(email),
    subject: email.subject,
    email: prospect.email,
    company: companyName(prospect),
  });
  return true;
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

function parseAgentLogPayload(payload) {
  if (!payload) return {};
  if (typeof payload === 'object') return payload;
  try {
    return JSON.parse(payload);
  } catch (_) {
    return {};
  }
}

function messageFromReplyClassifiedLog(row) {
  const payload = parseAgentLogPayload(row.payload);
  return {
    id: payload.gmail_message_id || row.id,
    from: payload.from || '',
    subject: payload.subject || '(no subject)',
    body: payload.body || '',
    headers: payload.headers || {},
  };
}

async function runInboundBackfillDryRun() {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT id, payload
    FROM agent_log
    WHERE agent_name = 'riley'
      AND action = 'reply_classified'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '30 days'
    ORDER BY ran_at ASC, id ASC
  `, [CLIENT_ID]);

  const rows = [];
  for (const row of res.rows) {
    const message = messageFromReplyClassifiedLog(row);
    const result = await classifyInbound(message, { dryRun: true, logHaiku: false });
    rows.push({
      original_id: row.id,
      sender: normalizeEmailAddress(message.from),
      subject: message.subject,
      new_tier: result.tier,
      new_action: result.action,
      matched_prospect_id: result.prospectId || null,
      reason: result.reason,
    });
  }

  return rows;
}

async function runLiveInboundDryRun(options = {}) {
  const hours = Math.max(1, Number(options.hours || 24));
  const days = Math.max(1, Math.ceil(hours / 24));
  const auth = await getAuthClient();
  if (!auth) throw new Error('Gmail auth unavailable');

  const emails = await getInboundEmails(auth, {
    query: `in:inbox newer_than:${days}d -label:"Riley/Processed" -from:me`,
    maxResults: options.maxResults || 50,
  });

  const cutoff = Date.now() - hours * 60 * 60 * 1000;
  const rows = [];
  for (const email of emails) {
    const timestamp = email.date ? new Date(email.date).getTime() : Date.now();
    if (Number.isFinite(timestamp) && timestamp < cutoff) continue;
    const result = await classifyInbound(email, { dryRun: true, logHaiku: false });
    rows.push({
      gmail_message_id: email.id,
      sender: normalizeEmailAddress(email.from),
      subject: email.subject,
      new_tier: result.tier,
      new_action: result.action,
      matched_prospect_id: result.prospectId || null,
      reason: result.reason,
    });
  }

  return rows;
}

// Reset prospects that were contacted but have shown no touchpoint activity
// in 21+ days back to 'cold' so they re-enter the cold outreach pool.
async function resetStaleContactedProspects() {
  const pool = require('./db');
  try {
    const res = await pool.query(`
      UPDATE prospects p
      SET status = 'cold', updated_at = NOW()
      WHERE p.client_id = $1
        AND p.status = 'contacted'
        AND NOT EXISTS (
          SELECT 1 FROM touchpoints t
          WHERE t.prospect_id = p.id
            AND t.client_id = p.client_id
            AND t.created_at > NOW() - INTERVAL '21 days'
        )
      RETURNING p.id
    `, [CLIENT_ID]);
    if (res.rows.length > 0) {
      console.log(`[Riley] Reset ${res.rows.length} stale contacted prospect(s) to cold (21d no activity)`);
      await db.logAgentAction(AGENT_NAME, 'stale_contacted_reset', null, null,
        { reset_count: res.rows.length, client_id: CLIENT_ID }, 'success').catch(() => {});
    }
  } catch (err) {
    console.error('[Riley] resetStaleContactedProspects error:', err.message);
  }
}

// ── GMAIL LABELING ────────────────────────────────────────────────────
const RILEY_LABELS = {
  root: 'Riley',
  processed: 'Riley/Processed',
  reply: 'Riley/Processed/Reply',
  statusUpdate: 'Riley/Processed/StatusUpdate',
  unidentified: 'Riley/Processed/Unidentified',
  noise: 'Riley/Noise',
};
let gmailLabelCache = null;

async function getOrCreateRileyLabels(auth) {
  const gmail = google.gmail({ version: 'v1', auth });
  if (gmailLabelCache) return gmailLabelCache;

  const existing = await gmail.users.labels.list({ userId: 'me' });
  const byName = new Map((existing.data.labels || []).map(label => [label.name, label.id]));

  for (const name of Object.values(RILEY_LABELS)) {
    if (byName.has(name)) continue;
    const created = await gmail.users.labels.create({
      userId: 'me',
      requestBody: {
        name,
        labelListVisibility: 'labelShow',
        messageListVisibility: 'show',
      },
    });
    byName.set(name, created.data.id);
  }

  gmailLabelCache = byName;
  return byName;
}

function labelNameForTier(tier) {
  if (tier === 1) return RILEY_LABELS.reply;
  if (tier === 2) return RILEY_LABELS.statusUpdate;
  if (tier === 3) return RILEY_LABELS.unidentified;
  if (tier === 4) return RILEY_LABELS.noise;
  return RILEY_LABELS.processed;
}

async function applyRileyLabels(auth, messageId, tier) {
  const gmail = google.gmail({ version: 'v1', auth });
  const labels = await getOrCreateRileyLabels(auth);
  const names = [RILEY_LABELS.processed, labelNameForTier(tier)];
  const addLabelIds = names.map(name => labels.get(name)).filter(Boolean);
  if (!addLabelIds.length) return;
  await gmail.users.messages.modify({
    userId: 'me',
    id: messageId,
    requestBody: { addLabelIds }
  });
}

async function safeApplyRileyLabels(auth, messageId, tier) {
  try {
    await applyRileyLabels(auth, messageId, tier);
  } catch (err) {
    console.error(`[Riley] Failed to apply Gmail labels to message ${messageId}:`, err.message);
  }
}

// ── MAIN ──────────────────────────────────────────────────────────────
async function run() {
  const runId = makeRunId();
  let attempts = 0;
  let successes = 0;
  let skipped = 0;
  let errorSample = null;
  const finish = async (extra = {}) => {
    const result = { attempts, successes, skipped, errorSample, ...extra };
    await reportRileyRun({ runId, ...result });
    return result;
  };

  try {
  console.log('\n🤝 Riley — Inbound Triage Agent');
  console.log('─────────────────────────────────\n');
  const clientConfig = await getClientConfig(CLIENT_ID);
  if (!clientConfig) throw new Error(`Active client not found: ${CLIENT_ID}`);

  await resetStaleContactedProspects();

  if (CLIENT_ID !== 1) {
    console.log('Riley currently monitors jacob@gopulseforge.com only. MSHI triage is manual until forwarding or a second OAuth is configured.');
    return finish({ idle: true, reason: 'non_client_1' });
  }

  let emails = [];
  let auth = null;
  let gmailFailed = false;
  try {
    auth = await getAuthClient();
    if (!auth) {
      gmailFailed = true;
      console.log('[Riley] Gmail auth unavailable after token refresh failure — exiting triage gracefully.');
      await processEventNotifications();
      attempts = 1;
      successes = 0;
      errorSample = { error: 'Gmail auth unavailable after token refresh failure', client_id: CLIENT_ID };
      return finish({ failed: true, reason: 'gmail_auth_unavailable' });
    }
    emails = await getUnreadEmails(auth);
  } catch (err) {
    gmailFailed = true;
    console.error('[Riley] Gmail processing failed:', err.message);
    attempts = 1;
    successes = 0;
    errorSample = { error: err.message, client_id: CLIENT_ID };
    await db.logAgentAction(AGENT_NAME, 'triage_summary', null, null, { error: err.message, client_id: CLIENT_ID }, 'failed').catch(() => {});
  }

  if (!emails.length) {
    if (!gmailFailed) {
      console.log('No unread emails to process.');
      await db.logAgentAction(AGENT_NAME, 'triage_summary', null, null, { emails_processed: 0 }, 'success');
    }
    await processEventNotifications();
    if (gmailFailed) return finish({ failed: true, reason: 'gmail_failed' });
    return finish({ idle: true, reason: 'zero_unread' });
  }

  console.log(`Found ${emails.length} unprocessed inbox emails. Classifying...\n`);

  let stats = {
    tier1_reply: 0,
    tier2_status_update: 0,
    tier3_unidentified: 0,
    tier4_noise: 0,
    interested: 0,
    not_now: 0,
    negative: 0,
    unsubscribe: 0,
    wrong_person: 0,
    out_of_office: 0,
    unknown: 0,
  };

  for (const email of emails) {
    const stillActive = await getClientConfig(CLIENT_ID);
    if (!stillActive) {
      throw new Error(`[Riley] Client ${CLIENT_ID} deactivated mid-run — aborting`);
    }

    console.log(`Processing: ${email.subject} — ${email.from}`);

    try {
      attempts++;
      const inbound = await classifyInbound(email, { logHaiku: true });
      console.log(`  [Riley] Tier ${inbound.tier} → ${inbound.action}: ${inbound.reason}`);

      if (inbound.tier === 4) {
        stats.tier4_noise += 1;
        await logInboundNoise(email, inbound.reason);
        await safeApplyRileyLabels(auth, email.id, inbound.tier);
        successes++;
        continue;
      }

      if (inbound.tier === 3) {
        stats.tier3_unidentified += 1;
        await logInboundUnidentified(email, inbound.reason);
        await safeApplyRileyLabels(auth, email.id, inbound.tier);
        successes++;
        continue;
      }

      if (inbound.tier === 2) {
        stats.tier2_status_update += 1;
        if (!inbound.prospectId) {
          await db.logAgentAction(
            AGENT_NAME,
            inbound.action,
            null,
            null,
            {
              gmail_message_id: email.id,
              gmail_thread_id: email.threadId || null,
              gmail_thread_link: gmailThreadLink(email),
              sender: normalizeEmailAddress(email.from),
              from: email.from,
              subject: email.subject,
              snippet: snippet(email.body),
              reason: inbound.reason,
              client_id: CLIENT_ID,
            },
            'completed'
          );
          await safeApplyRileyLabels(auth, email.id, inbound.tier);
          successes++;
          continue;
        }
        await updateProspectStatus(inbound.prospectId, inbound.newStatus, inbound.autoResponderUntil || null);
        const statusSignalType = inbound.newStatus === 'auto_responder'
          ? 'email_out_of_office'
          : inbound.newStatus === 'bounced'
            ? 'email_hard_bounced'
            : inbound.newStatus === 'do_not_email'
              ? 'email_negative_reply'
              : null;
        if (statusSignalType) {
          await safeIngestNormalizedSignal({
            client_id: CLIENT_ID,
            prospect_id: inbound.prospectId,
            event_type: statusSignalType,
            event_timestamp: email.date || new Date(),
            source: 'riley_gmail_status',
            source_record_id: email.id,
            metadata: { classification: inbound.reason, operational_status: inbound.newStatus },
          });
        }
        await safeApplyRileyLabels(auth, email.id, inbound.tier);
        successes++;
        continue;
      }

      const prospect = await findProspectById(inbound.prospectId);
      if (!prospect) {
        console.log('  [Riley] Classifier returned missing prospect — logging unidentified for ' + email.from);
        await logInboundUnidentified(email, 'matched_prospect_not_found');
        await safeApplyRileyLabels(auth, email.id, 3);
        successes++;
        continue;
      }

      const result = await classifyReply(email);
      console.log(`  → ${result.classification}: ${result.reason}`);

      stats.tier1_reply += 1;
      stats[result.classification] = (stats[result.classification] || 0) + 1;

      await logReplyClassified(prospect, email, result);
      await logTriageAction(prospect, email, result.classification, result.suggested_reply);

      await updateProspectFromReply(prospect, result.classification);
      await logInboundTouchpoint(prospect, email, result.classification);
      await safeIngestRileyReplySignal({
        prospect,
        email,
        classification: result.classification,
        clientId: CLIENT_ID,
      });
      if (result.classification === 'interested') {
        await depositInterestedAction(prospect, email, result.suggested_reply);
        await notifyWarmReply(prospect, email, result.classification);
      }

      // Reply/unsubscribe both shift engagement — recompute the ICP score so
      // the new signal (or penalty) is reflected immediately.
      const eventType = result.classification === 'unsubscribe' ? 'unsubscribe' : 'reply';
      await recalcICPAfterEmailEvent(prospect.id, CLIENT_ID, eventType);

      await safeApplyRileyLabels(auth, email.id, inbound.tier);
      successes++;
      await new Promise(r => setTimeout(r, 1000));
    } catch (err) {
      console.error(`  [Riley] Failed to process ${email.from}:`, err.message);
      errorSample = errorSample || { from: email.from, subject: email.subject, error: err.message, client_id: CLIENT_ID };
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
  return finish();
  } catch (err) {
    errorSample = errorSample || { error: err.message, client_id: CLIENT_ID };
    return finish({ failed: true });
  }
}

// ── WARM-OPEN GATE ──────────────────────────────────────────────────────────
// Single source of truth for the open-based warm signal trigger. We require:
//   • at least 2 email opens for the prospect
//   • at least 5 minutes between the first open and the most recent open
// This filters out preview-pane artifacts that fire in seconds while still
// catching genuine re-engagement when someone comes back to the email later.
const QUALIFYING_OPEN_MIN_COUNT = 2;
const QUALIFYING_OPEN_MIN_SPREAD_MINUTES = 5;

async function qualifyingOpenSignal(prospectId, clientId = CLIENT_ID) {
  if (!prospectId) return { qualifies: false, total_opens: 0, spread_minutes: 0 };
  const pool = require('./db');
  await ensureOpenSignalSchema(pool);
  const res = await pool.query(`
    SELECT
      COUNT(*)::int AS total_opens,
      MIN(event_at) AS first_open,
      MAX(event_at) AS last_open,
      EXTRACT(EPOCH FROM (MAX(event_at) - MIN(event_at))) / 60 AS spread_minutes
    FROM email_events ee
    WHERE ee.prospect_id = $1
      AND ee.client_id = $2
      AND ee.event_type IN ('opened', 'open')
      AND ee.open_source = $3::open_source
      AND (
        EXISTS (
          SELECT 1
          FROM email_events sent
          WHERE sent.client_id = ee.client_id
            AND sent.prospect_id = ee.prospect_id
            AND sent.event_type IN ('sent', 'delivered')
            AND (
              (ee.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ee.brevo_message_id)
              OR (
                ee.brevo_message_id IS NULL
                AND LOWER(sent.recipient_email) = LOWER(ee.recipient_email)
                AND sent.subject_line IS NOT DISTINCT FROM ee.subject_line
                AND sent.event_at <= ee.event_at
              )
            )
        )
        OR EXISTS (
          SELECT 1
          FROM agent_log al
          WHERE al.client_id = ee.client_id
            AND al.prospect_id = ee.prospect_id
            AND al.agent_name = 'emmett'
            AND al.action = 'email_sent'
            AND (
              (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
              OR (
                ee.brevo_message_id IS NULL
                AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                AND al.ran_at <= ee.event_at
              )
            )
        )
      )
  `, [prospectId, clientId, OPEN_SOURCE.HUMAN]);
  const row = res.rows[0] || {};
  const total = Number(row.total_opens || 0);
  const spread = Number(row.spread_minutes || 0);
  return {
    qualifies: total >= QUALIFYING_OPEN_MIN_COUNT && spread >= QUALIFYING_OPEN_MIN_SPREAD_MINUTES,
    total_opens: total,
    spread_minutes: spread,
    first_open: row.first_open || null,
    last_open: row.last_open || null,
  };
}

module.exports = {
  run,
  getAuthClient,
  classifyInbound,
  classifyReply,
  normalizeReplyClassification,
  parseReturnDate,
  runInboundBackfillDryRun,
  runLiveInboundDryRun,
  updateProspectStatus,
  depositWarmSignalAction,
  logSignalDroppedNoProspect,
  prospectExists,
  qualifyingOpenSignal,
  recalcICPAfterEmailEvent,
  QUALIFYING_OPEN_MIN_COUNT,
  QUALIFYING_OPEN_MIN_SPREAD_MINUTES,
};

if (require.main === module) {
  run().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
