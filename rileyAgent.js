require('dotenv').config();
const { google } = require('googleapis');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const fs = require('fs');
const path = require('path');

const AGENT_NAME = 'riley';
const CREDENTIALS_PATH = './gmail_credentials.json';
const TOKEN_PATH = './gmail_token.json';
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify'
];

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── AUTH ──────────────────────────────────────────────────────────────
async function getAuthClient() {
  const credentials = process.env.GMAIL_CREDENTIALS ? JSON.parse(process.env.GMAIL_CREDENTIALS) : JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
  const { client_secret, client_id, redirect_uris } = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

  if (process.env.GMAIL_TOKEN) {
    const token = JSON.parse(process.env.GMAIL_TOKEN);
    oAuth2Client.setCredentials(token);
    return oAuth2Client;
  }

  if (fs.existsSync(TOKEN_PATH)) {
    const token = JSON.parse(fs.readFileSync(TOKEN_PATH));
    oAuth2Client.setCredentials(token);
    return oAuth2Client;
  }

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
async function getUnreadEmails(auth) {
  const gmail = google.gmail({ version: 'v1', auth });
  const res = await gmail.users.messages.list({
    userId: 'me',
    q: 'is:unread -from:me -from:warmupinbox.com -from:amazonses.com -from:dmarc -from:noreply-dmarc -from:linkedin.com -from:google.com -from:brevo.com -from:mailer-daemon -from:calendly.com -from:notifications -subject:"Unlock" -subject:"Supercharge" -subject:"Optimize" -subject:"Reimagine" -subject:"Curious About Your Approach" -subject:"Quick Question About Your"',
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

    let body = '';
    const parts = full.data.payload.parts || [full.data.payload];
    for (const part of parts) {
      if (part.mimeType === 'text/plain' && part.body?.data) {
        body = Buffer.from(part.body.data, 'base64').toString('utf8');
        break;
      }
    }

    emails.push({ id: msg.id, subject, from, date, body: body.slice(0, 1000) });
  }

  return emails;
}

// ── CLASSIFY ──────────────────────────────────────────────────────────
async function classifyEmail(email) {
  const prompt = `You are Riley, an inbound email triage agent for Pulseforge, an AI marketing agency run by Jacob Maynard in Manchester NH.

Classify this email into exactly one category:
- interested: asks a question, mentions timing, wants more info, says yes
- not_now: busy, not right time, maybe later, polite decline
- unsubscribe: remove me, stop emailing, not interested, unsubscribe
- out_of_office: auto-reply, vacation, away message
- wrong_person: not the decision maker, wrong company, wrong person
- negative: angry, aggressive, threatening, spam complaint

Email:
From: ${email.from}
Subject: ${email.subject}
Body: ${email.body}

Respond with JSON only: { "classification": "...", "reason": "...", "suggested_reply": "..." }
For suggested_reply: write a short, warm, human reply from Jacob if classification is interested or not_now. Leave blank for others.`;

  const response = await anthropic.messages.create({
    model: 'claude-opus-4-5',
    max_tokens: 500,
    messages: [{ role: 'user', content: prompt }]
  });

  try {
    const text = response.content[0].text;
    const match = text.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
    return { classification: 'unknown', reason: 'no json found', suggested_reply: '' };
  } catch(e) {
    return { classification: 'unknown', reason: 'parse error: ' + e.message, suggested_reply: '' };
  }
}

// ── PROSPECT MATCHING ─────────────────────────────────────────────────
async function findProspectByEmail(fromEmail) {
  const pool = require('./db');
  const emailMatch = fromEmail.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
  if (!emailMatch) return null;
  const email = emailMatch[0].toLowerCase();
  const res = await pool.query(
    'SELECT * FROM prospects WHERE LOWER(email) = $1 LIMIT 1',
    [email]
  );
  return res.rows[0] || null;
}

async function updateProspectFromReply(prospect, classification) {
  const pool = require('./db');
  if (classification === 'interested') {
    await pool.query(
      'UPDATE prospects SET status = $1, updated_at = NOW() WHERE id = $2',
      ['warm', prospect.id]
    );
    console.log(`  [Riley] ${prospect.email} upgraded to warm`);
  }
  if (classification === 'unsubscribe') {
    await pool.query(
      'UPDATE prospects SET do_not_contact = true, updated_at = NOW() WHERE id = $1',
      [prospect.id]
    );
    console.log(`  [Riley] ${prospect.email} marked do_not_contact`);
  }
  if (classification === 'not_now') {
    await pool.query(
      'UPDATE prospects SET status = $1, updated_at = NOW() WHERE id = $2',
      ['not_now', prospect.id]
    );
    console.log(`  [Riley] ${prospect.email} marked not_now`);
  }
}

async function logInboundTouchpoint(prospect, email, classification) {
  const pool = require('./db');
  await pool.query(
    'INSERT INTO touchpoints (prospect_id, channel, action_type, content_summary, outcome, sentiment, created_at) VALUES ($1, $2, $3, $4, $5, $6, NOW())',
    [
      prospect.id,
      'email',
      'inbound',
      email.subject,
      JSON.stringify({ from: email.from, classification }),
      classification === 'interested' ? 'positive' : classification === 'negative' ? 'negative' : 'neutral'
    ]
  );
}

async function depositInterestedAction(prospect, email, suggestedReply) {
  const pool = require('./db');
  const bizName = prospect.notes?.split('—')[0]?.trim() || prospect.email;
  await pool.query(
    'INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, created_at) VALUES ($1, $2, $3, $4, $5, $6, NOW())',
    [
      'riley',
      'reply_required',
      'Warm reply from ' + bizName,
      prospect.email + ' replied to your outreach. Suggested reply below — approve or edit before sending.',
      JSON.stringify({ prospect_id: prospect.id, email: prospect.email, subject: email.subject, from: email.from, body: email.body, suggested_reply: suggestedReply }),
      'pending'
    ]
  );
  console.log('  [Riley] Action deposited for Max');
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

// ── MAIN ──────────────────────────────────────────────────────────────
async function run() {
  console.log('\n🤝 Riley — Inbound Triage Agent');
  console.log('─────────────────────────────────\n');

  const auth = await getAuthClient();
  const emails = await getUnreadEmails(auth);

  if (!emails.length) {
    console.log('No unread emails to process.');
    await db.logAgentAction(AGENT_NAME, 'triage', null, null, { emails_processed: 0 }, 'success');
    return;
  }

  console.log(`Found ${emails.length} unread emails. Classifying...\n`);

  let stats = { interested: 0, not_now: 0, unsubscribe: 0, out_of_office: 0, wrong_person: 0, negative: 0 };

  for (const email of emails) {
    console.log(`Processing: ${email.subject} — ${email.from}`);

    const prospect = await findProspectByEmail(email.from);
    if (!prospect) {
      console.log('  [Riley] No prospect match — skipping classification for ' + email.from);
      await markAsRead(auth, email.id);
      continue;
    }
    const result = await classifyEmail(email);
    console.log(`  → ${result.classification}: ${result.reason}`);

    stats[result.classification] = (stats[result.classification] || 0) + 1;

    // Log to Second Brain
    await db.logAgentAction(
      AGENT_NAME,
      'classify_email',
      null,
      null,
      {
        from: email.from,
        subject: email.subject,
        classification: result.classification,
        reason: result.reason,
        suggested_reply: result.suggested_reply
      },
      result.classification === 'negative' ? 'flagged' : 'success'
    );

    await updateProspectFromReply(prospect, result.classification);
    await logInboundTouchpoint(prospect, email, result.classification);
    if (result.classification === 'interested') {
      await depositInterestedAction(prospect, email, result.suggested_reply);
    }

    // Mark as read so we don't reprocess
    await markAsRead(auth, email.id);

    // Small delay
    await new Promise(r => setTimeout(r, 1000));
  }

  console.log('\n─── RILEY SUMMARY ───────────────────────────────');
  Object.entries(stats).forEach(([k, v]) => { if (v > 0) console.log(`  ${k}: ${v}`); });
  console.log('─────────────────────────────────────────────────\n');

  await db.logAgentAction(AGENT_NAME, 'triage', null, null, { ...stats, emails_processed: emails.length }, 'success');

  console.log('Riley complete.');
}

run().catch(console.error);
