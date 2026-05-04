require('dotenv').config();
const axios = require('axios');
const pool = require('./db');
const db = require('./dbClient');

const AGENT_NAME = 'cal';
const MAX_CALLS_PER_RUN = 10;
// Needs https://www.googleapis.com/auth/calendar scope — separate from the GBP token
const CALENDAR_REFRESH_TOKEN = process.env.GOOGLE_CALENDAR_REFRESH_TOKEN;

// ── QUERY ──────────────────────────────────────────────────────────────────
async function getCallCandidates() {
  const res = await pool.query(`
    SELECT
      p.id, p.first_name, p.last_name, p.email, p.phone,
      p.status, p.icp_score, p.do_not_contact,
      c.name  AS company_name,
      c.industry,
      c.location
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id
    WHERE p.status = 'warm'
      AND p.do_not_contact = false
      AND p.phone IS NOT NULL AND p.phone != ''
      AND p.icp_score >= 60
      AND (
        SELECT COUNT(*) FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
          AND t.created_at > NOW() - INTERVAL '14 days'
      ) >= 3
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t2
        WHERE t2.prospect_id = p.id
          AND t2.channel = 'call'
          AND t2.created_at > NOW() - INTERVAL '7 days'
      )
    ORDER BY p.icp_score DESC
    LIMIT $1
  `, [MAX_CALLS_PER_RUN]);
  return res.rows;
}

// ── BLAND.AI ───────────────────────────────────────────────────────────────
function buildCallTask(prospect, companyName) {
  const firstName = prospect.first_name || 'there';
  const business  = companyName || 'your business';

  return `You are Cal, an AI calling agent for Pulseforge — a local marketing and automation company in Manchester, NH. You are calling on behalf of Jacob Maynard.

YOUR GOAL: Book a 20-minute discovery call with Jacob.

OPENING:
"Hi, is this ${firstName}? Hey ${firstName}, my name is Cal — I'm calling on behalf of Jacob Maynard at Pulseforge. We're a local marketing and automation company out of Manchester. Do you have two minutes?"

IF YES:
"Jacob actually put together a quick look at what an automated marketing system could look like specifically for ${business}. He'd love to walk you through it on a free 20-minute call — no pitch, just a look. Would you have time this week?"

IF THEY ASK WHAT IT COSTS:
"The call is completely free. Jacob looks at your specific situation and gives you an honest read on whether it makes sense. No obligation at all."

ONCE THEY AGREE:
Ask for their preferred day and time this week. Then confirm their email address. Say: "Perfect — I'll have Jacob's team send a calendar invite to [their email]."

OBJECTION HANDLING:
- "Not interested": "Totally understand — I won't take up more of your time. The call is really just a look — Jacob's worked with a few businesses in the area and it's been useful even for owners who didn't move forward. Worth 20 minutes?"
- "Too busy": "I hear you — that's actually why most owners we work with got started. Jacob can do a call whenever works for you, even early morning or end of day."
- "Already have marketing": "That makes sense. The call is really about the automated side — a lot of owners are doing marketing but losing time on the repetitive parts. That's the gap Jacob fills."
- "Send me an email": "Happy to do that. Jacob may have already sent something — it can hit spam. What email should I make sure it reaches?"
- If directly asked "Are you a real person or AI?": Answer honestly — "I'm an AI assistant calling on Jacob's behalf. Jacob himself will be on the discovery call."

DO NOT make promises about revenue or results. Do not push past two objections — end the call politely if they're not interested.`;
}

async function initiateCall(prospect, companyName) {
  const appUrl = process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app';

  const res = await axios.post('https://api.bland.ai/v1/calls', {
    phone_number:          prospect.phone,
    task:                  buildCallTask(prospect, companyName),
    voice:                 'nat',
    wait_for_greeting:     true,
    record:                true,
    answered_by_enabled:   true,
    amd:                   true,
    noise_cancellation:    true,
    interruption_threshold: 100,
    max_duration:          10,
    webhook:               `${appUrl}/webhooks/bland`,
    metadata: {
      prospect_id:  prospect.id,
      company_name: companyName,
      agent:        AGENT_NAME,
    },
  }, {
    headers: {
      authorization:  process.env.BLAND_API_KEY,
      'Content-Type': 'application/json',
    },
  });

  return res.data;
}

// ── GOOGLE CALENDAR ────────────────────────────────────────────────────────
// Requires a refresh token obtained with scope: https://www.googleapis.com/auth/calendar
// Set GOOGLE_CALENDAR_REFRESH_TOKEN in Railway — different from the GBP token.
async function createCalendarEvent(prospectName, businessName, agreedTimeISO) {
  if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET || !CALENDAR_REFRESH_TOKEN) {
    console.log('Cal: GOOGLE_CALENDAR_REFRESH_TOKEN not set — skipping calendar event');
    return null;
  }

  try {
    const tokenRes = await axios.post('https://oauth2.googleapis.com/token', {
      client_id:     process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
      refresh_token: CALENDAR_REFRESH_TOKEN,
      grant_type:    'refresh_token',
    });
    const token = tokenRes.data.access_token;

    const start = new Date(agreedTimeISO);
    if (isNaN(start.getTime())) {
      console.log('Cal: Cannot parse agreed time for calendar:', agreedTimeISO);
      return null;
    }
    const end = new Date(start.getTime() + 20 * 60 * 1000);

    const event = {
      summary: `Discovery Call — ${prospectName} (${businessName})`,
      description: `20-minute discovery call booked by Cal (Pulseforge AI calling agent).\n\nProspect: ${prospectName}\nBusiness: ${businessName}`,
      start: { dateTime: start.toISOString(), timeZone: 'America/New_York' },
      end:   { dateTime: end.toISOString(),   timeZone: 'America/New_York' },
    };

    const res = await axios.post(
      'https://www.googleapis.com/calendar/v3/calendars/primary/events',
      event,
      { headers: { Authorization: `Bearer ${token}` } }
    );

    console.log('Cal: Calendar event created:', res.data.htmlLink);
    return res.data;
  } catch (err) {
    console.error('Cal: Calendar creation failed:', err.response?.data || err.message);
    return null;
  }
}

// ── TELEGRAM ───────────────────────────────────────────────────────────────
async function notify(text) {
  const BOT = process.env.TELEGRAM_BOT_TOKEN;
  const CHAT = process.env.TELEGRAM_CHAT_ID;
  if (!BOT || !CHAT) return;
  await axios.post(`https://api.telegram.org/bot${BOT}/sendMessage`, { chat_id: CHAT, text })
    .catch(err => console.error('Cal Telegram error:', err.message));
}

// ── MAIN ───────────────────────────────────────────────────────────────────
async function run() {
  console.log('\nCal agent running...\n');

  if (!process.env.BLAND_API_KEY) {
    console.warn('Cal: BLAND_API_KEY not set — exiting. Add it to Railway env vars to activate calling.');
    return;
  }

  const candidates = await getCallCandidates();
  console.log(`Found ${candidates.length} candidate${candidates.length !== 1 ? 's' : ''} for calling.\n`);

  if (!candidates.length) {
    console.log('No qualifying prospects to call this run.');
    await db.logAgentAction(AGENT_NAME, 'run', null, null, { calls_initiated: 0 }, 'success');
    return;
  }

  let initiated = 0;

  for (const prospect of candidates) {
    const companyName = prospect.company_name || '';
    const fullName = [prospect.first_name, prospect.last_name].filter(Boolean).join(' ') || 'Unknown';

    console.log(`Calling: ${fullName} — ${companyName} (${prospect.phone})`);

    try {
      const result = await initiateCall(prospect, companyName);
      const callId = result.call_id;

      console.log(`  ✓ Initiated — call_id: ${callId}`);

      await db.logTouchpoint(
        prospect.id,
        'call',
        'outbound',
        `Outbound call via Bland.ai — ${companyName}`,
        'pending',
        'neutral',
        AGENT_NAME,
        callId
      );

      await db.logAgentAction(
        AGENT_NAME,
        'initiate_call',
        prospect.id,
        null,
        { call_id: callId, phone: prospect.phone, company: companyName },
        'success'
      );

      await notify([
        `📞 Cal initiated a call`,
        ``,
        `Prospect: ${fullName}`,
        `Business: ${companyName || 'Unknown'}`,
        `Phone: ${prospect.phone}`,
        `ICP Score: ${prospect.icp_score}`,
        `Call ID: ${callId}`,
      ].join('\n'));

      initiated++;
      await new Promise(r => setTimeout(r, 2000));

    } catch (err) {
      console.error(`  ✗ Failed to call ${fullName}:`, err.response?.data || err.message);
      await db.logAgentAction(
        AGENT_NAME,
        'initiate_call',
        prospect.id,
        null,
        { phone: prospect.phone, error: err.message },
        'error'
      );
    }
  }

  await db.logAgentAction(AGENT_NAME, 'run', null, null, { calls_initiated: initiated }, 'success');
  console.log(`\nCal complete — ${initiated} call${initiated !== 1 ? 's' : ''} initiated.`);
}

module.exports = { createCalendarEvent, notify };

run().catch(console.error);
