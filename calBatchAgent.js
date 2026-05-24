require('dotenv').config();
const axios = require('axios');
const pool = require('./db');
const db = require('./dbClient');

// Cal Batch Agent
// Schedule this in cron-jobs.org to run daily at 10am Eastern:
// GET /cron/cal_batch?secret=YOUR_CRON_SECRET

const AGENT_NAME = 'cal';
const MAX_BATCH_SIZE = 25;
const BLAND_BATCH_URL = 'https://api.bland.ai/v2/batches/create';

const BASE_PROMPT = `You are Cal, an appointment setter for Pulseforge, an AI marketing and automation agency based in Manchester, New Hampshire. You are calling local service business owners in Southern New Hampshire on behalf of Jacob Maynard.

Your only goal is to book a 15-minute discovery call between the prospect and Jacob. You are not selling anything on this call.

Before speaking, wait for a human to say something. If you hear ringing, hold music, or an automated system, wait silently until a real person speaks. Do not begin your opener until you hear a live human voice respond.

OPENING:
"Hey, how's it going? This is Cal calling from Pulseforge — we work with local service businesses in Southern New Hampshire on getting more customers without adding more to your plate. I'll keep this really quick — is getting more consistent customers something you're actively working on right now, or is the timing just not great?"

IF THEY SAY YES OR MAYBE:
"Perfect. Jacob, our founder, has a 15-minute call where he shows local business owners exactly what this looks like for their specific business. No pitch, just a walkthrough. Would [day] or [day] work better for you?"
Then offer two specific time slots and book using https://calendly.com/jacob-gopulseforge/new-meeting

IF THEY ASK WHAT YOU DO:
"We build automated outreach systems for local service businesses — the system finds local prospects, follows up on their behalf, and keeps their name visible between jobs. It runs in the background while they run the business. Jacob can show you the whole thing in 15 minutes."

IF THEY ASK HOW MUCH IT COSTS:
"That's exactly what Jacob covers on the call — and there's actually a 90-day free trial right now so there's nothing to lose. Want to grab that 15 minutes with him?"

IF THEY SAY NOT INTERESTED:
"Totally understand. Can I ask — is it the timing or just not something you're looking at right now?" If they confirm not interested, thank them and end the call professionally. Never push.

IF THEY SAY SEND ME MORE INFO:
"Absolutely, I can have Jacob send something over. What's the best email for you?"

IF THEY SAY CALL ME BACK:
"Of course — when is a better time to reach you?"

VOICEMAIL SCRIPT:
"Hey, this is Cal calling from Pulseforge in Manchester. We help local service businesses in Southern New Hampshire get more customers automatically. Jacob wanted to reach out personally — give us a call back at your convenience or I'll try you again in a couple days. Have a great day."

RULES:
Never use dashes in speech.
Keep responses short and conversational.
Never read long paragraphs out loud.
Never claim to be human if asked directly — say I'm an AI assistant working with Jacob at Pulseforge.
Never discuss pricing beyond the 90-day free trial.
Never book calls outside of 9am to 5pm Eastern Monday through Friday.
End every call professionally regardless of outcome.`;

function formatPhoneNumber(phone) {
  if (!phone) return null;

  const digits = String(phone).replace(/\D/g, '');

  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;

  return null;
}

function extractBusinessName(notes) {
  if (!notes) return '';
  return String(notes).split(' — ')[0].trim();
}

function formatDateForBatchName(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

async function getBatchCandidates() {
  const res = await pool.query(`
    SELECT id, first_name, phone, notes
    FROM prospects p
    WHERE p.phone IS NOT NULL
      AND p.phone != ''
      AND p.status = 'cold'
      AND p.do_not_contact = false
      AND NOT EXISTS (
        SELECT 1
        FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'phone'
          AND t.action_type = 'outbound'
      )
    ORDER BY p.icp_score DESC NULLS LAST, p.created_at ASC
    LIMIT $1
  `, [MAX_BATCH_SIZE]);

  return res.rows;
}

function buildCallData(prospects) {
  const skipped = [];
  const formattedProspects = [];

  for (const prospect of prospects) {
    const phoneNumber = formatPhoneNumber(prospect.phone);
    const businessName = extractBusinessName(prospect.notes);

    if (!phoneNumber) {
      skipped.push({ prospect_id: prospect.id, phone: prospect.phone, reason: 'invalid_phone' });
      continue;
    }

    formattedProspects.push({
      prospect,
      businessName,
      callData: {
        phone_number: phoneNumber,
        first_name: prospect.first_name || '',
        business_name: businessName,
      },
    });
  }

  return { formattedProspects, skipped };
}

async function createBlandBatch(callData) {
  const payload = {
    description: `Pulseforge Auto Batch — ${formatDateForBatchName()}`,
    call_objects: callData.map(call => ({
      phone_number: call.phone_number,
      request_data: {
        first_name: call.first_name,
        business_name: call.business_name,
      },
    })),
    global: {
      task: BASE_PROMPT,
      voice: 'walter',
      wait_for_greeting: true,
      interruption_threshold: 1500,
      from: process.env.BLAND_PHONE_NUMBER,
      max_duration: 2,
    },
  };

  const res = await axios.post(BLAND_BATCH_URL, payload, {
    headers: {
      authorization: process.env.BLAND_API_KEY,
      'Content-Type': 'application/json',
    },
  });

  return { payload, response: res.data };
}

async function logBatchTouchpoints(formattedProspects, batchResponse) {
  const externalRef = batchResponse?.data?.batch_id || batchResponse?.batch_id || batchResponse?.id || null;

  for (const item of formattedProspects) {
    await db.logTouchpoint(
      item.prospect.id,
      'phone',
      'outbound',
      `Cal batch call — ${item.businessName}`,
      'pending',
      'neutral',
      AGENT_NAME,
      externalRef
    );
  }
}

async function run() {
  const startedAt = Date.now();
  console.log('\nCal batch agent running...\n');

  if (!process.env.BLAND_API_KEY || !process.env.BLAND_PHONE_NUMBER) {
    const missing = {
      BLAND_API_KEY: Boolean(process.env.BLAND_API_KEY),
      BLAND_PHONE_NUMBER: Boolean(process.env.BLAND_PHONE_NUMBER),
    };
    console.warn('Cal batch: Bland.ai credentials not fully configured — exiting.');
    await db.logAgentAction(AGENT_NAME, 'batch_call', null, null, { missing }, 'failed', 'Missing Bland.ai configuration');
    return;
  }

  try {
    const prospects = await getBatchCandidates();
    const { formattedProspects, skipped } = buildCallData(prospects);

    console.log(`Found ${prospects.length} candidate${prospects.length !== 1 ? 's' : ''}.`);
    if (skipped.length) console.log(`Skipped ${skipped.length} candidate${skipped.length !== 1 ? 's' : ''} with invalid phone numbers.`);

    if (!formattedProspects.length) {
      await db.logAgentAction(
        AGENT_NAME,
        'batch_call',
        null,
        null,
        { prospects_found: prospects.length, calls_queued: 0, skipped },
        'success',
        null,
        Date.now() - startedAt
      );
      console.log('No valid prospects to include in this batch.');
      return;
    }

    const callData = formattedProspects.map(item => item.callData);
    const { payload, response } = await createBlandBatch(callData);

    await logBatchTouchpoints(formattedProspects, response);

    await db.logAgentAction(
      AGENT_NAME,
      'batch_call',
      null,
      null,
      {
        batch_name: payload.description,
        calls_queued: callData.length,
        skipped,
        bland_response: response,
      },
      'success',
      null,
      Date.now() - startedAt
    );

    console.log(`Cal batch complete — queued ${callData.length} call${callData.length !== 1 ? 's' : ''}.`);
  } catch (err) {
    console.error('Cal batch failed:', err.response?.data || err.message);
    await db.logAgentAction(
      AGENT_NAME,
      'batch_call',
      null,
      null,
      { error: err.response?.data || err.message },
      'failed',
      err.message,
      Date.now() - startedAt
    );
  }
}

module.exports = {
  BASE_PROMPT,
  buildCallData,
  createBlandBatch,
  extractBusinessName,
  formatPhoneNumber,
  getBatchCandidates,
  run,
};

if (require.main === module) {
  run().catch(err => {
    console.error('[CalBatch] Fatal error:', err.message);
    process.exit(1);
  });
}
