require('dotenv').config();
const axios = require('axios');
const pool = require('./db');
const db = require('./dbClient');
const { recalculateICP } = require('./utils/icpScoring');
const {
  DISPOSITION_SET,
  applyProspectDisposition: applyStructuredDisposition,
  ensureCallDispositionSchema: ensureStructuredDispositionSchema,
  resolveCallbackAt,
} = require('./utils/callDispositions');

// Cal Batch Agent
// Schedule this in cron-jobs.org to run daily at 10am Eastern:
// GET /cron/cal_batch?secret=YOUR_CRON_SECRET

const AGENT_NAME = 'cal';
const MAX_BATCH_SIZE = 25;
const BLAND_BATCH_URL = 'https://api.bland.ai/v2/batches/create';
const BLAND_CALLS_URL = 'https://api.bland.ai/v1/calls';
const POLL_INTERVAL_MS = Number(process.env.CAL_BATCH_POLL_INTERVAL_MS || 15000);
const POLL_TIMEOUT_MS = Number(process.env.CAL_BATCH_POLL_TIMEOUT_MS || 8 * 60 * 1000);

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
    WITH queued AS (
      SELECT
        p.id, p.first_name, p.phone, p.notes, p.client_id,
        c.name AS company_name,
        q.id AS cal_queue_id,
        q.reason AS cal_queue_reason,
        q.priority AS cal_queue_priority
      FROM cal_queue q
      JOIN prospects p ON p.id = q.prospect_id AND p.client_id = q.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE q.status = 'pending'
        AND p.phone IS NOT NULL
        AND p.phone != ''
        AND COALESCE(p.do_not_contact, false) = false
      ORDER BY q.priority ASC, q.created_at ASC
      LIMIT $1
    ),
    fill AS (
      SELECT
        p.id, p.first_name, p.phone, p.notes, p.client_id,
        c.name AS company_name,
        NULL::integer AS cal_queue_id,
        NULL::text AS cal_queue_reason,
        NULL::integer AS cal_queue_priority
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.phone IS NOT NULL
      AND p.phone != ''
      AND p.status = 'cold'
      AND COALESCE(p.do_not_contact, false) = false
      AND NOT EXISTS (SELECT 1 FROM queued q WHERE q.id = p.id)
      AND NOT EXISTS (
        SELECT 1
        FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.channel = 'call'
          AND t.action_type = 'outbound'
          AND t.agent_id = 'cal'
      )
      ORDER BY p.icp_score DESC NULLS LAST, p.created_at ASC
      LIMIT $1
    )
    SELECT * FROM queued
    UNION ALL
    SELECT * FROM fill
    LIMIT $1
  `, [MAX_BATCH_SIZE]);

  return res.rows;
}

function buildCallData(prospects) {
  const skipped = [];
  const formattedProspects = [];

  for (const prospect of prospects) {
    const phoneNumber = formatPhoneNumber(prospect.phone);
    const businessName = prospect.company_name || extractBusinessName(prospect.notes);

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
        metadata: {
          prospect_id: prospect.id,
          client_id: prospect.client_id || 1,
          company_name: businessName,
          cal_queue_id: prospect.cal_queue_id || null,
          agent: AGENT_NAME,
        },
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
      metadata: call.metadata,
      request_data: {
        first_name: call.first_name,
        business_name: call.business_name,
      },
    })),
    global: {
      task: BASE_PROMPT,
      voice: 'walter',
      wait_for_greeting: true,
      answered_by_enabled: true,
      amd: true,
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

async function ensureCallDispositionSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS cal_queue (
      id SERIAL PRIMARY KEY,
      prospect_id UUID NOT NULL,
      client_id INTEGER NOT NULL,
      priority INTEGER NOT NULL DEFAULT 1,
      reason TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending'
    )
  `);
  await pool.query(`
    ALTER TABLE cal_queue ADD COLUMN IF NOT EXISTS disposition TEXT;
    ALTER TABLE cal_queue ADD COLUMN IF NOT EXISTS disposition_notes TEXT;
    ALTER TABLE cal_queue ADD COLUMN IF NOT EXISTS called_at TIMESTAMP;
  `);
  await ensureStructuredDispositionSchema(pool);
}

async function logBatchTouchpoints(formattedProspects, batchResponse) {
  const externalRef = batchResponse?.data?.batch_id || batchResponse?.batch_id || batchResponse?.id || null;

  for (const item of formattedProspects) {
    await db.logTouchpoint(
      item.prospect.id,
      'call',
      'outbound',
      `Cal batch call — ${item.businessName}`,
      'pending',
      'neutral',
      AGENT_NAME,
      externalRef
    );
  }
}

function getBatchId(batchResponse) {
  return batchResponse?.data?.batch_id || batchResponse?.batch_id || batchResponse?.id || null;
}

async function fetchBatchCalls(batchId) {
  const res = await axios.get(BLAND_CALLS_URL, {
    headers: { authorization: process.env.BLAND_API_KEY },
    params: { batch_id: batchId, limit: MAX_BATCH_SIZE },
  });
  if (Array.isArray(res.data?.calls)) return res.data.calls;
  if (Array.isArray(res.data?.data)) return res.data.data;
  if (Array.isArray(res.data)) return res.data;
  return [];
}

async function fetchCallDetails(callId) {
  const res = await axios.get(`${BLAND_CALLS_URL}/${callId}`, {
    headers: { authorization: process.env.BLAND_API_KEY },
  });
  return res.data;
}

function isCallFinished(call) {
  const status = String(call.status || call.queue_status || '').toLowerCase();
  return call.completed === true ||
    ['completed', 'complete', 'failed', 'busy', 'no-answer', 'canceled', 'unknown', 'call_error', 'complete_error', 'queue_error', 'pre_queue_error'].includes(status);
}

async function pollBatchCallDetails(batchId, expectedCount) {
  const started = Date.now();
  let latestCalls = [];

  while (Date.now() - started < POLL_TIMEOUT_MS) {
    latestCalls = await fetchBatchCalls(batchId);
    const finished = latestCalls.filter(isCallFinished);
    console.log(`Bland batch ${batchId}: ${finished.length}/${expectedCount} call result${expectedCount !== 1 ? 's' : ''} ready.`);

    if (latestCalls.length >= expectedCount && finished.length >= expectedCount) break;
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }

  const details = [];
  for (const call of latestCalls.filter(isCallFinished)) {
    if (!call.call_id) continue;
    try {
      details.push(await fetchCallDetails(call.call_id));
    } catch (err) {
      console.warn(`Could not fetch Bland call details for ${call.call_id}:`, err.response?.data || err.message);
      details.push(call);
    }
  }
  return details;
}

function callDurationSeconds(details) {
  const corrected = Number(details?.corrected_duration);
  if (Number.isFinite(corrected) && corrected >= 0) return Math.round(corrected);
  const minutes = Number(details?.call_length);
  if (Number.isFinite(minutes) && minutes >= 0) return Math.round(minutes * 60);
  return null;
}

function textForDisposition(details) {
  const parts = [
    details?.status,
    details?.queue_status,
    details?.answered_by,
    details?.error_message,
    details?.summary,
    details?.concatenated_transcript,
    JSON.stringify(details?.analysis || {}),
  ];
  return parts.filter(Boolean).join(' ').toLowerCase();
}

function mapBlandDisposition(details) {
  const answeredBy = String(details?.answered_by || '').toLowerCase();
  const status = String(details?.status || details?.queue_status || '').toLowerCase();
  const text = textForDisposition(details);

  if (/\bwrong (number|person)\b|not (the right|correct) number/.test(text)) return 'wrong_number';
  if (/not in service|number.*(disconnected|invalid|not found)|temporarily unavailable/.test(text)) return 'disconnected';
  if (status === 'no-answer' || answeredBy === 'no-answer' || status === 'busy' || status === 'canceled') return 'no_answer';
  if (answeredBy === 'voicemail' || /voicemail|answering machine/.test(text)) return 'voicemail';
  if (/call (me )?back|callback|better time|try again|later today|tomorrow|next week|next month/.test(text)) return 'answered_callback';
  if (/not interested|no thanks|don't call|do not call|stop calling|remove me/.test(text)) return 'answered_not_interested';
  if (/booked|scheduled|calendly|discovery call|interested|sounds good|send me|yes[,.\s]|yeah[,.\s]|sure[,.\s]/.test(text)) return 'answered_interested';
  if (answeredBy === 'human' || answeredBy === 'unknown') return 'answered_not_interested';
  if (status === 'failed') return 'disconnected';
  return 'no_answer';
}

function extractNotes(details) {
  return String(details?.summary || details?.error_message || details?.concatenated_transcript || '').slice(0, 1000);
}

function nextBusinessDayTen() {
  const date = new Date();
  date.setDate(date.getDate() + 1);
  date.setHours(10, 0, 0, 0);
  while ([0, 6].includes(date.getDay())) {
    date.setDate(date.getDate() + 1);
  }
  return date;
}

function extractCallbackAt(details) {
  const candidates = [
    details?.analysis?.callback_at,
    details?.analysis?.callback_time,
    details?.variables?.callback_at,
    details?.variables?.callback_time,
  ].filter(Boolean);
  for (const candidate of candidates) {
    const date = new Date(candidate);
    if (!isNaN(date.getTime())) return date;
  }
  return nextBusinessDayTen();
}

async function applyProspectDisposition(prospectId, clientId, disposition, details) {
  const requestedCallback = disposition === 'answered_callback' ? extractCallbackAt(details) : null;
  await applyStructuredDisposition(pool, {
    prospectId,
    clientId,
    disposition,
    callbackAt: resolveCallbackAt(disposition, requestedCallback),
  });

  if (['answered_interested', 'answered_callback', 'answered_not_interested', 'voicemail'].includes(disposition)) {
    await recalculateICP(prospectId, {
      clientId,
      reason: `call_disposition:${disposition}`,
    }).catch(err => console.warn(`ICP recalc failed for ${prospectId}:`, err.message));
  }
}

async function recordCallDisposition(item, details) {
  const clientId = Number(item.prospect.client_id || details?.metadata?.client_id || 1);
  const disposition = mapBlandDisposition(details);
  if (!DISPOSITION_SET.has(disposition)) return null;

  const durationSeconds = callDurationSeconds(details);
  const notes = extractNotes(details);
  const queueId = item.prospect.cal_queue_id || details?.metadata?.cal_queue_id || null;

  await pool.query(
    `INSERT INTO call_dispositions
      (prospect_id, client_id, call_duration_seconds, disposition, notes, cal_queue_id, source)
     VALUES ($1, $2, $3, $4, $5, $6, 'cal')`,
    [item.prospect.id, clientId, durationSeconds, disposition, notes, queueId]
  );

  if (queueId) {
    await pool.query(
      `UPDATE cal_queue
       SET status = 'completed', disposition = $1, disposition_notes = $2, called_at = NOW()
       WHERE id = $3`,
      [disposition, notes, queueId]
    );
  }

  await applyProspectDisposition(item.prospect.id, clientId, disposition, details);

  await db.logAgentAction(
    AGENT_NAME,
    'call_disposition',
    item.prospect.id,
    null,
    {
      prospect_id: item.prospect.id,
      disposition,
      call_duration: durationSeconds,
      company: item.businessName,
      call_id: details?.call_id || null,
      cal_queue_id: queueId,
    },
    'success'
  );

  return { prospect_id: item.prospect.id, disposition, call_duration: durationSeconds, company: item.businessName };
}

function matchCallToProspect(details, formattedProspects, usedIds) {
  const metadataProspectId = details?.metadata?.prospect_id || details?.variables?.prospect_id;
  if (metadataProspectId) {
    const byMetadata = formattedProspects.find(item => String(item.prospect.id) === String(metadataProspectId) && !usedIds.has(item.prospect.id));
    if (byMetadata) return byMetadata;
  }

  const to = formatPhoneNumber(details?.to || details?.request_data?.phone_number || details?.variables?.to);
  if (!to) return null;
  return formattedProspects.find(item => item.callData.phone_number === to && !usedIds.has(item.prospect.id)) || null;
}

async function processBatchDispositions(formattedProspects, batchResponse) {
  const batchId = getBatchId(batchResponse);
  if (!batchId) {
    console.warn('No Bland batch_id returned; skipping disposition polling.');
    return [];
  }

  const callDetails = await pollBatchCallDetails(batchId, formattedProspects.length);
  const usedIds = new Set();
  const recorded = [];

  for (const details of callDetails) {
    const item = matchCallToProspect(details, formattedProspects, usedIds);
    if (!item) continue;
    usedIds.add(item.prospect.id);
    const disposition = await recordCallDisposition(item, details);
    if (disposition) recorded.push(disposition);
  }

  return recorded;
}

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
    await ensureCallDispositionSchema();

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
    const dispositions = await processBatchDispositions(formattedProspects, response);

    await db.logAgentAction(
      AGENT_NAME,
      'batch_call',
      null,
      null,
      {
        batch_name: payload.description,
        calls_queued: callData.length,
        dispositions_recorded: dispositions.length,
        skipped,
        bland_response: response,
      },
      'success',
      null,
      Date.now() - startedAt
    );

    console.log(`Cal batch complete — queued ${callData.length} call${callData.length !== 1 ? 's' : ''}, recorded ${dispositions.length} disposition${dispositions.length !== 1 ? 's' : ''}.`);
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
  ensureCallDispositionSchema,
  extractBusinessName,
  formatPhoneNumber,
  getBatchCandidates,
  mapBlandDisposition,
  processBatchDispositions,
  run,
};

if (require.main === module) {
  run().catch(err => {
    console.error('[CalBatch] Fatal error:', err.message);
    process.exit(1);
  });
}
