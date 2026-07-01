require('dotenv').config();

const Anthropic = require('@anthropic-ai/sdk');
const pool = require('./db');
const { PROJECTS, ROUTING_CONTEXT } = require('./utils/miraWorld');

const anthropic = new Anthropic();

const AGENT_NAME = 'mira_classifier';
const MODEL = 'claude-haiku-4-5-20251001';
const MAX_TOKENS = 500;
const DEFAULT_LIMIT = 10;
const CONFIDENCE_THRESHOLD = 0.7;
const WORKER_INTERVAL_MS = 15 * 60 * 1000; // every 15 minutes
const ADVISORY_LOCK_KEY = 91720260603;

const VALID_CLASSIFICATIONS = [
  'task',
  'client_note',
  'blocker',
  'idea',
  'content_seed',
  'decision_needed',
  'reference',
  'reminder',
];

let intervalHandle = null;
let intervalRunning = false;

function truncate(value, max = 500) {
  const text = value === undefined || value === null ? '' : String(value);
  return text.length > max ? text.slice(0, max) : text;
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
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Classifier response was not valid JSON');
    return JSON.parse(match[0]);
  }
}

// Pull live context from Postgres. Clients, prospects, and people come from the
// DB; projects come from the static PROJECTS constant above.
async function buildContext() {
  const [clients, prospects, people, corrections] = await Promise.all([
    pool.query(`
      SELECT id, name, business_name, city, state, email, sender_name, vertical, verticals
      FROM clients
      WHERE active = true
      ORDER BY id
    `),
    pool.query(`
      SELECT p.id, p.first_name, p.last_name, p.email, p.vertical, p.status, p.client_id,
             co.name AS company_name
      FROM prospects p
      JOIN clients cl ON cl.id = p.client_id AND cl.active = true
      LEFT JOIN companies co ON co.id = p.company_id AND co.client_id = p.client_id
      WHERE COALESCE(p.do_not_contact, false) = false
        AND COALESCE(p.mira_archived, false) = false
        AND COALESCE(p.status, 'cold') IN ('cold', 'warm', 'hot')
        AND (
          p.client_id <> 1
          OR CONCAT_WS(' ', p.service_area_match, p.linkedin_location, co.location)
             ~* '(Providence|Rhode Island|Pawtucket|Cranston|Warwick|[ ,]RI[ ,0-9])'
        )
      ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
      LIMIT 60
    `),
    pool.query(`
      SELECT u.name, u.role, u.client_id
      FROM users u
      LEFT JOIN clients cl ON cl.id = u.client_id
      WHERE u.active = true
        AND (u.client_id IS NULL OR cl.active = true)
      ORDER BY u.name
    `),
    pool.query(`
      SELECT mc.capture_id, mc.original_class, mc.corrected_class, mc.note, mc.created_at
      FROM mira_corrections mc
      JOIN capture_inbox ci ON ci.id = mc.capture_id
      WHERE COALESCE(mc.archived, false) = false
        AND COALESCE(ci.archived, false) = false
        AND mc.original_class <> mc.corrected_class
      ORDER BY mc.created_at DESC
      LIMIT 20
    `),
  ]);

  return {
    clients: clients.rows,
    prospects: prospects.rows,
    people: people.rows,
    corrections: corrections.rows,
  };
}

function formatClients(clients) {
  if (!clients.length) return '- (none on record)';
  return clients
    .map(c => {
      const label = c.name || c.business_name || `Client ${c.id}`;
      const loc = [c.city, c.state].filter(Boolean).join(', ');
      const vertical = c.vertical || (Array.isArray(c.verticals) ? c.verticals.join('/') : '');
      const parts = [
        `${label} (client_id=${c.id})`,
        loc,
        vertical,
        c.sender_name ? `contact: ${c.sender_name}` : '',
      ].filter(Boolean);
      return `- ${parts.join(' — ')}`;
    })
    .join('\n');
}

function formatProspects(prospects) {
  if (!prospects.length) return '- (none on record)';
  return prospects
    .map(p => {
      const name = [p.first_name, p.last_name].filter(Boolean).join(' ') || p.email || `prospect ${p.id}`;
      const company = p.company_name ? ` @ ${p.company_name}` : '';
      const scope = p.client_id ? ` (client_id=${p.client_id})` : '';
      return `- ${name}${company} [${p.status || 'cold'}]${scope}`;
    })
    .join('\n');
}

function formatPeople(people) {
  if (!people.length) return '- (none on record)';
  return people
    .map(u => {
      const scope = u.client_id ? ` (client_id=${u.client_id})` : '';
      return `- ${u.name} — ${u.role}${scope}`;
    })
    .join('\n');
}

function formatProjects(projects) {
  return projects.map(p => `- ${p}`).join('\n');
}

function formatCorrections(corrections) {
  if (!corrections.length) {
    return '(No corrections yet. Use your best judgment.)';
  }
  return corrections
    .map(c => {
      const note = c.note ? ` — Jacob's note: ${c.note}` : '';
      return `- capture #${c.capture_id}: Mira said "${c.original_class}", Jacob corrected to "${c.corrected_class}"${note}`;
    })
    .join('\n');
}

function buildSystemPrompt(context) {
  return `You are Mira, a classification agent for Jacob Maynard. Your job is to take raw captured thoughts (text or voice transcripts) and classify them into one of 8 categories, plus extract any client reference if relevant.

JACOB'S WORLD (use this to resolve references):

Active clients:
${formatClients(context.clients)}

Active prospects / people in outreach campaigns:
${formatProspects(context.prospects)}

Active people (team / setters / closers):
${formatPeople(context.people)}

Active long-term projects:
${formatProjects(PROJECTS)}

CLIENT ROUTING DISTINCTION:
${ROUTING_CONTEXT}

CLASSIFICATION CATEGORIES:

1. task - Something Jacob needs to do. Has an action verb. Has him as the owner. Examples: "Review the Providence service-business send queue," "Call the next Anchor law firm prospect."

2. client_note - Information about a specific client or prospect. Not an action, just intel. Examples: "The Providence salon owner prefers a Thursday callback," "The Manchester CPA already has a cleaner but wants a backup quote."

3. blocker - Something preventing progress on something else. Examples: "Can't restart Vera until GBP API reapplication window opens June 25," "Waiting on signed LOA from new prospect."

4. idea - Future possibility, not actionable now. Often speculative or strategic. Examples: "What if we offered setter-as-a-service as a separate product?" "School could use the methodology document as marketing."

5. content_seed - Something Jacob could write or post about. Often a thought, observation, or framing. Examples: "Insight about why ADHD founders abandon their own systems," "Story about the bartending shift that became a sales lesson."

6. decision_needed - Requires Jacob's judgment. Cannot be auto-routed. Examples: "Should Anchor prioritize CPA firms or law firms this week?" "Do I take this Upwork job at $30/hr?"

7. reference - Info Jacob needs to find later. Contact info, rates, snippets, URLs, addresses. Examples: "The Anchor prospect's main office number is XXX-XXX-XXXX," "Providence campaign opens were 41% last week."

8. reminder - Time-bound surface. Has a specific date or time when it needs to come back. Examples: "Remind me June 25 to reapply for GBP API," "Ping me Friday at 4pm to call the Anchor CPA lead."

OUTPUT FORMAT (JSON, no other text):

{
  "classification": "task" | "client_note" | "blocker" | "idea" | "content_seed" | "decision_needed" | "reference" | "reminder",
  "confidence": 0.0-1.0,
  "client_id": <int or null>,
  "suggested_routing": {
    "todoist_project": <string or null, only if classification = task>,
    "remind_at": <ISO timestamp or null, only if classification = reminder>,
    "brand": <"jacob_unbound" | "remnant_builder" | null, only if classification = content_seed>,
    "blocking": <string or null, what this blocks, only if classification = blocker>
  },
  "classifier_notes": <brief reasoning, used if Jacob reviews>
}

CORRECTION HISTORY (recent mistakes Mira has made and how Jacob corrected them):

${formatCorrections(context.corrections)}

NOW CLASSIFY THE CAPTURE PROVIDED IN THE USER MESSAGE. Respond with the JSON object only, no other text.`;
}

function getContentToClassify(row) {
  if (row.content_type === 'voice') {
    return (row.transcript || '').trim();
  }
  // text (and link, which Telegram captures as text) use raw_text
  return (row.raw_text || '').trim();
}

function normalizeClientId(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isInteger(n) ? n : null;
}

function normalizeConfidence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

async function getPendingCaptures(limit = DEFAULT_LIMIT) {
  const { rows } = await pool.query(`
    SELECT id, content_type, raw_text, transcript, voice_url, photo_url, link_url
    FROM capture_inbox
    WHERE status IN ('new', 'transcribed')
      AND classification IS NULL
      AND COALESCE(archived, false) = false
    ORDER BY received_at ASC
    LIMIT $1
  `, [limit]);

  return rows;
}

async function markManualReview(row, reason) {
  await pool.query(`
    UPDATE capture_inbox
    SET status = 'review_needed',
        routed_to_table = 'manual_review',
        routed_to_id = $1,
        classifier_notes = COALESCE(classifier_notes, $2),
        processed_at = NOW()
    WHERE id = $3
  `, [reason, reason.replaceAll('_', ' '), row.id]);
  return { id: row.id, status: 'review_needed', reason };
}

async function classifyCapture(row, systemPrompt) {
  if (row.content_type === 'photo' || row.content_type === 'document') {
    return markManualReview(row, `unsupported_${row.content_type}_capture`);
  }

  if (row.content_type === 'voice' && !String(row.transcript || '').trim()) {
    if (row.voice_url) return { id: row.id, status: 'pending_transcription' };
    return markManualReview(row, 'voice_capture_missing_audio_or_transcript');
  }

  const content = getContentToClassify(row);

  if (!content) {
    return markManualReview(row, 'empty_capture');
  }

  const message = await anthropic.messages.create({
    model: MODEL,
    max_tokens: MAX_TOKENS,
    system: systemPrompt,
    messages: [
      { role: 'user', content: `CAPTURE TO CLASSIFY:\n\n${content}` },
    ],
  });

  const parsed = parseJson(extractClaudeText(message));

  const classification = VALID_CLASSIFICATIONS.includes(parsed.classification)
    ? parsed.classification
    : null;
  if (!classification) {
    throw new Error(`Classifier returned invalid classification: ${parsed.classification}`);
  }

  const confidence = normalizeConfidence(parsed.confidence);
  const clientId = normalizeClientId(parsed.client_id);
  const classifierNotes = parsed.classifier_notes ? String(parsed.classifier_notes) : null;
  const nextStatus = confidence < CONFIDENCE_THRESHOLD ? 'review_needed' : 'classified';

  await pool.query(`
    UPDATE capture_inbox
    SET classification = $1,
        confidence = $2,
        client_id = $3,
        classifier_notes = $4,
        status = $5
    WHERE id = $6
  `, [classification, confidence, clientId, classifierNotes, nextStatus, row.id]);

  console.log(`[${AGENT_NAME}] capture_id=${row.id} -> ${classification} (${confidence}) status=${nextStatus}`);
  return { id: row.id, status: nextStatus, classification, confidence, client_id: clientId };
}

async function logRun(action, payload, status, errorMsg = null) {
  try {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
      VALUES ($1, $2, $3, $4, $5, NOW(), $6)
    `, [AGENT_NAME, action, payload, status, errorMsg, null]);
  } catch (err) {
    console.error(`[${AGENT_NAME}] agent_log write failed:`, err.message);
  }
}

async function withWorkerLock(fn) {
  const lock = await pool.query('SELECT pg_try_advisory_lock($1) AS locked', [ADVISORY_LOCK_KEY]);
  if (!lock.rows[0]?.locked) {
    return { skipped: true, reason: 'worker_already_running' };
  }

  try {
    return await fn();
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [ADVISORY_LOCK_KEY]).catch(err => {
      console.error(`[${AGENT_NAME}] advisory unlock failed:`, err.message);
    });
  }
}

async function run(params = {}) {
  const limit = Math.max(1, Number(params.limit || DEFAULT_LIMIT));

  return withWorkerLock(async () => {
    const rows = await getPendingCaptures(limit);
    if (!rows.length) {
      return { scanned: 0, classified: 0, review_needed: 0, skipped: 0, failed: 0 };
    }

    const systemPrompt = buildSystemPrompt(await buildContext());

    let classified = 0;
    let reviewNeeded = 0;
    let skipped = 0;
    let failed = 0;
    const results = [];

    for (const row of rows) {
      try {
        const result = await classifyCapture(row, systemPrompt);
        results.push(result);
        if (result.status === 'classified') classified++;
        else if (result.status === 'review_needed') reviewNeeded++;
        else if (result.status === 'pending_transcription') skipped++;
        else if (result.status === 'skipped') skipped++;
      } catch (err) {
        failed++;
        console.error(`[${AGENT_NAME}] capture_id=${row.id} failed:`, err.message);
        results.push({ id: row.id, status: 'error', error: truncate(err.message) });
      }
    }

    const summary = { scanned: rows.length, classified, review_needed: reviewNeeded, skipped, failed };
    await logRun('classify_batch', summary, failed && !classified && !reviewNeeded ? 'failed' : 'success');
    return { ...summary, results };
  });
}

function startMiraClassifierWorker(options = {}) {
  if (intervalHandle) return intervalHandle;
  const intervalMs = Math.max(
    60_000,
    Number(options.intervalMs || process.env.MIRA_CLASSIFIER_INTERVAL_MS || WORKER_INTERVAL_MS)
  );

  intervalHandle = setInterval(() => {
    if (intervalRunning) return;
    intervalRunning = true;
    run()
      .catch(err => console.error(`[${AGENT_NAME}] worker error:`, err.message))
      .finally(() => {
        intervalRunning = false;
      });
  }, intervalMs);

  intervalHandle.unref?.();
  console.log(`[${AGENT_NAME}] worker started interval=${intervalMs}ms`);
  return intervalHandle;
}

module.exports = {
  run,
  startMiraClassifierWorker,
  buildContext,
  buildSystemPrompt,
};

if (require.main === module) {
  run()
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
      return pool.end();
    })
    .catch(async err => {
      console.error(`[${AGENT_NAME}] fatal:`, err.message);
      await pool.end().catch(() => {});
      process.exit(1);
    });
}
