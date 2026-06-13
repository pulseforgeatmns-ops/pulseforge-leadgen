require('dotenv').config();

const pool = require('../db');

const AGENT_NAME = 'mira_anchor';
const MODEL = 'claude-haiku-4-5-20251001';
const ANCHOR_TZ = process.env.MIRA_TIMEZONE || 'America/New_York';
const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';

// Jacob's standing context, passed to Haiku so it can weigh candidates the way
// he would. Solo founder, fragmented attention, three live workstreams.
const JACOB_CONTEXT =
  'Jacob is a solo founder with fragmented attention. His active workstreams are ' +
  'Pulseforge (his lead-gen agency), MSHI (a client he runs outreach for), and ' +
  'Upwork (freelance income). He has limited focused hours per day and needs the ' +
  'one or two highest-leverage moves surfaced, not a to-do list.';

let _anthropic = null;
function getAnthropic() {
  if (!_anthropic) {
    const Anthropic = require('@anthropic-ai/sdk');
    _anthropic = new Anthropic();
  }
  return _anthropic;
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
    const arrayMatch = text.match(/\[[\s\S]*\]/);
    if (arrayMatch) return JSON.parse(arrayMatch[0]);
    const objMatch = text.match(/\{[\s\S]*\}/);
    if (objMatch) return JSON.parse(objMatch[0]);
    throw new Error('Anchor response was not valid JSON');
  }
}

// Today's date (YYYY-MM-DD) in Jacob's timezone — drives the anchor_date key.
function todayEtDate() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: ANCHOR_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date());
}

// Current hour (0-23) in Jacob's timezone.
function currentEtHour() {
  const hour = new Intl.DateTimeFormat('en-US', {
    timeZone: ANCHOR_TZ,
    hour: '2-digit',
    hour12: false,
  }).format(new Date());
  return Number(hour) % 24;
}

// The morning window in which a plain reply is read as the day's anchor.
function isWithinAnchorWindow() {
  const hour = currentEtHour();
  return hour >= 6 && hour < 12;
}

async function getAnchorForToday() {
  const { rows } = await pool.query(
    `SELECT id, anchor_date, primary_anchor, secondary_anchors, completion_notes
     FROM daily_anchors
     WHERE anchor_date = $1`,
    [todayEtDate()]
  );
  return rows[0] || null;
}

// ---------------------------------------------------------------------------
// Candidate gathering (used by the morning digest)
// ---------------------------------------------------------------------------

async function fetchStaleTodoistTasks(maxAgeDays = 3) {
  const token = process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN;
  if (!token) return [];

  const cutoff = Date.now() - maxAgeDays * 24 * 60 * 60 * 1000;
  const stale = [];
  let cursor = null;
  let pages = 0;

  do {
    const url = new URL(`${TODOIST_API_BASE}/tasks`);
    url.searchParams.set('limit', '200');
    if (cursor) url.searchParams.set('cursor', cursor);

    const response = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!response.ok) break;

    const body = await response.json();
    const results = Array.isArray(body) ? body : (body.results || []);
    for (const task of results) {
      const created = task.added_at || task.created_at;
      if (!created) continue;
      const ts = new Date(created).getTime();
      if (Number.isFinite(ts) && ts < cutoff) {
        stale.push({ id: String(task.id), content: task.content || '', created_at: created });
      }
    }

    cursor = (Array.isArray(body) ? null : body.next_cursor) || null;
    pages += 1;
  } while (cursor && pages < 10);

  return stale;
}

async function fetchUnresolvedBlockers() {
  const { rows } = await pool.query(`
    SELECT id, content, blocking, created_at
    FROM blockers
    WHERE resolved = false
    ORDER BY created_at ASC
    LIMIT 20
  `);
  return rows;
}

// Recent client notes with no follow-up task captured within 48h of the note.
async function fetchUnactionedClientNotes() {
  const { rows } = await pool.query(`
    SELECT cn.id, cn.content, cn.client_id, cn.created_at,
           cl.name AS client_name, cl.business_name
    FROM client_notes cn
    LEFT JOIN clients cl ON cl.id = cn.client_id
    WHERE cn.created_at >= NOW() - INTERVAL '7 days'
      AND NOT EXISTS (
        SELECT 1
        FROM capture_inbox ci
        WHERE ci.classification = 'task'
          AND ci.client_id IS NOT DISTINCT FROM cn.client_id
          AND ci.received_at BETWEEN cn.created_at AND cn.created_at + INTERVAL '48 hours'
      )
    ORDER BY cn.created_at DESC
    LIMIT 10
  `);
  return rows;
}

function buildCandidateBlock(staleTasks, blockers, notes) {
  const sections = [];

  if (staleTasks.length) {
    const lines = staleTasks
      .slice(0, 15)
      .map(t => `- [stale task] ${t.content} (open since ${String(t.created_at).slice(0, 10)})`);
    sections.push(`STALE TODOIST TASKS (open > 3 days):\n${lines.join('\n')}`);
  }

  if (blockers.length) {
    const lines = blockers.map(b => {
      const blocking = b.blocking ? ` (blocking: ${b.blocking})` : '';
      return `- [blocker] ${b.content}${blocking}`;
    });
    sections.push(`UNRESOLVED BLOCKERS:\n${lines.join('\n')}`);
  }

  if (notes.length) {
    const lines = notes.map(n => {
      const client = n.client_name || n.business_name || (n.client_id ? `client ${n.client_id}` : 'unknown client');
      return `- [client note, not yet actioned] ${client}: ${n.content}`;
    });
    sections.push(`CLIENT NOTES WITH NO FOLLOW-UP TASK:\n${lines.join('\n')}`);
  }

  return sections.join('\n\n');
}

async function selectTopAnchors(candidateBlock) {
  const systemPrompt = `You are Mira, an accountability layer for Jacob. ${JACOB_CONTEXT}

You will be given a list of candidate items pulled from Jacob's stale Todoist tasks, unresolved blockers, and client notes that never got a follow-up. Select the THREE candidates most worth anchoring his day around — the moves with the highest leverage given his fragmented attention and three workstreams. If fewer than three candidates exist, return only what is available.

For each pick, write a short, concrete anchor phrased as something he could commit to doing today (an action, not a restatement of the raw item), plus one brief clause of reasoning.

Respond with JSON only, no other text:
[
  { "option": "<short actionable anchor>", "reason": "<brief why-this-matters>" }
]`;

  const message = await getAnthropic().messages.create({
    model: MODEL,
    max_tokens: 500,
    system: systemPrompt,
    messages: [
      { role: 'user', content: `CANDIDATE ITEMS:\n\n${candidateBlock}` },
    ],
  });

  const parsed = parseJson(extractClaudeText(message));
  if (!Array.isArray(parsed)) return [];
  return parsed
    .filter(p => p && typeof p.option === 'string' && p.option.trim())
    .slice(0, 3)
    .map(p => ({ option: p.option.trim(), reason: (p.reason || '').toString().trim() }));
}

async function logAnchorOptions(picks) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [
    AGENT_NAME,
    'anchor_options',
    JSON.stringify({ anchor_date: todayEtDate(), options: picks.map(p => p.option), picks }),
    'success',
    null,
  ]);
}

function formatAnchorSection(picks) {
  const numbered = picks
    .map((p, i) => `${i + 1}. ${p.option}${p.reason ? ` (${p.reason})` : ''}`)
    .join('\n');

  return [
    '',
    '🎯 Today\'s anchor',
    '',
    'Three options for today\'s anchor:',
    numbered,
    '',
    'What\'s the one thing if nothing else?',
  ].join('\n');
}

// Build the accountability appendix for the morning digest. Best-effort: any
// failure returns '' so the core digest still sends.
async function buildAnchorAppendix() {
  try {
    const [staleTasks, blockers, notes] = await Promise.all([
      fetchStaleTodoistTasks().catch(err => {
        console.error('[mira_anchor] Todoist fetch failed:', err.message);
        return [];
      }),
      fetchUnresolvedBlockers(),
      fetchUnactionedClientNotes(),
    ]);

    if (!staleTasks.length && !blockers.length && !notes.length) {
      return '';
    }

    const candidateBlock = buildCandidateBlock(staleTasks, blockers, notes);
    const picks = await selectTopAnchors(candidateBlock);
    if (!picks.length) return '';

    await logAnchorOptions(picks).catch(err => {
      console.error('[mira_anchor] logging options failed:', err.message);
    });

    return formatAnchorSection(picks);
  } catch (err) {
    console.error('[mira_anchor] buildAnchorAppendix failed:', err.message);
    return '';
  }
}

// ---------------------------------------------------------------------------
// Reply parsing (used by the Telegram webhook)
// ---------------------------------------------------------------------------

async function getTodayAnchorOptions() {
  const { rows } = await pool.query(`
    SELECT payload
    FROM agent_log
    WHERE agent_name = $1 AND action = 'anchor_options'
      AND payload->>'anchor_date' = $2
    ORDER BY ran_at DESC
    LIMIT 1
  `, [AGENT_NAME, todayEtDate()]);

  const payload = rows[0]?.payload;
  if (!payload) return [];
  const parsed = typeof payload === 'string' ? JSON.parse(payload) : payload;
  return Array.isArray(parsed.options) ? parsed.options : [];
}

async function parseAnchorReply(text) {
  const options = await getTodayAnchorOptions();
  const optionsBlock = options.length
    ? options.map((opt, i) => `${i + 1}. ${opt}`).join('\n')
    : '(No options were offered this morning.)';

  const systemPrompt = `You are Mira, parsing Jacob's reply to this morning's anchor prompt. ${JACOB_CONTEXT}

This morning Jacob was offered these anchor options:
${optionsBlock}

His reply may be:
- a number ("1", "2", "3") referencing one of the options above — resolve it to that option's text
- a quoted or typed option ("Email Brad")
- free-form prose describing what he wants to anchor on
- a no-anchor message ("skip today", "out today", "sick", "family emergency", "taking the day") — these mean no anchor should be set

Extract a single primary anchor and any secondary anchors. If the reply is a no-anchor message, set "no_anchor" to true, leave "primary_anchor" null, and put a brief reason in "completion_notes".

Respond with JSON only, no other text:
{
  "no_anchor": true | false,
  "primary_anchor": "<string or null>",
  "secondary_anchors": ["<string>", ...],
  "completion_notes": "<brief reason if no_anchor, else null>"
}`;

  const message = await getAnthropic().messages.create({
    model: MODEL,
    max_tokens: 300,
    system: systemPrompt,
    messages: [
      { role: 'user', content: `JACOB'S REPLY:\n\n${text}` },
    ],
  });

  const parsed = parseJson(extractClaudeText(message));
  const noAnchor = parsed.no_anchor === true;
  const secondary = Array.isArray(parsed.secondary_anchors)
    ? parsed.secondary_anchors.map(s => String(s).trim()).filter(Boolean)
    : [];

  return {
    no_anchor: noAnchor,
    primary_anchor: noAnchor ? null : (parsed.primary_anchor ? String(parsed.primary_anchor).trim() : null),
    secondary_anchors: noAnchor ? [] : secondary,
    completion_notes: parsed.completion_notes ? String(parsed.completion_notes).trim() : null,
  };
}

// Insert today's anchor. ON CONFLICT guards against a double submit racing the
// pre-check. Returns the inserted row, or null if one already existed.
async function insertAnchor({ primary_anchor, secondary_anchors, completion_notes, source_capture_id = null }) {
  const { rows } = await pool.query(`
    INSERT INTO daily_anchors
      (anchor_date, primary_anchor, secondary_anchors, set_at, completion_notes, source_capture_id)
    VALUES ($1, $2, $3, NOW(), $4, $5)
    ON CONFLICT (anchor_date) DO NOTHING
    RETURNING id, anchor_date, primary_anchor, secondary_anchors, completion_notes
  `, [
    todayEtDate(),
    primary_anchor || null,
    secondary_anchors && secondary_anchors.length ? secondary_anchors : null,
    completion_notes || null,
    source_capture_id,
  ]);
  return rows[0] || null;
}

module.exports = {
  todayEtDate,
  isWithinAnchorWindow,
  getAnchorForToday,
  buildAnchorAppendix,
  parseAnchorReply,
  insertAnchor,
};
