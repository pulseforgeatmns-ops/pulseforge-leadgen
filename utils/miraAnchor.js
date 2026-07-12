require('dotenv').config();

const pool = require('../db');
const { JACOB_CONTEXT, LIVE_WORKSTREAMS, isActiveTodoistContextItem } = require('./miraWorld');

const AGENT_NAME = 'mira_anchor';
const MODEL = 'claude-haiku-4-5-20251001';
const ANCHOR_TZ = process.env.MIRA_TIMEZONE || 'America/New_York';
const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';
const DEFAULT_ANCHOR_CLIENT_ID = 1;

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

function clientNameForAnchor(clientId) {
  const match = LIVE_WORKSTREAMS.find(workstream => Number(workstream.client_id) === Number(clientId));
  return match?.name || `client ${clientId}`;
}

function formatAnchorClientConfirmation(clientId, assumed = false) {
  const assumption = assumed ? ', defaulted because no active Telegram client context was found' : '';
  return `${clientNameForAnchor(clientId)} (client ${clientId}${assumption})`;
}

function buildAnchorSetConfirmation({ clientId, assumed = false, anchorText }) {
  return `Anchor set for ${formatAnchorClientConfirmation(clientId, assumed)}: "${anchorText}"`;
}

function buildAnchorClearConfirmation({ clientId, assumed = false }) {
  return `Anchor cleared for ${formatAnchorClientConfirmation(clientId, assumed)}.`;
}

function parseAnchorSetIntent(text) {
  const trimmed = String(text || '').trim();
  if (!trimmed) return null;

  const slash = trimmed.match(/^\/anchor(?:@[a-z0-9_]+)?(?:\s+([\s\S]*))?$/i);
  if (slash) {
    const anchorText = String(slash[1] || '').trim();
    return {
      matched: true,
      action: /^clear$/i.test(anchorText) ? 'clear' : 'set',
      anchorText,
    };
  }

  if (/^clear\s+anchor\s*$/i.test(trimmed)) {
    return { matched: true, action: 'clear', anchorText: '' };
  }

  const phrasePatterns = [
    /^set\s+today['’]?s\s+anchor(?:\s+(?:to|as|is))?\s*[:\-]?\s*([\s\S]+)$/i,
    /^set\s+anchor(?:\s+(?:to|as|is))?\s*[:\-]?\s*([\s\S]+)$/i,
    /^today['’]?s\s+anchor\s+is\s*[:\-]?\s*([\s\S]+)$/i,
    /^anchor\s*:\s*([\s\S]+)$/i,
  ];

  for (const pattern of phrasePatterns) {
    const match = trimmed.match(pattern);
    if (match) {
      return { matched: true, action: 'set', anchorText: String(match[1] || '').trim() };
    }
  }

  return null;
}

async function getAnchorForToday(clientId = DEFAULT_ANCHOR_CLIENT_ID) {
  const { rows } = await pool.query(
    `SELECT id, client_id, anchor_date, primary_anchor, secondary_anchors, completion_notes
     FROM daily_anchors
     WHERE client_id = $1
       AND anchor_date = $2`,
    [clientId, todayEtDate()]
  );
  return rows[0] || null;
}

// ---------------------------------------------------------------------------
// Candidate gathering (used by the morning digest)
// ---------------------------------------------------------------------------

async function fetchTodoistCollection(path) {
  const token = process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN;
  if (!token) return [];

  const rows = [];
  let cursor = null;

  do {
    const url = new URL(`${TODOIST_API_BASE}${path}`);
    url.searchParams.set('limit', '200');
    if (cursor) url.searchParams.set('cursor', cursor);

    const response = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!response.ok) break;

    const body = await response.json();
    rows.push(...(Array.isArray(body) ? body : (body.results || [])));
    cursor = (Array.isArray(body) ? null : body.next_cursor) || null;
  } while (cursor);

  return rows;
}

function taskDateValue(task) {
  const raw = task?.due?.datetime || task?.due?.date || null;
  const ts = raw ? new Date(raw).getTime() : NaN;
  return Number.isFinite(ts) ? ts : null;
}

function rankTodoistTasks(tasks, projectMap, now = new Date()) {
  const nowMs = now.getTime();
  const today = new Intl.DateTimeFormat('en-CA', {
    timeZone: ANCHOR_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(now);

  return tasks
    .map(task => {
      const project = projectMap.get(String(task.project_id)) || null;
      const createdAt = task.added_at || task.created_at || null;
      const createdMs = createdAt ? new Date(createdAt).getTime() : NaN;
      const ageDays = Number.isFinite(createdMs) ? Math.max(0, Math.floor((nowMs - createdMs) / 86400000)) : 999;
      const dueValue = taskDateValue(task);
      const dueText = task?.due?.datetime || task?.due?.date || null;
      const dueTodayOrEarlier = dueText ? String(dueText).slice(0, 10) <= today : false;
      let relevance = project === 'Anchor Outreach' ? 500 : project === 'Inbox' ? 180 : 140;
      if (dueTodayOrEarlier) relevance += 350;
      if (ageDays <= 3) relevance += 250;
      relevance -= Math.min(ageDays, 90);
      return {
        id: String(task.id),
        content: task.content || '',
        project,
        created_at: createdAt,
        age_days: ageDays,
        due: dueText,
        due_value: dueValue,
        relevance,
      };
    })
    .filter(task => isActiveTodoistContextItem(task.project, task.content))
    .sort((a, b) =>
      b.relevance - a.relevance
      || (a.due_value ?? Number.MAX_SAFE_INTEGER) - (b.due_value ?? Number.MAX_SAFE_INTEGER)
      || String(b.created_at || '').localeCompare(String(a.created_at || ''))
      || a.id.localeCompare(b.id)
    );
}

async function fetchRelevantTodoistTasks() {
  const [projects, tasks] = await Promise.all([
    fetchTodoistCollection('/projects'),
    fetchTodoistCollection('/tasks'),
  ]);
  const projectMap = new Map(projects.map(project => [String(project.id), project.name || null]));

  return rankTodoistTasks(tasks, projectMap);
}

async function fetchUnresolvedBlockers() {
  const { rows } = await pool.query(`
    SELECT b.id, b.content, b.blocking, b.created_at
    FROM blockers b
    LEFT JOIN clients cl ON cl.id = b.client_id
    LEFT JOIN capture_inbox ci ON ci.id = b.capture_id
    WHERE b.resolved = false
      AND (b.client_id IS NULL OR cl.active = true)
      AND COALESCE(ci.archived, false) = false
    ORDER BY b.created_at ASC
  `);
  return rows;
}

// Recent client notes with no follow-up task captured within 48h of the note.
async function fetchUnactionedClientNotes() {
  const { rows } = await pool.query(`
    SELECT cn.id, cn.content, cn.client_id, cn.created_at,
           cl.name AS client_name, cl.business_name
    FROM client_notes cn
    JOIN clients cl ON cl.id = cn.client_id AND cl.active = true
    WHERE cn.created_at >= NOW() - INTERVAL '7 days'
      AND COALESCE(cn.archived, false) = false
      AND NOT EXISTS (
        SELECT 1
        FROM capture_inbox ci
        WHERE ci.classification = 'task'
          AND COALESCE(ci.archived, false) = false
          AND ci.client_id IS NOT DISTINCT FROM cn.client_id
          AND ci.received_at BETWEEN cn.created_at AND cn.created_at + INTERVAL '48 hours'
      )
    ORDER BY cn.created_at DESC
  `);
  return rows;
}

function buildCandidateBlock(tasks, blockers, notes) {
  const sections = [];

  if (tasks.length) {
    const lines = tasks.map(t => {
      const age = Number.isFinite(t.age_days) ? `${t.age_days}d old` : 'age unknown';
      const due = t.due ? `, due ${String(t.due).slice(0, 10)}` : '';
      return `- [active task, ${t.project}, ${age}${due}] ${t.content}`;
    });
    sections.push(`RELEVANT ACTIVE TODOIST TASKS (ranked deterministically):\n${lines.join('\n')}`);
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

You will be given relevant active Todoist tasks, unresolved blockers, and client notes that never got a follow-up. Select the THREE candidates most worth anchoring his day around. Prefer fresh or due Anchor Outreach work when it is actionable, while balancing Pulseforge Providence and Upwork. Never revive a parked project, setup-only alert, or retired client. If fewer than three candidates exist, return only what is available.

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
    const [tasks, blockers, notes] = await Promise.all([
      fetchRelevantTodoistTasks().catch(err => {
        console.error('[mira_anchor] Todoist fetch failed:', err.message);
        return [];
      }),
      fetchUnresolvedBlockers(),
      fetchUnactionedClientNotes(),
    ]);

    if (!tasks.length && !blockers.length && !notes.length) {
      return '';
    }

    const candidateBlock = buildCandidateBlock(tasks, blockers, notes);
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
async function setCurrentAnchor({
  client_id = DEFAULT_ANCHOR_CLIENT_ID,
  primary_anchor,
  secondary_anchors = [],
  completion_notes = null,
  source_capture_id = null,
}) {
  const { rows } = await pool.query(`
    INSERT INTO daily_anchors
      (client_id, anchor_date, primary_anchor, secondary_anchors, set_at, completion_notes, source_capture_id)
    VALUES ($1, $2, $3, $4, NOW(), $5, $6)
    ON CONFLICT (client_id, anchor_date) DO UPDATE
      SET primary_anchor = EXCLUDED.primary_anchor,
          secondary_anchors = EXCLUDED.secondary_anchors,
          set_at = NOW(),
          completion_notes = EXCLUDED.completion_notes,
          source_capture_id = EXCLUDED.source_capture_id
    RETURNING id, client_id, anchor_date, primary_anchor, secondary_anchors, completion_notes
  `, [
    client_id,
    todayEtDate(),
    primary_anchor || null,
    secondary_anchors && secondary_anchors.length ? secondary_anchors : null,
    completion_notes || null,
    source_capture_id,
  ]);
  return rows[0] || null;
}

async function clearCurrentAnchor(clientId = DEFAULT_ANCHOR_CLIENT_ID) {
  return setCurrentAnchor({
    client_id: clientId,
    primary_anchor: null,
    secondary_anchors: [],
    completion_notes: null,
  });
}

async function insertAnchor({
  client_id = DEFAULT_ANCHOR_CLIENT_ID,
  primary_anchor,
  secondary_anchors,
  completion_notes,
  source_capture_id = null,
}) {
  const { rows } = await pool.query(`
    INSERT INTO daily_anchors
      (client_id, anchor_date, primary_anchor, secondary_anchors, set_at, completion_notes, source_capture_id)
    VALUES ($1, $2, $3, $4, NOW(), $5, $6)
    ON CONFLICT (client_id, anchor_date) DO NOTHING
    RETURNING id, client_id, anchor_date, primary_anchor, secondary_anchors, completion_notes
  `, [
    client_id,
    todayEtDate(),
    primary_anchor || null,
    secondary_anchors && secondary_anchors.length ? secondary_anchors : null,
    completion_notes || null,
    source_capture_id,
  ]);
  return rows[0] || null;
}

module.exports = {
  DEFAULT_ANCHOR_CLIENT_ID,
  todayEtDate,
  isWithinAnchorWindow,
  getAnchorForToday,
  buildAnchorAppendix,
  parseAnchorSetIntent,
  setCurrentAnchor,
  clearCurrentAnchor,
  clientNameForAnchor,
  formatAnchorClientConfirmation,
  buildAnchorSetConfirmation,
  buildAnchorClearConfirmation,
  parseAnchorReply,
  insertAnchor,
  rankTodoistTasks,
  fetchRelevantTodoistTasks,
  buildCandidateBlock,
  JACOB_CONTEXT,
};
