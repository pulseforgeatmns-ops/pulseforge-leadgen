require('dotenv').config();

const Anthropic = require('@anthropic-ai/sdk');
const pool = require('../../db');
const { getRuntimeClientId } = require('../../utils/clientContext');
const {
  ensureAgentLessonsSchema,
  insertProposedLesson,
  parseJsonObject,
} = require('../../utils/agentLessons');

const anthropic = new Anthropic();
const AGENT = 'paige';
const MODEL = process.env.REFLECTION_MODEL || 'claude-opus-4-8';

function parseCliOptions(argv = process.argv.slice(2)) {
  const options = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--client-id' || arg === '--client_id') options.client_id = argv[++i];
    else if (arg === '--window-start') options.windowStart = argv[++i];
    else if (arg === '--window-end') options.windowEnd = argv[++i];
    else if (arg === '--dry-run') options.dryRun = true;
  }
  return options;
}

function startOfUtcDay(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function defaultReflectionWindow(now = new Date()) {
  const today = startOfUtcDay(now);
  const day = today.getUTCDay();
  if (day === 0) {
    const start = new Date(today);
    start.setUTCDate(start.getUTCDate() - 6);
    return { windowStart: start, windowEnd: today };
  }

  const currentMonday = new Date(today);
  currentMonday.setUTCDate(currentMonday.getUTCDate() - ((day + 6) % 7));
  const previousMonday = new Date(currentMonday);
  previousMonday.setUTCDate(previousMonday.getUTCDate() - 7);
  return { windowStart: previousMonday, windowEnd: currentMonday };
}

function coerceWindow(options = {}) {
  const fallback = defaultReflectionWindow();
  const windowStart = options.windowStart ? new Date(options.windowStart) : fallback.windowStart;
  const windowEnd = options.windowEnd ? new Date(options.windowEnd) : fallback.windowEnd;
  if (!Number.isFinite(windowStart.getTime())) throw new Error('Invalid windowStart');
  if (!Number.isFinite(windowEnd.getTime())) throw new Error('Invalid windowEnd');
  if (windowStart >= windowEnd) throw new Error('windowStart must be before windowEnd');
  return { windowStart, windowEnd };
}

function extractText(message) {
  return (message.content || [])
    .filter(block => block && block.type === 'text' && typeof block.text === 'string')
    .map(block => block.text)
    .join('\n')
    .trim();
}

async function gatherPaigeReviewInput(clientId, windowStart, windowEnd) {
  const res = await pool.query(`
    WITH queued_posts AS (
      SELECT to_jsonb(pc) AS row_data
      FROM pending_comments pc
      WHERE pc.client_id = $1
        AND pc.created_at >= $2
        AND pc.created_at < $3
        AND pc.channel IN (
          'facebook_page',
          'google_business',
          'blog',
          'linkedin_page',
          'linkedin_personal'
        )
    ),
    generation_failures AS (
      SELECT to_jsonb(al) AS row_data
      FROM agent_log al
      WHERE al.client_id = $1
        AND al.ran_at >= $2
        AND al.ran_at < $3
        AND LOWER(REPLACE(COALESCE(al.agent_name, ''), '_agent', '')) = 'paige'
        AND al.action IN ('content_failed', 'linkedin_content_skipped')
    )
    SELECT jsonb_build_object(
      'agent', $4::text,
      'client_id', $1::int,
      'window_start', $2::timestamptz,
      'window_end', $3::timestamptz,
      'queued_posts', COALESCE(
        (SELECT jsonb_agg(row_data ORDER BY (row_data->>'created_at')::timestamptz) FROM queued_posts),
        '[]'::jsonb
      ),
      'generation_failures', COALESCE(
        (SELECT jsonb_agg(row_data ORDER BY (row_data->>'ran_at')::timestamptz) FROM generation_failures),
        '[]'::jsonb
      )
    ) AS review_input
  `, [clientId, windowStart, windowEnd, AGENT]);
  return res.rows[0]?.review_input || {};
}

function countReviewRows(input) {
  return Number(input.queued_posts?.length || 0) + Number(input.generation_failures?.length || 0);
}

function collectSourceIds(input) {
  return [
    ...(input.queued_posts || []),
    ...(input.generation_failures || []),
  ]
    .map(row => String(row?.id || '').trim())
    .filter(Boolean);
}

async function hasReviewedWindow(agent, windowStart, windowEnd, sourceIds) {
  if (!sourceIds.length) return false;
  const res = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM agent_lessons
    WHERE agent = $1
      AND window_start = $2
      AND window_end = $3
      AND source_run_ids ?| $4::text[]
  `, [agent, windowStart, windowEnd, sourceIds]);
  return Number(res.rows[0]?.count || 0) > 0;
}

function buildReviewPrompt(input) {
  return `Review this completed Paige window as a fresh-context auditor.

Ground truth:
- queued_posts are Paige drafts that entered the human approval queue. Their status is the queue decision.
- If present on a queue row, use rejection_reason, review_reason, reviewer_notes, fabrication_flag, and fabrication_notes as human-labeled evidence.
- generation_failures are Paige drafts rejected by Paige's hard validators or quality gates before approval queue insertion.

Rules:
- Phase 1 is Paige only. Do not generalize to other agents.
- Extract only reusable lessons supported by the supplied rows.
- Prefer no lesson over a speculative lesson.
- Do not create lessons from unlabeled or ambiguous outcomes.
- Do not recommend replacing the rulebook. Lessons must be additive guardrails.
- Write guardrail_text as one direct line Paige can follow in a future system prompt.
- source_run_ids must contain the pending_comments ids or agent_log ids that support the lesson.
- For every lesson, choose scope explicitly:
  - "global" means a general craft or safety rule that should apply to every Paige client, such as not inventing anecdotes, metrics, quotes, timelines, or unsupported client results.
  - "client" means a lesson specific to this client's audience, brand voice, local market, hook rotation, channel mix, or source material.
  - Do not default. Explain the evidence behind the scope choice inside the evidence field.

Return only strict JSON with this exact shape:
{
  "lessons": [
    {
      "scope": "global|client",
      "category": "fabrication|tone|format|hook|other",
      "lesson": "...",
      "guardrail_text": "...",
      "evidence": "...",
      "source_run_ids": [123],
      "severity": "low|med|high"
    }
  ]
}

Review input:
${JSON.stringify(input, null, 2)}`;
}

async function run(options = {}) {
  const clientId = getRuntimeClientId(options);
  const { windowStart, windowEnd } = coerceWindow(options);
  await ensureAgentLessonsSchema();

  const input = await gatherPaigeReviewInput(clientId, windowStart, windowEnd);
  const reviewedRows = countReviewRows(input);
  const sourceIds = collectSourceIds(input);
  if (options.dryRun) {
    return {
      success: true,
      dry_run: true,
      client_id: clientId,
      window_start: windowStart.toISOString(),
      window_end: windowEnd.toISOString(),
      reviewed_rows: reviewedRows,
      review_schema: {
        lessons: [{
          scope: 'global|client',
          category: 'fabrication|tone|format|hook|other',
          lesson: 'string',
          guardrail_text: 'string',
          evidence: 'string',
          source_run_ids: ['pending_comments.id or agent_log.id'],
          severity: 'low|med|high',
        }],
      },
      review_input: input,
    };
  }

  if (reviewedRows === 0) {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, [
      'reflection',
      'paige_reflection_pass',
      JSON.stringify({
        window_start: windowStart.toISOString(),
        window_end: windowEnd.toISOString(),
        reviewed_rows: 0,
        lessons_proposed: 0,
        skipped: true,
      }),
      'skipped',
      clientId,
    ]);
    return { success: true, skipped: true, reviewed_rows: 0, lessons_proposed: 0, client_id: clientId };
  }

  if (await hasReviewedWindow(AGENT, windowStart, windowEnd, sourceIds)) {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, [
      'reflection',
      'paige_reflection_pass',
      JSON.stringify({
        window_start: windowStart.toISOString(),
        window_end: windowEnd.toISOString(),
        reviewed_rows: reviewedRows,
        lessons_proposed: 0,
        duplicate_window_skipped: true,
      }),
      'skipped',
      clientId,
    ]);
    return {
      success: true,
      agent: AGENT,
      client_id: clientId,
      window_start: windowStart.toISOString(),
      window_end: windowEnd.toISOString(),
      reviewed_rows: reviewedRows,
      lessons_proposed: 0,
      duplicate_window_skipped: true,
    };
  }

  const message = await anthropic.messages.create({
    model: MODEL,
    max_tokens: 1800,
    system: [
      'You are an independent audit reviewer for Pulseforge agent runs.',
      'You are a fresh-context reviewer, not the agent that produced the work.',
      'You only propose lessons for human review. You never activate guardrails.',
      'Return JSON only. No prose. No markdown fences.',
    ].join(' '),
    messages: [{ role: 'user', content: buildReviewPrompt(input) }],
  });

  const parsed = parseJsonObject(extractText(message));
  const lessons = Array.isArray(parsed.lessons) ? parsed.lessons : [];
  const stored = [];
  for (const lesson of lessons) {
    const row = await insertProposedLesson({
      agent: AGENT,
      clientId,
      windowStart,
      windowEnd,
      lesson,
    });
    if (row) stored.push(row);
  }

  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [
    'reflection',
    'paige_reflection_pass',
    JSON.stringify({
      model: MODEL,
      window_start: windowStart.toISOString(),
      window_end: windowEnd.toISOString(),
      reviewed_rows: reviewedRows,
      lessons_returned: lessons.length,
      lessons_proposed: stored.length,
      lesson_ids: stored.map(row => row.id),
      duplicate_lessons_skipped: lessons.length - stored.length,
    }),
    'success',
    clientId,
  ]);

  return {
    success: true,
    agent: AGENT,
    client_id: clientId,
    model: MODEL,
    window_start: windowStart.toISOString(),
    window_end: windowEnd.toISOString(),
    reviewed_rows: reviewedRows,
    lessons_proposed: stored.length,
    duplicate_lessons_skipped: lessons.length - stored.length,
    lesson_ids: stored.map(row => row.id),
  };
}

module.exports = {
  run,
  _test: {
    coerceWindow,
    countReviewRows,
    collectSourceIds,
    defaultReflectionWindow,
    extractText,
    buildReviewPrompt,
    hasReviewedWindow,
  },
};

if (require.main === module) {
  run(parseCliOptions()).then(result => {
    console.log(JSON.stringify(result, null, 2));
    process.exit(result?.success ? 0 : 1);
  }).catch(err => {
    console.error('[reflection] Fatal error:', err.message);
    process.exit(1);
  });
}
