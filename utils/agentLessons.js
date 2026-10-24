const pool = require('../db');

const VALID_CATEGORIES = new Set(['fabrication', 'tone', 'format', 'hook', 'other']);
const VALID_SEVERITIES = new Set(['low', 'med', 'high']);
const VALID_STATUSES = new Set(['proposed', 'active', 'retired']);

let schemaPromise;
const ACTIVE_GUARDRAILS_SQL = `
    SELECT id, guardrail_text, category, severity, created_at, client_id
    FROM agent_lessons
    WHERE agent = $1
      AND status = 'active'
      AND (client_id = $2 OR client_id IS NULL)
    ORDER BY
      CASE severity WHEN 'high' THEN 3 WHEN 'med' THEN 2 ELSE 1 END DESC,
      created_at DESC
`;

function ensureAgentLessonsSchema() {
  if (!schemaPromise) {
    schemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS agent_lessons (
          id               SERIAL PRIMARY KEY,
          agent            TEXT NOT NULL,
          client_id        INTEGER,
          created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
          window_start     TIMESTAMPTZ NOT NULL,
          window_end       TIMESTAMPTZ NOT NULL,
          category         TEXT NOT NULL,
          lesson           TEXT NOT NULL,
          guardrail_text   TEXT NOT NULL,
          evidence         TEXT,
          source_run_ids   JSONB,
          severity         TEXT NOT NULL DEFAULT 'low',
          status           TEXT NOT NULL DEFAULT 'proposed',
          confirmed_by     TEXT,
          confirmed_at     TIMESTAMPTZ
        )
      `);
      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_agent_lessons_active
          ON agent_lessons (agent, status)
      `);
      await pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_lessons_dedupe
          ON agent_lessons (agent, client_id, guardrail_text)
      `);
      await pool.query(`
        ALTER TABLE agent_lessons
          ALTER COLUMN client_id DROP NOT NULL
      `);
      await pool.query('DROP INDEX IF EXISTS idx_agent_lessons_dedupe');
      await pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_lessons_dedupe_scope
          ON agent_lessons (agent, COALESCE(client_id, -1), guardrail_text)
      `);
    })().catch(err => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

function normalizeCategory(category) {
  const value = String(category || '').trim().toLowerCase();
  return VALID_CATEGORIES.has(value) ? value : 'other';
}

function normalizeSeverity(severity) {
  const value = String(severity || '').trim().toLowerCase();
  return VALID_SEVERITIES.has(value) ? value : 'low';
}

function normalizeScope(scope) {
  const value = String(scope || '').trim().toLowerCase();
  return value === 'global' ? 'global' : 'client';
}

function stripJsonFences(text) {
  return String(text || '')
    .trim()
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();
}

function parseJsonObject(text) {
  const defenced = stripJsonFences(text);
  try {
    return JSON.parse(defenced);
  } catch (_) {
    const match = defenced.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('Claude response did not contain a JSON object');
    return JSON.parse(match[0]);
  }
}

function normalizeSourceRunIds(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map(item => {
      if (Number.isInteger(item)) return item;
      const text = String(item || '').trim();
      return text ? text : null;
    })
    .filter(item => item != null);
}

async function insertProposedLesson({ agent, clientId, windowStart, windowEnd, lesson }) {
  await ensureAgentLessonsSchema();
  const category = normalizeCategory(lesson.category);
  const severity = normalizeSeverity(lesson.severity);
  const scope = normalizeScope(lesson.scope);
  const scopedClientId = scope === 'global' ? null : clientId;
  const lessonText = String(lesson.lesson || '').trim();
  const guardrailText = String(lesson.guardrail_text || '').trim();
  if (!lessonText || !guardrailText) return null;

  const duplicate = await pool.query(`
    SELECT id
    FROM agent_lessons
    WHERE agent = $1
      AND guardrail_text = $2
  `, [agent, guardrailText]);
  if (duplicate.rows.length) return null;

  const res = await pool.query(`
    INSERT INTO agent_lessons
      (agent, client_id, window_start, window_end, category, lesson, guardrail_text,
       evidence, source_run_ids, severity, status)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, 'proposed')
    ON CONFLICT DO NOTHING
    RETURNING *
  `, [
    agent,
    scopedClientId,
    windowStart,
    windowEnd,
    category,
    lessonText,
    guardrailText,
    String(lesson.evidence || '').trim() || null,
    JSON.stringify(normalizeSourceRunIds(lesson.source_run_ids)),
    severity,
  ]);
  return res.rows[0] || null;
}

async function getActiveGuardrails(agent, clientId) {
  const res = await pool.query(ACTIVE_GUARDRAILS_SQL, [agent, clientId]);
  return res.rows;
}

async function listLessons({ agent = 'paige', clientId, status = 'proposed' } = {}) {
  await ensureAgentLessonsSchema();
  if (!VALID_STATUSES.has(status)) throw new Error(`Invalid lesson status: ${status}`);
  const params = [agent, status];
  const scopeClause = clientId == null ? '' : 'AND (client_id = $3 OR client_id IS NULL)';
  if (clientId != null) params.push(clientId);
  const res = await pool.query(`
    SELECT id, agent, client_id,
           CASE WHEN client_id IS NULL THEN 'global' ELSE 'client' END AS scope,
           created_at, window_start, window_end, category,
           lesson, guardrail_text, evidence, source_run_ids, severity, status,
           confirmed_by, confirmed_at
    FROM agent_lessons
    WHERE agent = $1
      AND status = $2
      ${scopeClause}
    ORDER BY
      CASE severity WHEN 'high' THEN 3 WHEN 'med' THEN 2 ELSE 1 END DESC,
      created_at DESC
  `, params);
  return res.rows;
}

function normalizeScopeOverride(options = {}) {
  if (options.global && options.clientId != null) {
    throw new Error('Use either --global or --client-id, not both');
  }
  if (options.global) return { hasOverride: true, clientId: null };
  if (options.clientId != null) return { hasOverride: true, clientId: options.clientId };
  return { hasOverride: false, clientId: null };
}

async function promoteLesson(id, confirmedBy, options = {}) {
  await ensureAgentLessonsSchema();
  const actor = String(confirmedBy || '').trim();
  if (!actor) throw new Error('--confirmed-by is required when promoting a lesson');
  const scopeOverride = normalizeScopeOverride(options);
  const res = await pool.query(`
    UPDATE agent_lessons
       SET status = 'active',
           client_id = CASE WHEN $3 THEN $4 ELSE client_id END,
           confirmed_by = $2,
           confirmed_at = NOW()
     WHERE id = $1
       AND status = 'proposed'
     RETURNING *
  `, [id, actor, scopeOverride.hasOverride, scopeOverride.clientId]);
  return res.rows[0] || null;
}

async function retireLesson(id, confirmedBy = null) {
  await ensureAgentLessonsSchema();
  const res = await pool.query(`
    UPDATE agent_lessons
       SET status = 'retired',
           confirmed_by = COALESCE($2, confirmed_by),
           confirmed_at = COALESCE(confirmed_at, CASE WHEN $2 IS NULL THEN confirmed_at ELSE NOW() END)
     WHERE id = $1
       AND status IN ('proposed', 'active')
     RETURNING *
  `, [id, confirmedBy ? String(confirmedBy).trim() : null]);
  return res.rows[0] || null;
}

module.exports = {
  ensureAgentLessonsSchema,
  getActiveGuardrails,
  insertProposedLesson,
  listLessons,
  ACTIVE_GUARDRAILS_SQL,
  parseJsonObject,
  promoteLesson,
  retireLesson,
  normalizeScope,
  stripJsonFences,
};
