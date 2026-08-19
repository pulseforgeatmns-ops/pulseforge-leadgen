'use strict';

/**
 * SPEC-116 — persist operator scorecards and learning.
 */

const defaultPool = require('../db');

async function ensureOperatorScorecardSchema(pool = defaultPool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS operator_scorecards (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      status TEXT NOT NULL,
      version INTEGER NOT NULL DEFAULT 1,
      business_goal TEXT,
      business_stage TEXT,
      profile TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      is_runtime BOOLEAN NOT NULL DEFAULT FALSE,
      approved_at TIMESTAMPTZ,
      approved_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS operator_scorecard_learning (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      scorecard_id TEXT,
      metric_key TEXT,
      metric_name TEXT,
      action TEXT NOT NULL,
      reason TEXT,
      suppress BOOLEAN NOT NULL DEFAULT FALSE,
      prioritize BOOLEAN NOT NULL DEFAULT FALSE,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function persistScorecard(scorecard, pool = defaultPool) {
  if (!scorecard || !scorecard.id) return null;
  await ensureOperatorScorecardSchema(pool);
  const payload = JSON.parse(JSON.stringify(scorecard));
  await pool.query(
    `INSERT INTO operator_scorecards (
        id, tenant_id, client_id, status, version, business_goal, business_stage,
        profile, payload, is_runtime, approved_at, approved_by, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
      ON CONFLICT (id) DO UPDATE SET
        tenant_id = EXCLUDED.tenant_id,
        client_id = EXCLUDED.client_id,
        status = EXCLUDED.status,
        version = EXCLUDED.version,
        business_goal = EXCLUDED.business_goal,
        business_stage = EXCLUDED.business_stage,
        profile = EXCLUDED.profile,
        payload = EXCLUDED.payload,
        is_runtime = EXCLUDED.is_runtime,
        approved_at = EXCLUDED.approved_at,
        approved_by = EXCLUDED.approved_by,
        updated_at = NOW()`,
    [
      scorecard.id,
      scorecard.tenantId || scorecard.tenant_id || '',
      scorecard.clientId || scorecard.client_id || null,
      scorecard.status || 'draft',
      scorecard.version || 1,
      scorecard.businessGoal || null,
      scorecard.businessStage || null,
      scorecard.profile || null,
      payload,
      scorecard.isRuntime === true,
      scorecard.approvedAt || null,
      scorecard.approvedBy || null,
    ]
  );
  return scorecard;
}

async function persistLearning(row, pool = defaultPool) {
  if (!row || !row.id) return null;
  await ensureOperatorScorecardSchema(pool);
  await pool.query(
    `INSERT INTO operator_scorecard_learning (
        id, tenant_id, client_id, scorecard_id, metric_key, metric_name, action,
        reason, suppress, prioritize, payload, created_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (id) DO NOTHING`,
    [
      row.id,
      row.tenantId || row.tenant_id || '',
      row.clientId || row.client_id || null,
      row.scorecardId || null,
      row.metricKey || row.metric_key || null,
      row.metricName || row.metric_name || null,
      row.action,
      row.reason || null,
      row.suppress === true,
      row.prioritize === true,
      row,
    ]
  );
  return row;
}

async function loadScorecard(id, pool = defaultPool) {
  await ensureOperatorScorecardSchema(pool);
  const { rows } = await pool.query(
    `SELECT payload FROM operator_scorecards WHERE id = $1`,
    [id]
  );
  return rows[0] ? rows[0].payload : null;
}

async function loadTenantScorecards(tenantId, pool = defaultPool) {
  await ensureOperatorScorecardSchema(pool);
  const { rows } = await pool.query(
    `SELECT payload FROM operator_scorecards WHERE tenant_id = $1 ORDER BY updated_at DESC`,
    [String(tenantId)]
  );
  return rows.map((row) => row.payload);
}

async function loadApprovedScorecard(tenantId, pool = defaultPool) {
  await ensureOperatorScorecardSchema(pool);
  const { rows } = await pool.query(
    `SELECT payload FROM operator_scorecards
      WHERE tenant_id = $1 AND status = 'approved'
      ORDER BY approved_at DESC NULLS LAST, updated_at DESC
      LIMIT 1`,
    [String(tenantId)]
  );
  return rows[0] ? rows[0].payload : null;
}

async function loadLearning(tenantId, pool = defaultPool) {
  await ensureOperatorScorecardSchema(pool);
  const { rows } = await pool.query(
    `SELECT * FROM operator_scorecard_learning WHERE tenant_id = $1 ORDER BY created_at ASC`,
    [String(tenantId)]
  );
  return rows.map((row) => ({
    id: row.id,
    tenantId: row.tenant_id,
    clientId: row.client_id,
    scorecardId: row.scorecard_id,
    metricKey: row.metric_key,
    metricName: row.metric_name,
    action: row.action,
    reason: row.reason,
    suppress: row.suppress,
    prioritize: row.prioritize,
  }));
}

module.exports = {
  ensureOperatorScorecardSchema,
  persistScorecard,
  persistLearning,
  loadScorecard,
  loadTenantScorecards,
  loadApprovedScorecard,
  loadLearning,
};
