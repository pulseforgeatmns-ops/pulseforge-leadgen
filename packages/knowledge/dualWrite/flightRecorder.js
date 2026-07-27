'use strict';

const fs = require('fs');
const path = require('path');
const { FLIGHT_STAGES, FLIGHT_STAGE_ORDER } = require('./operationalEvents');

/**
 * Apply SPEC-014 dual-write schema (outbox, ledger, flight stages).
 * Idempotent — safe to call on boot.
 *
 * @param {{ query: Function }} pool
 * @param {{ sqlPath?: string }} [options]
 */
async function ensureDualWriteSchema(pool, options = {}) {
  if (!pool || typeof pool.query !== 'function') {
    throw new Error('ensureDualWriteSchema requires a pg pool');
  }
  // gen_random_uuid() — prefer pgcrypto; fall back to uuid-ossp if needed
  try {
    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`);
  } catch {
    // Extension may already exist or lack privilege — table DDL still uses DEFAULT
  }
  const sqlPath =
    options.sqlPath ||
    path.join(__dirname, '../../../migrations/2026-07-26-knowledge-dual-write.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');
  await pool.query(sql);
}

/**
 * Record a Flight Recorder stage. Idempotent on (flight_id, stage).
 */
async function recordFlightStage(pool, input = {}) {
  if (!pool || typeof pool.query !== 'function') {
    throw new Error('recordFlightStage requires a pg pool');
  }
  if (!input.flightId || !input.tenantId || !input.stage) {
    throw new Error('recordFlightStage requires flightId, tenantId, stage');
  }
  const res = await pool.query(
    `INSERT INTO knowledge_flight_stages
       (flight_id, tenant_id, entity_id, entity_type, stage, status, occurred_at, metadata)
     VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7::timestamptz, NOW()), $8::jsonb)
     ON CONFLICT (flight_id, stage) DO UPDATE
       SET status = EXCLUDED.status,
           occurred_at = EXCLUDED.occurred_at,
           metadata = knowledge_flight_stages.metadata || EXCLUDED.metadata
     RETURNING *`,
    [
      String(input.flightId),
      String(input.tenantId),
      input.entityId != null ? String(input.entityId) : null,
      input.entityType || null,
      String(input.stage),
      input.status || 'complete',
      input.occurredAt || null,
      JSON.stringify(input.metadata || {}),
    ]
  );
  return res.rows[0];
}

/**
 * Load a flight journey (ordered stages + checklist against FLIGHT_STAGE_ORDER).
 */
async function getFlightJourney(pool, { flightId, tenantId } = {}) {
  if (!flightId) throw new Error('getFlightJourney requires flightId');
  const params = [String(flightId)];
  let sql = `SELECT * FROM knowledge_flight_stages WHERE flight_id = $1`;
  if (tenantId != null) {
    sql += ` AND tenant_id = $2`;
    params.push(String(tenantId));
  }
  sql += ` ORDER BY occurred_at ASC`;
  const res = await pool.query(sql, params);
  const byStage = new Map(res.rows.map((r) => [r.stage, r]));
  const checklist = FLIGHT_STAGE_ORDER.map((stage) => {
    const row = byStage.get(stage);
    return {
      stage,
      label: stageLabel(stage),
      status: row ? row.status : 'pending',
      occurredAt: row ? row.occurred_at : null,
      metadata: row ? row.metadata : null,
      complete: Boolean(row && row.status === 'complete'),
    };
  });
  return {
    flightId: String(flightId),
    tenantId: tenantId != null ? String(tenantId) : res.rows[0]?.tenant_id || null,
    entityId: res.rows[0]?.entity_id || null,
    entityType: res.rows[0]?.entity_type || null,
    stages: checklist,
    recorded: res.rows,
    completeCount: checklist.filter((s) => s.complete).length,
    totalStages: checklist.length,
  };
}

/**
 * List recent flights for admin panel.
 */
async function listRecentFlights(pool, { tenantId, limit = 25 } = {}) {
  const lim = Math.min(Number(limit) || 25, 100);
  if (tenantId != null) {
    const res = await pool.query(
      `SELECT flight_id, tenant_id, entity_id, entity_type, MAX(occurred_at) AS occurred_at
       FROM knowledge_flight_stages
       WHERE tenant_id = $1
       GROUP BY flight_id, tenant_id, entity_id, entity_type
       ORDER BY MAX(occurred_at) DESC
       LIMIT $2`,
      [String(tenantId), lim]
    );
    return res.rows;
  }
  const res = await pool.query(
    `SELECT flight_id, tenant_id, entity_id, entity_type, MAX(occurred_at) AS occurred_at
     FROM knowledge_flight_stages
     GROUP BY flight_id, tenant_id, entity_id, entity_type
     ORDER BY MAX(occurred_at) DESC
     LIMIT $1`,
    [lim]
  );
  return res.rows;
}

function stageLabel(stage) {
  const labels = {
    [FLIGHT_STAGES.PROSPECT_DISCOVERED]: 'Prospect Discovered',
    [FLIGHT_STAGES.KNOWLEDGE_WRITTEN]: 'Knowledge Written',
    [FLIGHT_STAGES.REASONING_GENERATED]: 'Reasoning Generated',
    [FLIGHT_STAGES.MEMORY_UPDATED]: 'Memory Updated',
    [FLIGHT_STAGES.BRIEFING_UPDATED]: 'Briefing Updated',
    [FLIGHT_STAGES.COMMAND_DECK_REFRESHED]: 'Command Deck Refreshed',
    [FLIGHT_STAGES.VIEWED_BY_OPERATOR]: 'Viewed by Operator',
    [FLIGHT_STAGES.OUTCOME_RECORDED]: 'Outcome Recorded',
  };
  return labels[stage] || stage;
}

module.exports = {
  ensureDualWriteSchema,
  recordFlightStage,
  getFlightJourney,
  listRecentFlights,
  stageLabel,
  FLIGHT_STAGES,
  FLIGHT_STAGE_ORDER,
};
