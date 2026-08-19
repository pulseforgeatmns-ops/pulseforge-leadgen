'use strict';

/**
 * SPEC-117 — persist send plans, outcomes, and learning.
 */

const defaultPool = require('../db');

async function ensureEmmettOutboundSchema(pool = defaultPool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS emmett_inbox_snapshots (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      local_date DATE NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS emmett_send_plans (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      local_date DATE NOT NULL,
      status TEXT NOT NULL,
      recommended_capacity INTEGER,
      approved_capacity INTEGER,
      allow_legacy_sequences BOOLEAN NOT NULL DEFAULT FALSE,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      approved_at TIMESTAMPTZ,
      approved_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS emmett_governor_acks (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      plan_id TEXT,
      outcome TEXT NOT NULL,
      operator_id TEXT,
      note TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS emmett_outbound_outcomes (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      prospect_id INTEGER,
      outcome_type TEXT NOT NULL,
      sinks JSONB NOT NULL DEFAULT '[]'::jsonb,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS emmett_outbound_learning (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      sink TEXT NOT NULL,
      outcome_id TEXT,
      outcome_type TEXT,
      statement TEXT,
      auto_applied BOOLEAN NOT NULL DEFAULT FALSE,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function persistPlan(plan, pool = defaultPool) {
  if (!plan?.id) return null;
  await ensureEmmettOutboundSchema(pool);
  await pool.query(
    `INSERT INTO emmett_send_plans (
        id, tenant_id, client_id, local_date, status, recommended_capacity,
        approved_capacity, allow_legacy_sequences, payload, approved_at, approved_by, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (id) DO UPDATE SET
        status = EXCLUDED.status,
        recommended_capacity = EXCLUDED.recommended_capacity,
        approved_capacity = EXCLUDED.approved_capacity,
        allow_legacy_sequences = EXCLUDED.allow_legacy_sequences,
        payload = EXCLUDED.payload,
        approved_at = EXCLUDED.approved_at,
        approved_by = EXCLUDED.approved_by,
        updated_at = NOW()`,
    [
      plan.id,
      plan.tenantId,
      plan.clientId || null,
      plan.localDate,
      plan.status,
      plan.recommendedCapacity,
      plan.approvedCapacity,
      plan.allowLegacySequences === true,
      JSON.parse(JSON.stringify(plan)),
      plan.approvedAt || null,
      plan.approvedBy || null,
    ]
  );
  return plan;
}

async function persistAck(ack, pool = defaultPool) {
  if (!ack?.id) return null;
  await ensureEmmettOutboundSchema(pool);
  await pool.query(
    `INSERT INTO emmett_governor_acks (id, tenant_id, plan_id, outcome, operator_id, note, payload)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     ON CONFLICT (id) DO NOTHING`,
    [ack.id, ack.tenantId, ack.planId || null, ack.outcome, ack.operatorId, ack.note, JSON.parse(JSON.stringify(ack))]
  );
  return ack;
}

async function persistOutcome(outcome, pool = defaultPool) {
  if (!outcome?.id) return null;
  await ensureEmmettOutboundSchema(pool);
  await pool.query(
    `INSERT INTO emmett_outbound_outcomes (
        id, tenant_id, client_id, prospect_id, outcome_type, sinks, payload, event_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (id) DO NOTHING`,
    [
      outcome.id,
      outcome.tenantId,
      outcome.clientId || null,
      outcome.prospectId || null,
      outcome.type,
      JSON.stringify(outcome.sinks || []),
      JSON.parse(JSON.stringify(outcome)),
      outcome.eventAt,
    ]
  );
  return outcome;
}

async function persistLearning(row, pool = defaultPool) {
  if (!row?.id) return null;
  await ensureEmmettOutboundSchema(pool);
  await pool.query(
    `INSERT INTO emmett_outbound_learning (
        id, tenant_id, client_id, sink, outcome_id, outcome_type, statement, auto_applied, payload
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (id) DO NOTHING`,
    [
      row.id,
      row.tenantId,
      row.clientId || null,
      row.sink,
      row.outcomeId,
      row.outcomeType,
      row.statement,
      row.autoApplied === true,
      JSON.parse(JSON.stringify(row)),
    ]
  );
  return row;
}

async function loadTenantPlans(tenantId, pool = defaultPool) {
  await ensureEmmettOutboundSchema(pool);
  const res = await pool.query(
    `SELECT payload FROM emmett_send_plans WHERE tenant_id = $1 ORDER BY updated_at`,
    [String(tenantId)]
  );
  return res.rows.map((row) => row.payload).filter(Boolean);
}

async function loadLearning(tenantId, pool = defaultPool) {
  await ensureEmmettOutboundSchema(pool);
  const res = await pool.query(
    `SELECT payload FROM emmett_outbound_learning WHERE tenant_id = $1 ORDER BY created_at`,
    [String(tenantId)]
  );
  return res.rows.map((row) => row.payload).filter(Boolean);
}

module.exports = {
  ensureEmmettOutboundSchema,
  persistPlan,
  persistAck,
  persistOutcome,
  persistLearning,
  loadTenantPlans,
  loadLearning,
};
