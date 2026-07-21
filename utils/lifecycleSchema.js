'use strict';

// Phase A2 canonical lifecycle schema. Additive only: new event + note tables
// and one tenant-config column. Legacy fields (prospects.status,
// prospects.setter_status, prospects.notes, prospects.callback_at,
// activity_log, call_dispositions) are never rewritten or dropped here.
// The controlled forward/rollback SQL lives in
// migrations/2026-07-21-phase-a2-canonical-lifecycle.sql; this module is the
// idempotent startup reconcile, matching the closerSchema/callDispositions
// pattern.

let lifecycleSchemaPromise = null;

async function ensureLifecycleSchema(db) {
  if (!lifecycleSchemaPromise) {
    lifecycleSchemaPromise = applyLifecycleSchema(db).catch(err => {
      lifecycleSchemaPromise = null;
      throw err;
    });
  }
  return lifecycleSchemaPromise;
}

function resetLifecycleSchemaCache() {
  lifecycleSchemaPromise = null;
}

async function applyLifecycleSchema(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS prospect_lifecycle_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL,
      prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      from_stage TEXT,
      to_stage TEXT NOT NULL,
      from_status TEXT,
      to_status TEXT,
      from_setter_status TEXT,
      to_setter_status TEXT,
      disposition TEXT,
      disposition_id INTEGER,
      callback_at TIMESTAMPTZ,
      reason TEXT,
      actor_type TEXT,
      actor_id TEXT,
      actor_name TEXT,
      source TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      idempotency_key TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  // Phase B additive: structured lifecycle reason codes (nurture,
  // data_remediation, terminal_suppression). Free-text `reason` is unchanged.
  await db.query(`
    ALTER TABLE prospect_lifecycle_events
      ADD COLUMN IF NOT EXISTS lifecycle_reason TEXT
      CHECK (lifecycle_reason IS NULL OR lifecycle_reason IN ('nurture', 'data_remediation', 'terminal_suppression'))
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS prospect_lifecycle_events_idempotency_idx
      ON prospect_lifecycle_events (client_id, idempotency_key)
      WHERE idempotency_key IS NOT NULL
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS prospect_lifecycle_events_prospect_idx
      ON prospect_lifecycle_events (client_id, prospect_id, created_at DESC)
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS prospect_notes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL,
      prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      note_type TEXT NOT NULL DEFAULT 'operator'
        CHECK (note_type IN ('operator', 'call', 'research', 'system')),
      text TEXT NOT NULL,
      author_id INTEGER,
      author_name TEXT,
      source TEXT NOT NULL DEFAULT 'workspace',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS prospect_notes_prospect_idx
      ON prospect_notes (client_id, prospect_id, created_at DESC)
  `);

  // Tenant-configurable qualification threshold (Phase A2 §11). NULL means
  // "use current production defaults" — visibility 70, queue display 40.
  // Production membership must not change until the shadow delta report
  // (scripts/thresholdDeltaReport.js) is reviewed and approved.
  await db.query(`
    ALTER TABLE clients
      ADD COLUMN IF NOT EXISTS setter_qualification_threshold INTEGER
  `);
}

module.exports = { ensureLifecycleSchema, resetLifecycleSchemaCache };
