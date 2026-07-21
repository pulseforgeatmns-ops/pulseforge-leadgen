-- Phase A2 — Canonical Prospect Workspace and Lifecycle Convergence.
-- Additive and reversible. No legacy field (prospects.status,
-- prospects.setter_status, prospects.notes, prospects.callback_at,
-- activity_log, call_dispositions) is rewritten or dropped.
--
-- Rollback: 2026-07-21-phase-a2-canonical-lifecycle.rollback.sql

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- One canonical transition event per lifecycle write. Every stage change
-- (Pipeline move, call disposition, callback scheduling, booked handoff,
-- dead outcome) produces exactly one row here.
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
);

CREATE UNIQUE INDEX IF NOT EXISTS prospect_lifecycle_events_idempotency_idx
  ON prospect_lifecycle_events (client_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS prospect_lifecycle_events_prospect_idx
  ON prospect_lifecycle_events (client_id, prospect_id, created_at DESC);

-- Structured note storage. Legacy prospects.notes stays readable and is
-- surfaced separately as legacy notes; all NEW notes write here.
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
);

CREATE INDEX IF NOT EXISTS prospect_notes_prospect_idx
  ON prospect_notes (client_id, prospect_id, created_at DESC);

-- Tenant-configurable qualification threshold. NULL = keep current
-- production defaults (visibility 70 / queue display 40). Values here take
-- effect for the queue display filter only, and only after the
-- threshold-delta report is approved.
ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS setter_qualification_threshold INTEGER;

COMMIT;
