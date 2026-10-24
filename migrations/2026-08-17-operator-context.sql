-- SPEC-104 Persistent Operator Context
-- Max's living operational mental model per client — rebuilt on meaningful events.
-- Additive only. Rollback: migrations/2026-08-17-operator-context.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS operator_contexts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_rebuild_trigger TEXT,
  last_rebuild_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, client_id)
);

CREATE INDEX IF NOT EXISTS operator_contexts_client_idx
  ON operator_contexts (client_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS operator_contexts_tenant_idx
  ON operator_contexts (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS operator_context_rebuild_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  trigger TEXT NOT NULL,
  version INTEGER NOT NULL,
  context_version_before INTEGER,
  rebuilt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS operator_context_rebuild_events_client_idx
  ON operator_context_rebuild_events (client_id, rebuilt_at DESC);
