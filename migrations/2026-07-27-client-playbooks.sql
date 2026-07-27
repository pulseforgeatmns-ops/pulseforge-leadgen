-- SPEC-028 Client Playbooks — first-class versioned strategy assets (ADR-015)
-- Additive only. Rollback: migrations/2026-07-27-client-playbooks.rollback.sql

CREATE TABLE IF NOT EXISTS client_playbooks (
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  client_id TEXT,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  target_markets JSONB NOT NULL DEFAULT '[]'::jsonb,
  value_propositions JSONB NOT NULL DEFAULT '[]'::jsonb,
  ideal_customer JSONB NOT NULL DEFAULT '{}'::jsonb,
  brand_voice TEXT NOT NULL DEFAULT 'professional',
  preferred_channels JSONB NOT NULL DEFAULT '[]'::jsonb,
  outreach_sequence JSONB NOT NULL DEFAULT '[]'::jsonb,
  offers JSONB NOT NULL DEFAULT '[]'::jsonb,
  constraints JSONB NOT NULL DEFAULT '[]'::jsonb,
  success_metrics JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes TEXT,
  parent_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, version)
);

CREATE INDEX IF NOT EXISTS client_playbooks_client_idx
  ON client_playbooks (client_id);
CREATE INDEX IF NOT EXISTS client_playbooks_status_idx
  ON client_playbooks (status);
CREATE INDEX IF NOT EXISTS client_playbooks_name_idx
  ON client_playbooks (name);
