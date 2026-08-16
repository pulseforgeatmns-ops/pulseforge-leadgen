-- SPEC-100 Max ↔ Scout Acquisition Intelligence Loop
-- Tenant-scoped AO snapshot written only by Max after evaluation.
-- Additive. Rollback: migrations/2026-08-16-scout-acquisition-intelligence.rollback.sql

CREATE TABLE IF NOT EXISTS acquisition_intelligence_state (
  tenant_id TEXT PRIMARY KEY,
  summary TEXT NOT NULL DEFAULT 'Pipeline steady',
  opportunity_count INTEGER NOT NULL DEFAULT 0,
  timely_count INTEGER NOT NULL DEFAULT 0,
  segment_highlights JSONB NOT NULL DEFAULT '[]'::jsonb,
  unknowns JSONB NOT NULL DEFAULT '[]'::jsonb,
  accepted_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
  rejected_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
  unresolved_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
  materiality TEXT NOT NULL DEFAULT 'immaterial',
  priority_impact JSONB,
  evaluation_id TEXT,
  result_id TEXT,
  delegation_id TEXT,
  opportunities JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_intelligence_state_updated_idx
  ON acquisition_intelligence_state (updated_at DESC);
