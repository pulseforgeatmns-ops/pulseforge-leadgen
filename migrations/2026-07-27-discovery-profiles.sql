-- SPEC-024 Discovery Profiles — first-class versioned business assets
-- Additive only. Rollback: migrations/2026-07-27-discovery-profiles.rollback.sql

CREATE TABLE IF NOT EXISTS discovery_profiles (
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  tenant_id TEXT,
  client_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  industry_targets JSONB NOT NULL DEFAULT '[]'::jsonb,
  geography JSONB NOT NULL DEFAULT '{}'::jsonb,
  target_count INTEGER NOT NULL DEFAULT 50,
  required_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  preferred_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  excluded_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  ranking_weights JSONB NOT NULL DEFAULT '{}'::jsonb,
  minimum_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.75,
  deduplication_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
  review_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'active',
  parent_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, version)
);

CREATE INDEX IF NOT EXISTS discovery_profiles_name_idx
  ON discovery_profiles (name);
CREATE INDEX IF NOT EXISTS discovery_profiles_status_idx
  ON discovery_profiles (status);
CREATE INDEX IF NOT EXISTS discovery_profiles_tenant_idx
  ON discovery_profiles (tenant_id);
