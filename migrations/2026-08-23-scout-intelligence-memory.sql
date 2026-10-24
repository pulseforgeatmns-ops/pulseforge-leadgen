-- SPEC-143 — Scout Acquisition Intelligence Memory
-- Durable investigation knowledge that compounds across missions.
-- Additive. Rollback: migrations/2026-08-23-scout-intelligence-memory.rollback.sql

CREATE TABLE IF NOT EXISTS scout_intelligence_memory (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  memory_type TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  label TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_count INTEGER NOT NULL DEFAULT 1,
  verification_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'active',
  mission_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, memory_type, entity_key)
);

CREATE INDEX IF NOT EXISTS scout_intelligence_memory_tenant_type_idx
  ON scout_intelligence_memory (tenant_id, memory_type);

CREATE INDEX IF NOT EXISTS scout_intelligence_memory_tenant_updated_idx
  ON scout_intelligence_memory (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS scout_intelligence_memory_edges (
  tenant_id TEXT NOT NULL,
  from_id TEXT NOT NULL,
  to_id TEXT NOT NULL,
  relation TEXT NOT NULL DEFAULT 'related',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, from_id, to_id, relation)
);

CREATE INDEX IF NOT EXISTS scout_intelligence_memory_edges_tenant_idx
  ON scout_intelligence_memory_edges (tenant_id);
