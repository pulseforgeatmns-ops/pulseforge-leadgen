-- SPEC-070 Intelligence Seed Libraries (v0.1)
-- Curated reference/guidance for Max and future services.
-- NOT observed evidence — keep separate from market_* and knowledge_*.
-- Additive only. Rollback: migrations/2026-08-04-intelligence-seed-libraries.rollback.sql

CREATE TABLE IF NOT EXISTS intelligence_seed_libraries (
  library_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  category TEXT NOT NULL
    CHECK (category IN (
      'sales_methodology',
      'industry_playbook',
      'offer_positioning',
      'operating_preferences',
      'market_reference'
    )),
  source_type TEXT NOT NULL
    CHECK (source_type IN (
      'curated_operator',
      'public_method_summary',
      'internal_preference',
      'market_background'
    )),
  trust_level TEXT NOT NULL
    CHECK (trust_level IN ('high', 'medium', 'low', 'provisional')),
  scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary TEXT NOT NULL,
  content_text TEXT,
  content_ref TEXT,
  tags TEXT[] NOT NULL DEFAULT '{}'::text[],
  provenance JSONB NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  version INTEGER NOT NULL DEFAULT 1
    CHECK (version >= 1),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT intelligence_seed_libraries_content_present
    CHECK (content_text IS NOT NULL OR content_ref IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS intelligence_seed_libraries_category_idx
  ON intelligence_seed_libraries (category);

CREATE INDEX IF NOT EXISTS intelligence_seed_libraries_trust_level_idx
  ON intelligence_seed_libraries (trust_level);

CREATE INDEX IF NOT EXISTS intelligence_seed_libraries_enabled_idx
  ON intelligence_seed_libraries (enabled);

CREATE INDEX IF NOT EXISTS intelligence_seed_libraries_tags_gin_idx
  ON intelligence_seed_libraries USING GIN (tags);

CREATE INDEX IF NOT EXISTS intelligence_seed_libraries_scope_gin_idx
  ON intelligence_seed_libraries USING GIN (scope);
