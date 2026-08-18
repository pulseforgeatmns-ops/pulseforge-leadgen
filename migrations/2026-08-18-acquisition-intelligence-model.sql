-- SPEC-112 Acquisition Intelligence Model (v1)
-- Client-expertise intelligence Scout reasons over before acquisition.
-- NOT operating fact — keep separate from knowledge claims and SPEC-110 objects.
-- Additive only. Rollback: migrations/2026-08-18-acquisition-intelligence-model.rollback.sql

CREATE TABLE IF NOT EXISTS aim_models (
  id TEXT PRIMARY KEY,
  client_key TEXT NOT NULL,
  client_id INTEGER,
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  status TEXT NOT NULL
    CHECK (status IN ('draft', 'complete', 'superseded')),
  mission JSONB NOT NULL DEFAULT '{}'::jsonb,
  icp JSONB NOT NULL DEFAULT '{}'::jsonb,
  transformation JSONB NOT NULL DEFAULT '{}'::jsonb,
  pain_ontology JSONB NOT NULL DEFAULT '{}'::jsonb,
  knowledge JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_operating_fact BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_key, version)
);

CREATE INDEX IF NOT EXISTS aim_models_client_key_idx
  ON aim_models (client_key);

CREATE INDEX IF NOT EXISTS aim_models_status_idx
  ON aim_models (status);

CREATE TABLE IF NOT EXISTS aim_qualifications (
  id BIGSERIAL PRIMARY KEY,
  client_key TEXT NOT NULL,
  prospect_id TEXT,
  prospect_name TEXT,
  icp_fit INTEGER NOT NULL,
  pain_match INTEGER NOT NULL,
  evidence_quality INTEGER NOT NULL,
  buying_readiness INTEGER NOT NULL,
  confidence INTEGER NOT NULL,
  overall_recommendation TEXT NOT NULL
    CHECK (overall_recommendation IN ('pursue', 'nurture', 'watch', 'reject', 'unknown')),
  top_pain_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_operating_fact BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS aim_qualifications_client_key_idx
  ON aim_qualifications (client_key);

CREATE INDEX IF NOT EXISTS aim_qualifications_prospect_idx
  ON aim_qualifications (client_key, prospect_id);

CREATE TABLE IF NOT EXISTS aim_pain_knowledge (
  client_key TEXT NOT NULL,
  pain_id TEXT NOT NULL,
  label TEXT,
  definition TEXT,
  observable_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  common_objections JSONB NOT NULL DEFAULT '[]'::jsonb,
  typical_language JSONB NOT NULL DEFAULT '[]'::jsonb,
  recommended_messaging JSONB NOT NULL DEFAULT '[]'::jsonb,
  discovery_questions JSONB NOT NULL DEFAULT '[]'::jsonb,
  case_studies JSONB NOT NULL DEFAULT '[]'::jsonb,
  success_stories JSONB NOT NULL DEFAULT '[]'::jsonb,
  unknowns JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (client_key, pain_id)
);
