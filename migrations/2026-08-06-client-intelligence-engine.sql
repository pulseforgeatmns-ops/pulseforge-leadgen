-- SPEC-083 Client Intelligence Engine (v1 thin slice)
-- Additive only. Rollback: migrations/2026-08-06-client-intelligence-engine.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS cie_interview_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'NEW'
    CHECK (status IN (
      'NEW',
      'DISCOVERY',
      'CLARIFICATION',
      'VALIDATION',
      'BLUEPRINT_GENERATION',
      'CLIENT_REVIEW',
      'APPROVED'
    )),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  current_stage TEXT NOT NULL DEFAULT 'Identity',
  summary TEXT,
  confidence_score NUMERIC(4, 3),
  interview_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cie_interview_sessions_client_status_idx
  ON cie_interview_sessions (client_id, status, started_at DESC);

CREATE TABLE IF NOT EXISTS cie_interview_turns (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id UUID NOT NULL
    REFERENCES cie_interview_sessions(id) ON DELETE CASCADE,
  speaker TEXT NOT NULL CHECK (speaker IN ('assistant', 'client', 'system')),
  message TEXT NOT NULL DEFAULT '',
  goal TEXT,
  asked_because TEXT,
  derived_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cie_interview_turns_session_idx
  ON cie_interview_turns (session_id, created_at ASC);

CREATE TABLE IF NOT EXISTS cie_evidence (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  session_id UUID NOT NULL
    REFERENCES cie_interview_sessions(id) ON DELETE CASCADE,
  source TEXT NOT NULL DEFAULT 'interview',
  source_turn_id UUID
    REFERENCES cie_interview_turns(id) ON DELETE SET NULL,
  category TEXT NOT NULL,
  statement TEXT NOT NULL,
  confidence NUMERIC(4, 3) NOT NULL DEFAULT 0.7,
  type TEXT NOT NULL DEFAULT 'EXPLICIT'
    CHECK (type IN ('EXPLICIT', 'INFERRED', 'OBSERVED', 'CLIENT_EDITED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cie_evidence_session_category_idx
  ON cie_evidence (session_id, category);

CREATE INDEX IF NOT EXISTS cie_evidence_client_idx
  ON cie_evidence (client_id, created_at DESC);

CREATE TABLE IF NOT EXISTS cie_business_blueprints (
  id UUID NOT NULL,
  client_id INTEGER NOT NULL,
  session_id UUID NOT NULL
    REFERENCES cie_interview_sessions(id) ON DELETE RESTRICT,
  version TEXT NOT NULL DEFAULT '1.0',
  status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'in_review', 'approved', 'superseded')),
  generated_by TEXT NOT NULL DEFAULT 'CIE-v1',
  sections JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  playbook_id TEXT,
  playbook_version TEXT,
  section_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  parent_blueprint_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, version)
);

CREATE INDEX IF NOT EXISTS cie_business_blueprints_client_status_idx
  ON cie_business_blueprints (client_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS cie_business_blueprints_session_idx
  ON cie_business_blueprints (session_id);
