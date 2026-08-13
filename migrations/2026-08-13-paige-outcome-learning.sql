-- SPEC-093 Paige Outcome Learning Loop (v1 thin slice)
-- Durable evidence-supported content learnings derived from SPEC-092 outcomes.
-- Reasoning layer only — no autonomous publishing or strategy mutation.
-- Additive. Rollback: migrations/2026-08-13-paige-outcome-learning.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS content_learnings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  tenant_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  learning_type TEXT NOT NULL
    CHECK (learning_type IN (
      'distribution_pattern',
      'topic_performance',
      'message_resonance',
      'audience_response',
      'format_performance',
      'business_outcome_pattern',
      'conversion_pattern',
      'language_adoption',
      'objection_pattern',
      'talent_signal',
      'partnership_signal',
      'content_sequence_pattern',
      'other'
    )),
  statement TEXT NOT NULL,
  scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  objective TEXT,
  topic TEXT,
  format TEXT,
  audience_type TEXT,
  channel TEXT,
  confidence NUMERIC(5, 4) NOT NULL DEFAULT 0
    CHECK (confidence >= 0 AND confidence <= 1),
  observation_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0
    CHECK (observation_confidence >= 0 AND observation_confidence <= 1),
  generalization_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0
    CHECK (generalization_confidence >= 0 AND generalization_confidence <= 1),
  sample_size INTEGER NOT NULL DEFAULT 0 CHECK (sample_size >= 0),
  supporting_publication_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  contradicting_publication_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_summary TEXT NOT NULL DEFAULT '',
  uncertainty_summary TEXT,
  status TEXT NOT NULL DEFAULT 'signal'
    CHECK (status IN ('signal', 'emerging', 'supported', 'contradicted', 'stale')),
  first_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, fingerprint)
);

CREATE INDEX IF NOT EXISTS content_learnings_client_status_idx
  ON content_learnings (client_id, status, last_evaluated_at DESC);

CREATE INDEX IF NOT EXISTS content_learnings_tenant_type_idx
  ON content_learnings (tenant_id, learning_type, last_evaluated_at DESC);

CREATE INDEX IF NOT EXISTS content_learnings_client_objective_idx
  ON content_learnings (client_id, objective, confidence DESC);
