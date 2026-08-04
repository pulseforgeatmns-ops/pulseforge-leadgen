-- SPEC-065 Market Intelligence Foundation (v0.1)
-- Structured observations + company profiles on top of SPEC-061 raw archive.
-- Additive only. Rollback: migrations/2026-08-03-market-intelligence-foundation.rollback.sql
-- Does NOT touch CRM companies/prospects. No scoring or recommendation columns.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS market_observations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email_id UUID NOT NULL REFERENCES market_emails(id) ON DELETE CASCADE,
  company_id UUID NOT NULL REFERENCES market_companies(id) ON DELETE RESTRICT,
  category TEXT NOT NULL
    CHECK (category IN ('identity', 'campaign', 'messaging', 'format', 'personalization')),
  field TEXT NOT NULL,
  value_text TEXT NOT NULL DEFAULT '',
  value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_quote TEXT NOT NULL DEFAULT '',
  evidence_path TEXT NOT NULL DEFAULT 'body_text'
    CHECK (evidence_path IN ('subject', 'body_text', 'body_html', 'links', 'headers', 'from')),
  extractor TEXT NOT NULL DEFAULT 'deterministic_v1',
  extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT market_observations_email_field_value_unique
    UNIQUE (email_id, category, field, value_text)
);

CREATE INDEX IF NOT EXISTS market_observations_company_idx
  ON market_observations (company_id, category, field);

CREATE INDEX IF NOT EXISTS market_observations_email_idx
  ON market_observations (email_id);

CREATE INDEX IF NOT EXISTS market_observations_field_value_idx
  ON market_observations (field, value_text);

CREATE TABLE IF NOT EXISTS market_company_profiles (
  company_id UUID PRIMARY KEY REFERENCES market_companies(id) ON DELETE CASCADE,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  emails_observed INTEGER NOT NULL DEFAULT 0,
  distinct_offers INTEGER NOT NULL DEFAULT 0,
  avg_sequence_length NUMERIC(10, 2),
  primary_positioning TEXT,
  current_cta TEXT,
  latest_direction TEXT,
  profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  rebuilt_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS market_company_profiles_last_seen_idx
  ON market_company_profiles (last_seen_at DESC NULLS LAST);
