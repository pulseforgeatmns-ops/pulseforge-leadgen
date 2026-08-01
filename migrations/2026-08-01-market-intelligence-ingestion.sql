-- SPEC-061 Market Intelligence Ingestion (v0.1)
-- Ingest labeled Gmail marketing emails into a queryable raw-evidence corpus.
-- Additive only. Rollback: migrations/2026-08-01-market-intelligence-ingestion.rollback.sql
-- Does NOT touch CRM companies/prospects tables.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS market_companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  domain TEXT,
  name TEXT NOT NULL,
  is_unknown BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS market_companies_domain_uidx
  ON market_companies (LOWER(domain))
  WHERE domain IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS market_companies_unknown_singleton_uidx
  ON market_companies ((is_unknown))
  WHERE is_unknown = TRUE;

CREATE TABLE IF NOT EXISTS market_emails (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES market_companies(id) ON DELETE RESTRICT,
  gmail_id TEXT NOT NULL,
  thread_id TEXT,
  message_id TEXT,
  subject TEXT NOT NULL DEFAULT '',
  body_text TEXT NOT NULL DEFAULT '',
  body_html TEXT,
  from_name TEXT,
  from_email TEXT NOT NULL DEFAULT '',
  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  links JSONB NOT NULL DEFAULT '[]'::jsonb,
  attachments JSONB NOT NULL DEFAULT '[]'::jsonb,
  received_at TIMESTAMPTZ NOT NULL,
  sent_at TIMESTAMPTZ,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT market_emails_gmail_id_unique UNIQUE (gmail_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS market_emails_message_id_uidx
  ON market_emails (LOWER(message_id))
  WHERE message_id IS NOT NULL AND message_id <> '';

CREATE INDEX IF NOT EXISTS market_emails_company_received_idx
  ON market_emails (company_id, received_at ASC);

CREATE INDEX IF NOT EXISTS market_emails_received_idx
  ON market_emails (received_at DESC);

CREATE INDEX IF NOT EXISTS market_emails_from_email_idx
  ON market_emails (LOWER(from_email));

CREATE TABLE IF NOT EXISTS market_intel_sync_state (
  id TEXT PRIMARY KEY DEFAULT 'default',
  label TEXT NOT NULL DEFAULT 'MARKET_INTEL',
  days INTEGER NOT NULL DEFAULT 365,
  last_synced_at TIMESTAMPTZ,
  last_run_stats JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO market_companies (name, is_unknown)
SELECT 'Unknown Company', TRUE
WHERE NOT EXISTS (
  SELECT 1 FROM market_companies WHERE is_unknown = TRUE
);
