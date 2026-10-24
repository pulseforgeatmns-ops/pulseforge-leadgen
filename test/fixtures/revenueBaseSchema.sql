-- Production-faithful pre-migration base schema.
--
-- This fixture mirrors the ACTUAL production key shapes observed in the
-- Phase 1.6A restore (phase16a_revenue_restore_validation):
--   clients:   PRIMARY KEY (id)
--   companies: PRIMARY KEY (id) only — NO UNIQUE (client_id, id)
--   prospects: PRIMARY KEY (id), UNIQUE (email) — NO UNIQUE (client_id, id)
--   campaigns: DOES NOT EXIST
-- The 2026-07-21 production attempt failed (SQLSTATE 42830) because the old
-- fixture invented composite tenant keys and a campaigns table that
-- production does not have. Do not add them back; the phase1 revenue
-- migration provisions what it needs idempotently.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE clients (
  id INTEGER PRIMARY KEY,
  name TEXT,
  enabled_agents JSONB NOT NULL DEFAULT '["scout"]'::jsonb,
  autosend_enabled BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  name TEXT
);
CREATE TABLE prospects (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
  first_name TEXT, last_name TEXT,
  email TEXT UNIQUE,
  phone TEXT
);
CREATE TABLE touchpoints (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, prospect_id UUID,
  channel TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE agent_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER, prospect_id UUID,
  agent_name TEXT, ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), status TEXT
);
