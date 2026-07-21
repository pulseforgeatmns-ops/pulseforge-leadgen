CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE clients (
  id INTEGER PRIMARY KEY,
  name TEXT,
  enabled_agents JSONB NOT NULL DEFAULT '["scout"]'::jsonb,
  autosend_enabled BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id), name TEXT,
  UNIQUE (client_id,id)
);
CREATE TABLE prospects (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id),
  company_id UUID, first_name TEXT, last_name TEXT, email TEXT, phone TEXT,
  UNIQUE (client_id,id), FOREIGN KEY (client_id,company_id) REFERENCES companies(client_id,id)
);
CREATE TABLE campaigns (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id),
  UNIQUE (client_id,id)
);
CREATE TABLE touchpoints (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, prospect_id UUID,
  channel TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE agent_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER, prospect_id UUID,
  agent_name TEXT, ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), status TEXT
);
