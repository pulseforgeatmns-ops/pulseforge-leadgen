-- SPEC-114 Client Tenant Creation & Activation (v1)
-- Operator-provisioned tenant workspace. Additive only.
-- Rollback: migrations/2026-08-18-client-tenant-workspace.rollback.sql

ALTER TABLE clients ADD COLUMN IF NOT EXISTS primary_contact TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS industry TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS country TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS timezone TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS logo_url TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS notes TEXT;

CREATE TABLE IF NOT EXISTS tenant_workspaces (
  client_id INTEGER PRIMARY KEY REFERENCES clients(id),
  tenant_key TEXT NOT NULL UNIQUE,
  knowledge_namespace TEXT NOT NULL,
  mission_namespace TEXT NOT NULL,
  prospect_namespace TEXT NOT NULL,
  outcome_namespace TEXT NOT NULL,
  aim_namespace TEXT NOT NULL,
  platform_knowledge_isolated BOOLEAN NOT NULL DEFAULT TRUE,
  provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS tenant_workspaces_tenant_key_idx
  ON tenant_workspaces (tenant_key);
