-- SPEC-115 Client Registration & Workspace Provisioning (v1)
-- Additive only. Rollback: migrations/2026-08-19-client-registration-workspace.rollback.sql

ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMPTZ;

UPDATE users SET email_verified = TRUE WHERE email_verified IS NULL;
ALTER TABLE users ALTER COLUMN email_verified SET DEFAULT FALSE;
ALTER TABLE users ALTER COLUMN email_verified SET NOT NULL;

ALTER TABLE clients ADD COLUMN IF NOT EXISTS team_size TEXT;

ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS origin TEXT;
ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS lifecycle TEXT;
ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS campaign_namespace TEXT;
ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS memory_namespace TEXT;

UPDATE tenant_workspaces
SET origin = COALESCE(origin, 'operator'),
    lifecycle = COALESCE(lifecycle, 'provisioned'),
    campaign_namespace = COALESCE(campaign_namespace, 'tenant:' || client_id::text || ':campaign'),
    memory_namespace = COALESCE(memory_namespace, 'tenant:' || client_id::text || ':memory');

CREATE TABLE IF NOT EXISTS account_verification_tokens (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id),
  token_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  used_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS account_verification_tokens_user_idx
  ON account_verification_tokens (user_id);
