-- Rollback SPEC-115 Client Registration & Workspace Provisioning

DROP TABLE IF EXISTS account_verification_tokens;

ALTER TABLE tenant_workspaces DROP COLUMN IF EXISTS memory_namespace;
ALTER TABLE tenant_workspaces DROP COLUMN IF EXISTS campaign_namespace;
ALTER TABLE tenant_workspaces DROP COLUMN IF EXISTS lifecycle;
ALTER TABLE tenant_workspaces DROP COLUMN IF EXISTS origin;

ALTER TABLE clients DROP COLUMN IF EXISTS team_size;

ALTER TABLE users DROP COLUMN IF EXISTS email_verified_at;
ALTER TABLE users DROP COLUMN IF EXISTS email_verified;
ALTER TABLE users DROP COLUMN IF EXISTS phone;
