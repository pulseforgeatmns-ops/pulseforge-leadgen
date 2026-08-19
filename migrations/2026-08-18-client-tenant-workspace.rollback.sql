-- Rollback SPEC-114 tenant workspace provision.

DROP INDEX IF EXISTS tenant_workspaces_tenant_key_idx;
DROP TABLE IF EXISTS tenant_workspaces;

ALTER TABLE clients DROP COLUMN IF EXISTS notes;
ALTER TABLE clients DROP COLUMN IF EXISTS logo_url;
ALTER TABLE clients DROP COLUMN IF EXISTS timezone;
ALTER TABLE clients DROP COLUMN IF EXISTS country;
ALTER TABLE clients DROP COLUMN IF EXISTS industry;
ALTER TABLE clients DROP COLUMN IF EXISTS primary_contact;
