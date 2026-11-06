-- Rollback for 2026-11-06-scout-tenant-search-memory-all-tenants.sql.
-- Fails (by design) if any non-10 tenant memory rows exist: those rows are
-- permanent purchase records and are never deleted to make a rollback pass.
BEGIN;
SET LOCAL lock_timeout='5s';
DROP INDEX IF EXISTS scout_tenant_checkpoint_query_lookup;
DROP INDEX IF EXISTS scout_tenant_page_root_lookup;
DROP INDEX IF EXISTS scout_tenant_member_place_lookup;
ALTER TABLE scout_tenant_query_owners DROP CONSTRAINT scout_tenant_query_owners_tenant_id_check;
ALTER TABLE scout_tenant_query_owners ADD CONSTRAINT scout_tenant_query_owners_tenant_id_check CHECK (tenant_id = '10');
ALTER TABLE scout_tenant_place_observations DROP CONSTRAINT scout_tenant_place_observations_tenant_id_check;
ALTER TABLE scout_tenant_place_observations ADD CONSTRAINT scout_tenant_place_observations_tenant_id_check CHECK (tenant_id = '10');
COMMIT;
