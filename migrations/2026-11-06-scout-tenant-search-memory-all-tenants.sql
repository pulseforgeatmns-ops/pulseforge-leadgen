-- SPEC-SCOUT-P0-008. Separately approved operator migration; never startup DDL.
-- Lets a durable-path tenant other than 10 use tenant-wide search memory once
-- it is ALSO explicitly opted in (SCOUT_TENANT_DISCOVERY_MEMORY_TENANTS).
-- Applying this alone changes no runtime behavior: memory readiness for a
-- non-10 tenant fails closed until this runs, and stays off until opted in.
-- Rows stay immutable (existing triggers untouched); tenant isolation is the
-- (tenant_id, key) primary key plus tenant-scoped reads in the service.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
ALTER TABLE scout_tenant_query_owners DROP CONSTRAINT scout_tenant_query_owners_tenant_id_check;
ALTER TABLE scout_tenant_query_owners ADD CONSTRAINT scout_tenant_query_owners_tenant_id_check
  CHECK (tenant_id ~ '^[1-9][0-9]{0,9}$');
ALTER TABLE scout_tenant_place_observations DROP CONSTRAINT scout_tenant_place_observations_tenant_id_check;
ALTER TABLE scout_tenant_place_observations ADD CONSTRAINT scout_tenant_place_observations_tenant_id_check
  CHECK (tenant_id ~ '^[1-9][0-9]{0,9}$');
-- Same lookups as the Tenant 10 partial indexes, for every tenant.
CREATE INDEX IF NOT EXISTS scout_tenant_checkpoint_query_lookup
  ON scout_query_checkpoints(tenant_id,query_key,plan_version);
CREATE INDEX IF NOT EXISTS scout_tenant_page_root_lookup
  ON scout_query_pages(tenant_id,root_query_key);
CREATE INDEX IF NOT EXISTS scout_tenant_member_place_lookup
  ON scout_candidate_memberships(tenant_id,(raw_candidate->>'placeId'));
COMMIT;
