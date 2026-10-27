-- LOCAL PROPOSAL: separately approved operator migration only; never startup DDL.
-- A new table only. No recovery/control/history/consumption row is changed.
-- Gate production separately: exact reviewed revision, closed Tenant10 controls,
-- catalog preflight, and explicit schema approval. No provider call or grant here.
-- Transaction/lock timeout makes failure atomic. Repeat after an uncertain result
-- only after catalog inspection: this script is deliberately create-once and an
-- existing relation causes a rollback, not replacement of an audit table.
-- Recovery: retain table and all audit rows; disable/revoke authority, never DROP,
-- TRUNCATE, delete history, reset budgets, or apply a destructive down migration.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
CREATE TABLE scout_bounded_research_grants (
  manifest_hash text PRIMARY KEY CHECK(manifest_hash ~ '^[a-f0-9]{64}$'),
  approval_ref text NOT NULL UNIQUE CHECK(approval_ref ~ '^[a-zA-Z0-9_.:-]{1,120}$'),
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  manifest jsonb NOT NULL CHECK(coalesce(jsonb_typeof(manifest)='object'
    AND manifest->>'version'='2' AND manifest->>'tenantId'=tenant_id
    AND manifest->>'approvalRef'=approval_ref
    AND manifest->'researchAuthority'->>'kind'='bounded_places_research'
    AND manifest->'researchAuthority'->>'provider'='google_places',false)),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  installed_at timestamptz,
  revoked_at timestamptz,
  revoke_approval_ref text,
  CHECK(installed_at IS NOT NULL OR revoked_at IS NOT NULL),
  CHECK((revoked_at IS NULL AND revoke_approval_ref IS NULL)
    OR (revoked_at IS NOT NULL AND revoke_approval_ref IS NOT NULL AND length(btrim(revoke_approval_ref))>0))
);
-- Expired grants must still be explicitly closed. A second concurrent authority
-- is not an implicit renewal of the first allowance.
CREATE UNIQUE INDEX scout_bounded_research_one_open_tenant
  ON scout_bounded_research_grants(tenant_id) WHERE revoked_at IS NULL;
CREATE FUNCTION scout_bounded_research_permanent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'bounded_research_permanent_record'; END $$;
CREATE FUNCTION scout_bounded_research_revoke_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW)-ARRAY['revoked_at','revoke_approval_ref'])
      IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['revoked_at','revoke_approval_ref'])
    OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
    OR NEW.revoke_approval_ref IS NULL OR length(btrim(NEW.revoke_approval_ref))=0
  THEN RAISE EXCEPTION 'bounded_research_permanent_grant'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER scout_bounded_research_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_bounded_research_grants
  FOR EACH STATEMENT EXECUTE FUNCTION scout_bounded_research_permanent();
CREATE TRIGGER scout_bounded_research_revoke_only
  BEFORE UPDATE ON scout_bounded_research_grants
  FOR EACH ROW EXECUTE FUNCTION scout_bounded_research_revoke_once();
COMMIT;
