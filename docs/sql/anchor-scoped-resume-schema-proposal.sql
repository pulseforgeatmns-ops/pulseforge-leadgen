-- SPEC-262 PROPOSAL ONLY. Manual, separately approved shared-schema operation.
-- Never loaded by startup, ensureSchema, migrations, package scripts or runtime.
-- Only tenant 10 recovery gates must be deployed/closed and its workers drained.
-- Other tenants keep their existing runtime policy; no global pause is required.
-- SQL attestations cannot prove deployment/drain/approval; the operator must.
-- Required session settings (real approvals intentionally not supplied):
--   anchor_scoped_resume.tenant_10_gate_verified = 'closed'
--   anchor_scoped_resume.verified_source_revision = reviewed 40-hex revision
--   anchor_scoped_resume.schema_approval_ref = explicit shared-schema approval
-- If the review column is absent, additionally require:
--   anchor_scoped_resume.add_review_column = 'approved_nullable_no_default'
--
-- Shared footprint: prospects belongs to all tenants and email/SMS/call readers.
-- Adding its boolean leaves every old/new value NULL, with NO default/backfill.
-- Existing readers requiring literal false continue to reject unknown values.
-- Existing explicit true/false values and prior default/nullability are preserved.
-- Calls keep prior runtime semantics; no ao_call_suppressed column is added.
-- Old spreadsheet intake preflight still requires its original NOT NULL DEFAULT
-- false contract and intentionally rejects this narrower nullable/no-default DDL.
-- That compatibility blocker requires a separate reviewed intake change; do not
-- weaken its preflight or silently add defaults/backfill to pass it.
-- Review initialization is NOT SQL here: installScopedResume performs only an
-- explicitly approved identity/fingerprint-scoped NULL->false CAS and audit.
-- No grants are installed, no source becomes approval, no true hold is released.
-- The three SPEC-261 tables/triggers are not altered. Retain this audit/consumption
-- schema on rollback. No down SQL and no refund/reset/truncate path is provided.
BEGIN;
-- Bound shared-table lock acquisition and manual DDL execution.
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
DO $$ BEGIN
  IF current_setting('anchor_scoped_resume.tenant_10_gate_verified',true) IS DISTINCT FROM 'closed'
    OR coalesce(current_setting('anchor_scoped_resume.verified_source_revision',true),'') !~ '^[a-f0-9]{40}$'
    OR length(btrim(coalesce(current_setting('anchor_scoped_resume.schema_approval_ref',true),'')))=0
  THEN RAISE EXCEPTION 'anchor_scoped_resume_schema_approval_required'; END IF;
END $$;
SELECT pg_advisory_xact_lock(hashtextextended('anchor-email-recovery:10',0));
SELECT singleton FROM anchor_email_recovery_control WHERE singleton=true FOR UPDATE;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM anchor_email_recovery_control WHERE singleton=true AND maintenance_armed=true AND manifest IS NULL)
  THEN RAISE EXCEPTION 'anchor_scoped_resume_tenant_10_closed_required'; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='public.prospects'::regclass AND attname='ao_outreach_review_required' AND NOT attisdropped) THEN
    IF current_setting('anchor_scoped_resume.add_review_column',true) IS DISTINCT FROM 'approved_nullable_no_default'
    THEN RAISE EXCEPTION 'anchor_scoped_resume_review_column_approval_required'; END IF;
    ALTER TABLE public.prospects ADD COLUMN ao_outreach_review_required boolean NULL;
  ELSIF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='public.prospects'::regclass
    AND attname='ao_outreach_review_required' AND NOT attisdropped AND atttypid='boolean'::regtype AND attgenerated='') THEN
    RAISE EXCEPTION 'anchor_scoped_resume_review_column_incompatible';
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS anchor_scoped_resume_grants (
  manifest_id text PRIMARY KEY CHECK(btrim(manifest_id)<>''),
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  program_id text NOT NULL CHECK(btrim(program_id)<>''),
  manifest_hash text NOT NULL CHECK(manifest_hash ~ '^[a-f0-9]{64}$'),
  manifest jsonb NOT NULL CHECK(jsonb_typeof(manifest)='object' AND manifest->>'kind'='scoped_resume'
    AND manifest->>'id'=manifest_id AND manifest->>'tenantId'=tenant_id AND manifest->>'programId'=program_id),
  approval_ref text NOT NULL CHECK(btrim(approval_ref)<>''),
  operator_id text NOT NULL CHECK(btrim(operator_id)<>''),
  operator_role text NOT NULL CHECK(operator_role IN ('admin','manager')),
  review_initializations jsonb NOT NULL CHECK(jsonb_typeof(review_initializations)='array'),
  installed_at timestamptz NOT NULL,
  revoked_at timestamptz NULL,
  revoke_approval_ref text NULL,
  revoked_by text NULL,
  revoked_role text NULL,
  CHECK((revoked_at IS NULL AND revoke_approval_ref IS NULL AND revoked_by IS NULL AND revoked_role IS NULL)
    OR (revoked_at IS NOT NULL AND revoke_approval_ref IS NOT NULL AND revoked_by IS NOT NULL AND revoked_role IS NOT NULL
      AND length(btrim(revoke_approval_ref))>0 AND length(btrim(revoked_by))>0 AND revoked_role IN ('admin','manager')))
);
CREATE TABLE IF NOT EXISTS anchor_scoped_resume_consumption (
  operation_id text PRIMARY KEY CHECK(btrim(operation_id)<>''),
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  program_id text NOT NULL CHECK(btrim(program_id)<>''),
  manifest_id text NOT NULL REFERENCES anchor_scoped_resume_grants(manifest_id),
  capability text NOT NULL CHECK(capability IN ('research','preparation')),
  operation text NOT NULL CHECK((capability='research' AND operation='research_existing_source') OR
    (capability='preparation' AND operation IN ('generate_deterministic_existing_template','freeze','refill','approve_canonical_envelope'))),
  local_day date NOT NULL,
  item_count integer NOT NULL CHECK(item_count>=0 AND item_count<=100),
  identities jsonb NOT NULL CHECK(jsonb_typeof(identities)='array' AND jsonb_array_length(identities)>0),
  approval_ref text NOT NULL CHECK(btrim(approval_ref)<>''),
  consumed_at timestamptz NOT NULL,
  CHECK((operation IN ('freeze','refill') AND item_count=jsonb_array_length(identities))
    OR (operation NOT IN ('freeze','refill') AND item_count=0))
);
CREATE INDEX IF NOT EXISTS anchor_scoped_resume_consumption_caps ON anchor_scoped_resume_consumption(tenant_id,program_id,capability,local_day);
CREATE OR REPLACE FUNCTION anchor_scoped_resume_revoke_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW)-ARRAY['revoked_at','revoke_approval_ref','revoked_by','revoked_role'])
    IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['revoked_at','revoke_approval_ref','revoked_by','revoked_role'])
    OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL OR NEW.revoke_approval_ref IS NULL
    OR NEW.revoked_by IS NULL OR NEW.revoked_role IS NULL
  THEN RAISE EXCEPTION 'anchor_scoped_resume_permanent_grant'; END IF;
  RETURN NEW;
END $$;
DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_scoped_resume_grants_permanent' AND tgrelid='anchor_scoped_resume_grants'::regclass) THEN
    CREATE TRIGGER anchor_scoped_resume_grants_permanent BEFORE DELETE OR TRUNCATE ON anchor_scoped_resume_grants FOR EACH STATEMENT EXECUTE FUNCTION anchor_recovery_immutable();
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_scoped_resume_grant_revoke_once' AND tgrelid='anchor_scoped_resume_grants'::regclass) THEN
    CREATE TRIGGER anchor_scoped_resume_grant_revoke_once BEFORE UPDATE ON anchor_scoped_resume_grants FOR EACH ROW EXECUTE FUNCTION anchor_scoped_resume_revoke_once();
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_scoped_resume_consumption_permanent' AND tgrelid='anchor_scoped_resume_consumption'::regclass) THEN
    CREATE TRIGGER anchor_scoped_resume_consumption_permanent BEFORE UPDATE OR DELETE OR TRUNCATE ON anchor_scoped_resume_consumption FOR EACH STATEMENT EXECUTE FUNCTION anchor_recovery_immutable();
  END IF;
END $$;
COMMIT;
