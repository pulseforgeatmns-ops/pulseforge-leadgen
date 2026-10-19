-- SPEC-263 PROPOSAL ONLY: separately approved manual extension after SPEC-262.
-- Never invoked by startup or runtime. Adds no tables, grants, rows or defaults.
-- Retains every consumption/quarantine row and its immutable triggers.
-- Only the closed tenant-10 recovery scope is affected; no global runtime pause.
-- Required explicit session attestations (not supplied by this file):
-- anchor_crm_preparation.tenant_10_gate_verified = 'closed'
-- anchor_crm_preparation.verified_source_revision = reviewed 40-hex source revision
-- anchor_crm_preparation.schema_approval_ref = actual operator approval reference
-- SQL cannot verify the deployed revision or worker drain; operator evidence is required.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
DO $$ BEGIN
  IF current_setting('anchor_crm_preparation.tenant_10_gate_verified',true) IS DISTINCT FROM 'closed'
    OR coalesce(current_setting('anchor_crm_preparation.verified_source_revision',true),'') !~ '^[a-f0-9]{40}$'
    OR length(btrim(coalesce(current_setting('anchor_crm_preparation.schema_approval_ref',true),'')))=0
  THEN RAISE EXCEPTION 'anchor_crm_preparation_schema_approval_required'; END IF;
END $$;
SELECT pg_advisory_xact_lock(hashtextextended('anchor-email-recovery:10',0));
SELECT singleton FROM anchor_email_recovery_control WHERE singleton=true FOR UPDATE;
DO $bridge$
DECLARE op text; counts text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM anchor_email_recovery_control WHERE singleton=true AND maintenance_armed=true AND manifest IS NULL)
  THEN RAISE EXCEPTION 'anchor_crm_preparation_tenant_10_closed_required'; END IF;
  -- Exact known constraint forms only; schema drift is a review blocker.
  SELECT pg_get_constraintdef(oid) INTO op FROM pg_constraint
    WHERE conrelid='anchor_scoped_resume_consumption'::regclass AND contype='c' AND convalidated
      AND conname='anchor_scoped_resume_consumption_check';
  SELECT pg_get_constraintdef(oid) INTO counts FROM pg_constraint
    WHERE conrelid='anchor_scoped_resume_consumption'::regclass AND contype='c' AND convalidated
      AND conname='anchor_scoped_resume_consumption_check1';
  IF op IS NOT DISTINCT FROM $expected$CHECK ((((capability = 'research'::text) AND (operation = 'research_existing_source'::text)) OR ((capability = 'preparation'::text) AND (operation = ANY (ARRAY['prepare_existing_crm'::text, 'generate_deterministic_existing_template'::text, 'freeze'::text, 'refill'::text, 'approve_canonical_envelope'::text])))))$expected$
    AND counts IS NOT DISTINCT FROM $expected$CHECK ((((operation = ANY (ARRAY['prepare_existing_crm'::text, 'freeze'::text, 'refill'::text])) AND (item_count = jsonb_array_length(identities))) OR ((operation <> ALL (ARRAY['prepare_existing_crm'::text, 'freeze'::text, 'refill'::text])) AND (item_count = 0))))$expected$ THEN RETURN; END IF;
  IF op IS DISTINCT FROM $expected$CHECK ((((capability = 'research'::text) AND (operation = 'research_existing_source'::text)) OR ((capability = 'preparation'::text) AND (operation = ANY (ARRAY['generate_deterministic_existing_template'::text, 'freeze'::text, 'refill'::text, 'approve_canonical_envelope'::text])))))$expected$
    OR counts IS DISTINCT FROM $expected$CHECK ((((operation = ANY (ARRAY['freeze'::text, 'refill'::text])) AND (item_count = jsonb_array_length(identities))) OR ((operation <> ALL (ARRAY['freeze'::text, 'refill'::text])) AND (item_count = 0))))$expected$
  THEN RAISE EXCEPTION 'anchor_crm_preparation_schema_drift'; END IF;
  ALTER TABLE anchor_scoped_resume_consumption
    DROP CONSTRAINT anchor_scoped_resume_consumption_check,
    DROP CONSTRAINT anchor_scoped_resume_consumption_check1,
    ADD CONSTRAINT anchor_scoped_resume_consumption_check CHECK (
      (capability='research' AND operation='research_existing_source') OR
      (capability='preparation' AND operation IN ('prepare_existing_crm','generate_deterministic_existing_template','freeze','refill','approve_canonical_envelope'))),
    ADD CONSTRAINT anchor_scoped_resume_consumption_check1 CHECK (
      (operation IN ('prepare_existing_crm','freeze','refill') AND item_count=jsonb_array_length(identities)) OR
      (operation NOT IN ('prepare_existing_crm','freeze','refill') AND item_count=0));
END $bridge$;
COMMIT;
