-- LOCAL PROPOSAL: separately approved operator migration only; never startup DDL.
-- New tables only. No recovery/control/quarantine/ticket/checkpoint row is changed.
-- Authorized Scout backlog recovery: a one-shot, operator-installed authority that
-- re-processes already persisted Tenant 10 candidate-work tickets through the
-- existing claim/finish/ownership/admission path. It grants no discovery, Places,
-- enrichment, verification, preparation or email rights.
-- Gate production separately: exact reviewed revision, closed Tenant 10 controls,
-- catalog preflight, and explicit schema approval. No grant is installed here.
-- Create-once: an existing relation rolls the whole script back.
-- Recovery: retain tables and audit rows; revoke authority, never DROP, TRUNCATE,
-- delete history or apply a destructive down migration.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
CREATE TABLE scout_backlog_recovery_grants (
  manifest_hash text PRIMARY KEY CHECK(manifest_hash ~ '^[a-f0-9]{64}$'),
  approval_ref text NOT NULL UNIQUE CHECK(approval_ref ~ '^[a-zA-Z0-9_.:-]{1,120}$'),
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  search_fingerprint text NOT NULL CHECK(search_fingerprint ~ '^[a-f0-9]{64}$'),
  max_candidates integer NOT NULL CHECK(max_candidates BETWEEN 1 AND 20),
  manifest jsonb NOT NULL CHECK(coalesce(jsonb_typeof(manifest)='object'
    AND manifest->>'version'='1' AND manifest->>'kind'='scout_backlog_recovery'
    AND manifest->>'tenantId'=tenant_id AND manifest->>'approvalRef'=approval_ref
    AND manifest->>'fingerprint'=search_fingerprint
    AND (manifest->>'maxCandidates')::int=max_candidates
    AND jsonb_array_length(manifest->'candidates')=max_candidates,false)),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  installed_at timestamptz,
  revoked_at timestamptz,
  revoke_approval_ref text,
  CHECK(installed_at IS NOT NULL OR revoked_at IS NOT NULL),
  CHECK((revoked_at IS NULL AND revoke_approval_ref IS NULL)
    OR (revoked_at IS NOT NULL AND revoke_approval_ref IS NOT NULL AND length(btrim(revoke_approval_ref))>0)),
  UNIQUE(approval_ref,manifest_hash)
);
-- Expired grants must still be explicitly closed before another is installed.
CREATE UNIQUE INDEX scout_backlog_recovery_one_open_tenant
  ON scout_backlog_recovery_grants(tenant_id) WHERE revoked_at IS NULL;

-- Exactly one consumption per approval. The row is inserted in the same
-- transaction as the ticket claims, so consumption and claim commit together.
CREATE TABLE scout_backlog_recovery_runs (
  approval_ref text PRIMARY KEY,
  manifest_hash text NOT NULL UNIQUE,
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  search_fingerprint text NOT NULL,
  dependency_key text NOT NULL,
  execution_token uuid NOT NULL,
  state text NOT NULL DEFAULT 'claimed' CHECK(state IN ('claimed','completed','stopped')),
  stop_reason text CHECK(stop_reason IS NULL OR stop_reason ~ '^[a-z0-9_]{1,120}$'),
  result jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  CHECK((state='claimed') = (finished_at IS NULL)),
  CHECK(state<>'stopped' OR stop_reason IS NOT NULL),
  FOREIGN KEY(approval_ref,manifest_hash) REFERENCES scout_backlog_recovery_grants(approval_ref,manifest_hash)
);
CREATE UNIQUE INDEX scout_backlog_recovery_one_inflight_tenant
  ON scout_backlog_recovery_runs(tenant_id) WHERE state='claimed';

-- Per-candidate disposition at consumption time. Each claimed row references the
-- existing persisted candidate-work ticket; nothing here creates candidate work.
CREATE TABLE scout_backlog_recovery_items (
  approval_ref text NOT NULL REFERENCES scout_backlog_recovery_runs(approval_ref),
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  search_fingerprint text NOT NULL,
  plan_version text NOT NULL,
  ticket_version text NOT NULL,
  candidate_key text NOT NULL CHECK(candidate_key ~ '^[a-f0-9]{64}$'),
  disposition text NOT NULL CHECK(disposition IN ('claimed','skipped')),
  skip_reason text,
  claim_token uuid,
  membership_revision bigint,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(approval_ref,candidate_key),
  CHECK((disposition='claimed' AND claim_token IS NOT NULL AND membership_revision IS NOT NULL AND skip_reason IS NULL)
    OR (disposition='skipped' AND claim_token IS NULL AND skip_reason ~ '^[a-z0-9_]{1,120}$'))
);

CREATE FUNCTION scout_backlog_recovery_permanent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'scout_backlog_recovery_permanent_record'; END $$;
CREATE FUNCTION scout_backlog_recovery_grant_revoke_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW)-ARRAY['revoked_at','revoke_approval_ref'])
      IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['revoked_at','revoke_approval_ref'])
    OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
    OR NEW.revoke_approval_ref IS NULL OR length(btrim(NEW.revoke_approval_ref))=0
  THEN RAISE EXCEPTION 'scout_backlog_recovery_permanent_grant'; END IF;
  RETURN NEW;
END $$;
CREATE FUNCTION scout_backlog_recovery_run_finish_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW)-ARRAY['state','stop_reason','result','finished_at'])
      IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','stop_reason','result','finished_at'])
    OR OLD.state<>'claimed' OR NEW.state NOT IN ('completed','stopped')
  THEN RAISE EXCEPTION 'scout_backlog_recovery_permanent_run'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER scout_backlog_recovery_grants_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_backlog_recovery_grants
  FOR EACH STATEMENT EXECUTE FUNCTION scout_backlog_recovery_permanent();
CREATE TRIGGER scout_backlog_recovery_grants_revoke_only
  BEFORE UPDATE ON scout_backlog_recovery_grants
  FOR EACH ROW EXECUTE FUNCTION scout_backlog_recovery_grant_revoke_once();
CREATE TRIGGER scout_backlog_recovery_runs_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_backlog_recovery_runs
  FOR EACH STATEMENT EXECUTE FUNCTION scout_backlog_recovery_permanent();
CREATE TRIGGER scout_backlog_recovery_runs_finish_only
  BEFORE UPDATE ON scout_backlog_recovery_runs
  FOR EACH ROW EXECUTE FUNCTION scout_backlog_recovery_run_finish_once();
CREATE TRIGGER scout_backlog_recovery_items_immutable
  BEFORE UPDATE OR DELETE OR TRUNCATE ON scout_backlog_recovery_items
  FOR EACH STATEMENT EXECUTE FUNCTION scout_backlog_recovery_permanent();
COMMIT;
