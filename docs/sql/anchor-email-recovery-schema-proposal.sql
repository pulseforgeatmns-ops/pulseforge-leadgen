-- SPEC-261 PROPOSAL ONLY. This file is intentionally outside migrations/.
-- No server/bootstrap/ensureSchema, package script or migration runner imports it.
-- It must never be copied into an automatic migration path.
--
-- A separately authorized operator must first deploy the close-capable source,
-- arm ANCHOR_EMAIL_RECOVERY_MAINTENANCE=true, stop/drain older workers, and verify
-- all tenant-10 CRM email leaves and tenant-10 governed preparation are closed.
-- SPEC262 preserves non-Anchor and call/SMS legacy branches; this is not a global pause.
-- Set the three session attestations below only
-- after that verification. These SQL settings record operator assertions; SQL
-- cannot prove which binaries are currently deployed. Missing/open/unknown
-- evidence aborts before DDL. These are NOT supplied by application code.
--   anchor_email_recovery.maintenance_armed = 'true'
--   anchor_email_recovery.provider_gate_verified = 'closed'
--   anchor_email_recovery.verified_source_revision = reviewed 40-hex commit
--
-- No ALTER prospects; no review/call-flag defaults, backfills or row release.
-- The shared-table review policy requires a separate approval. Preflight must
-- report absent ao_outreach_review_required as blocking, with counts by tenant.
-- Rollback is operational: retain this latch/quarantine/reservation schema and
-- the close-capable provider gate, set manifest=NULL, keep maintenance armed.
-- Do not revert to older ungated or superseded broad-gate binaries. There is intentionally no down SQL.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
DO $$ BEGIN
  IF current_setting('anchor_email_recovery.maintenance_armed', true) IS DISTINCT FROM 'true'
     OR current_setting('anchor_email_recovery.provider_gate_verified', true) IS DISTINCT FROM 'closed'
     OR coalesce(current_setting('anchor_email_recovery.verified_source_revision', true), '') !~ '^[a-f0-9]{40}$'
  THEN RAISE EXCEPTION 'anchor_recovery_verified_closed_gate_required'; END IF;
END $$;
SELECT pg_advisory_xact_lock(hashtextextended('anchor-email-recovery:10',0));
CREATE TABLE IF NOT EXISTS anchor_email_recovery_control (
  singleton boolean PRIMARY KEY CHECK (singleton),
  maintenance_armed boolean NOT NULL CHECK (maintenance_armed),
  manifest jsonb NULL
);
INSERT INTO anchor_email_recovery_control(singleton,maintenance_armed,manifest)
VALUES(true,true,NULL) ON CONFLICT(singleton) DO NOTHING;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM anchor_email_recovery_control WHERE singleton=true AND maintenance_armed=true AND manifest IS NULL)
  THEN RAISE EXCEPTION 'anchor_recovery_maintenance_required'; END IF;
END $$;
CREATE TABLE IF NOT EXISTS anchor_email_quarantines (
  id text PRIMARY KEY,
  tenant_id text NOT NULL CHECK (tenant_id='10'),
  item_id text NOT NULL UNIQUE CHECK (btrim(item_id)<>''),
  envelope_id text NOT NULL CHECK (btrim(envelope_id)<>''),
  program_id text NOT NULL CHECK (btrim(program_id)<>''),
  mission_id text NOT NULL CHECK (btrim(mission_id)<>''),
  canonical_id text NOT NULL CHECK (btrim(canonical_id)<>''),
  attempt_token text NOT NULL CHECK (attempt_token ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{0,5}[1-9])?\+00$'),
  recipient_email text NOT NULL CHECK (recipient_email=lower(btrim(recipient_email)) AND position('@' in recipient_email)>1),
  prospect_id text NOT NULL CHECK (btrim(prospect_id)<>''),
  company_id text NOT NULL CHECK (btrim(company_id)<>''),
  candidate_id text NOT NULL CHECK (btrim(candidate_id)<>''),
  expected_revision text NOT NULL CHECK (btrim(expected_revision)<>''),
  company_name_key text NOT NULL CHECK (length(btrim(company_name_key))>=3),
  company_domain text NOT NULL CHECK (company_domain=lower(btrim(company_domain)) AND position('.' in company_domain)>1),
  expected_status text NOT NULL CHECK (expected_status IN ('attempted','uncertain')),
  expected_reason text NOT NULL CHECK (btrim(expected_reason)<>''),
  approval_ref text NOT NULL CHECK (btrim(approval_ref)<>''),
  decision_hash text NOT NULL UNIQUE CHECK (decision_hash ~ '^[a-f0-9]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (id='quarantine_'||decision_hash)
);
CREATE INDEX IF NOT EXISTS anchor_email_quarantines_recipient ON anchor_email_quarantines(tenant_id,recipient_email);
CREATE INDEX IF NOT EXISTS anchor_email_quarantines_prospect ON anchor_email_quarantines(tenant_id,prospect_id);
CREATE INDEX IF NOT EXISTS anchor_email_quarantines_company ON anchor_email_quarantines(tenant_id,company_id);
CREATE TABLE IF NOT EXISTS anchor_email_recovery_reservations (
  manifest_id text NOT NULL CHECK (btrim(manifest_id)<>''),
  item_id text NOT NULL UNIQUE CHECK (btrim(item_id)<>''),
  attempt_token text NOT NULL CHECK (attempt_token ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{0,5}[1-9])?\+00$'),
  prospect_id text NOT NULL CHECK (btrim(prospect_id)<>''),
  company_id text NOT NULL CHECK (btrim(company_id)<>''),
  recipient_email text NOT NULL CHECK (recipient_email=lower(btrim(recipient_email)) AND position('@' in recipient_email)>1),
  used_at timestamptz NOT NULL DEFAULT now(),
  dispatch_started_at timestamptz NULL,
  PRIMARY KEY(manifest_id,item_id)
);
CREATE OR REPLACE FUNCTION anchor_recovery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'anchor_recovery_permanent_record'; END $$;
CREATE OR REPLACE FUNCTION anchor_recovery_dispatch_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (NEW.manifest_id,NEW.item_id,NEW.attempt_token,NEW.prospect_id,NEW.company_id,NEW.recipient_email,NEW.used_at)
     IS DISTINCT FROM (OLD.manifest_id,OLD.item_id,OLD.attempt_token,OLD.prospect_id,OLD.company_id,OLD.recipient_email,OLD.used_at)
     OR OLD.dispatch_started_at IS NOT NULL OR NEW.dispatch_started_at IS NULL
  THEN RAISE EXCEPTION 'anchor_recovery_permanent_reservation'; END IF;
  RETURN NEW;
END $$;
DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_recovery_control_permanent' AND tgrelid='anchor_email_recovery_control'::regclass) THEN
    CREATE TRIGGER anchor_recovery_control_permanent BEFORE DELETE OR TRUNCATE ON anchor_email_recovery_control FOR EACH STATEMENT EXECUTE FUNCTION anchor_recovery_immutable();
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_quarantine_permanent' AND tgrelid='anchor_email_quarantines'::regclass) THEN
    CREATE TRIGGER anchor_quarantine_permanent BEFORE UPDATE OR DELETE OR TRUNCATE ON anchor_email_quarantines FOR EACH STATEMENT EXECUTE FUNCTION anchor_recovery_immutable();
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_reservation_permanent' AND tgrelid='anchor_email_recovery_reservations'::regclass) THEN
    CREATE TRIGGER anchor_reservation_permanent BEFORE DELETE OR TRUNCATE ON anchor_email_recovery_reservations FOR EACH STATEMENT EXECUTE FUNCTION anchor_recovery_immutable();
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='anchor_reservation_dispatch_once' AND tgrelid='anchor_email_recovery_reservations'::regclass) THEN
    CREATE TRIGGER anchor_reservation_dispatch_once BEFORE UPDATE ON anchor_email_recovery_reservations FOR EACH ROW EXECUTE FUNCTION anchor_recovery_dispatch_once();
  END IF;
END $$;
COMMIT;
