-- SPEC-SCOUT-P0-003 Governed research-only activation (Tenant 10).
-- LOCAL PROPOSAL: separately approved operator migration only; never startup DDL.
-- New tables only. No recovery/control/quarantine/envelope/mission/policy row is
-- read or changed here, and no research mission, grant, cycle, candidate or spend
-- row is created. acquisition_missions and acquisition_outbound_programs are not
-- touched: a research mission lives only in scout_research_missions, which no
-- mission-orchestration, preparation or outbound path reads.
-- Gate production separately: exact reviewed revision, closed Tenant 10 recovery
-- controls, catalog preflight and explicit schema approval.
-- Create-once: an existing relation aborts the transaction instead of replacing
-- an audit table. Lock/statement timeouts make failure atomic.
-- Rollback: see migrations/2026-10-28-scout-research-only-grants.rollback.sql.
-- Preferred recovery is to revoke the open grant and keep every audit row.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

-- Immutable, separately approved research-only mission (facility-buyer ICP).
-- It is discovery scope only and never an outbound/AMO mission.
CREATE TABLE scout_research_missions (
  id text PRIMARY KEY CHECK (id ~ '^mission_anchor_facility_research_[a-f0-9]{20}$'),
  tenant_id text NOT NULL CHECK (tenant_id = '10'),
  program_id text NOT NULL,
  predecessor_mission_id text NOT NULL,
  target_successor_mission_id text NOT NULL,
  approval_ref text NOT NULL UNIQUE CHECK (approval_ref ~ '^[a-zA-Z0-9_.:-]{1,120}$'),
  payload jsonb NOT NULL CHECK (coalesce(jsonb_typeof(payload) = 'object' AND payload->>'id' = id
    AND payload->>'tenantId' = tenant_id AND payload->>'researchOnly' = 'true' AND payload->>'outboundEligible' = 'false'
    AND payload->>'predecessorMissionId' = predecessor_mission_id
    AND payload->'structuredMission'->>'immutable' = 'true', false)),
  mission_hash text NOT NULL UNIQUE CHECK (mission_hash ~ '^[a-f0-9]{64}$'),
  targeting_hash text NOT NULL CHECK (targeting_hash ~ '^[a-f0-9]{64}$'),
  operator_id text NOT NULL CHECK (operator_id ~ '^[1-9][0-9]{0,18}$'),
  approver_session_hash text NOT NULL CHECK (approver_session_hash ~ '^[a-f0-9]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE scout_research_only_grants (
  grant_id text PRIMARY KEY CHECK (grant_id ~ '^[a-f0-9]{64}$'),
  approval_ref text NOT NULL UNIQUE CHECK (approval_ref ~ '^[a-zA-Z0-9_.:-]{1,120}$'),
  tenant_id text NOT NULL CHECK (tenant_id = '10'),
  program_id text NOT NULL,
  mission_id text NOT NULL,
  research_mission_id text NOT NULL REFERENCES scout_research_missions(id),
  search_fingerprint text NOT NULL CHECK (search_fingerprint ~ '^[a-f0-9]{64}$'),
  manifest jsonb NOT NULL CHECK (coalesce(jsonb_typeof(manifest) = 'object'
    AND manifest->>'version' = '2' AND manifest->>'kind' = 'scout_research_only'
    AND manifest->>'researchMissionId' = research_mission_id
    AND manifest->>'tenantId' = tenant_id AND manifest->>'approvalRef' = approval_ref
    AND manifest->>'programId' = program_id AND manifest->>'missionId' = mission_id
    AND manifest->>'fingerprint' = search_fingerprint
    AND manifest->'permissions' = '{"discovery":true,"enrichment":false,"verification":false,"preparation":false,"outbound":false}'::jsonb
    AND manifest->'providers' = '["google_places"]'::jsonb, false)),
  operator_id text NOT NULL CHECK (operator_id ~ '^[1-9][0-9]{0,18}$'),
  operator_role text NOT NULL CHECK (operator_role = 'admin'),
  approver_session_hash text NOT NULL CHECK (approver_session_hash ~ '^[a-f0-9]{64}$'),
  max_cycles integer NOT NULL CHECK (max_cycles BETWEEN 1 AND 8),
  daily_google_usd_micros integer NOT NULL CHECK (daily_google_usd_micros BETWEEN 1 AND 5000000),
  not_before timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  revoked_at timestamptz,
  revoke_approval_ref text,
  revoked_by text,
  CHECK (expires_at > not_before AND expires_at - not_before <= interval '7 days'),
  CHECK ((revoked_at IS NULL AND revoke_approval_ref IS NULL AND revoked_by IS NULL)
    OR (revoked_at IS NOT NULL AND length(btrim(coalesce(revoke_approval_ref, ''))) > 0
      AND length(btrim(coalesce(revoked_by, ''))) > 0))
);
-- One open authority at a time. An expired grant still has to be revoked
-- before a successor is installed: a new grant is never an implicit renewal.
CREATE UNIQUE INDEX scout_research_only_one_open_tenant
  ON scout_research_only_grants(tenant_id) WHERE revoked_at IS NULL;

CREATE TABLE scout_research_only_cycles (
  grant_id text NOT NULL REFERENCES scout_research_only_grants(grant_id),
  cycle_number integer NOT NULL CHECK (cycle_number BETWEEN 1 AND 8),
  tenant_id text NOT NULL CHECK (tenant_id = '10'),
  cycle_id text NOT NULL UNIQUE CHECK (length(cycle_id) BETWEEN 1 AND 200),
  execution_token uuid NOT NULL UNIQUE,
  state text NOT NULL DEFAULT 'claimed' CHECK (state IN ('claimed', 'completed', 'stopped', 'abandoned')),
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  lease_expires_at timestamptz NOT NULL,
  finished_at timestamptz,
  result jsonb,
  PRIMARY KEY (grant_id, cycle_number),
  CHECK ((state = 'claimed') = (finished_at IS NULL))
);
-- At most one live cycle per grant, even across workers and restarts.
CREATE UNIQUE INDEX scout_research_only_one_live_cycle
  ON scout_research_only_cycles(grant_id) WHERE state = 'claimed';

-- Tenant 10 Google ceiling. Each possible HTTP attempt is reserved here at the
-- priced upper bound before the shared meter is asked. Reservations are never
-- refunded or reset, including on error, timeout, retry or restart.
CREATE TABLE scout_research_google_spend (
  id uuid PRIMARY KEY,
  tenant_id text NOT NULL CHECK (tenant_id = '10'),
  grant_id text NOT NULL REFERENCES scout_research_only_grants(grant_id),
  cycle_number integer NOT NULL,
  usage_day date NOT NULL,
  operation text NOT NULL CHECK (operation ~ '^[a-z0-9_]{1,40}$'),
  reserved_usd_micros integer NOT NULL CHECK (reserved_usd_micros BETWEEN 1 AND 5000000),
  api_usage_id uuid UNIQUE,
  state text NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved', 'ok', 'error', 'uncertain')),
  http_status integer,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  FOREIGN KEY (grant_id, cycle_number) REFERENCES scout_research_only_cycles(grant_id, cycle_number)
);
CREATE INDEX scout_research_google_spend_day ON scout_research_google_spend(tenant_id, usage_day);

-- Research-only discoveries are retained here, never in scout_unenriched, so no
-- enrichment, inventory, preparation or send path can see them. Reconciliation
-- into an authorized mission's queue is a separate, one-time, explicit step.
CREATE TABLE scout_research_candidates (
  id bigserial PRIMARY KEY,
  tenant_id text NOT NULL CHECK (tenant_id = '10'),
  research_mission_id text NOT NULL REFERENCES scout_research_missions(id),
  grant_id text NOT NULL,
  cycle_number integer NOT NULL,
  company text NOT NULL CHECK (length(btrim(company)) BETWEEN 1 AND 1024),
  name_key text NOT NULL CHECK (length(name_key) BETWEEN 1 AND 1024),
  domain text NOT NULL CHECK (length(domain) BETWEEN 1 AND 512),
  website_url text,
  vertical text,
  location text,
  notes text,
  discovered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  reconciled_at timestamptz,
  reconciled_mission_id text,
  reconciled_queue_id bigint,
  reconcile_approval_ref text,
  FOREIGN KEY (grant_id, cycle_number) REFERENCES scout_research_only_cycles(grant_id, cycle_number),
  CHECK ((reconciled_at IS NULL) = (reconciled_mission_id IS NULL) AND (reconciled_at IS NULL) = (reconcile_approval_ref IS NULL))
);
CREATE UNIQUE INDEX scout_research_candidates_domain ON scout_research_candidates(tenant_id, lower(domain));
CREATE UNIQUE INDEX scout_research_candidates_name ON scout_research_candidates(tenant_id, name_key);

CREATE FUNCTION scout_research_only_permanent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'research_only_permanent_record'; END $$;
CREATE FUNCTION scout_research_only_revoke_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - ARRAY['revoked_at','revoke_approval_ref','revoked_by'])
      IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['revoked_at','revoke_approval_ref','revoked_by'])
    OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
  THEN RAISE EXCEPTION 'research_only_permanent_grant'; END IF;
  RETURN NEW;
END $$;
-- Cycles only move forward from claimed; consumption is never refunded.
CREATE FUNCTION scout_research_only_cycle_forward() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - ARRAY['state','finished_at','result'])
      IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state','finished_at','result'])
    OR OLD.state <> 'claimed' OR NEW.state = 'claimed'
  THEN RAISE EXCEPTION 'research_only_permanent_cycle'; END IF;
  RETURN NEW;
END $$;
-- A debit keeps its amount, day and identity; only its settlement is recorded.
CREATE FUNCTION scout_research_google_spend_settle_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - ARRAY['state','http_status','finished_at','api_usage_id'])
      IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state','http_status','finished_at','api_usage_id'])
    OR (OLD.api_usage_id IS NOT NULL AND NEW.api_usage_id IS DISTINCT FROM OLD.api_usage_id)
    OR (OLD.state <> 'reserved' AND (to_jsonb(NEW) - 'api_usage_id') IS DISTINCT FROM (to_jsonb(OLD) - 'api_usage_id'))
  THEN RAISE EXCEPTION 'research_google_spend_permanent'; END IF;
  RETURN NEW;
END $$;

-- A retained candidate keeps its identity; reconciliation is recorded once.
CREATE FUNCTION scout_research_candidates_reconcile_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - ARRAY['reconciled_at','reconciled_mission_id','reconciled_queue_id','reconcile_approval_ref'])
      IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['reconciled_at','reconciled_mission_id','reconciled_queue_id','reconcile_approval_ref'])
    OR OLD.reconciled_at IS NOT NULL OR NEW.reconciled_at IS NULL
  THEN RAISE EXCEPTION 'research_candidate_permanent'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER scout_research_missions_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_research_missions
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_missions_no_update
  BEFORE UPDATE ON scout_research_missions
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_candidates_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_research_candidates
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_candidates_reconcile
  BEFORE UPDATE ON scout_research_candidates
  FOR EACH ROW EXECUTE FUNCTION scout_research_candidates_reconcile_once();
CREATE TRIGGER scout_research_only_grants_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_research_only_grants
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_only_grants_revoke_only
  BEFORE UPDATE ON scout_research_only_grants
  FOR EACH ROW EXECUTE FUNCTION scout_research_only_revoke_once();
CREATE TRIGGER scout_research_only_cycles_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_research_only_cycles
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_only_cycles_forward_only
  BEFORE UPDATE ON scout_research_only_cycles
  FOR EACH ROW EXECUTE FUNCTION scout_research_only_cycle_forward();
CREATE TRIGGER scout_research_google_spend_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_research_google_spend
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_google_spend_settle
  BEFORE UPDATE ON scout_research_google_spend
  FOR EACH ROW EXECUTE FUNCTION scout_research_google_spend_settle_only();
COMMIT;
