-- SPEC-SCOUT-P0-004: operator-approved additive migration, never startup DDL.
-- No existing grant/claim/mission/candidate/spend row is changed. In particular,
-- no authority or administrator approval is backfilled for historical grants.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

CREATE TABLE scout_research_cycle_releases (
  release_id text PRIMARY KEY CHECK (release_id ~ '^[a-f0-9]{64}$'),
  grant_id text NOT NULL REFERENCES scout_research_only_grants(grant_id),
  tenant_id text NOT NULL CHECK (tenant_id='10'),
  previous_allowance integer NOT NULL CHECK (previous_allowance BETWEEN 0 AND 7),
  released_cycles integer NOT NULL CHECK (released_cycles BETWEEN 1 AND 8),
  administrator_id text NOT NULL CHECK (administrator_id ~ '^[1-9][0-9]{0,18}$'),
  approver_session_hash text NOT NULL CHECK (approver_session_hash ~ '^[a-f0-9]{64}$'),
  approval_reference text NOT NULL UNIQUE CHECK (approval_reference ~ '^[a-zA-Z0-9_.:-]{1,120}$'),
  authorization_statement text NOT NULL CHECK (length(btrim(authorization_statement)) BETWEEN 1 AND 2000),
  kind text NOT NULL CHECK (kind IN ('initial_install','administrative')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (released_cycles=previous_allowance+1),
  CHECK ((previous_allowance=0)=(kind='initial_install')),
  UNIQUE(grant_id,released_cycles)
);

CREATE FUNCTION scout_research_release_forward() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE g scout_research_only_grants%ROWTYPE; allowance integer; consumed integer;
BEGIN
  NEW.created_at := clock_timestamp();
  -- Same row lock as the application claim/release paths, across all workers.
  SELECT * INTO g FROM scout_research_only_grants WHERE grant_id=NEW.grant_id FOR UPDATE;
  IF NOT FOUND OR g.tenant_id<>NEW.tenant_id OR g.revoked_at IS NOT NULL
    OR clock_timestamp()<g.not_before OR clock_timestamp()>=g.expires_at THEN
    RAISE EXCEPTION 'research_only_release_grant_inactive';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM users WHERE id::text=NEW.administrator_id AND role='admin' AND active=true
    AND email_verified=true AND password_change_required=false AND (client_id IS NULL OR client_id=10)) THEN
    RAISE EXCEPTION 'research_only_operator_invalid';
  END IF;
  SELECT COALESCE(max(released_cycles),0) INTO allowance FROM scout_research_cycle_releases WHERE grant_id=NEW.grant_id;
  IF NEW.previous_allowance<>allowance OR NEW.released_cycles<>allowance+1 OR NEW.released_cycles>g.max_cycles THEN
    RAISE EXCEPTION 'research_only_release_allowance_changed';
  END IF;
  IF NEW.kind='initial_install' THEN
    IF NEW.administrator_id<>g.operator_id OR NEW.approver_session_hash<>g.approver_session_hash
      OR NEW.approval_reference<>g.approval_ref
      OR NEW.authorization_statement IS DISTINCT FROM g.manifest->'authorizationEvidence'->>'statement'
      OR EXISTS(SELECT 1 FROM scout_research_only_cycles WHERE grant_id=NEW.grant_id) THEN
      RAISE EXCEPTION 'research_only_release_initial_invalid';
    END IF;
  ELSE
    SELECT count(*) INTO consumed FROM scout_research_only_cycles WHERE grant_id=NEW.grant_id;
    IF consumed<allowance OR EXISTS(SELECT 1 FROM scout_research_only_cycles WHERE grant_id=NEW.grant_id
      AND state='claimed' AND lease_expires_at>clock_timestamp()) THEN
      RAISE EXCEPTION 'research_only_release_not_due';
    END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE FUNCTION scout_research_cycle_released() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE g scout_research_only_grants%ROWTYPE; allowance integer; consumed integer;
BEGIN
  SELECT * INTO g FROM scout_research_only_grants WHERE grant_id=NEW.grant_id FOR UPDATE;
  IF NOT FOUND OR g.tenant_id<>NEW.tenant_id OR g.revoked_at IS NOT NULL
    OR clock_timestamp()<g.not_before OR clock_timestamp()>=g.expires_at THEN
    RAISE EXCEPTION 'research_only_grant_window_inactive';
  END IF;
  SELECT COALESCE(max(released_cycles),0) INTO allowance FROM scout_research_cycle_releases WHERE grant_id=NEW.grant_id;
  SELECT count(*) INTO consumed FROM scout_research_only_cycles WHERE grant_id=NEW.grant_id;
  IF consumed>=g.max_cycles THEN RAISE EXCEPTION 'research_only_cycles_exhausted'; END IF;
  IF consumed>=allowance OR NEW.cycle_number>allowance THEN RAISE EXCEPTION 'research_only_awaiting_cycle_release'; END IF;
  IF NEW.state<>'claimed' OR NEW.cycle_number<>consumed+1 THEN RAISE EXCEPTION 'research_only_cycle_sequence_invalid'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER scout_research_releases_no_delete BEFORE DELETE OR TRUNCATE ON scout_research_cycle_releases
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_releases_no_update BEFORE UPDATE ON scout_research_cycle_releases
  FOR EACH STATEMENT EXECUTE FUNCTION scout_research_only_permanent();
CREATE TRIGGER scout_research_releases_forward BEFORE INSERT ON scout_research_cycle_releases
  FOR EACH ROW EXECUTE FUNCTION scout_research_release_forward();
CREATE TRIGGER scout_research_cycles_release_guard BEFORE INSERT ON scout_research_only_cycles
  FOR EACH ROW EXECUTE FUNCTION scout_research_cycle_released();
COMMIT;
