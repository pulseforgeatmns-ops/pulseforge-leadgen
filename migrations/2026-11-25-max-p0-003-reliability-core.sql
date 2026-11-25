-- MAX-P0-003 — Reliability Core (operator migration; never startup DDL).
--
-- Additive only: three new tables, no change to existing tables or rows.
-- Idempotent (IF NOT EXISTS). Safe to run before the application deploy; the
-- application fails closed (refuses external dispatch and keyed requests) until
-- these tables exist.
--
-- Rollback (only before any row is written, or after an approved export):
--   DROP TABLE max_reliability_audit, max_request_receipts, max_external_action_claims;
--   DROP FUNCTION max_reliability_forbid_delete(), max_reliability_receipt_delete_guard();
-- Rows in max_external_action_claims are the duplicate-dispatch barrier for
-- irreversible actions: never truncate or expire them while the application runs.

BEGIN;

CREATE TABLE IF NOT EXISTS max_external_action_claims (
  tenant_id                 TEXT        NOT NULL,
  idempotency_key           TEXT        NOT NULL,
  action_kind               TEXT        NOT NULL,
  request_fingerprint       TEXT        NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
  attempt                   INTEGER     NOT NULL DEFAULT 1 CHECK (attempt >= 1),
  state                     TEXT        NOT NULL CHECK (state IN (
                              'claimed', 'dispatch_attempted', 'provider_acknowledged', 'verified',
                              'failed_definitive', 'outcome_unknown', 'abandoned_not_dispatched')),
  mission_id                TEXT,
  subject_ref               TEXT,
  provider                  TEXT,
  provider_ref              TEXT,
  provider_idempotency_key  TEXT,
  correlation_id            TEXT,
  owner_token               TEXT        NOT NULL,
  error_class               TEXT,
  claimed_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  dispatch_attempted_at     TIMESTAMPTZ,
  acknowledged_at           TIMESTAMPTZ,
  verified_at               TIMESTAMPTZ,
  resolved_at               TIMESTAMPTZ,
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idempotency_key),
  -- Every state that implies the provider may have been invoked records when
  -- the dispatch marker was committed.
  CHECK (state NOT IN ('dispatch_attempted', 'provider_acknowledged', 'verified', 'outcome_unknown')
         OR dispatch_attempted_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS max_external_action_claims_unresolved
  ON max_external_action_claims (tenant_id, state, updated_at)
  WHERE state IN ('claimed', 'dispatch_attempted', 'outcome_unknown');

CREATE INDEX IF NOT EXISTS max_external_action_claims_mission
  ON max_external_action_claims (tenant_id, mission_id);

CREATE TABLE IF NOT EXISTS max_request_receipts (
  tenant_id            TEXT        NOT NULL,
  scope                TEXT        NOT NULL,
  idempotency_key      TEXT        NOT NULL,
  request_fingerprint  TEXT        NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
  state                TEXT        NOT NULL CHECK (state IN ('in_progress', 'completed', 'failed', 'interrupted')),
  owner_token          TEXT        NOT NULL,
  correlation_id       TEXT,
  http_status          INTEGER,
  response             JSONB,
  error_code           TEXT,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at         TIMESTAMPTZ,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope, idempotency_key)
);

CREATE INDEX IF NOT EXISTS max_request_receipts_in_progress
  ON max_request_receipts (tenant_id, updated_at) WHERE state = 'in_progress';

CREATE TABLE IF NOT EXISTS max_reliability_audit (
  id                BIGSERIAL   PRIMARY KEY,
  tenant_id         TEXT        NOT NULL,
  correlation_id    TEXT,
  subject_kind      TEXT        NOT NULL,
  subject_key_hash  TEXT,
  event             TEXT        NOT NULL,
  from_state        TEXT,
  to_state          TEXT,
  detail            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS max_reliability_audit_tenant
  ON max_reliability_audit (tenant_id, id);
CREATE INDEX IF NOT EXISTS max_reliability_audit_correlation
  ON max_reliability_audit (tenant_id, correlation_id) WHERE correlation_id IS NOT NULL;

-- Durable barriers are append/transition-only. No retention job, cleanup cron
-- or ad-hoc statement may expire them: a missing claim or receipt would let an
-- irreversible action run again.
--   * claims: never deleted, never truncated;
--   * receipts: only an in_progress row may be deleted (the pre-execution
--     release after lock contention); completed/failed/interrupted are permanent;
--   * audit: never deleted, never truncated.
-- DROP TABLE (the documented rollback) is unaffected.
CREATE OR REPLACE FUNCTION max_reliability_forbid_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'MAX-P0-003: % on % is forbidden (durable idempotency barrier)', TG_OP, TG_TABLE_NAME
    USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE OR REPLACE FUNCTION max_reliability_receipt_delete_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state = 'in_progress' THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'MAX-P0-003: DELETE of a % receipt is forbidden (durable idempotency barrier)', OLD.state
    USING ERRCODE = 'insufficient_privilege';
END;
$$;

DROP TRIGGER IF EXISTS max_external_action_claims_no_delete ON max_external_action_claims;
CREATE TRIGGER max_external_action_claims_no_delete BEFORE DELETE ON max_external_action_claims
  FOR EACH ROW EXECUTE FUNCTION max_reliability_forbid_delete();
DROP TRIGGER IF EXISTS max_external_action_claims_no_truncate ON max_external_action_claims;
CREATE TRIGGER max_external_action_claims_no_truncate BEFORE TRUNCATE ON max_external_action_claims
  FOR EACH STATEMENT EXECUTE FUNCTION max_reliability_forbid_delete();

DROP TRIGGER IF EXISTS max_request_receipts_no_delete ON max_request_receipts;
CREATE TRIGGER max_request_receipts_no_delete BEFORE DELETE ON max_request_receipts
  FOR EACH ROW EXECUTE FUNCTION max_reliability_receipt_delete_guard();
DROP TRIGGER IF EXISTS max_request_receipts_no_truncate ON max_request_receipts;
CREATE TRIGGER max_request_receipts_no_truncate BEFORE TRUNCATE ON max_request_receipts
  FOR EACH STATEMENT EXECUTE FUNCTION max_reliability_forbid_delete();

DROP TRIGGER IF EXISTS max_reliability_audit_no_delete ON max_reliability_audit;
CREATE TRIGGER max_reliability_audit_no_delete BEFORE DELETE ON max_reliability_audit
  FOR EACH ROW EXECUTE FUNCTION max_reliability_forbid_delete();
DROP TRIGGER IF EXISTS max_reliability_audit_no_truncate ON max_reliability_audit;
CREATE TRIGGER max_reliability_audit_no_truncate BEFORE TRUNCATE ON max_reliability_audit
  FOR EACH STATEMENT EXECUTE FUNCTION max_reliability_forbid_delete();

COMMIT;
