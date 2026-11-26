-- MAX-P0-004 — Durable execution ownership (operator migration; never startup DDL).
--
-- Additive and backwards compatible. Run only after the MAX-P0-003 migration.
-- The application fails closed for durable delegation while this table is absent.
-- Rollback: redeploy prior code and retain these tables. Do not drop rows while
-- tasks or action-ledger claims may still require recovery/reconciliation.

BEGIN;

CREATE TABLE IF NOT EXISTS max_durable_execution_tasks (
  id                    UUID PRIMARY KEY,
  tenant_id             TEXT NOT NULL,
  objective_id          TEXT,
  mission_id            TEXT,
  parent_task_id        UUID,
  logical_key           TEXT NOT NULL,
  input_fingerprint     TEXT NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
  specialist            TEXT NOT NULL,
  execution_scope       JSONB NOT NULL DEFAULT '{}'::jsonb,
  immutable_input       JSONB NOT NULL DEFAULT '{}'::jsonb,
  dependency_refs       JSONB NOT NULL DEFAULT '[]'::jsonb,
  state                 TEXT NOT NULL CHECK (state IN (
                        'requested','queued','blocked_dependency','claimed','running',
                        'awaiting_verification','recovery_required','completed','failed',
                        'cancelled','escalated')),
  ownership_token       TEXT,
  fence_epoch           BIGINT NOT NULL DEFAULT 0 CHECK (fence_epoch >= 0),
  lease_expires_at      TIMESTAMPTZ,
  heartbeat_at          TIMESTAMPTZ,
  deadline_at           TIMESTAMPTZ,
  retry_budget          INTEGER NOT NULL DEFAULT 0 CHECK (retry_budget >= 0),
  recovery_attempts     INTEGER NOT NULL DEFAULT 0 CHECK (recovery_attempts >= 0),
  progress_checkpoint   JSONB,
  result_ref            JSONB,
  evidence_refs         JSONB NOT NULL DEFAULT '[]'::jsonb,
  recovery_reason       TEXT,
  cancellation_reason   TEXT,
  escalation_reason     TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  accepted_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at          TIMESTAMPTZ,
  UNIQUE (tenant_id, logical_key),
  UNIQUE (tenant_id, id),
  -- A parent is always a task of the SAME tenant (composite key), and never the
  -- task itself; parent_task_id is immutable (guard trigger), so no cycles.
  FOREIGN KEY (tenant_id, parent_task_id) REFERENCES max_durable_execution_tasks(tenant_id, id),
  CONSTRAINT max_durable_execution_tasks_not_own_parent CHECK (parent_task_id IS NULL OR parent_task_id <> id),
  -- An owner and a lease exist exactly while the task is claimed/running.
  CONSTRAINT max_durable_execution_tasks_owner_matches_state CHECK (
    (state IN ('claimed','running')) = (ownership_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
  CONSTRAINT max_durable_execution_tasks_retry_budget_bound CHECK (retry_budget <= 100)
);

CREATE INDEX IF NOT EXISTS max_durable_execution_tasks_recovery
  ON max_durable_execution_tasks (tenant_id, state, lease_expires_at, deadline_at);
CREATE INDEX IF NOT EXISTS max_durable_execution_tasks_mission
  ON max_durable_execution_tasks (tenant_id, mission_id, parent_task_id);
-- At most one active (claimed/running) task per mission per tenant.
CREATE UNIQUE INDEX IF NOT EXISTS max_durable_execution_tasks_one_active_per_mission
  ON max_durable_execution_tasks (tenant_id, mission_id)
  WHERE mission_id IS NOT NULL AND state IN ('claimed','running');

CREATE TABLE IF NOT EXISTS max_durable_execution_task_events (
  id                    BIGSERIAL PRIMARY KEY,
  task_id               UUID NOT NULL,
  tenant_id             TEXT NOT NULL,
  fence_epoch           BIGINT NOT NULL,
  event                 TEXT NOT NULL,
  from_state            TEXT,
  to_state              TEXT,
  detail                JSONB NOT NULL DEFAULT '{}'::jsonb,
  at                    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  -- An event can only describe a task of the same tenant.
  CONSTRAINT max_durable_execution_task_events_tenant_task_fkey
    FOREIGN KEY (tenant_id, task_id) REFERENCES max_durable_execution_tasks(tenant_id, id)
);
CREATE INDEX IF NOT EXISTS max_durable_execution_task_events_task
  ON max_durable_execution_task_events (tenant_id, task_id, id);

-- Link the P0-003 external-action safety barrier to the durable task. The
-- ledger is still authoritative for dispatch/reconciliation; this reference
-- makes incomplete effects visible to recovery and verification.
ALTER TABLE max_external_action_claims
  ADD COLUMN IF NOT EXISTS task_id UUID;
ALTER TABLE max_external_action_claims
  DROP CONSTRAINT IF EXISTS max_external_action_claims_task_id_fkey;
ALTER TABLE max_external_action_claims
  DROP CONSTRAINT IF EXISTS max_external_action_claims_tenant_task_fkey;
ALTER TABLE max_external_action_claims
  ADD CONSTRAINT max_external_action_claims_tenant_task_fkey
  FOREIGN KEY (tenant_id, task_id) REFERENCES max_durable_execution_tasks(tenant_id, id);
-- Once an action is attributed to a task it stays attributed to that task, so
-- an unresolved or delivered effect cannot be detached to let verification pass.
CREATE OR REPLACE FUNCTION max_external_action_claim_task_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  -- A never-delivered claim (abandoned_not_dispatched / failed_definitive) may
  -- be re-attributed only as part of a reclaim back to 'claimed'.
  IF OLD.task_id IS NOT NULL AND NEW.task_id IS DISTINCT FROM OLD.task_id
     AND NOT (NEW.task_id IS NOT NULL AND NEW.state = 'claimed'
              AND OLD.state IN ('abandoned_not_dispatched','failed_definitive')) THEN
    RAISE EXCEPTION 'MAX-P0-004: an external action task link is immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS max_external_action_claims_task_guard ON max_external_action_claims;
CREATE TRIGGER max_external_action_claims_task_guard BEFORE UPDATE OF task_id ON max_external_action_claims
  FOR EACH ROW EXECUTE FUNCTION max_external_action_claim_task_guard();
CREATE INDEX IF NOT EXISTS max_external_action_claims_task
  ON max_external_action_claims (tenant_id, task_id, state) WHERE task_id IS NOT NULL;

-- Identity, tenant, accepted input and parentage are immutable after
-- acceptance; the fence epoch never decreases. Enforced for direct database
-- callers too, not only the application store.
CREATE OR REPLACE FUNCTION max_durable_execution_task_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.id IS DISTINCT FROM OLD.id
     OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
     OR NEW.logical_key IS DISTINCT FROM OLD.logical_key
     OR NEW.input_fingerprint IS DISTINCT FROM OLD.input_fingerprint
     OR NEW.objective_id IS DISTINCT FROM OLD.objective_id
     OR NEW.mission_id IS DISTINCT FROM OLD.mission_id
     OR NEW.parent_task_id IS DISTINCT FROM OLD.parent_task_id
     OR NEW.specialist IS DISTINCT FROM OLD.specialist
     OR NEW.execution_scope IS DISTINCT FROM OLD.execution_scope
     OR NEW.immutable_input IS DISTINCT FROM OLD.immutable_input
     OR NEW.dependency_refs IS DISTINCT FROM OLD.dependency_refs
     OR NEW.deadline_at IS DISTINCT FROM OLD.deadline_at
     OR NEW.retry_budget IS DISTINCT FROM OLD.retry_budget
     OR NEW.created_at IS DISTINCT FROM OLD.created_at
     OR NEW.accepted_at IS DISTINCT FROM OLD.accepted_at THEN
    RAISE EXCEPTION 'MAX-P0-004: accepted task identity and input are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.fence_epoch < OLD.fence_epoch OR NEW.recovery_attempts < OLD.recovery_attempts THEN
    RAISE EXCEPTION 'MAX-P0-004: fence epoch and recovery attempts never decrease'
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.ownership_token IS NOT NULL AND NEW.ownership_token IS DISTINCT FROM OLD.ownership_token
     AND NEW.fence_epoch <= OLD.fence_epoch THEN
    RAISE EXCEPTION 'MAX-P0-004: a new owner requires a new fence epoch'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state IN ('completed','failed','cancelled','escalated') THEN
    RAISE EXCEPTION 'MAX-P0-004: terminal task rows are final'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS max_durable_execution_tasks_guard ON max_durable_execution_tasks;
CREATE TRIGGER max_durable_execution_tasks_guard BEFORE UPDATE ON max_durable_execution_tasks
  FOR EACH ROW EXECUTE FUNCTION max_durable_execution_task_guard();

-- Task events are append-only.
CREATE OR REPLACE FUNCTION max_durable_execution_task_event_immutable() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'MAX-P0-004: durable task events are append-only'
    USING ERRCODE = 'check_violation';
END $$;
DROP TRIGGER IF EXISTS max_durable_execution_task_events_no_update ON max_durable_execution_task_events;
CREATE TRIGGER max_durable_execution_task_events_no_update BEFORE UPDATE ON max_durable_execution_task_events
  FOR EACH ROW EXECUTE FUNCTION max_durable_execution_task_event_immutable();

-- P4: a keyed request receipt names the durable task it created, so a replay,
-- conflict or interruption reports the task's live state. Same tenant only
-- (composite key); linked once by the receipt owner, then immutable.
ALTER TABLE max_request_receipts
  ADD COLUMN IF NOT EXISTS task_id UUID;
ALTER TABLE max_request_receipts
  DROP CONSTRAINT IF EXISTS max_request_receipts_tenant_task_fkey;
ALTER TABLE max_request_receipts
  ADD CONSTRAINT max_request_receipts_tenant_task_fkey
  FOREIGN KEY (tenant_id, task_id) REFERENCES max_durable_execution_tasks(tenant_id, id);
CREATE OR REPLACE FUNCTION max_request_receipt_task_guard() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.task_id IS NOT NULL AND NEW.task_id IS DISTINCT FROM OLD.task_id THEN
    RAISE EXCEPTION 'MAX-P0-004: a request receipt task link is immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS max_request_receipts_task_guard ON max_request_receipts;
CREATE TRIGGER max_request_receipts_task_guard BEFORE UPDATE OF task_id ON max_request_receipts
  FOR EACH ROW EXECUTE FUNCTION max_request_receipt_task_guard();

-- Task state and event history are durable recovery evidence. P0-003 supplies
-- this function and is a required predecessor migration.
DROP TRIGGER IF EXISTS max_durable_execution_tasks_no_delete ON max_durable_execution_tasks;
CREATE TRIGGER max_durable_execution_tasks_no_delete BEFORE DELETE ON max_durable_execution_tasks
  FOR EACH ROW EXECUTE FUNCTION max_reliability_forbid_delete();
DROP TRIGGER IF EXISTS max_durable_execution_tasks_no_truncate ON max_durable_execution_tasks;
CREATE TRIGGER max_durable_execution_tasks_no_truncate BEFORE TRUNCATE ON max_durable_execution_tasks
  FOR EACH STATEMENT EXECUTE FUNCTION max_reliability_forbid_delete();
DROP TRIGGER IF EXISTS max_durable_execution_task_events_no_delete ON max_durable_execution_task_events;
CREATE TRIGGER max_durable_execution_task_events_no_delete BEFORE DELETE ON max_durable_execution_task_events
  FOR EACH ROW EXECUTE FUNCTION max_reliability_forbid_delete();
DROP TRIGGER IF EXISTS max_durable_execution_task_events_no_truncate ON max_durable_execution_task_events;
CREATE TRIGGER max_durable_execution_task_events_no_truncate BEFORE TRUNCATE ON max_durable_execution_task_events
  FOR EACH STATEMENT EXECUTE FUNCTION max_reliability_forbid_delete();

COMMIT;
