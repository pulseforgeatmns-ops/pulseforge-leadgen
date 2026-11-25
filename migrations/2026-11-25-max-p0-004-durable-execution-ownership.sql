-- MAX-P0-004 — Durable execution ownership (operator migration; never startup DDL).
--
-- Additive and backwards compatible. Run only after the MAX-P0-003 migration.
-- The application fails closed for durable delegation while this table is absent.
-- Rollback: redeploy prior code and retain this table. Do not drop rows while
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
  FOREIGN KEY (tenant_id, parent_task_id) REFERENCES max_durable_execution_tasks(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS max_durable_execution_tasks_recovery
  ON max_durable_execution_tasks (tenant_id, state, lease_expires_at, deadline_at);
CREATE INDEX IF NOT EXISTS max_durable_execution_tasks_mission
  ON max_durable_execution_tasks (tenant_id, mission_id, parent_task_id);

CREATE TABLE IF NOT EXISTS max_durable_execution_task_events (
  id                    BIGSERIAL PRIMARY KEY,
  task_id               UUID NOT NULL REFERENCES max_durable_execution_tasks(id),
  tenant_id             TEXT NOT NULL,
  fence_epoch           BIGINT NOT NULL,
  event                 TEXT NOT NULL,
  from_state            TEXT,
  to_state              TEXT,
  detail                JSONB NOT NULL DEFAULT '{}'::jsonb,
  at                    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
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
CREATE INDEX IF NOT EXISTS max_external_action_claims_task
  ON max_external_action_claims (tenant_id, task_id, state) WHERE task_id IS NOT NULL;

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
