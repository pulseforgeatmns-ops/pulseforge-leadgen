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
  parent_task_id        UUID REFERENCES max_durable_execution_tasks(id),
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
  UNIQUE (tenant_id, logical_key)
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

COMMIT;
