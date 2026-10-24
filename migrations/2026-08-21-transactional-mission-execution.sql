-- SPEC-131 Transactional Mission Execution audit
-- Independent of mission state so rollback records do not mutate the mission.

CREATE TABLE IF NOT EXISTS acquisition_mission_execution_audit (
  id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  mission_id TEXT,
  tenant_id TEXT,
  mission_version INTEGER,
  specialist TEXT,
  stage TEXT,
  preconditions JSONB NOT NULL DEFAULT '{}'::jsonb,
  duration_ms INTEGER,
  commit_status TEXT NOT NULL,
  rollback_reason TEXT,
  error_class TEXT,
  exception TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_mission_execution_audit_txn_idx
  ON acquisition_mission_execution_audit (transaction_id);

CREATE INDEX IF NOT EXISTS acquisition_mission_execution_audit_mission_idx
  ON acquisition_mission_execution_audit (mission_id, at DESC);
