-- Rollback SPEC-131 Transactional Mission Execution audit

DROP INDEX IF EXISTS acquisition_mission_execution_audit_mission_idx;
DROP INDEX IF EXISTS acquisition_mission_execution_audit_txn_idx;
DROP TABLE IF EXISTS acquisition_mission_execution_audit;
