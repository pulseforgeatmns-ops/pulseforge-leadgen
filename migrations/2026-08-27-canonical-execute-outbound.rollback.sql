-- Rollback SPEC-071 outbound execution evidence table.

DROP INDEX IF EXISTS acquisition_mission_outbound_executions_prospect_idx;
DROP INDEX IF EXISTS acquisition_mission_outbound_executions_mission_idx;
DROP INDEX IF EXISTS acquisition_mission_outbound_executions_identity_sent_idx;
DROP TABLE IF EXISTS acquisition_mission_outbound_executions;
