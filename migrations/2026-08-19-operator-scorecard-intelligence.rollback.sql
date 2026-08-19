-- Rollback SPEC-116 Operator Scorecard Intelligence

DROP INDEX IF EXISTS operator_scorecard_learning_tenant_idx;
DROP TABLE IF EXISTS operator_scorecard_learning;
DROP INDEX IF EXISTS operator_scorecards_client_status_idx;
DROP INDEX IF EXISTS operator_scorecards_tenant_status_idx;
DROP TABLE IF EXISTS operator_scorecards;
