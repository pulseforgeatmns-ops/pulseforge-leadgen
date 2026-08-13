-- Rollback SPEC-095 operator_objectives
DROP INDEX IF EXISTS operator_objectives_client_status_idx;
DROP INDEX IF EXISTS operator_objectives_tenant_scope_idx;
DROP INDEX IF EXISTS operator_objectives_tenant_status_idx;
DROP TABLE IF EXISTS operator_objectives;
