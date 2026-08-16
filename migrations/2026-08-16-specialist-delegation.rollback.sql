-- Rollback SPEC-098 specialist delegation contract
DROP INDEX IF EXISTS specialist_evaluations_tenant_delegation_idx;
DROP INDEX IF EXISTS specialist_evaluations_tenant_result_idx;
DROP TABLE IF EXISTS specialist_evaluations;

DROP INDEX IF EXISTS specialist_results_tenant_created_idx;
DROP INDEX IF EXISTS specialist_results_tenant_delegation_idx;
DROP TABLE IF EXISTS specialist_results;

DROP INDEX IF EXISTS specialist_delegations_tenant_specialist_idx;
DROP INDEX IF EXISTS specialist_delegations_tenant_status_idx;
DROP INDEX IF EXISTS specialist_delegations_tenant_created_idx;
DROP TABLE IF EXISTS specialist_delegations;
