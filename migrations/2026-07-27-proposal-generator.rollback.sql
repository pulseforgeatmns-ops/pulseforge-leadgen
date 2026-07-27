-- Rollback SPEC-027B proposal_versions

DROP INDEX IF EXISTS idx_proposal_versions_tenant_created;
DROP INDEX IF EXISTS idx_proposal_versions_client;
DROP INDEX IF EXISTS idx_proposal_versions_mission;
DROP INDEX IF EXISTS idx_proposal_versions_opportunity;
DROP TABLE IF EXISTS proposal_versions;
