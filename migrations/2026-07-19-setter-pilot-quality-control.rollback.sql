BEGIN;

-- Operational history is intentionally retained. Restoring the prior Pipeline
-- requires only this feature flag change and never a database rollback.
UPDATE clients
SET setter_pipeline_v2_enabled = false,
    setter_pipeline_v2_configured_at = NOW()
WHERE setter_pipeline_v2_enabled = true;

COMMIT;
