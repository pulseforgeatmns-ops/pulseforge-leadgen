-- SPEC-252 rollback — drop tenant outreach scheduling table.

BEGIN;

DROP TABLE IF EXISTS tenant_outreach_scheduled_sends;

COMMIT;
