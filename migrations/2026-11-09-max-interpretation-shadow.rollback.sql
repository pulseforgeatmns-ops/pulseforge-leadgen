-- Rollback for 2026-11-09-max-interpretation-shadow.sql (operator-applied).
-- Prefer the flag rollback first: unset MAX_INTERPRETATION_SHADOW stops all
-- writes immediately and needs no DDL. Drop the table only when observations
-- are no longer wanted; this permanently deletes them. Nothing else reads it.
BEGIN;
DROP INDEX IF EXISTS max_interpretation_shadow_tenant_recent;
DROP TABLE IF EXISTS max_interpretation_shadow;
COMMIT;
