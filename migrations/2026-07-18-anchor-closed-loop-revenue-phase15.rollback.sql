-- Operational rollback for Revenue Phase 1.5. Preserve ledger and audit evidence.
BEGIN;

UPDATE revenue_feature_flags SET
  revenue_schema_enabled = FALSE,
  revenue_operator_reads_enabled = FALSE,
  revenue_operator_writes_enabled = FALSE,
  revenue_max_reads_enabled = FALSE,
  revenue_followup_recommendations_enabled = FALSE,
  updated_at = NOW(),
  updated_by = 'phase15-operational-rollback';

REVOKE UPDATE, DELETE, TRUNCATE ON revenue_events FROM PUBLIC;

COMMIT;
