-- Rollback SPEC-101 specialist result payload persistence.

ALTER TABLE specialist_results
  DROP COLUMN IF EXISTS payload;
