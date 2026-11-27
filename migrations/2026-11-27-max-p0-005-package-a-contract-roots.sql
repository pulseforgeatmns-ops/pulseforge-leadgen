-- MAX-P0-005 Package A.  Additive, operator-applied only; never startup DDL.
-- Preflight: existing canonical roots must be unique before this migration.
BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM max_durable_execution_tasks
    WHERE mission_id IS NULL AND objective_id IS NOT NULL AND (specialist='max_objective' OR execution_scope->>'role'='root')
    GROUP BY tenant_id, objective_id HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'MAX-P0-005 preflight failed: duplicate objective roots';
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS max_durable_execution_tasks_one_objective_root
  ON max_durable_execution_tasks (tenant_id, objective_id)
  WHERE mission_id IS NULL AND objective_id IS NOT NULL
    AND (specialist='max_objective' OR execution_scope->>'role'='root');

COMMIT;
