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
  IF EXISTS (
    SELECT 1 FROM max_durable_execution_task_events
    WHERE event='objective_wake' AND detail ? 'wakeIdentity'
    GROUP BY tenant_id, task_id, detail->>'wakeIdentity' HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'MAX-P0-005 preflight failed: duplicate objective wakes';
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS max_durable_execution_tasks_one_objective_root
  ON max_durable_execution_tasks (tenant_id, objective_id)
  WHERE mission_id IS NULL AND objective_id IS NOT NULL
    AND (specialist='max_objective' OR execution_scope->>'role'='root');

CREATE UNIQUE INDEX IF NOT EXISTS max_durable_execution_task_events_one_objective_wake
  ON max_durable_execution_task_events (tenant_id, task_id, (detail->>'wakeIdentity'))
  WHERE event='objective_wake' AND detail ? 'wakeIdentity';

-- IF NOT EXISTS does not repair a same-named malformed index. Verify the
-- catalog definition and enforcement flags before committing, so callers can
-- treat these as durable invariants rather than index-name conventions.
DO $$
DECLARE root_ok boolean; wake_ok boolean;
BEGIN
  SELECT i.indisunique AND i.indisvalid AND i.indisready AND i.indnkeyatts=2
    AND i.indkey::text = format('%s %s',
      (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_tasks'::regclass AND attname='tenant_id' AND NOT attisdropped),
      (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_tasks'::regclass AND attname='objective_id' AND NOT attisdropped))
    AND regexp_replace(replace(lower(pg_get_expr(i.indpred, i.indrelid)), '::text', ''), '[^a-z0-9_]', '', 'g')
      = 'mission_idisnullandobjective_idisnotnullandspecialistmax_objectiveorexecutionscoperoleroot'
    INTO root_ok
  FROM pg_index i WHERE i.indexrelid=to_regclass('max_durable_execution_tasks_one_objective_root');

  SELECT i.indisunique AND i.indisvalid AND i.indisready AND i.indnkeyatts=3
    AND i.indkey::text = format('%s %s 0',
      (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_task_events'::regclass AND attname='tenant_id' AND NOT attisdropped),
      (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_task_events'::regclass AND attname='task_id' AND NOT attisdropped))
    AND regexp_replace(replace(lower(pg_get_expr(i.indexprs, i.indrelid)), '::text', ''), '[^a-z0-9_]', '', 'g') = 'detailwakeidentity'
    AND regexp_replace(replace(lower(pg_get_expr(i.indpred, i.indrelid)), '::text', ''), '[^a-z0-9_]', '', 'g') = 'eventobjective_wakeanddetailwakeidentity'
    INTO wake_ok
  FROM pg_index i WHERE i.indexrelid=to_regclass('max_durable_execution_task_events_one_objective_wake');

  IF root_ok IS DISTINCT FROM true THEN RAISE EXCEPTION 'MAX-P0-005 invariant failed: canonical objective-root index is missing or invalid'; END IF;
  IF wake_ok IS DISTINCT FROM true THEN RAISE EXCEPTION 'MAX-P0-005 invariant failed: objective-wake deduplication index is missing or invalid'; END IF;
END $$;

COMMIT;
