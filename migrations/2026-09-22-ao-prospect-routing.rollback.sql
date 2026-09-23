BEGIN;

DROP TRIGGER IF EXISTS ao_cancel_suppressed_tasks ON prospects;
DROP FUNCTION IF EXISTS ao_cancel_suppressed_prospect_tasks();
DROP TABLE IF EXISTS ao_advisory_debriefs;
DROP TABLE IF EXISTS ao_prospect_tasks;

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_prospect_motion_check;
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_assignment_category_check;
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_advisory_stage_check;
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_next_action_check;

ALTER TABLE prospects
  DROP COLUMN IF EXISTS prospect_motion,
  DROP COLUMN IF EXISTS ao_fit_score,
  DROP COLUMN IF EXISTS ao_fit_reason,
  DROP COLUMN IF EXISTS assigned_ao_id,
  DROP COLUMN IF EXISTS ao_assignment_reason,
  DROP COLUMN IF EXISTS ao_assignment_category,
  DROP COLUMN IF EXISTS recommended_angle,
  DROP COLUMN IF EXISTS recommended_first_action,
  DROP COLUMN IF EXISTS advisory_stage,
  DROP COLUMN IF EXISTS last_debrief_status,
  DROP COLUMN IF EXISTS next_action,
  DROP COLUMN IF EXISTS next_action_owner;

-- next_action_due_at and prospects_next_action_idx belong to the pre-existing
-- Max orchestration migration and must survive this rollback.
DROP INDEX IF EXISTS ao_tasks_relationship_ao_routing_uidx;
DROP INDEX IF EXISTS prospects_client_id_id_ao_routing_uidx;
DROP INDEX IF EXISTS users_client_id_id_ao_routing_uidx;

COMMIT;
