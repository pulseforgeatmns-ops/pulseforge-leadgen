BEGIN;

ALTER TABLE warm_trigger_fires
  DROP CONSTRAINT IF EXISTS warm_trigger_fires_resolved_action_check;

ALTER TABLE warm_trigger_fires
  ADD CONSTRAINT warm_trigger_fires_resolved_action_check
  CHECK (
    resolved_action IS NULL
    OR resolved_action IN (
      'working_now',
      'today',
      'tomorrow',
      'auto_escalated',
      'closed_phantom_signal'
    )
  );

COMMIT;
