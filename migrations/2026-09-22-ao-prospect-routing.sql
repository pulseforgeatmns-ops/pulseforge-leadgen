BEGIN;

SELECT pg_advisory_xact_lock(20260922, 696);

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS prospect_motion TEXT,
  ADD COLUMN IF NOT EXISTS ao_fit_score INTEGER,
  ADD COLUMN IF NOT EXISTS ao_fit_reason TEXT,
  ADD COLUMN IF NOT EXISTS assigned_ao_id INTEGER REFERENCES users(id),
  ADD COLUMN IF NOT EXISTS ao_assignment_reason TEXT,
  ADD COLUMN IF NOT EXISTS ao_assignment_category TEXT,
  ADD COLUMN IF NOT EXISTS recommended_angle TEXT,
  ADD COLUMN IF NOT EXISTS recommended_first_action TEXT,
  ADD COLUMN IF NOT EXISTS advisory_stage TEXT DEFAULT 'unassigned',
  ADD COLUMN IF NOT EXISTS last_debrief_status TEXT,
  ADD COLUMN IF NOT EXISTS next_action TEXT,
  ADD COLUMN IF NOT EXISTS next_action_owner TEXT,
  ADD COLUMN IF NOT EXISTS next_action_due_at TIMESTAMPTZ;

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_prospect_motion_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_prospect_motion_check
  CHECK (prospect_motion IS NULL OR prospect_motion IN ('EMAIL_LED', 'AO_LED', 'HYBRID', 'NURTURE', 'SUPPRESS'));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_assignment_category_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_ao_assignment_category_check
  CHECK (ao_assignment_category IS NULL OR ao_assignment_category IN (
    'UNFAIR_ADVANTAGE', 'HIGH_VALUE_ICP', 'ROUTE_CLUSTER',
    'WALK_IN_OPPORTUNITY', 'WARM_SIGNAL', 'FOLLOW_UP_REQUIRED'
  ));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_advisory_stage_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_advisory_stage_check
  CHECK (advisory_stage IS NULL OR advisory_stage IN (
    'unassigned', 'routed', 'tasked', 'in_progress', 'debrief_pending', 'debrief_complete', 'closed'
  ));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_next_action_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_next_action_check
  CHECK (next_action IS NULL OR next_action IN (
    'BOOK_ASSESSMENT', 'SEND_INFO', 'AO_FOLLOW_UP', 'JAKE_REVIEW', 'NURTURE', 'SUPPRESS', 'NEEDS_RESEARCH'
  ));

CREATE TABLE IF NOT EXISTS ao_prospect_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  prospect_id UUID NOT NULL,
  assigned_ao_id INTEGER NOT NULL,
  assignment_category TEXT NOT NULL
    CHECK (assignment_category IN (
      'UNFAIR_ADVANTAGE', 'HIGH_VALUE_ICP', 'ROUTE_CLUSTER',
      'WALK_IN_OPPORTUNITY', 'WARM_SIGNAL', 'FOLLOW_UP_REQUIRED'
    )),
  motion TEXT NOT NULL CHECK (motion IN ('EMAIL_LED', 'AO_LED', 'HYBRID', 'NURTURE')),
  priority TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN ('normal', 'high', 'warm')),
  account_name TEXT,
  segment TEXT,
  location TEXT,
  why_account_matters TEXT,
  recommended_angle TEXT,
  first_action TEXT,
  discovery_objective TEXT,
  suggested_opener TEXT,
  desired_next_outcome TEXT,
  required_log_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
  deadline DATE,
  required_debrief BOOLEAN NOT NULL DEFAULT true,
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'in_progress', 'completed', 'cancelled')),
  routing_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ao_advisory_debriefs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  prospect_id UUID NOT NULL,
  task_id UUID,
  ao_owner_id INTEGER NOT NULL,
  person_spoken_to TEXT,
  role TEXT,
  decision_maker TEXT,
  current_cleaning_solution TEXT,
  stated_context TEXT,
  problem_or_risk TEXT,
  opportunity_timing TEXT CHECK (opportunity_timing IS NULL OR opportunity_timing IN ('now', 'later', 'not_at_all')),
  opportunity_type TEXT,
  opportunity_strength TEXT CHECK (opportunity_strength IS NULL OR opportunity_strength IN ('strong', 'moderate', 'weak', 'unclear')),
  blocker TEXT,
  recommended_next_step TEXT,
  recommended_message TEXT,
  follow_up_due_at TIMESTAMPTZ,
  next_owner TEXT,
  prescribed_before_diagnosing BOOLEAN,
  real_reason_to_continue BOOLEAN,
  specific_dated_next_step BOOLEAN,
  next_action TEXT CHECK (next_action IS NULL OR next_action IN (
    'BOOK_ASSESSMENT', 'SEND_INFO', 'AO_FOLLOW_UP', 'JAKE_REVIEW', 'NURTURE', 'SUPPRESS', 'NEEDS_RESEARCH'
  )),
  coaching_feedback TEXT,
  debrief_quality TEXT CHECK (debrief_quality IS NULL OR debrief_quality IN ('complete', 'incomplete', 'weak')),
  evaluation JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prospects_ao_routing
  ON prospects(client_id, assigned_ao_id, prospect_motion)
  WHERE prospect_motion IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ao_prospect_tasks_owner_deadline
  ON ao_prospect_tasks(assigned_ao_id, client_id, deadline, status)
  WHERE status IN ('open', 'in_progress');

CREATE INDEX IF NOT EXISTS idx_ao_advisory_debriefs_prospect
  ON ao_advisory_debriefs(prospect_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ao_advisory_debriefs_owner
  ON ao_advisory_debriefs(ao_owner_id, client_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS users_client_id_id_ao_routing_uidx
  ON users(client_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS prospects_client_id_id_ao_routing_uidx
  ON prospects(client_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS ao_tasks_relationship_ao_routing_uidx
  ON ao_prospect_tasks(client_id, id, prospect_id, assigned_ao_id);

ALTER TABLE prospects
  DROP CONSTRAINT IF EXISTS prospects_tenant_assigned_ao_fkey,
  ADD CONSTRAINT prospects_tenant_assigned_ao_fkey
    FOREIGN KEY (client_id, assigned_ao_id) REFERENCES users(client_id, id);

WITH ranked AS (
  SELECT id, ROW_NUMBER() OVER (
    PARTITION BY client_id, prospect_id
    ORDER BY created_at DESC, id DESC
  ) AS position
  FROM ao_prospect_tasks
  WHERE status IN ('open', 'in_progress')
)
UPDATE ao_prospect_tasks t
SET status = 'cancelled', completed_at = NOW()
FROM ranked r
WHERE t.id = r.id AND r.position > 1;

CREATE UNIQUE INDEX IF NOT EXISTS ao_prospect_tasks_one_active_per_prospect
  ON ao_prospect_tasks(client_id, prospect_id)
  WHERE status IN ('open', 'in_progress');
CREATE UNIQUE INDEX IF NOT EXISTS ao_advisory_debriefs_one_per_task
  ON ao_advisory_debriefs(task_id)
  WHERE task_id IS NOT NULL;

UPDATE ao_prospect_tasks t
SET status = 'cancelled', completed_at = NOW()
FROM prospects p
WHERE p.client_id = t.client_id
  AND p.id = t.prospect_id
  AND t.status IN ('open', 'in_progress')
  AND (COALESCE(p.do_not_contact, false) = true OR p.prospect_motion = 'SUPPRESS');

CREATE OR REPLACE FUNCTION ao_cancel_suppressed_prospect_tasks()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF COALESCE(NEW.do_not_contact, false) = true OR NEW.prospect_motion = 'SUPPRESS' THEN
    UPDATE ao_prospect_tasks
    SET status = 'cancelled', completed_at = NOW()
    WHERE client_id = NEW.client_id
      AND prospect_id = NEW.id
      AND status IN ('open', 'in_progress');
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ao_cancel_suppressed_tasks ON prospects;
CREATE TRIGGER ao_cancel_suppressed_tasks
AFTER UPDATE OF do_not_contact, prospect_motion ON prospects
FOR EACH ROW EXECUTE FUNCTION ao_cancel_suppressed_prospect_tasks();

ALTER TABLE ao_prospect_tasks
  DROP CONSTRAINT IF EXISTS ao_tasks_tenant_prospect_fkey,
  DROP CONSTRAINT IF EXISTS ao_tasks_tenant_owner_fkey,
  ADD CONSTRAINT ao_tasks_tenant_prospect_fkey
    FOREIGN KEY (client_id, prospect_id) REFERENCES prospects(client_id, id),
  ADD CONSTRAINT ao_tasks_tenant_owner_fkey
    FOREIGN KEY (client_id, assigned_ao_id) REFERENCES users(client_id, id);

ALTER TABLE ao_advisory_debriefs
  DROP CONSTRAINT IF EXISTS ao_debriefs_tenant_prospect_fkey,
  DROP CONSTRAINT IF EXISTS ao_debriefs_tenant_owner_fkey,
  DROP CONSTRAINT IF EXISTS ao_debriefs_task_relationship_fkey,
  ADD CONSTRAINT ao_debriefs_tenant_prospect_fkey
    FOREIGN KEY (client_id, prospect_id) REFERENCES prospects(client_id, id),
  ADD CONSTRAINT ao_debriefs_tenant_owner_fkey
    FOREIGN KEY (client_id, ao_owner_id) REFERENCES users(client_id, id),
  ADD CONSTRAINT ao_debriefs_task_relationship_fkey
    FOREIGN KEY (client_id, task_id, prospect_id, ao_owner_id)
    REFERENCES ao_prospect_tasks(client_id, id, prospect_id, assigned_ao_id);

COMMIT;
