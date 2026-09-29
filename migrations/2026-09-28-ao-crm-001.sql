-- SPEC-AO-CRM-001: Durable AO prospect state + activity history

BEGIN;

SELECT pg_advisory_xact_lock(20260928, 1);

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS ao_current_status TEXT,
  ADD COLUMN IF NOT EXISTS opportunity_stage TEXT,
  ADD COLUMN IF NOT EXISTS ao_account_priority TEXT NOT NULL DEFAULT 'normal',
  ADD COLUMN IF NOT EXISTS ao_why_account_matters TEXT,
  ADD COLUMN IF NOT EXISTS ao_next_action TEXT,
  ADD COLUMN IF NOT EXISTS ao_last_touch_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS ao_last_touch_type TEXT,
  ADD COLUMN IF NOT EXISTS ao_last_outcome TEXT,
  ADD COLUMN IF NOT EXISTS help_requested BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS help_reason TEXT,
  ADD COLUMN IF NOT EXISTS help_requested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS help_resolved_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS help_manager_note TEXT,
  ADD COLUMN IF NOT EXISTS ao_disqualification_reason TEXT,
  ADD COLUMN IF NOT EXISTS ao_paused BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_current_status_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_ao_current_status_check
  CHECK (ao_current_status IS NULL OR ao_current_status IN (
    'researching', 'ready_to_call', 'call_attempted', 'contacted', 'gatekeeper_reached',
    'decision_maker_reached', 'follow_up_needed', 'warm', 'walkthrough_target',
    'walkthrough_booked', 'proposal_needed', 'proposal_sent', 'won', 'lost',
    'not_a_fit', 'dead'
  ));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_opportunity_stage_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_opportunity_stage_check
  CHECK (opportunity_stage IS NULL OR opportunity_stage IN (
    'prospect', 'qualified', 'engaged', 'walkthrough', 'proposal',
    'closed_won', 'closed_lost', 'disqualified'
  ));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_account_priority_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_ao_account_priority_check
  CHECK (ao_account_priority IN ('normal', 'high', 'warm'));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_next_action_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_ao_next_action_check
  CHECK (ao_next_action IS NULL OR ao_next_action IN (
    'research_contact', 'call', 'visit', 'email', 'follow_up', 'ask_jake',
    'book_walkthrough', 'send_information', 'prepare_proposal', 'check_back_later',
    'disqualify', 'no_action'
  ));

CREATE TABLE IF NOT EXISTS ao_prospect_activity (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  prospect_id UUID NOT NULL,
  tenant_id INTEGER NOT NULL REFERENCES clients(id),
  ao_id INTEGER REFERENCES users(id),
  activity_type TEXT NOT NULL CHECK (activity_type IN (
    'call', 'visit', 'email', 'note', 'status_change', 'follow_up_set',
    'help_requested', 'walkthrough_booked', 'proposal_sent', 'disqualified', 'outcome_logged'
  )),
  outcome TEXT,
  notes TEXT,
  previous_status TEXT,
  new_status TEXT,
  previous_next_action TEXT,
  new_next_action TEXT,
  next_action_date DATE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_prospect_activity_prospect_created
  ON ao_prospect_activity (prospect_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ao_prospect_activity_tenant_ao_created
  ON ao_prospect_activity (tenant_id, ao_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_prospects_ao_crm_owner
  ON prospects (client_id, assigned_ao_id, ao_current_status)
  WHERE assigned_ao_id IS NOT NULL;

ALTER TABLE ao_prospect_activity
  DROP CONSTRAINT IF EXISTS ao_activity_tenant_prospect_fkey,
  ADD CONSTRAINT ao_activity_tenant_prospect_fkey
    FOREIGN KEY (tenant_id, prospect_id) REFERENCES prospects(client_id, id);

-- Extend outcome log for CRM-facing outcomes (keeps backward compatibility)
ALTER TABLE ao_prospect_updates DROP CONSTRAINT IF EXISTS ao_prospect_updates_outcome_type_check;
ALTER TABLE ao_prospect_updates ADD CONSTRAINT ao_prospect_updates_outcome_type_check
  CHECK (outcome_type IN (
    'called_no_answer',
    'left_voicemail',
    'sent_email',
    'spoke_gatekeeper',
    'spoke_decision_maker',
    'booked_assessment',
    'not_interested',
    'not_fit',
    'follow_up_later',
    'needs_research',
    'other',
    'interested',
    'asked_to_follow_up',
    'bad_fit',
    'booked_walkthrough',
    'needs_jake'
  ));

COMMIT;
