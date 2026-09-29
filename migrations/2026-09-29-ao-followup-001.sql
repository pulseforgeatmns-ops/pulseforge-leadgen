-- SPEC-AO-FOLLOWUP-001: AO Follow-Up Composer drafts

BEGIN;

SELECT pg_advisory_xact_lock(20260929, 1);

CREATE TABLE IF NOT EXISTS ao_followup_drafts (
  id BIGSERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES clients(id),
  account_id UUID NOT NULL,
  assigned_ao_id INTEGER NOT NULL REFERENCES users(id),

  status TEXT NOT NULL,
  approval_path TEXT NOT NULL,
  approval_reason TEXT,

  recommended_followup_angle TEXT NOT NULL,
  subject_line TEXT,
  email_draft TEXT,
  alternate_short_note TEXT,
  next_action_after_send TEXT NOT NULL,

  input_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  doctrine_checks JSONB NOT NULL DEFAULT '{}'::jsonb,
  warnings JSONB NOT NULL DEFAULT '[]'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_followup_drafts_tenant_account
  ON ao_followup_drafts (tenant_id, account_id);

CREATE INDEX IF NOT EXISTS idx_ao_followup_drafts_ao_created
  ON ao_followup_drafts (assigned_ao_id, created_at DESC);

ALTER TABLE ao_followup_drafts
  DROP CONSTRAINT IF EXISTS ao_followup_drafts_tenant_account_fkey,
  ADD CONSTRAINT ao_followup_drafts_tenant_account_fkey
    FOREIGN KEY (tenant_id, account_id) REFERENCES prospects(client_id, id);

ALTER TABLE ao_prospect_activity DROP CONSTRAINT IF EXISTS ao_prospect_activity_activity_type_check;
ALTER TABLE ao_prospect_activity ADD CONSTRAINT ao_prospect_activity_activity_type_check
  CHECK (activity_type IN (
    'call', 'visit', 'email', 'note', 'status_change', 'follow_up_set',
    'help_requested', 'walkthrough_booked', 'proposal_sent', 'disqualified', 'outcome_logged',
    'followup_draft_created'
  ));

COMMIT;
