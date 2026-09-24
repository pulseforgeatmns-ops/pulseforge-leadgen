-- SPEC-AO-005: AO Daily Command Center — prospect update log

BEGIN;

CREATE TABLE IF NOT EXISTS ao_prospect_updates (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  ao_user_id TEXT,
  prospect_id TEXT NOT NULL,
  outcome_type TEXT NOT NULL CHECK (outcome_type IN (
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
    'other'
  )),
  notes TEXT,
  next_action TEXT,
  next_action_due_at TIMESTAMPTZ,
  advisory_stage TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_prospect_updates_tenant_ao_created
  ON ao_prospect_updates (tenant_id, ao_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ao_prospect_updates_prospect_created
  ON ao_prospect_updates (prospect_id, created_at DESC);

COMMIT;
