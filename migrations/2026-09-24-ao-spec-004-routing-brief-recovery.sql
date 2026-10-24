-- SPEC-AO-004: AO routing issue flags, recoverable conversations, prospect brief audit

BEGIN;

CREATE TABLE IF NOT EXISTS ao_routing_issue_flags (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  ao_user_id TEXT,
  session_id TEXT,
  conversation_id TEXT,
  message_id TEXT,
  prospect_id TEXT,
  mission_id TEXT,
  route_observed JSONB,
  route_expected TEXT,
  issue_type TEXT NOT NULL CHECK (issue_type IN (
    'wrong_route',
    'wrong_prospect',
    'wrong_mission',
    'lost_context',
    'should_have_opened_brief',
    'should_have_opened_conversation',
    'treated_as_done_incorrectly',
    'other'
  )),
  notes TEXT,
  decision_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_routing_flags_tenant_created
  ON ao_routing_issue_flags (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ao_routing_flags_session
  ON ao_routing_issue_flags (session_id)
  WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ao_routing_flags_decision
  ON ao_routing_issue_flags (decision_id)
  WHERE decision_id IS NOT NULL;

ALTER TABLE ao_max_sessions
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reopened_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS closed_by TEXT,
  ADD COLUMN IF NOT EXISTS reopened_by TEXT,
  ADD COLUMN IF NOT EXISTS prospect_id TEXT,
  ADD COLUMN IF NOT EXISTS mission_id TEXT;

UPDATE ao_max_sessions
SET status = CASE WHEN completed = true THEN 'done' ELSE 'active' END
WHERE status = 'active' AND completed = true;

ALTER TABLE ao_max_sessions DROP CONSTRAINT IF EXISTS ao_max_sessions_status_check;
ALTER TABLE ao_max_sessions ADD CONSTRAINT ao_max_sessions_status_check
  CHECK (status IN ('active', 'done', 'archived', 'closed', 'reopened'));

CREATE INDEX IF NOT EXISTS idx_ao_max_sessions_owner_status
  ON ao_max_sessions (ao_owner_id, client_id, status, updated_at DESC)
  WHERE mode = 'conversation';

COMMIT;
