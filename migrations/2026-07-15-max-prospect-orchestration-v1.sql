-- Max Prospect Orchestration V1, Phase 1.
-- Schema and immutable audit records only. This migration does not backfill,
-- send outreach, alter sequence enrollment, run enrichment, or create tasks.

BEGIN;

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS max_orchestration_config JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS lifecycle_state TEXT,
  ADD COLUMN IF NOT EXISTS previous_lifecycle_state TEXT,
  ADD COLUMN IF NOT EXISTS state_changed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS warmth_score INTEGER,
  ADD COLUMN IF NOT EXISTS warmth_score_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS warmth_score_version TEXT,
  ADD COLUMN IF NOT EXISTS state_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS state_reason_summary TEXT,
  ADD COLUMN IF NOT EXISTS next_best_action TEXT,
  ADD COLUMN IF NOT EXISTS next_action_due_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_action_status TEXT,
  ADD COLUMN IF NOT EXISTS operator_required BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS operator_priority TEXT,
  ADD COLUMN IF NOT EXISTS operator_reason TEXT,
  ADD COLUMN IF NOT EXISTS last_meaningful_signal_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_human_open_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_reply_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_positive_reply_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS recycle_eligible_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS recycle_reason TEXT,
  ADD COLUMN IF NOT EXISTS active_sequence_type TEXT,
  ADD COLUMN IF NOT EXISTS active_sequence_id TEXT,
  ADD COLUMN IF NOT EXISTS downgrade_candidate_since TIMESTAMPTZ;

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_lifecycle_state_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_lifecycle_state_check CHECK (
  lifecycle_state IS NULL OR lifecycle_state IN (
    'cold', 'heating', 'warm', 'hot', 'engaged', 'nurture', 'recycle', 'disqualified', 'null'
  )
);
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_previous_lifecycle_state_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_previous_lifecycle_state_check CHECK (
  previous_lifecycle_state IS NULL OR previous_lifecycle_state IN (
    'cold', 'heating', 'warm', 'hot', 'engaged', 'nurture', 'recycle', 'disqualified', 'null'
  )
);
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_warmth_score_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_warmth_score_check CHECK (
  warmth_score IS NULL OR warmth_score BETWEEN 0 AND 100
);
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_next_action_status_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_next_action_status_check CHECK (
  next_action_status IS NULL OR next_action_status IN ('pending', 'executing', 'completed', 'failed', 'cancelled')
);
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_operator_priority_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_operator_priority_check CHECK (
  operator_priority IS NULL OR operator_priority IN ('low', 'normal', 'high', 'urgent')
);
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_active_sequence_type_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_active_sequence_type_check CHECK (
  active_sequence_type IS NULL OR active_sequence_type IN ('cold', 'warm', 'nurture', 'none')
);

CREATE INDEX IF NOT EXISTS prospects_lifecycle_state_idx
  ON prospects (client_id, lifecycle_state, warmth_score DESC);
CREATE INDEX IF NOT EXISTS prospects_next_action_idx
  ON prospects (client_id, next_action_status, next_action_due_at)
  WHERE next_action_status = 'pending';

CREATE TABLE IF NOT EXISTS prospect_signal_events (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  company_id UUID REFERENCES companies(id),
  event_type TEXT NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  source_record_id TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, source, source_record_id)
);
CREATE INDEX IF NOT EXISTS prospect_signal_events_prospect_time_idx
  ON prospect_signal_events (client_id, prospect_id, event_timestamp DESC);

CREATE TABLE IF NOT EXISTS max_decisions (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  company_id UUID REFERENCES companies(id),
  trigger_event_type TEXT,
  trigger_event_id TEXT,
  idempotency_key TEXT NOT NULL,
  decision_version TEXT NOT NULL,
  score_version TEXT NOT NULL,
  current_state TEXT NOT NULL,
  recommended_state TEXT NOT NULL,
  warmth_score INTEGER NOT NULL CHECK (warmth_score BETWEEN 0 AND 100),
  score_components JSONB NOT NULL DEFAULT '[]'::jsonb,
  reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  reason_summary TEXT NOT NULL,
  next_best_action TEXT,
  actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  operator_required BOOLEAN NOT NULL DEFAULT FALSE,
  operator_priority TEXT,
  is_shadow BOOLEAN NOT NULL DEFAULT TRUE,
  config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  processing_duration_ms INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS max_decisions_prospect_created_idx
  ON max_decisions (client_id, prospect_id, created_at DESC);

CREATE TABLE IF NOT EXISTS prospect_state_transitions (
  id BIGSERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  decision_id TEXT REFERENCES max_decisions(id),
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  warmth_score INTEGER CHECK (warmth_score BETWEEN 0 AND 100),
  reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  reason_summary TEXT NOT NULL,
  trigger_event_type TEXT,
  trigger_event_id TEXT,
  decision_source TEXT NOT NULL CHECK (decision_source IN ('max_autonomous', 'operator_manual', 'system_rule', 'migration')),
  action_selected TEXT,
  operator_required BOOLEAN NOT NULL DEFAULT FALSE,
  is_shadow BOOLEAN NOT NULL DEFAULT FALSE,
  applied BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (decision_id)
);
CREATE INDEX IF NOT EXISTS prospect_state_transitions_funnel_idx
  ON prospect_state_transitions (client_id, from_state, to_state, created_at DESC);

CREATE TABLE IF NOT EXISTS max_actions (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  decision_id TEXT NOT NULL REFERENCES max_decisions(id),
  action_type TEXT NOT NULL,
  action_status TEXT NOT NULL CHECK (action_status IN ('queued', 'executing', 'completed', 'failed', 'cancelled', 'skipped')),
  autonomy_level TEXT NOT NULL CHECK (autonomy_level IN ('autonomous', 'guardrailed_autonomous', 'operator_required', 'prohibited')),
  idempotency_key TEXT NOT NULL,
  input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_code TEXT,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS max_actions_decision_idx ON max_actions (decision_id, created_at);
CREATE INDEX IF NOT EXISTS max_actions_status_idx ON max_actions (client_id, action_status, created_at DESC);

CREATE OR REPLACE FUNCTION reject_max_orchestration_append_only_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is append-only; % is not permitted', TG_TABLE_NAME, TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS prospect_signal_events_append_only ON prospect_signal_events;
CREATE TRIGGER prospect_signal_events_append_only
  BEFORE UPDATE OR DELETE ON prospect_signal_events
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();
DROP TRIGGER IF EXISTS max_decisions_append_only ON max_decisions;
CREATE TRIGGER max_decisions_append_only
  BEFORE UPDATE OR DELETE ON max_decisions
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();
DROP TRIGGER IF EXISTS prospect_state_transitions_append_only ON prospect_state_transitions;
CREATE TRIGGER prospect_state_transitions_append_only
  BEFORE UPDATE OR DELETE ON prospect_state_transitions
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();
DROP TRIGGER IF EXISTS max_actions_append_only ON max_actions;
CREATE TRIGGER max_actions_append_only
  BEFORE UPDATE OR DELETE ON max_actions
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();

COMMIT;
