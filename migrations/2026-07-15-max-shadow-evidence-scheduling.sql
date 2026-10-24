-- Max shadow evidence accumulation and scheduling readiness only.
-- This migration does not update prospects, sequences, tasks, sends, or enrichment.

BEGIN;

ALTER TABLE max_orchestration_metrics
  DROP CONSTRAINT IF EXISTS max_orchestration_metrics_metric_name_check;
ALTER TABLE max_orchestration_metrics
  ADD CONSTRAINT max_orchestration_metrics_metric_name_check CHECK (metric_name IN (
    'max_decisions_total','max_state_transition_recommendations_total',
    'max_action_recommendations_total','max_evaluation_failures_total',
    'max_duplicate_events_suppressed_total','max_decay_evaluations_total',
    'max_manual_overrides_total','signal_to_decision_duration',
    'decision_processing_duration','decay_batch_duration',
    'live_signal_to_decision_latency','live_processing_latency',
    'historical_backfill_event_age','historical_backfill_processing_latency',
    'manual_recalculation_processing_latency','decay_processing_latency'
  ));

ALTER TABLE max_recommendation_reviews
  ADD COLUMN IF NOT EXISTS score_component_explanation JSONB,
  ADD COLUMN IF NOT EXISTS source_data_trustworthy BOOLEAN,
  ADD COLUMN IF NOT EXISTS source_data_notes TEXT;

ALTER TABLE max_rollout_readiness_config
  ADD COLUMN IF NOT EXISTS recovery_artifact_found BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recovery_hash_verified BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recovery_archive_readable BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recovery_restore_procedure_documented BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recovery_durable_storage_verified BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS decay_schedule_configured BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS decay_schedule_verified BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS decay_schedule_reference TEXT,
  ADD COLUMN IF NOT EXISTS decay_schedule_command TEXT,
  ADD COLUMN IF NOT EXISTS decay_schedule_frequency TEXT,
  ADD COLUMN IF NOT EXISTS decay_schedule_timezone TEXT;

CREATE TABLE IF NOT EXISTS max_meeting_outcome_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE RESTRICT,
  company_id UUID REFERENCES companies(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL CHECK (event_type IN ('meeting_cancelled','meeting_showed','meeting_no_showed')),
  source TEXT NOT NULL,
  source_record_id TEXT NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  original_event_timestamp TIMESTAMPTZ,
  confidence TEXT NOT NULL CHECK (confidence IN ('confirmed_provider','confirmed_operator')),
  correction_of_event_id UUID REFERENCES max_meeting_outcome_events(id) ON DELETE RESTRICT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source, source_record_id, event_type)
);
CREATE INDEX IF NOT EXISTS max_meeting_outcome_events_prospect_time_idx
  ON max_meeting_outcome_events (client_id, prospect_id, event_timestamp DESC);

DROP TRIGGER IF EXISTS max_meeting_outcome_events_append_only ON max_meeting_outcome_events;
CREATE TRIGGER max_meeting_outcome_events_append_only
  BEFORE UPDATE OR DELETE ON max_meeting_outcome_events
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();

COMMIT;
