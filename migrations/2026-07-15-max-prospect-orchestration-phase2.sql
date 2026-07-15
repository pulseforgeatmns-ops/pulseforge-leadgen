-- Max Prospect Orchestration V1, Phase 2.
-- Shadow integrations, manual canonical overrides, and observability only.
-- Requires 2026-07-15-max-prospect-orchestration-v1.sql.

BEGIN;

ALTER TABLE prospect_signal_events
  DROP CONSTRAINT IF EXISTS prospect_signal_events_client_id_source_source_record_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS prospect_signal_events_source_type_uidx
  ON prospect_signal_events (client_id, source, source_record_id, event_type)
  WHERE source_record_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS prospect_signal_events_decay_candidates_idx
  ON prospect_signal_events (client_id, event_type, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS max_decisions_recommended_state_idx
  ON max_decisions (client_id, recommended_state, created_at DESC);

CREATE TABLE IF NOT EXISTS manual_lifecycle_overrides (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  signal_event_id TEXT NOT NULL REFERENCES prospect_signal_events(id),
  operator_user_id INTEGER REFERENCES users(id),
  operator_identity TEXT,
  from_state TEXT NOT NULL,
  requested_state TEXT NOT NULL,
  reason TEXT NOT NULL,
  source TEXT NOT NULL,
  terminal_restore BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (from_state IN ('cold','heating','warm','hot','engaged','nurture','recycle','disqualified','null')),
  CHECK (requested_state IN ('cold','heating','warm','hot','engaged','nurture','recycle','disqualified','null'))
);
CREATE INDEX IF NOT EXISTS manual_lifecycle_overrides_prospect_idx
  ON manual_lifecycle_overrides (client_id, prospect_id, created_at DESC);

CREATE TABLE IF NOT EXISTS max_orchestration_metrics (
  id BIGSERIAL PRIMARY KEY,
  client_id INTEGER,
  metric_name TEXT NOT NULL CHECK (metric_name IN (
    'max_decisions_total',
    'max_state_transition_recommendations_total',
    'max_action_recommendations_total',
    'max_evaluation_failures_total',
    'max_duplicate_events_suppressed_total',
    'max_decay_evaluations_total',
    'max_manual_overrides_total',
    'signal_to_decision_duration',
    'decision_processing_duration',
    'decay_batch_duration'
  )),
  metric_value NUMERIC NOT NULL DEFAULT 1,
  prospect_id UUID REFERENCES prospects(id),
  signal_event_id TEXT,
  decision_id TEXT,
  dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS max_orchestration_metrics_name_time_idx
  ON max_orchestration_metrics (client_id, metric_name, recorded_at DESC);

DROP TRIGGER IF EXISTS manual_lifecycle_overrides_append_only ON manual_lifecycle_overrides;
CREATE TRIGGER manual_lifecycle_overrides_append_only
  BEFORE UPDATE OR DELETE ON manual_lifecycle_overrides
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();

DROP TRIGGER IF EXISTS max_orchestration_metrics_append_only ON max_orchestration_metrics;
CREATE TRIGGER max_orchestration_metrics_append_only
  BEFORE UPDATE OR DELETE ON max_orchestration_metrics
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();

COMMIT;
