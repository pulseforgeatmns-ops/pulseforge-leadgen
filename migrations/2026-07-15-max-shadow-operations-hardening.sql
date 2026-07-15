BEGIN;

ALTER TABLE max_orchestration_metrics
  DROP CONSTRAINT IF EXISTS max_orchestration_metrics_metric_name_check;
ALTER TABLE max_orchestration_metrics
  ADD CONSTRAINT max_orchestration_metrics_metric_name_check CHECK (metric_name IN (
    'max_decisions_total',
    'max_state_transition_recommendations_total',
    'max_action_recommendations_total',
    'max_evaluation_failures_total',
    'max_duplicate_events_suppressed_total',
    'max_decay_evaluations_total',
    'max_manual_overrides_total',
    'signal_to_decision_duration',
    'decision_processing_duration',
    'decay_batch_duration',
    'live_signal_to_decision_latency',
    'historical_backfill_event_age',
    'historical_backfill_processing_latency',
    'manual_recalculation_processing_latency',
    'decay_processing_latency'
  ));

ALTER TABLE max_rollout_readiness_config
  ADD COLUMN IF NOT EXISTS shadow_observation_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS minimum_total_reviews INTEGER NOT NULL DEFAULT 100,
  ADD COLUMN IF NOT EXISTS minimum_reviews_by_transition JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS terminal_review_requirement TEXT NOT NULL DEFAULT 'every',
  ADD COLUMN IF NOT EXISTS minimum_agreement_rate NUMERIC,
  ADD COLUMN IF NOT EXISTS maximum_failure_rate NUMERIC,
  ADD COLUMN IF NOT EXISTS maximum_oscillation_rate NUMERIC,
  ADD COLUMN IF NOT EXISTS rollback_reference_verified BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recovery_snapshot_reference TEXT,
  ADD COLUMN IF NOT EXISTS recovery_snapshot_verified BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DO $$ BEGIN
  ALTER TABLE max_rollout_readiness_config
    ADD CONSTRAINT max_rollout_minimum_total_reviews_check CHECK (minimum_total_reviews > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE max_rollout_readiness_config
    ADD CONSTRAINT max_rollout_terminal_review_requirement_check
    CHECK (terminal_review_requirement IN ('every', 'sampled'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE max_rollout_readiness_config
    ADD CONSTRAINT max_rollout_rate_bounds_check CHECK (
      (minimum_agreement_rate IS NULL OR minimum_agreement_rate BETWEEN 0 AND 100)
      AND (maximum_failure_rate IS NULL OR maximum_failure_rate BETWEEN 0 AND 100)
      AND (maximum_oscillation_rate IS NULL OR maximum_oscillation_rate BETWEEN 0 AND 100)
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS max_decay_run_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL,
  job_type TEXT NOT NULL DEFAULT 'max_decay' CHECK (job_type = 'max_decay'),
  mode TEXT NOT NULL CHECK (mode IN ('dry-run', 'shadow-write')),
  status TEXT NOT NULL CHECK (status IN ('running','completed','failed','skipped_overlap','cancelled')),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  lock_acquired BOOLEAN NOT NULL DEFAULT FALSE,
  client_scope INTEGER REFERENCES clients(id) ON DELETE RESTRICT,
  batch_limit INTEGER NOT NULL CHECK (batch_limit BETWEEN 1 AND 2000),
  start_cursor UUID,
  end_cursor UUID,
  candidates_found INTEGER NOT NULL DEFAULT 0,
  prospects_evaluated INTEGER NOT NULL DEFAULT 0,
  scores_changed INTEGER NOT NULL DEFAULT 0,
  downgrade_candidates INTEGER NOT NULL DEFAULT 0,
  recommendations_created INTEGER NOT NULL DEFAULT 0,
  decisions_created INTEGER NOT NULL DEFAULT 0,
  errors INTEGER NOT NULL DEFAULT 0,
  error_stage TEXT,
  error_code TEXT,
  error_summary TEXT,
  retryable BOOLEAN,
  operational_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
  deployment_commit TEXT,
  details JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS max_decay_run_events_run_time_idx
  ON max_decay_run_events (run_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS max_decay_run_events_recent_idx
  ON max_decay_run_events (recorded_at DESC);

DROP TRIGGER IF EXISTS max_decay_run_events_append_only ON max_decay_run_events;
CREATE TRIGGER max_decay_run_events_append_only
  BEFORE UPDATE OR DELETE ON max_decay_run_events
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();

COMMIT;
