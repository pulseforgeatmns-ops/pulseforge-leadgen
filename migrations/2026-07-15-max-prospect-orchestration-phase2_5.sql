-- Max Prospect Orchestration V1, Phase 2.5.
-- Review and rollout-readiness records only. Nothing here changes prospect
-- status, enrollment, scheduled sends, tasks, or enrichment behavior.
-- Requires the Phase 1 migration followed by the Phase 2 migration.

BEGIN;

ALTER TABLE prospect_signal_events DROP CONSTRAINT IF EXISTS prospect_signal_events_client_fk;
ALTER TABLE prospect_signal_events
  ADD CONSTRAINT prospect_signal_events_client_fk
  FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE prospect_signal_events VALIDATE CONSTRAINT prospect_signal_events_client_fk;

ALTER TABLE max_decisions DROP CONSTRAINT IF EXISTS max_decisions_client_fk;
ALTER TABLE max_decisions
  ADD CONSTRAINT max_decisions_client_fk
  FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE max_decisions VALIDATE CONSTRAINT max_decisions_client_fk;

ALTER TABLE prospect_state_transitions DROP CONSTRAINT IF EXISTS prospect_state_transitions_client_fk;
ALTER TABLE prospect_state_transitions
  ADD CONSTRAINT prospect_state_transitions_client_fk
  FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE prospect_state_transitions VALIDATE CONSTRAINT prospect_state_transitions_client_fk;

ALTER TABLE max_actions DROP CONSTRAINT IF EXISTS max_actions_client_fk;
ALTER TABLE max_actions
  ADD CONSTRAINT max_actions_client_fk
  FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE max_actions VALIDATE CONSTRAINT max_actions_client_fk;

ALTER TABLE manual_lifecycle_overrides DROP CONSTRAINT IF EXISTS manual_lifecycle_overrides_client_fk;
ALTER TABLE manual_lifecycle_overrides
  ADD CONSTRAINT manual_lifecycle_overrides_client_fk
  FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE manual_lifecycle_overrides VALIDATE CONSTRAINT manual_lifecycle_overrides_client_fk;

ALTER TABLE max_orchestration_metrics DROP CONSTRAINT IF EXISTS max_orchestration_metrics_client_fk;
ALTER TABLE max_orchestration_metrics
  ADD CONSTRAINT max_orchestration_metrics_client_fk
  FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE max_orchestration_metrics VALIDATE CONSTRAINT max_orchestration_metrics_client_fk;

CREATE TABLE IF NOT EXISTS max_recommendation_reviews (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  decision_id TEXT NOT NULL REFERENCES max_decisions(id) ON DELETE RESTRICT,
  prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE RESTRICT,
  reviewer_identity TEXT NOT NULL,
  review_outcome TEXT NOT NULL CHECK (review_outcome IN (
    'agree','disagree','uncertain','bad_data','wrong_signal_classification','wrong_score','wrong_transition'
  )),
  notes TEXT,
  reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (decision_id, reviewer_identity)
);
CREATE INDEX IF NOT EXISTS max_recommendation_reviews_client_time_idx
  ON max_recommendation_reviews (client_id, reviewed_at DESC);
CREATE INDEX IF NOT EXISTS max_recommendation_reviews_outcome_idx
  ON max_recommendation_reviews (client_id, review_outcome, reviewed_at DESC);

CREATE TABLE IF NOT EXISTS max_rollout_readiness_config (
  client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE RESTRICT,
  phase3_allowlisted BOOLEAN NOT NULL DEFAULT FALSE,
  minimum_reviewed_samples INTEGER NOT NULL DEFAULT 100 CHECK (minimum_reviewed_samples > 0),
  rollback_documented BOOLEAN NOT NULL DEFAULT FALSE,
  rollback_reference TEXT,
  intended_first_transition TEXT,
  updated_by TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (phase3_allowlisted = FALSE OR (rollback_documented = TRUE AND rollback_reference IS NOT NULL))
);

DROP TRIGGER IF EXISTS max_recommendation_reviews_append_only ON max_recommendation_reviews;
CREATE TRIGGER max_recommendation_reviews_append_only
  BEFORE UPDATE OR DELETE ON max_recommendation_reviews
  FOR EACH ROW EXECUTE FUNCTION reject_max_orchestration_append_only_mutation();

COMMIT;
