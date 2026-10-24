-- Research alerts have no trading score/state and are not delivery receipts.
CREATE TABLE IF NOT EXISTS signal_prospective_internal_alerts (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES signal_research_observations(id),
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
