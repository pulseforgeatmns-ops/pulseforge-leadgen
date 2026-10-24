BEGIN;

CREATE TABLE IF NOT EXISTS scout_skip_log (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT,
  client_id INTEGER,
  vertical TEXT,
  location TEXT,
  search_query TEXT,
  discovery_method TEXT,
  skip_reason TEXT NOT NULL,
  candidate_identifier TEXT,
  detail JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scout_skip_log_client_reason_created
  ON scout_skip_log (client_id, skip_reason, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scout_skip_log_run ON scout_skip_log (run_id);

CREATE OR REPLACE VIEW scout_skip_summary_7d AS
SELECT DATE(created_at) AS day, client_id, vertical, location, skip_reason, COUNT(*) AS skip_count
FROM scout_skip_log
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY DATE(created_at), client_id, vertical, location, skip_reason
ORDER BY day DESC, client_id, skip_count DESC;

COMMIT;

-- Post-deploy validation (run after Scout has received traffic):
-- SELECT skip_reason, COUNT(*) AS count
-- FROM scout_skip_log
-- WHERE created_at > NOW() - INTERVAL '1 hour'
-- GROUP BY skip_reason
-- ORDER BY count DESC;
