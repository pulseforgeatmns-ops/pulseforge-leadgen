CREATE TABLE IF NOT EXISTS agent_run_health (
  id BIGSERIAL PRIMARY KEY,
  agent TEXT NOT NULL,
  client_id INTEGER,
  run_id TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  successes INTEGER NOT NULL DEFAULT 0,
  skipped INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL CHECK (state IN ('idle', 'working', 'stranded')),
  error_sample JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_run_health_agent_client_created
ON agent_run_health (agent, client_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_alert_dispatches (
  id BIGSERIAL PRIMARY KEY,
  agent TEXT NOT NULL,
  client_id INTEGER,
  run_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  error_sample JSONB,
  sent BOOLEAN NOT NULL DEFAULT FALSE,
  suppressed BOOLEAN NOT NULL DEFAULT FALSE,
  failure_reason TEXT,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_alert_dispatches_cooldown
ON agent_alert_dispatches (agent, client_id, sent_at DESC)
WHERE sent = TRUE;
