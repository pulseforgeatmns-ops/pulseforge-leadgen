CREATE TABLE IF NOT EXISTS agent_lessons (
  id               SERIAL PRIMARY KEY,
  agent            TEXT NOT NULL,
  client_id        INTEGER,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  window_start     TIMESTAMPTZ NOT NULL,
  window_end       TIMESTAMPTZ NOT NULL,
  category         TEXT NOT NULL,
  lesson           TEXT NOT NULL,
  guardrail_text   TEXT NOT NULL,
  evidence         TEXT,
  source_run_ids   JSONB,
  severity         TEXT NOT NULL DEFAULT 'low',
  status           TEXT NOT NULL DEFAULT 'proposed',
  confirmed_by     TEXT,
  confirmed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent_lessons_active
  ON agent_lessons (agent, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_lessons_dedupe
  ON agent_lessons (agent, client_id, guardrail_text);
