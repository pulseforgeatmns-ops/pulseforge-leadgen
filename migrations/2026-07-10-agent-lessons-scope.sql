ALTER TABLE agent_lessons
  ALTER COLUMN client_id DROP NOT NULL;

DROP INDEX IF EXISTS idx_agent_lessons_dedupe;

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_lessons_dedupe_scope
  ON agent_lessons (agent, COALESCE(client_id, -1), guardrail_text);
