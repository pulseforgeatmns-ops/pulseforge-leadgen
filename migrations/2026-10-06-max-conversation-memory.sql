-- MAX-MEMORY-001: Durable bounded conversational context for Max understanding

BEGIN;

SELECT pg_advisory_xact_lock(20261006, 1);

CREATE TABLE IF NOT EXISTS max_conversation_memory (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  conversation_id TEXT NOT NULL,
  actor_id TEXT,
  actor_role TEXT,

  entity_type TEXT,
  entity_id TEXT,

  semantic_type TEXT NOT NULL CHECK (semantic_type IN (
    'ACTIVE_ENTITY',
    'ACTIVE_CONTACT',
    'RECENT_THREAD',
    'REFERENCE_BINDING',
    'CORRECTION_CONTEXT',
    'TEMPORAL_CONTEXT',
    'OPEN_QUESTION',
    'OPEN_COMMITMENT'
  )),

  payload JSONB NOT NULL DEFAULT '{}'::jsonb,

  source_input_id TEXT,
  source_situation_model_id TEXT,
  record_fingerprint TEXT NOT NULL,

  confidence REAL,

  occurred_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_referenced_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  superseded_at TIMESTAMPTZ,

  UNIQUE (client_id, conversation_id, record_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_max_conversation_memory_scope_active
  ON max_conversation_memory (client_id, conversation_id, actor_id, semantic_type)
  WHERE superseded_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_max_conversation_memory_expires
  ON max_conversation_memory (expires_at)
  WHERE superseded_at IS NULL AND expires_at IS NOT NULL;

COMMIT;
