-- Reviewed operator migration only. No approval/authority data belongs here.
BEGIN;
CREATE TABLE IF NOT EXISTS max_conversation_context (
  tenant_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  revision TEXT NOT NULL,
  context JSONB NOT NULL CHECK (jsonb_typeof(context) = 'object' AND octet_length(context::text) <= 16384),
  expires_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, session_id)
);
CREATE INDEX IF NOT EXISTS max_conversation_context_expiry ON max_conversation_context(expires_at);
COMMIT;
