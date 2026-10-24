-- SPEC-JEV-002: observation evidence only; no production state or routing writes.
BEGIN;
CREATE TABLE IF NOT EXISTS decision_shadow_events (
  decision_id UUID PRIMARY KEY,
  event TEXT NOT NULL CHECK (event = 'DECISION_SHADOW_EVALUATED'),
  spec TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  mode TEXT NOT NULL CHECK (mode = 'shadow'),
  source TEXT,
  session_id TEXT,
  tenant_id TEXT,
  message_index INTEGER,
  message_chars INTEGER,
  message_truncated BOOLEAN,
  provider TEXT,
  requested_provider TEXT,
  requested_model TEXT,
  model TEXT,
  mission_id TEXT,
  current_route JSONB,
  routing_latency_ms INTEGER,
  status TEXT NOT NULL CHECK (status IN ('evaluated', 'error', 'fallback', 'skipped')),
  intent TEXT,
  confidence DOUBLE PRECISION,
  mission_bound_probability DOUBLE PRECISION,
  approval_probability DOUBLE PRECISION,
  inspection_probability DOUBLE PRECISION,
  requires_human_clarification BOOLEAN,
  risk_if_misrouted TEXT,
  recommended_route TEXT,
  route_matches BOOLEAN,
  comparison TEXT NOT NULL CHECK (comparison IN ('match', 'mismatch', 'unavailable')),
  latency_ms INTEGER,
  fallback_provider TEXT,
  fallback_reason TEXT,
  errors JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(errors) = 'array'),
  timestamp TIMESTAMPTZ NOT NULL,
  raw_redacted_response JSONB
);
CREATE INDEX IF NOT EXISTS decision_shadow_recent_idx
  ON decision_shadow_events (timestamp DESC, decision_id DESC);
CREATE INDEX IF NOT EXISTS decision_shadow_tenant_recent_idx
  ON decision_shadow_events (tenant_id, timestamp DESC, decision_id DESC);
CREATE INDEX IF NOT EXISTS decision_shadow_mismatch_idx
  ON decision_shadow_events (timestamp DESC, decision_id DESC) WHERE comparison = 'mismatch';
CREATE INDEX IF NOT EXISTS decision_shadow_error_idx
  ON decision_shadow_events (timestamp DESC, decision_id DESC)
  WHERE status = 'error' OR jsonb_array_length(errors) > 0;
COMMENT ON TABLE decision_shadow_events IS
  'SPEC-JEV-002 best-effort shadow observations; never authoritative routing or approval state';
COMMIT;
