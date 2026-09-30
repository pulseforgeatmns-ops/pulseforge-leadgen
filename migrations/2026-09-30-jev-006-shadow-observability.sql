-- SPEC-JEV-006: shadow observability only; no production routing writes.
BEGIN;

ALTER TABLE decision_shadow_events
  ADD COLUMN IF NOT EXISTS mission_stage TEXT,
  ADD COLUMN IF NOT EXISTS prod_route TEXT,
  ADD COLUMN IF NOT EXISTS prod_action TEXT,
  ADD COLUMN IF NOT EXISTS prod_kind TEXT,
  ADD COLUMN IF NOT EXISTS pending_decision_present BOOLEAN,
  ADD COLUMN IF NOT EXISTS pending_decision_kind TEXT,
  ADD COLUMN IF NOT EXISTS mismatch_classification TEXT,
  ADD COLUMN IF NOT EXISTS route_comparable BOOLEAN,
  ADD COLUMN IF NOT EXISTS error_reason TEXT,
  ADD COLUMN IF NOT EXISTS message_pattern_flags JSONB;

CREATE INDEX IF NOT EXISTS decision_shadow_comparable_idx
  ON decision_shadow_events (timestamp DESC, decision_id DESC)
  WHERE route_comparable = true;

CREATE INDEX IF NOT EXISTS decision_shadow_dangerous_mismatch_idx
  ON decision_shadow_events (timestamp DESC, decision_id DESC)
  WHERE mismatch_classification = 'dangerous_approval_vs_clarification';

CREATE TABLE IF NOT EXISTS decision_shadow_evidence (
  evidence_id UUID PRIMARY KEY,
  event TEXT NOT NULL CHECK (event IN ('PENDING_DECISION_CAPTURE_GUARDED', 'DECISION_SHADOW_WARNING')),
  spec TEXT NOT NULL,
  decision_id UUID,
  session_id TEXT,
  tenant_id TEXT,
  mission_id TEXT,
  pending_decision_kind TEXT,
  jev_would_reclassify BOOLEAN,
  deterministic_guard_blocked BOOLEAN,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
  timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS decision_shadow_evidence_recent_idx
  ON decision_shadow_evidence (timestamp DESC, evidence_id DESC);

CREATE INDEX IF NOT EXISTS decision_shadow_evidence_decision_idx
  ON decision_shadow_evidence (decision_id)
  WHERE decision_id IS NOT NULL;

COMMENT ON TABLE decision_shadow_evidence IS
  'SPEC-JEV-006 guard and warning audit rows; never authoritative routing state';

COMMIT;
