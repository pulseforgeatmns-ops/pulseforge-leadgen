-- SPEC-MAX-RELIABILITY-002: Evidence-bound decision & action execution

BEGIN;

SELECT pg_advisory_xact_lock(20261005, 2);

CREATE TABLE IF NOT EXISTS max_operational_decisions (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  trigger_type TEXT NOT NULL,
  trigger_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  triggering_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  canonical_state_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  subject_type TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  decision_type TEXT NOT NULL,
  candidate_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  selected_action JSONB,
  not_selected JSONB NOT NULL DEFAULT '[]'::jsonb,
  rationale TEXT,
  supporting_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  assumptions JSONB NOT NULL DEFAULT '[]'::jsonb,
  confidence NUMERIC,
  priority JSONB NOT NULL DEFAULT '{}'::jsonb,
  owner TEXT,
  authority_class TEXT NOT NULL CHECK (authority_class IN ('A', 'B', 'C', 'D')),
  execution_status TEXT NOT NULL,
  verification_status TEXT,
  idempotency_key TEXT NOT NULL,
  reevaluate_after TIMESTAMPTZ,
  receipt_summary TEXT,
  superseded_by TEXT REFERENCES max_operational_decisions(id),
  telemetry JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ,
  UNIQUE (client_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_max_operational_decisions_client_created
  ON max_operational_decisions (client_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_max_operational_decisions_subject
  ON max_operational_decisions (client_id, subject_type, subject_id, created_at DESC);

CREATE TABLE IF NOT EXISTS max_operational_action_intents (
  id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL REFERENCES max_operational_decisions(id) ON DELETE CASCADE,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  action_type TEXT NOT NULL,
  action_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  authority_class TEXT NOT NULL CHECK (authority_class IN ('A', 'B', 'C', 'D')),
  execution_status TEXT NOT NULL,
  verification_status TEXT,
  idempotency_key TEXT NOT NULL,
  output_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  UNIQUE (client_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_max_operational_action_intents_decision
  ON max_operational_action_intents (decision_id, created_at DESC);

CREATE TABLE IF NOT EXISTS max_ao_follow_up_tasks (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  decision_id TEXT REFERENCES max_operational_decisions(id),
  prospect_id TEXT,
  owner_id INTEGER,
  owner_name TEXT,
  account_name TEXT,
  prompt TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'completed', 'superseded', 'cancelled')),
  expectation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_max_ao_follow_up_tasks_open
  ON max_ao_follow_up_tasks (client_id, status, created_at DESC);

COMMIT;
