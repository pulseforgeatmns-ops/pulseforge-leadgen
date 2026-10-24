-- SPEC-MAX-RELIABILITY-003: Durable attention & operational continuity

BEGIN;

SELECT pg_advisory_xact_lock(20261005, 3);

CREATE TABLE IF NOT EXISTS max_attention_items (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  subject_type TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  source_decision_id TEXT REFERENCES max_operational_decisions(id),
  supporting_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL CHECK (status IN (
    'ACTIVE', 'WAITING', 'BLOCKED', 'OVERDUE', 'RESOLVED', 'SUPERSEDED'
  )),
  priority JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_review_at TIMESTAMPTZ,
  review_trigger TEXT NOT NULL CHECK (review_trigger IN ('TIME', 'STATE', 'EVIDENCE', 'MANUAL')),
  owner TEXT,
  audience_tier TEXT NOT NULL DEFAULT 'WAITING_SILENT' CHECK (audience_tier IN (
    'MAX_HANDLES', 'AO_HANDLES', 'OPERATOR_VISIBLE', 'WAITING_SILENT'
  )),
  parent_attention_id TEXT REFERENCES max_attention_items(id),
  dedup_fingerprint TEXT NOT NULL,
  resolution_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_evaluation_id TEXT,
  claim_token TEXT,
  claimed_at TIMESTAMPTZ,
  execution_failures INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_reviewed_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  UNIQUE (client_id, dedup_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_max_attention_items_due
  ON max_attention_items (client_id, next_review_at)
  WHERE status IN ('ACTIVE', 'WAITING', 'BLOCKED', 'OVERDUE');

CREATE INDEX IF NOT EXISTS idx_max_attention_items_subject
  ON max_attention_items (client_id, subject_type, subject_id, status);

CREATE TABLE IF NOT EXISTS max_attention_scheduler_runs (
  id TEXT PRIMARY KEY,
  client_id INTEGER REFERENCES clients(id),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  items_claimed INTEGER NOT NULL DEFAULT 0,
  items_evaluated INTEGER NOT NULL DEFAULT 0,
  items_failed INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  telemetry JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_max_attention_scheduler_runs_started
  ON max_attention_scheduler_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS max_attention_heartbeats (
  id TEXT PRIMARY KEY DEFAULT 'global',
  last_successful_cycle_at TIMESTAMPTZ,
  last_run_id TEXT REFERENCES max_attention_scheduler_runs(id),
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO max_attention_heartbeats (id)
VALUES ('global')
ON CONFLICT (id) DO NOTHING;

COMMIT;
