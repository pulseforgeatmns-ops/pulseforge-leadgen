-- Revenue Phase 1.5 productionization. No revenue data migration or autonomous action.
BEGIN;

REVOKE UPDATE, DELETE, TRUNCATE ON revenue_events FROM PUBLIC;

ALTER TABLE revenue_outcomes
  ADD COLUMN payment_count INTEGER NOT NULL DEFAULT 0 CHECK (payment_count >= 0),
  ADD COLUMN refund_count INTEGER NOT NULL DEFAULT 0 CHECK (refund_count >= 0);

CREATE TABLE revenue_feature_flags (
  client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE RESTRICT,
  revenue_schema_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  revenue_operator_reads_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  revenue_operator_writes_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  revenue_max_reads_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  revenue_followup_recommendations_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT,
  CHECK (NOT revenue_operator_reads_enabled OR revenue_schema_enabled),
  CHECK (NOT revenue_operator_writes_enabled OR revenue_schema_enabled),
  CHECK (NOT revenue_max_reads_enabled OR revenue_schema_enabled),
  CHECK (NOT revenue_followup_recommendations_enabled OR revenue_schema_enabled)
);

CREATE TABLE revenue_projection_rebuilds (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  correlation_id UUID NOT NULL UNIQUE,
  client_id INTEGER REFERENCES clients(id) ON DELETE RESTRICT,
  mode TEXT NOT NULL CHECK (mode IN ('dry_run','compare_only','apply')),
  date_from TIMESTAMPTZ,
  date_to TIMESTAMPTZ,
  ledger_event_count INTEGER NOT NULL DEFAULT 0,
  projected_outcome_count INTEGER NOT NULL DEFAULT 0,
  mismatched_record_count INTEGER NOT NULL DEFAULT 0,
  unexplained_event_count INTEGER NOT NULL DEFAULT 0,
  before_totals JSONB NOT NULL DEFAULT '{}'::jsonb,
  after_totals JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL CHECK (status IN ('running','passed','failed','rolled_back')),
  error_code TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX revenue_projection_rebuilds_client_completed_idx
  ON revenue_projection_rebuilds(client_id, completed_at DESC);

CREATE TABLE revenue_reconciliation_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  correlation_id UUID NOT NULL,
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  ledger_totals JSONB NOT NULL,
  projection_totals JSONB NOT NULL,
  source_totals JSONB NOT NULL,
  mismatches JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL CHECK (status IN ('passed','failed')),
  reconciled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (correlation_id, client_id)
);

CREATE INDEX revenue_reconciliation_runs_client_time_idx
  ON revenue_reconciliation_runs(client_id, reconciled_at DESC);

CREATE TABLE revenue_operational_metrics (
  client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE RESTRICT,
  duplicate_rejection_count BIGINT NOT NULL DEFAULT 0,
  failed_transition_count BIGINT NOT NULL DEFAULT 0,
  tenant_mismatch_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE revenue_operator_audit (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  action TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_entity_type TEXT,
  source_entity_id UUID,
  resulting_events JSONB NOT NULL DEFAULT '[]'::jsonb,
  correlation_id UUID NOT NULL,
  source_system TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  financial_delta JSONB NOT NULL DEFAULT '{}'::jsonb,
  attribution_confidence TEXT NOT NULL DEFAULT 'unattributed',
  UNIQUE (client_id, source_system, idempotency_key)
);

CREATE INDEX revenue_operator_audit_client_time_idx
  ON revenue_operator_audit(client_id, occurred_at DESC);

COMMIT;
