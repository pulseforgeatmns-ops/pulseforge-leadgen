-- SPEC-014 Knowledge Dual-Write & Operational Readiness
-- Durable outbox, persistent sync ledger, flight recorder stages.

CREATE TABLE IF NOT EXISTS knowledge_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  entity_id TEXT,
  entity_type TEXT,
  source TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  sync_envelope JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'processing', 'applied', 'failed', 'dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at TIMESTAMPTZ,
  UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS knowledge_outbox_status_retry_idx
  ON knowledge_outbox (status, next_retry_at)
  WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS knowledge_outbox_tenant_created_idx
  ON knowledge_outbox (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS knowledge_sync_ledger (
  tenant_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  marked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  record JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS knowledge_sync_ledger_marked_idx
  ON knowledge_sync_ledger (tenant_id, marked_at DESC);

CREATE TABLE IF NOT EXISTS knowledge_flight_stages (
  id BIGSERIAL PRIMARY KEY,
  flight_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  entity_id TEXT,
  entity_type TEXT,
  stage TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'complete'
    CHECK (status IN ('pending', 'complete', 'failed', 'skipped')),
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (flight_id, stage)
);

CREATE INDEX IF NOT EXISTS knowledge_flight_stages_tenant_entity_idx
  ON knowledge_flight_stages (tenant_id, entity_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS knowledge_flight_stages_flight_idx
  ON knowledge_flight_stages (flight_id, occurred_at ASC);
