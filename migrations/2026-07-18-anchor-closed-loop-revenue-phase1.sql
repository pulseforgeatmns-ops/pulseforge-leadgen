-- Anchor closed-loop revenue Phase 1. No external sends or autonomous actions.
BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE customers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  customer_type TEXT NOT NULL CHECK (customer_type IN ('residential','commercial','short_term_rental','property_manager','partner','other')),
  display_name TEXT NOT NULL,
  primary_email TEXT,
  primary_phone TEXT,
  company_id UUID,
  source_prospect_id UUID,
  status TEXT NOT NULL DEFAULT 'prospective' CHECK (status IN ('prospective','active','inactive','churned','blocked')),
  archived_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, id),
  UNIQUE (client_id, source_prospect_id),
  FOREIGN KEY (client_id, company_id) REFERENCES companies(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, source_prospect_id) REFERENCES prospects(client_id, id) ON DELETE RESTRICT
);

CREATE TABLE opportunities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  customer_id UUID,
  prospect_id UUID,
  company_id UUID,
  service_type TEXT NOT NULL,
  estimated_value_cents BIGINT NOT NULL CHECK (estimated_value_cents >= 0),
  estimated_cost_cents BIGINT CHECK (estimated_cost_cents >= 0),
  expected_close_date DATE,
  stage TEXT NOT NULL DEFAULT 'identified' CHECK (stage IN ('identified','contacted','qualified','quoted','booked','won','lost','cancelled')),
  source TEXT NOT NULL,
  lead_source_detail TEXT,
  campaign_id UUID,
  sequence_id TEXT,
  attribution_status TEXT NOT NULL CHECK (attribution_status IN ('confirmed','deterministic','inferred','unattributed','disputed')),
  human_owner TEXT,
  closed_at TIMESTAMPTZ,
  closed_reason TEXT,
  archived_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, id),
  FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, prospect_id) REFERENCES prospects(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, company_id) REFERENCES companies(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, campaign_id) REFERENCES campaigns(client_id, id) ON DELETE RESTRICT,
  CHECK (customer_id IS NOT NULL OR prospect_id IS NOT NULL)
);

CREATE TABLE revenue_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  opportunity_id UUID,
  customer_id UUID NOT NULL,
  service_type TEXT NOT NULL,
  service_address TEXT,
  scheduled_start TIMESTAMPTZ NOT NULL,
  actual_start TIMESTAMPTZ,
  actual_end TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'scheduled' CHECK (status IN ('scheduled','en_route','in_progress','completed','partially_completed','customer_disputed','cancelled','failed')),
  assigned_team TEXT,
  quoted_amount_cents BIGINT NOT NULL CHECK (quoted_amount_cents >= 0),
  final_amount_cents BIGINT CHECK (final_amount_cents >= 0),
  estimated_direct_cost_cents BIGINT CHECK (estimated_direct_cost_cents >= 0),
  actual_direct_cost_cents BIGINT CHECK (actual_direct_cost_cents >= 0),
  completion_notes TEXT,
  completion_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  cancelled_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, id),
  FOREIGN KEY (client_id, opportunity_id) REFERENCES opportunities(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, id) ON DELETE RESTRICT
);

CREATE TABLE revenue_payments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  job_id UUID NOT NULL,
  customer_id UUID NOT NULL,
  provider TEXT NOT NULL DEFAULT 'manual',
  external_payment_id TEXT,
  payment_method TEXT NOT NULL,
  amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
  status TEXT NOT NULL CHECK (status IN ('pending','succeeded','failed','reversed','partially_refunded','refunded')),
  received_at TIMESTAMPTZ,
  failed_at TIMESTAMPTZ,
  refunded_amount_cents BIGINT NOT NULL DEFAULT 0 CHECK (refunded_amount_cents >= 0 AND refunded_amount_cents <= amount_cents),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, id),
  FOREIGN KEY (client_id, job_id) REFERENCES revenue_jobs(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX revenue_payments_external_uidx
  ON revenue_payments(client_id, provider, external_payment_id)
  WHERE external_payment_id IS NOT NULL;

CREATE TABLE revenue_events (
  event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id UUID NOT NULL,
  source_system TEXT NOT NULL,
  source_event_id TEXT,
  event_version INTEGER NOT NULL DEFAULT 1 CHECK (event_version > 0),
  occurred_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  correlation_id UUID NOT NULL,
  causation_id UUID,
  idempotency_key TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  supersedes_event_id UUID REFERENCES revenue_events(event_id) ON DELETE RESTRICT,
  is_compensating_event BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE (client_id, source_system, idempotency_key)
);
CREATE UNIQUE INDEX revenue_events_external_uidx
  ON revenue_events(client_id, source_system, source_event_id)
  WHERE source_event_id IS NOT NULL;
CREATE OR REPLACE FUNCTION prevent_revenue_event_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'revenue_events is append-only';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER revenue_events_no_update_delete
  BEFORE UPDATE OR DELETE ON revenue_events
  FOR EACH ROW EXECUTE FUNCTION prevent_revenue_event_mutation();

CREATE TABLE revenue_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  customer_id UUID NOT NULL,
  prospect_id UUID,
  company_id UUID,
  opportunity_id UUID,
  job_id UUID NOT NULL,
  payment_id UUID,
  service_type TEXT NOT NULL,
  lead_source TEXT NOT NULL,
  lead_source_detail TEXT,
  campaign_id UUID,
  sequence_id TEXT,
  first_touch_agent TEXT,
  last_touch_agent TEXT,
  conversion_agent TEXT,
  human_owner TEXT,
  agent_touch_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  attribution_status TEXT NOT NULL CHECK (attribution_status IN ('confirmed','deterministic','inferred','unattributed','disputed')),
  booked_revenue_cents BIGINT NOT NULL DEFAULT 0,
  delivered_revenue_cents BIGINT NOT NULL DEFAULT 0,
  collected_revenue_cents BIGINT NOT NULL DEFAULT 0,
  refunded_revenue_cents BIGINT NOT NULL DEFAULT 0,
  estimated_direct_cost_cents BIGINT,
  actual_direct_cost_cents BIGINT,
  gross_profit_cents BIGINT,
  gross_margin NUMERIC(8,4),
  sales_cycle_days NUMERIC(12,2),
  time_to_payment_days NUMERIC(12,2),
  outcome_status TEXT NOT NULL CHECK (outcome_status IN ('booked','delivered','partially_paid','paid','refunded','partially_refunded','cancelled','disputed','written_off')),
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, id),
  UNIQUE (client_id, job_id),
  FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, prospect_id) REFERENCES prospects(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, company_id) REFERENCES companies(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, opportunity_id) REFERENCES opportunities(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, job_id) REFERENCES revenue_jobs(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, payment_id) REFERENCES revenue_payments(client_id, id) ON DELETE RESTRICT
);

CREATE TABLE revenue_follow_up_recommendations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  customer_id UUID NOT NULL,
  job_id UUID NOT NULL,
  recommendation_type TEXT NOT NULL CHECK (recommendation_type IN ('review','referral','recurring_service','reactivation')),
  eligible BOOLEAN NOT NULL,
  recommended_at TIMESTAMPTZ,
  reason TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'recommended' CHECK (status IN ('recommended','approved','dismissed','completed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, job_id, recommendation_type),
  FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, job_id) REFERENCES revenue_jobs(client_id, id) ON DELETE RESTRICT
);

CREATE INDEX opportunities_client_stage_idx ON opportunities(client_id, stage);
CREATE INDEX revenue_jobs_client_status_idx ON revenue_jobs(client_id, status);
CREATE INDEX revenue_events_client_occurred_idx ON revenue_events(client_id, occurred_at DESC);
CREATE INDEX revenue_outcomes_client_occurred_idx ON revenue_outcomes(client_id, occurred_at DESC);

COMMIT;
