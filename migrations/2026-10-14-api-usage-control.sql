-- Application estimates and request reservations; never provider invoices.
CREATE TABLE IF NOT EXISTS api_usage_events (
  id UUID PRIMARY KEY,
  provider TEXT NOT NULL CHECK (provider IN ('google_places', 'anthropic', 'google_gemini')),
  agent_name TEXT NOT NULL,
  tenant_id INTEGER,
  mission_id TEXT,
  operation TEXT NOT NULL,
  requested_model TEXT,
  model TEXT,
  status TEXT NOT NULL CHECK (status IN ('reserved', 'ok', 'error', 'uncertain')),
  reserved_usd NUMERIC(18,8),
  estimated_usd NUMERIC(18,8),
  pricing_basis TEXT NOT NULL,
  usage JSONB NOT NULL DEFAULT '{}'::jsonb,
  http_status INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS api_usage_events_provider_time_idx
  ON api_usage_events (provider, created_at);
CREATE INDEX IF NOT EXISTS api_usage_events_tenant_time_idx
  ON api_usage_events (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS api_spend_policies (
  provider TEXT PRIMARY KEY CHECK (provider IN ('google_places', 'anthropic', 'google_gemini')),
  daily_request_limit INTEGER CHECK (daily_request_limit >= 0),
  monthly_request_limit INTEGER CHECK (monthly_request_limit >= 0),
  daily_usd_limit NUMERIC(18,8) CHECK (daily_usd_limit >= 0),
  monthly_usd_limit NUMERIC(18,8) CHECK (monthly_usd_limit >= 0),
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS api_spend_policy_changes (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  actor TEXT NOT NULL,
  before_policy JSONB,
  after_policy JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Existing installations retain their original CREATE TABLE constraints.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'api_usage_events_provider_check'
    AND pg_get_constraintdef(oid) NOT LIKE '%google_gemini%') THEN
    ALTER TABLE api_usage_events DROP CONSTRAINT api_usage_events_provider_check;
    ALTER TABLE api_usage_events ADD CONSTRAINT api_usage_events_provider_check
      CHECK (provider IN ('google_places', 'anthropic', 'google_gemini'));
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'api_spend_policies_provider_check'
    AND pg_get_constraintdef(oid) NOT LIKE '%google_gemini%') THEN
    ALTER TABLE api_spend_policies DROP CONSTRAINT api_spend_policies_provider_check;
    ALTER TABLE api_spend_policies ADD CONSTRAINT api_spend_policies_provider_check
      CHECK (provider IN ('google_places', 'anthropic', 'google_gemini'));
  END IF;
END $$;
