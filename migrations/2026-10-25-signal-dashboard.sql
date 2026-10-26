-- SPEC-SIGNAL-DASH-001: isolated real-position intelligence. No paper/research writes.
CREATE TABLE IF NOT EXISTS signal_dashboard_records (
  tenant_id TEXT NOT NULL,
  wallet TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('watch','transaction','observation','candle','event','annotation','balance','cursor','quote','status')),
  token_address TEXT NOT NULL,
  record_id TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  known_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY(tenant_id,wallet,kind,token_address,record_id)
);
CREATE INDEX IF NOT EXISTS signal_dashboard_time ON signal_dashboard_records(tenant_id,wallet,kind,token_address,occurred_at DESC,known_at DESC);
CREATE TABLE IF NOT EXISTS signal_dashboard_provider_budget (
  provider TEXT PRIMARY KEY,
  next_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity'
);
