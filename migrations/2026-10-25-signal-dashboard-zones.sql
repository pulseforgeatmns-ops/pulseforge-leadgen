-- Isolated, append-only dashboard paper observations. No execution or cohort writes.
CREATE TABLE IF NOT EXISTS signal_dashboard_zone_events (
 tenant_id TEXT NOT NULL, wallet TEXT NOT NULL, token_address TEXT NOT NULL,
 zone_id TEXT NOT NULL, event_id TEXT NOT NULL,
 occurred_at TIMESTAMPTZ NOT NULL, known_at TIMESTAMPTZ NOT NULL,
 payload JSONB NOT NULL,
 PRIMARY KEY(tenant_id,wallet,token_address,event_id)
);
CREATE INDEX IF NOT EXISTS signal_dashboard_zone_history ON signal_dashboard_zone_events(tenant_id,wallet,token_address,zone_id,known_at DESC);
