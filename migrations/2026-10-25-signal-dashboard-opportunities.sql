-- Operational namespace only. Canonical prospective research tables are untouched.
CREATE TABLE IF NOT EXISTS signal_opportunity_decisions (
 tenant_id TEXT NOT NULL, wallet TEXT NOT NULL, token_address TEXT NOT NULL,
 id TEXT NOT NULL, definition_version TEXT NOT NULL, known_at TIMESTAMPTZ NOT NULL,
 payload JSONB NOT NULL, PRIMARY KEY(tenant_id,wallet,id)
);
CREATE INDEX IF NOT EXISTS signal_opportunity_latest ON signal_opportunity_decisions(tenant_id,wallet,token_address,known_at DESC);
CREATE TABLE IF NOT EXISTS signal_opportunity_notifications (
 tenant_id TEXT NOT NULL, wallet TEXT NOT NULL, id TEXT NOT NULL, token_address TEXT NOT NULL,
 channel TEXT NOT NULL CHECK(channel IN ('email','sms')), decision_id TEXT NOT NULL,
 state TEXT NOT NULL, payload JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), claimed_at TIMESTAMPTZ,
 receipt TEXT, PRIMARY KEY(tenant_id,wallet,id)
);
