-- DRAFT: review and explicit production approval required. Never auto-applied.
-- Operational-only additive outbox; no research tables or credentials.
CREATE TABLE IF NOT EXISTS signal_initial_sms_windows (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')),
 starts_at TIMESTAMPTZ NOT NULL, ends_at TIMESTAMPTZ NOT NULL,
 sms_limit INTEGER NOT NULL CHECK(sms_limit BETWEEN 0 AND 20), sms_reserved INTEGER NOT NULL DEFAULT 0,
 UNIQUE(id,tenant_id,namespace), CHECK(ends_at>starts_at AND ends_at<=starts_at+interval '24 hours'),
 CHECK(sms_reserved BETWEEN 0 AND sms_limit)
);
CREATE TABLE IF NOT EXISTS signal_initial_sms_deliveries (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')),
 window_id TEXT NOT NULL, event_id TEXT NOT NULL, channel TEXT NOT NULL DEFAULT 'sms' CHECK(channel='sms'),
 payload JSONB NOT NULL, status TEXT NOT NULL CHECK(status IN ('PENDING','SENDING','ACCEPTED','UNKNOWN','EXPIRED','SUPPRESSED','TEST_ONLY')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), claimed_at TIMESTAMPTZ, lease_until TIMESTAMPTZ, lease_token TEXT,
 authorized_at TIMESTAMPTZ, accepted_at TIMESTAMPTZ, provider_receipt TEXT,
 UNIQUE(tenant_id,namespace,event_id,channel),
 FOREIGN KEY(window_id,tenant_id,namespace) REFERENCES signal_initial_sms_windows(id,tenant_id,namespace),
 CHECK(namespace<>'test' OR status='TEST_ONLY')
);
CREATE INDEX IF NOT EXISTS signal_initial_sms_pending ON signal_initial_sms_deliveries(tenant_id,namespace,created_at) WHERE status='PENDING';
