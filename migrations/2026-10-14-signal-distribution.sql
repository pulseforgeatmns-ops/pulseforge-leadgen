-- Operational evidence only. Explicit opt-in rollout; no research tables or fixtures.
CREATE TABLE IF NOT EXISTS signal_distribution_monitors (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')),
 token_address TEXT NOT NULL, event_id TEXT NOT NULL, state JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, next_poll_at TIMESTAMPTZ NOT NULL,
 lease_until TIMESTAMPTZ, lease_token TEXT, UNIQUE(tenant_id,namespace,event_id,token_address), UNIQUE(id,tenant_id,namespace)
);
CREATE INDEX IF NOT EXISTS signal_distribution_poll_idx ON signal_distribution_monitors(next_poll_at,expires_at);
CREATE TABLE IF NOT EXISTS signal_distribution_transitions (
 id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL REFERENCES signal_distribution_monitors(id), tenant_id TEXT NOT NULL,
 namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')), payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL,
 UNIQUE(id,tenant_id,namespace), FOREIGN KEY(monitor_id,tenant_id,namespace) REFERENCES signal_distribution_monitors(id,tenant_id,namespace)
);
CREATE INDEX IF NOT EXISTS signal_distribution_transition_scope_idx ON signal_distribution_transitions(tenant_id,namespace,monitor_id,created_at);
CREATE TABLE IF NOT EXISTS signal_distribution_request_budget (
 id INTEGER PRIMARY KEY CHECK(id=1), blocked_until TIMESTAMPTZ
);
INSERT INTO signal_distribution_request_budget(id) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS signal_distribution_request_attempts (
 id BIGSERIAL PRIMARY KEY, lane TEXT NOT NULL, attempted_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS signal_distribution_attempt_time_idx ON signal_distribution_request_attempts(attempted_at);
CREATE TABLE IF NOT EXISTS signal_distribution_admissions (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')),
 token_address TEXT NOT NULL, external_message_id TEXT NOT NULL, reason TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS signal_distribution_observations (
 id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
 namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')),
 observed_at TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL,
 UNIQUE(monitor_id,observed_at), FOREIGN KEY(monitor_id,tenant_id,namespace) REFERENCES signal_distribution_monitors(id,tenant_id,namespace)
);

-- Immutable, explicitly configured operational windows. Legacy rows remain inert.
CREATE TABLE IF NOT EXISTS signal_distribution_activations (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, namespace TEXT NOT NULL CHECK(namespace IN ('operational','test')),
 starts_at TIMESTAMPTZ NOT NULL, ends_at TIMESTAMPTZ NOT NULL,
 sms_limit INTEGER NOT NULL CHECK(sms_limit BETWEEN 0 AND 20), sms_reserved INTEGER NOT NULL DEFAULT 0 CHECK(sms_reserved >= 0 AND sms_reserved <= sms_limit),
 CHECK(ends_at > starts_at AND ends_at <= starts_at + interval '24 hours'), UNIQUE(id,tenant_id,namespace)
);
ALTER TABLE signal_distribution_monitors ADD COLUMN IF NOT EXISTS activation_id TEXT;
DO $$ BEGIN
 ALTER TABLE signal_distribution_monitors ADD CONSTRAINT signal_distribution_monitor_activation_fk FOREIGN KEY(activation_id,tenant_id,namespace) REFERENCES signal_distribution_activations(id,tenant_id,namespace);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
