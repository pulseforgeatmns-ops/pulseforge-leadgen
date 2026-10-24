-- Opt-in operational delivery only; never research/cohort records.
CREATE TABLE IF NOT EXISTS signal_distribution_deliveries (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  namespace TEXT NOT NULL CHECK (namespace IN ('operational','test')),
  monitor_id TEXT NOT NULL,
  transition_id TEXT NOT NULL,
  channel TEXT NOT NULL CHECK (channel IN ('sms','email')),
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SENDING','ACCEPTED','UNKNOWN','SUPPRESSED','TEST_ONLY','EXPIRED')),
  suppression_reason TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_until TIMESTAMPTZ,
  accepted_at TIMESTAMPTZ,
  send_authorized_at TIMESTAMPTZ,
  provider_receipt TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(tenant_id,namespace,transition_id,channel),
  FOREIGN KEY (transition_id,tenant_id,namespace) REFERENCES signal_distribution_transitions(id,tenant_id,namespace),
  CHECK (namespace <> 'test' OR status = 'TEST_ONLY')
);
CREATE INDEX IF NOT EXISTS signal_distribution_delivery_claim
  ON signal_distribution_deliveries(tenant_id,namespace,channel,created_at) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS signal_distribution_delivery_cooldown
  ON signal_distribution_deliveries(tenant_id,namespace,monitor_id,channel,created_at);

ALTER TABLE signal_distribution_deliveries ADD COLUMN IF NOT EXISTS send_authorized_at TIMESTAMPTZ;
