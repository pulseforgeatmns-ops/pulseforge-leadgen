-- Dormant internal outbox. No delivery worker, public endpoint, or auto-enqueue.
CREATE TABLE IF NOT EXISTS signal_operator_alert_outbox (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL CHECK (source_id = 'telegram-front-runners'),
  external_message_id TEXT NOT NULL,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  evidence_id TEXT NOT NULL REFERENCES signal_raw_caller_evidence(id),
  payload JSONB NOT NULL,
  delivery_state TEXT NOT NULL DEFAULT 'PENDING' CHECK (delivery_state IN ('PENDING','DELIVERED')),
  transport_receipt TEXT,
  delivered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(source_id, external_message_id, token_address),
  CHECK (delivery_state <> 'DELIVERED' OR (transport_receipt IS NOT NULL AND delivered_at IS NOT NULL))
);
