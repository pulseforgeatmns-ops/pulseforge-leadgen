BEGIN;

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT UNIQUE;

CREATE INDEX IF NOT EXISTS idx_clients_stripe_customer_id
  ON clients (stripe_customer_id);

CREATE TABLE IF NOT EXISTS stripe_events (
  id            TEXT PRIMARY KEY,
  type          TEXT NOT NULL,
  livemode      BOOLEAN NOT NULL,
  payload       JSONB NOT NULL,
  processed_at  TIMESTAMPTZ,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stripe_invoices (
  id                    TEXT PRIMARY KEY,
  client_id             INTEGER REFERENCES clients(id),
  stripe_customer_id    TEXT,
  subscription_id       TEXT,
  status                TEXT NOT NULL,
  collection_method     TEXT,
  amount_due            INTEGER NOT NULL,
  amount_paid           INTEGER NOT NULL DEFAULT 0,
  amount_remaining      INTEGER NOT NULL DEFAULT 0,
  amount_refunded       INTEGER NOT NULL DEFAULT 0,
  amount_credited       INTEGER NOT NULL DEFAULT 0,
  currency              TEXT NOT NULL DEFAULT 'usd',
  due_date              TIMESTAMPTZ,
  hosted_invoice_url    TEXT,
  invoice_pdf           TEXT,
  paid_at               TIMESTAMPTZ,
  funds_settled_at      TIMESTAMPTZ,
  service_line          TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE stripe_invoices
  ADD COLUMN IF NOT EXISTS amount_refunded INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS amount_credited INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_stripe_invoices_client_id
  ON stripe_invoices (client_id);
CREATE INDEX IF NOT EXISTS idx_stripe_invoices_status
  ON stripe_invoices (status);

CREATE TABLE IF NOT EXISTS stripe_subscriptions (
  id                     TEXT PRIMARY KEY,
  client_id              INTEGER REFERENCES clients(id),
  stripe_customer_id     TEXT,
  status                 TEXT NOT NULL,
  collection_method      TEXT,
  current_period_start   TIMESTAMPTZ,
  current_period_end     TIMESTAMPTZ,
  cancel_at_period_end   BOOLEAN NOT NULL DEFAULT false,
  canceled_at            TIMESTAMPTZ,
  price_id               TEXT,
  amount                 INTEGER,
  interval               TEXT,
  service_line           TEXT,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stripe_subscriptions_client_id
  ON stripe_subscriptions (client_id);

-- Checkout Sessions are residential one-off/deposit payments, which do not
-- necessarily create an invoice. Keep them separate from invoice IDs.
CREATE TABLE IF NOT EXISTS stripe_checkout_sessions (
  id                    TEXT PRIMARY KEY,
  client_id             INTEGER REFERENCES clients(id),
  stripe_customer_id    TEXT,
  payment_intent_id     TEXT,
  invoice_id            TEXT,
  status                TEXT,
  payment_status        TEXT,
  amount_total          INTEGER NOT NULL DEFAULT 0,
  amount_refunded       INTEGER NOT NULL DEFAULT 0,
  currency              TEXT NOT NULL DEFAULT 'usd',
  service_line          TEXT,
  completed_at          TIMESTAMPTZ,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stripe_checkout_sessions_client_id
  ON stripe_checkout_sessions (client_id);
CREATE INDEX IF NOT EXISTS idx_stripe_checkout_sessions_payment_intent_id
  ON stripe_checkout_sessions (payment_intent_id);

-- Credit notes are durable reconciliation records. Invoice aggregate fields
-- are recalculated from this table by the webhook handler.
CREATE TABLE IF NOT EXISTS stripe_credit_notes (
  id                    TEXT PRIMARY KEY,
  invoice_id            TEXT,
  client_id             INTEGER REFERENCES clients(id),
  amount                INTEGER NOT NULL DEFAULT 0,
  currency              TEXT NOT NULL DEFAULT 'usd',
  status                TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stripe_credit_notes_invoice_id
  ON stripe_credit_notes (invoice_id);

COMMIT;
