-- SPEC-AO-MAILBOX-001 — AO communication identity (separate from mailbox credentials).

BEGIN;

CREATE TABLE IF NOT EXISTS ao_communication_identities (
  ao_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  tenant_id INTEGER NOT NULL,
  email_address TEXT NOT NULL,
  phone_number TEXT,
  reply_to_address TEXT NOT NULL,
  sender_enabled BOOLEAN NOT NULL DEFAULT false,
  mailbox_status TEXT NOT NULL DEFAULT 'not_configured',
  provider TEXT,
  mailbox_integration_id TEXT REFERENCES tenant_mailbox_integrations(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ao_communication_identities_mailbox_status_check CHECK (
    mailbox_status IN (
      'not_configured',
      'configured',
      'verification_pending',
      'ready',
      'auth_failed',
      'disabled'
    )
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ao_communication_identities_tenant_email_idx
  ON ao_communication_identities (tenant_id, lower(email_address));

CREATE INDEX IF NOT EXISTS ao_communication_identities_tenant_idx
  ON ao_communication_identities (tenant_id);

COMMENT ON TABLE ao_communication_identities IS
  'Branded AO outreach identity. Credentials live in tenant_mailbox_integrations; sending requires mailbox_status=ready AND sender_enabled=true.';

-- Seed Tony (Anchor Cleaning, tenant_id=10). No fabricated OAuth/SMTP secrets.
INSERT INTO ao_communication_identities (
  ao_id,
  tenant_id,
  email_address,
  phone_number,
  reply_to_address,
  sender_enabled,
  mailbox_status,
  provider,
  mailbox_integration_id
)
SELECT
  u.id,
  10,
  'tony@goanchorcleaning.com',
  '+1 978 505 1501',
  'tony@goanchorcleaning.com',
  false,
  'configured',
  NULL,
  NULL
FROM users u
WHERE u.client_id = 10
  AND u.role = 'ao'
  AND u.active = true
  AND u.name ILIKE 'Tony%'
ON CONFLICT (ao_id) DO UPDATE SET
  email_address = EXCLUDED.email_address,
  phone_number = EXCLUDED.phone_number,
  reply_to_address = EXCLUDED.reply_to_address,
  sender_enabled = EXCLUDED.sender_enabled,
  mailbox_status = EXCLUDED.mailbox_status,
  updated_at = NOW();

COMMIT;
