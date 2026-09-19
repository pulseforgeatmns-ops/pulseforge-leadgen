-- Anchor Google Workspace mailbox OAuth (reply ingestion only).
-- Additive, rerunnable. Password IMAP remains the default for other tenants.

ALTER TABLE tenant_mailbox_integrations
  ADD COLUMN IF NOT EXISTS imap_auth_mode TEXT NOT NULL DEFAULT 'PASSWORD';

ALTER TABLE tenant_mailbox_integrations
  ADD COLUMN IF NOT EXISTS oauth_refresh_secret_ref TEXT;

COMMENT ON COLUMN tenant_mailbox_integrations.imap_auth_mode IS
  'IMAP auth strategy: PASSWORD (default) or GOOGLE_OAUTH2 (XOAUTH2, no password fallback).';

COMMENT ON COLUMN tenant_mailbox_integrations.oauth_refresh_secret_ref IS
  'Env var name holding a Google OAuth refresh token when imap_auth_mode = GOOGLE_OAUTH2.';

-- Anchor Cleaning: migrate existing integration rows to OAuth reply polling.
UPDATE tenant_mailbox_integrations
SET
  provider_type = 'GOOGLE_WORKSPACE',
  imap_auth_mode = 'GOOGLE_OAUTH2',
  oauth_refresh_secret_ref = 'ANCHOR_GOOGLE_REFRESH_TOKEN',
  imap_host = COALESCE(NULLIF(trim(imap_host), ''), 'imap.gmail.com'),
  imap_port = COALESCE(imap_port, 993),
  imap_tls_mode = COALESCE(NULLIF(trim(imap_tls_mode), ''), 'SSL_TLS'),
  imap_secret_ref = NULL,
  shared_secret_ref = NULL,
  smtp_host = NULL,
  smtp_port = NULL,
  smtp_tls_mode = NULL,
  smtp_secret_ref = NULL,
  updated_at = NOW()
WHERE tenant_id = '10'
  AND lower(mailbox_address) = 'jacob@goanchorcleaning.com';

-- Ensure canonical integration id exists when absent (non-secret config only).
INSERT INTO tenant_mailbox_integrations (
  id, tenant_id, provider_type, mailbox_address, display_name,
  imap_host, imap_port, imap_tls_mode, imap_auth_mode, oauth_refresh_secret_ref,
  status, verification_state
)
SELECT
  'tmi_10_anchor_jacob',
  '10',
  'GOOGLE_WORKSPACE',
  'jacob@goanchorcleaning.com',
  'Jacob Maynard | Anchor Cleaning',
  'imap.gmail.com',
  993,
  'SSL_TLS',
  'GOOGLE_OAUTH2',
  'ANCHOR_GOOGLE_REFRESH_TOKEN',
  'unverified',
  '{}'::jsonb
WHERE NOT EXISTS (
  SELECT 1 FROM tenant_mailbox_integrations
  WHERE tenant_id = '10' AND lower(mailbox_address) = 'jacob@goanchorcleaning.com'
);
